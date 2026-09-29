// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/blob_storage.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <ctime>
#include <mutex>
#include <numeric>

#include "nosql/internal/checksum.hpp"
#include "nosql/internal/endian.hpp"
#include "nosql/internal/os.hpp"

namespace nosql {

namespace internal {

/// A read-only mapping of the archive. Only ever replaced by a larger one, so
/// a Blob holding a reference keeps its bytes addressable even while another
/// thread grows the file.
struct TarMapping
{
    os::Mapping m;
    ~TarMapping() { os::unmap(m); }
};

namespace {

// -------------------------------------------------------------- tar format ---

constexpr std::size_t kBlock = 512;
constexpr std::size_t kMarker = 2 * kBlock;     ///< end-of-archive: two zero blocks
constexpr std::size_t kBlocking = 20 * kBlock;  ///< tar(1)'s default record size
/// A member of at least writeBuffer / kDirectDivisor bytes (128 KiB with the
/// default buffer) is written straight through rather than copied into the
/// buffer: past that size the memcpy costs more than the syscall it would save.
constexpr std::size_t kDirectDivisor = 32;

constexpr std::uint64_t roundUp(std::uint64_t v, std::uint64_t to)
{
    return (v + to - 1) / to * to;
}

#pragma pack(push, 1)
struct UstarHeader
{
    char name[100];
    char mode[8];
    char uid[8];
    char gid[8];
    char size[12];
    char mtime[12];
    char chksum[8];
    char typeflag;
    char linkname[100];
    char magic[6];
    char version[2];
    char uname[32];
    char gname[32];
    char devmajor[8];
    char devminor[8];
    char prefix[155];
    char pad[12];
};
#pragma pack(pop)
static_assert(sizeof(UstarHeader) == kBlock);

void putOctal(char* dst, std::size_t width, std::uint64_t v)
{
    // width-1 digits then NUL, which is what every tar implementation reads.
    for (std::size_t i = width - 1; i-- > 0;) {
        dst[i] = char('0' + (v & 7));
        v >>= 3;
    }
    dst[width - 1] = '\0';
}

std::uint64_t parseOctal(const char* src, std::size_t width)
{
    std::uint64_t v = 0;
    for (std::size_t i = 0; i < width; ++i) {
        const char c = src[i];
        if (c == '\0' || c == ' ')
            break;
        if (c < '0' || c > '7')
            return v;
        v = (v << 3) | std::uint64_t(c - '0');
    }
    return v;
}

std::uint32_t headerChecksum(const UstarHeader& h)
{
    const auto* p = reinterpret_cast<const unsigned char*>(&h);
    std::uint32_t sum = 0;
    for (std::size_t i = 0; i < kBlock; ++i)
        sum += (i >= offsetof(UstarHeader, chksum) && i < offsetof(UstarHeader, chksum) + 8)
                   ? std::uint32_t(' ')
                   : std::uint32_t(p[i]);
    return sum;
}

void buildHeader(UstarHeader& h, std::string_view name, std::uint64_t size, std::uint64_t mtime)
{
    std::memset(&h, 0, sizeof h);
    std::memcpy(h.name, name.data(), name.size());
    std::memcpy(h.mode, "0000644", 8);
    std::memcpy(h.uid, "0000000", 8);
    std::memcpy(h.gid, "0000000", 8);
    putOctal(h.size, sizeof h.size, size);
    putOctal(h.mtime, sizeof h.mtime, mtime);
    h.typeflag = '0';
    std::memcpy(h.magic, "ustar", 6);
    std::memcpy(h.version, "00", 2);
    std::memcpy(h.uname, "nosql", 5);
    std::memcpy(h.gname, "nosql", 5);
    std::memset(h.chksum, ' ', sizeof h.chksum);
    const std::uint32_t sum = headerChecksum(h);
    putOctal(h.chksum, 7, sum);
    h.chksum[6] = '\0';
    h.chksum[7] = ' ';
}

bool blockIsZero(const UstarHeader& h)
{
    const auto* p = reinterpret_cast<const unsigned char*>(&h);
    for (std::size_t i = 0; i < kBlock; ++i)
        if (p[i])
            return false;
    return true;
}

/// ustar splits long names across `prefix` and `name`; join them back.
std::string headerName(const UstarHeader& h)
{
    const auto field = [](const char* p, std::size_t n) {
        const std::size_t len = ::strnlen(p, n);
        return std::string_view(p, len);
    };
    const std::string_view name = field(h.name, sizeof h.name);
    const std::string_view prefix = (std::memcmp(h.magic, "ustar", 5) == 0)
                                        ? field(h.prefix, sizeof h.prefix)
                                        : std::string_view();
    if (prefix.empty())
        return std::string(name);
    std::string out(prefix);
    out.push_back('/');
    out.append(name);
    return out;
}

// ------------------------------------------------------------- index value ---
//
// [u32 version][u32 flags][u64 offset][u64 size][u64 checksum][member name]

// The record version below is independent of the store's kFormatVersion.
constexpr std::size_t kIndexPrefix = 32;

void encodeIndexInto(char* p, std::uint64_t offset, std::uint64_t size, std::uint64_t checksum,
                     std::string_view name, bool known)
{
    writeLittle(p, std::uint32_t(5));
    writeLittle(p + 4, std::uint32_t(known));
    writeLittle(p + 8, offset);
    writeLittle(p + 16, size);
    writeLittle(p + 24, checksum);
    if (!name.empty())
        std::memcpy(p + kIndexPrefix, name.data(), name.size());
}

std::string encodeIndex(std::uint64_t offset, std::uint64_t size, std::uint64_t checksum,
                        std::string_view name, bool known = true)
{
    std::string out;
    out.resize(kIndexPrefix + name.size());
    encodeIndexInto(out.data(), offset, size, checksum, name, known);
    return out;
}

bool decodeIndex(Slice v, std::uint64_t& offset, std::uint64_t& size, std::uint64_t& checksum,
                 std::string_view& name, bool& known)
{
    if (v.size() < kIndexPrefix)
        return false;
    if (readLittle<std::uint32_t>(v.data()) != 5 || readLittle<std::uint32_t>(v.data() + 4) > 1)
        return false;
    known = readLittle<std::uint32_t>(v.data() + 4) != 0;
    offset = readLittle<std::uint64_t>(v.data() + 8);
    size = readLittle<std::uint64_t>(v.data() + 16);
    checksum = readLittle<std::uint64_t>(v.data() + 24);
    name = std::string_view(v.chars() + kIndexPrefix, v.size() - kIndexPrefix);
    return true;
}

}  // namespace
}  // namespace internal

using internal::kBlock;
using internal::kBlocking;
using internal::kDirectDivisor;
using internal::kMarker;
using internal::TarMapping;
using internal::UstarHeader;

// ---------------------------------------------------------------- Blob -----

bool Blob::verify() const
{
    if (!valid() || !checksumKnown_)
        return true;
    return internal::checksum64(data_, std::size_t(size_)) == checksum_;
}

// ---------------------------------------------------------------- Impl -----

struct BlobStorage::Impl
{
    Env* env = nullptr;
    std::string indexDb;
    std::string metaDb;
    std::filesystem::path path;
    internal::os::FileHandle fd = internal::os::kInvalidFile;
    bool readOnly = false;
    bool syncOnAppend = false;
    Access access = Access::Normal;
    std::size_t writeBuffer = 0;

    /// A writer creates both sub-databases when it opens. A read-only opener
    /// cannot, so it learns once whether they exist and, if not, answers every
    /// lookup with "absent" instead of asking a read transaction to create them.
    bool haveIndex = true;
    bool haveMeta = true;

    /// End of the last member; the end-of-archive marker starts here.
    std::uint64_t appendAt = 0;

    std::shared_ptr<const TarMapping> map;
    std::mutex remapMtx;
    std::mutex appendMtx;

    /// Set when fsync of the archive failed: the kernel may have dropped the
    /// dirty pages while a retry reports success, so nothing more is appended
    /// or acknowledged until the archive is reopened and checked.
    std::atomic<bool> failed{false};
    void syncArchive()
    {
        if (failed.load())
            throw Error(ErrorCode::BadTransaction, "archive sync failed; close and reopen it");
        try {
            internal::os::syncFile(fd, true);
        } catch (...) {
            failed.store(true);
            throw;
        }
    }

    ~Impl()
    {
        if (fd != internal::os::kInvalidFile)
            internal::os::closeFile(fd);
    }

    Db index(Txn& t) const { return indexDb.empty() ? t.db() : t.db(indexDb, DbFlags::Create); }
    Db meta(Txn& t) const { return t.db(metaDb, DbFlags::Create); }
    /// The index, or false when a read-only opener found none to read.
    bool indexFor(Txn& t, Db& out) const
    {
        if (!haveIndex)
            return false;
        out = index(t);
        return true;
    }

    /// A mapping that covers at least `need` bytes, or null when the archive
    /// is empty. Grows by remapping; existing mappings stay alive with their
    /// readers.
    std::shared_ptr<const TarMapping> mapped(std::uint64_t need)
    {
        auto cur = std::atomic_load_explicit(&map, std::memory_order_acquire);
        if (cur && cur->m.size >= need)
            return cur;
        if (!need)
            return nullptr;

        std::lock_guard<std::mutex> lk(remapMtx);
        cur = std::atomic_load_explicit(&map, std::memory_order_relaxed);
        if (cur && cur->m.size >= need)
            return cur;

        const std::uint64_t have = internal::os::fileSize(fd);
        if (have < need)
            throw Error(ErrorCode::Corrupted, "archive is shorter than the index expects");
        auto fresh = std::make_shared<TarMapping>();
        fresh->m = internal::os::mapFile(fd, std::size_t(have), false);
        if (access != Access::Normal)
            internal::os::advise(fresh->m, 0, fresh->m.size,
                                 access == Access::Random ? internal::os::Advice::Random
                                                          : internal::os::Advice::Sequential);
        std::atomic_store_explicit(&map, std::shared_ptr<const TarMapping>(fresh),
                                   std::memory_order_release);
        return fresh;
    }

    /// Turns an index record into a Blob pinned to a mapping that covers it.
    Blob blobFrom(Slice record)
    {
        std::uint64_t offset = 0, size = 0;
        std::uint64_t checksum = 0;
        bool known = false;
        std::string_view name;
        if (!internal::decodeIndex(record, offset, size, checksum, name, known))
            throw Error(ErrorCode::Corrupted, "malformed archive index record");

        Blob b;
        b.map_ = mapped(offset + size);
        // An empty member still exists; its anchor is the (non-null) offset.
        b.data_ = b.map_ ? b.map_->m.base + offset : nullptr;
        b.size_ = size;
        b.offset_ = offset;
        b.checksum_ = checksum;
        b.checksumKnown_ = known;
        b.name_.assign(name);
        return b;
    }

    void writeMarker()
    {
        static const std::byte zeros[kMarker] = {};
        internal::os::writeAt(fd, appendAt, zeros, kMarker);
        internal::os::resizeFile(fd, appendAt + kMarker);
    }

    /// Walks members from `from`, stopping at the end-of-archive marker or at
    /// the first structurally broken block. Returns where it stopped.
    std::uint64_t scan(std::uint64_t from, const std::function<void(const TarEntry&)>& fn) const
    {
        const std::uint64_t end = internal::os::fileSize(fd);
        std::uint64_t off = from;
        UstarHeader h;
        while (off + kBlock <= end) {
            internal::os::readAt(fd, off, &h, kBlock);
            if (internal::blockIsZero(h))
                break;
            const std::uint32_t want = std::uint32_t(internal::parseOctal(h.chksum, sizeof h.chksum));
            if (want != internal::headerChecksum(h))
                break;  // torn append or foreign data; treat as the end
            const std::uint64_t size = internal::parseOctal(h.size, sizeof h.size);
            const std::uint64_t dataAt = off + kBlock;
            if (dataAt + size > end)
                break;  // payload never made it to disk
            if (h.typeflag == '0' || h.typeflag == '\0') {
                TarEntry e;
                e.name = internal::headerName(h);
                e.offset = dataAt;
                e.size = size;
                if (fn)
                    fn(e);
            }
            off = dataAt + internal::roundUp(size, kBlock);
        }
        return off;
    }

    std::uint64_t readIndexedUpTo(Txn& t) const
    {
        if (!haveMeta)
            return 0;
        auto m = meta(t);
        if (auto v = m.get("indexedUpTo"))
            return v->as<internal::Little<std::uint64_t>>();
        return 0;
    }

    void writeIndexedUpTo(Txn& t, std::uint64_t upTo) const
    {
        meta(t).put("indexedUpTo", Slice::ref(internal::Little<std::uint64_t>(upTo)));
    }

    /// Indexes every member from archive offset `from` on, keyed by `keyOf`
    /// (or by member name), and moves the high-water mark to the append point.
    /// Header-only, so the payload checksum is unknown. Returns the count.
    std::uint64_t adopt(Txn& t, std::uint64_t from,
                        const std::function<std::string(std::string_view)>& keyOf)
    {
        Db db = index(t);
        std::uint64_t n = 0;
        scan(from, [&](const TarEntry& e) {
            const std::string key = keyOf ? keyOf(e.name) : e.name;
            db.put(key, internal::encodeIndex(e.offset, e.size, 0, e.name, false));
            ++n;
        });
        writeIndexedUpTo(t, appendAt);
        return n;
    }

    /// catchUp() inside a transaction the caller holds.
    std::uint64_t catchUpIn(Txn& t)
    {
        const std::uint64_t from = readIndexedUpTo(t);
        return from >= appendAt ? 0 : adopt(t, from, {});
    }
};

// -------------------------------------------------------------- lifecycle ---

BlobStorage::BlobStorage() noexcept = default;
BlobStorage::BlobStorage(BlobStorage&&) noexcept = default;
BlobStorage& BlobStorage::operator=(BlobStorage&&) noexcept = default;
BlobStorage::~BlobStorage() = default;

BlobStorage BlobStorage::open(Env& env, Slice indexDb)
{
    return open(env, indexDb, Options());
}

BlobStorage BlobStorage::open(Env& env, Slice indexDb, const Options& options)
{
    auto impl = std::make_unique<Impl>();
    impl->env = &env;
    impl->indexDb = indexDb.string();
    impl->metaDb = impl->indexDb.empty() ? std::string("__tar_meta") : impl->indexDb + "__tar_meta";
    impl->readOnly = options.readOnly_;
    impl->syncOnAppend = options.syncOnAppend_;
    impl->access = options.access_;
    impl->writeBuffer = options.writeBuffer_;

    const std::filesystem::path store = env.path();
    const std::filesystem::path dir = options.dir_.empty() ? store.parent_path() : options.dir_;
    std::string file = options.file_;
    if (file.empty())
        file = (impl->indexDb.empty() ? store.stem().string() : impl->indexDb) + ".tar";
    impl->path = dir.empty() ? std::filesystem::path(file) : dir / file;

    if (!dir.empty() && !options.readOnly_)
        std::filesystem::create_directories(dir);
    impl->fd = internal::os::openFile(impl->path, options.readOnly_, !options.readOnly_, false);

    BlobStorage s;
    s.impl_ = std::move(impl);
    Impl* im = s.impl_.get();

    // Where the next member goes, and where a torn tail (if any) begins.
    im->appendAt = im->scan(0, nullptr);
    if (options.readOnly_) {
        env.read([&](Txn& t) {
            im->haveIndex = im->indexDb.empty() || t.hasDb(im->indexDb);
            im->haveMeta = t.hasDb(im->metaDb);
        });
    } else {
        const std::uint64_t have = internal::os::fileSize(im->fd);
        if (have != im->appendAt + kMarker)
            im->writeMarker();  // normalise a torn or unterminated archive
        // One transaction creates the sub-databases (so read snapshots never
        // have to) and adopts whatever the index has not seen.
        const std::uint64_t added = env.write([&](Txn& t) -> std::uint64_t {
            (void)im->index(t);
            (void)im->meta(t);
            return options.catchUp_ ? im->catchUpIn(t) : 0;
        });
        if (added)
            im->writeMarker();  // an adopted tail had no end-of-archive block yet
    }
    return s;
}

const std::filesystem::path& BlobStorage::archivePath() const
{
    return impl_->path;
}

std::uint64_t BlobStorage::archiveSize() const
{
    return impl_->appendAt;
}

std::uint64_t BlobStorage::indexedUpTo() const
{
    return impl_->env->read([&](Txn& t) { return impl_->readIndexedUpTo(t); });
}

std::uint64_t BlobStorage::count() const
{
    return impl_->env->read([&](Txn& t) -> std::uint64_t {
        Db db;
        return impl_->indexFor(t, db) ? db.count() : 0;
    });
}

// ----------------------------------------------------------------- Writer --

struct BlobStorage::Writer::State
{
    Impl* impl = nullptr;
    Txn owned;
    Txn* txn = nullptr;  ///< &owned, or the caller's transaction
    Db index;            ///< resolved once per batch, not once per member
    std::vector<std::byte> buf;
    std::size_t capacity = 0;
    std::uint64_t bufStart = 0;  ///< archive offset of buf[0]
    bool wrote = false;

    /// Queued bytes still reach the archive when the Writer is dropped, so an
    /// abandoned batch is exactly what a crash mid-batch leaves: present,
    /// unindexed, adoptable. Lives here rather than in ~Writer so that a
    /// Writer overwritten by move-assignment flushes too.
    ~State()
    {
        try {
            flush();
        } catch (...) {
        }
    }

    /// Writes whatever is queued, as one write.
    void flush()
    {
        if (buf.empty())
            return;
        internal::os::writeAt(impl->fd, bufStart, buf.data(), buf.size());
        buf.clear();
    }

    void queue(const void* p, std::size_t n)
    {
        const auto* b = static_cast<const std::byte*>(p);
        buf.insert(buf.end(), b, b + n);
    }
};

BlobStorage::Writer::Writer() noexcept = default;
BlobStorage::Writer::Writer(Writer&&) noexcept = default;
BlobStorage::Writer& BlobStorage::Writer::operator=(Writer&&) noexcept = default;

BlobStorage::Writer::~Writer() = default;

BlobStorage::Writer BlobStorage::beginWrite()
{
    if (impl_->readOnly)
        throw Error(ErrorCode::ReadOnly, "archive was opened read-only");
    if (impl_->failed.load())
        throw Error(ErrorCode::BadTransaction, "archive sync failed; close and reopen it");
    Writer w;
    w.st_ = std::make_unique<Writer::State>();
    w.st_->impl = impl_.get();
    w.st_->owned = impl_->env->writeTxn();
    w.st_->txn = &w.st_->owned;
    w.st_->index = impl_->index(w.st_->owned);
    w.st_->capacity = impl_->writeBuffer;
    w.st_->buf.reserve(w.st_->capacity);
    return w;
}

BlobStorage::Writer BlobStorage::beginWrite(Txn& txn)
{
    if (impl_->readOnly)
        throw Error(ErrorCode::ReadOnly, "archive was opened read-only");
    if (!txn.valid() || txn.isReadOnly())
        throw Error(ErrorCode::InvalidArgument, "a Writer needs a write transaction");
    if (!txn.belongsTo(*impl_->env))
        throw Error(ErrorCode::InvalidArgument, "the transaction belongs to another store");
    Writer w;
    w.st_ = std::make_unique<Writer::State>();
    w.st_->impl = impl_.get();
    w.st_->txn = &txn;
    w.st_->index = impl_->index(txn);
    w.st_->capacity = impl_->writeBuffer;
    w.st_->buf.reserve(w.st_->capacity);
    return w;
}

void BlobStorage::Writer::add(Slice key, std::string_view name, Slice contents)
{
    if (!st_)
        throw Error(ErrorCode::InvalidArgument, "writer has been committed");
    if (name.empty() || name.size() > sizeof(UstarHeader::name))
        throw Error(ErrorCode::InvalidArgument, "member name must be 1..100 bytes");
    Impl* im = st_->impl;

    UstarHeader h;
    // Wall clock, not a monotonic counter: this field is what `tar tvf` and
    // every extraction tool report as the file's date.
    internal::buildHeader(h, name, contents.size(), std::uint64_t(std::time(nullptr)));

    static const std::byte pad[kBlock] = {};
    const std::size_t padding =
        std::size_t(internal::roundUp(contents.size(), kBlock) - contents.size());
    const std::uint64_t total = kBlock + contents.size() + padding;

    std::uint64_t at;
    {
        std::lock_guard<std::mutex> lk(im->appendMtx);
        at = im->appendAt;
        if (total >= st_->capacity / kDirectDivisor) {
            // Too big to be worth copying: one gathered write straight to the
            // file, behind whatever is already queued ahead of it.
            st_->flush();
            const internal::os::Span parts[3] = {
                {&h, kBlock},
                {contents.data(), contents.size()},
                {pad, padding},
            };
            internal::os::writeAtV(im->fd, at, parts, 3);
        } else {
            if (st_->buf.size() + total > st_->capacity)
                st_->flush();
            if (st_->buf.empty())
                st_->bufStart = at;
            st_->queue(&h, kBlock);
            st_->queue(contents.data(), contents.size());
            st_->queue(pad, padding);
        }
        im->appendAt = at + total;
    }

    const auto checksum = internal::checksum64(contents.data(), contents.size());
    // Written in place: no temporary string per member.
    const WritableSlice slot = st_->index.reserve(key, internal::kIndexPrefix + name.size());
    internal::encodeIndexInto(slot.chars(), at + kBlock, contents.size(), checksum, name, true);
    st_->wrote = true;
}

void BlobStorage::Writer::commit()
{
    if (!st_)
        return;
    Impl* im = st_->impl;
    if (st_->wrote) {
        st_->flush();
        im->writeMarker();
        // The archive must be on disk before the index claims to describe it,
        // or a crash could leave the index pointing past the file's end.
        if (im->syncOnAppend)
            im->syncArchive();
        im->writeIndexedUpTo(*st_->txn, im->appendAt);
    }
    if (st_->txn == &st_->owned)
        st_->owned.commit();
    st_.reset();
}

void BlobStorage::put(Slice key, std::string_view name, Slice contents)
{
    Writer w = beginWrite();
    w.add(key, name, contents);
    w.commit();
}

bool BlobStorage::erase(Slice key)
{
    if (impl_->readOnly)
        throw Error(ErrorCode::ReadOnly, "archive was opened read-only");
    return impl_->env->write([&](Txn& t) { return impl_->index(t).erase(key); });
}

void BlobStorage::finalize()
{
    if (impl_->readOnly)
        return;
    // tar(1) pads the archive out to its record size; do the same so the file
    // is byte-for-byte what the tools expect.
    const std::uint64_t padded = internal::roundUp(impl_->appendAt + kMarker, kBlocking);
    internal::os::resizeFile(impl_->fd, padded);
    static const std::byte zeros[kBlocking] = {};
    internal::os::writeAt(impl_->fd, impl_->appendAt, zeros, std::size_t(padded - impl_->appendAt));
    impl_->syncArchive();
}

void BlobStorage::sync()
{
    if (!impl_->readOnly)
        impl_->syncArchive();
}

// ------------------------------------------------------------------ reads --

Blob BlobStorage::find(Txn& txn, Slice key) const
{
    Db db;
    if (!impl_->indexFor(txn, db))
        return Blob();
    const auto v = db.get(key);
    return v ? impl_->blobFrom(*v) : Blob();
}

Blob BlobStorage::find(Slice key) const
{
    return impl_->env->read([&](Txn& t) { return find(t, key); });
}

Blob BlobStorage::at(Slice key) const
{
    Blob b = find(key);
    if (!b.valid())
        throw Error(ErrorCode::NotFound, "no such blob");
    return b;
}

bool BlobStorage::contains(Slice key) const
{
    return impl_->env->read([&](Txn& t) {
        Db db;
        return impl_->indexFor(t, db) && db.contains(key);
    });
}

std::size_t BlobStorage::findMany(Txn& txn, const Slice* keys, std::size_t count, Blob* out) const
{
    Db db;
    if (!impl_->indexFor(txn, db)) {
        for (std::size_t i = 0; i < count; ++i)
            out[i] = Blob();
        return 0;
    }

    // Probe in key order: neighbouring keys share index leaves, so a shuffled
    // batch walks each leaf once instead of revisiting it from the root.
    std::vector<std::uint32_t> order(count);
    std::iota(order.begin(), order.end(), 0u);
    if (count > 1)
        std::sort(order.begin(), order.end(), [&](std::uint32_t a, std::uint32_t b) {
            return keys[a].compare(keys[b]) < 0;
        });

    std::size_t found = 0;
    for (const std::uint32_t i : order) {
        const auto v = db.get(keys[i]);
        if (v) {
            out[i] = impl_->blobFrom(*v);
            ++found;
        } else {
            out[i] = Blob();
        }
    }
    return found;
}

std::vector<Blob> BlobStorage::findMany(Txn& txn, const std::vector<Slice>& keys) const
{
    std::vector<Blob> out(keys.size());
    findMany(txn, keys.data(), keys.size(), out.data());
    return out;
}

std::vector<Blob> BlobStorage::findMany(const std::vector<Slice>& keys) const
{
    return impl_->env->read([&](Txn& t) { return findMany(t, keys); });
}

void BlobStorage::prefetch(const Blob* blobs, std::size_t count) const
{
    // Sort by (mapping, offset) so adjacent members become one request, and a
    // batch that happens to be contiguous on disk costs one madvise.
    std::vector<const Blob*> live;
    live.reserve(count);
    for (std::size_t i = 0; i < count; ++i)
        if (blobs[i].valid() && blobs[i].size() > 0)
            live.push_back(&blobs[i]);
    if (live.empty())
        return;
    std::sort(live.begin(), live.end(), [](const Blob* a, const Blob* b) {
        if (a->map_ != b->map_)
            return a->map_ < b->map_;
        return a->offset() < b->offset();
    });

    const TarMapping* map = live[0]->map_.get();
    std::uint64_t begin = live[0]->offset();
    std::uint64_t end = begin + live[0]->size();
    for (std::size_t i = 1; i <= live.size(); ++i) {
        const bool last = i == live.size();
        // Members are 512-aligned with a header between them, so "adjacent"
        // means within one system page of the previous end.
        if (!last && live[i]->map_.get() == map && live[i]->offset() <= end + 4096) {
            end = std::max(end, live[i]->offset() + live[i]->size());
            continue;
        }
        internal::os::advise(map->m, std::size_t(begin), std::size_t(end - begin),
                             internal::os::Advice::WillNeed);
        if (last)
            break;
        map = live[i]->map_.get();
        begin = live[i]->offset();
        end = begin + live[i]->size();
    }
}

std::size_t BlobStorage::prefetch(Txn& txn, const Slice* keys, std::size_t count) const
{
    std::vector<Blob> blobs(count);
    const std::size_t found = findMany(txn, keys, count, blobs.data());
    prefetch(blobs.data(), blobs.size());
    return found;
}

void BlobStorage::warm() const
{
    if (const auto m = impl_->mapped(impl_->appendAt))
        internal::os::advise(m->m, 0, std::size_t(impl_->appendAt), internal::os::Advice::WillNeed);
}

std::uint64_t BlobStorage::forEachKey(Txn& txn, Slice prefix,
                                      const std::function<bool(Slice)>& fn) const
{
    Db db;
    if (!impl_->indexFor(txn, db))
        return 0;
    std::uint64_t n = 0;
    Db::Range range = prefix.empty() ? db.all() : db.prefix(prefix);
    for (const auto entry : range) {
        ++n;
        if (!fn(entry.key))
            break;
    }
    return n;
}

// ------------------------------------------------------- index maintenance --

std::uint64_t BlobStorage::catchUp()
{
    if (impl_->readOnly)
        return 0;
    Impl* im = impl_.get();
    const std::uint64_t added = im->env->write([&](Txn& t) { return im->catchUpIn(t); });
    if (added)
        im->writeMarker();  // an adopted tail had no end-of-archive block yet
    return added;
}

std::uint64_t BlobStorage::rebuildIndex(
    const std::function<std::string(std::string_view)>& keyOf)
{
    if (impl_->readOnly)
        throw Error(ErrorCode::ReadOnly, "archive was opened read-only");
    Impl* im = impl_.get();
    return im->env->write([&](Txn& t) {
        im->index(t).clear();
        return im->adopt(t, 0, keyOf);
    });
}

std::vector<TarEntry> BlobStorage::list() const
{
    std::vector<TarEntry> out;
    impl_->scan(0, [&](const TarEntry& e) { out.push_back(e); });
    return out;
}

}  // namespace nosql
