// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Segment writing on the primary, and segment-to-bundle extraction for a
// replica. See docs/replication.md, "Segments and Indexes" and "Memory and Capture Failures".
#include "nosql/internal/ship_log.hpp"

#include <algorithm>
#include <cstring>

#include "nosql/replication.hpp"
#include "nosql/internal/atomic_file.hpp"
#include "nosql/internal/checksum.hpp"

namespace nosql {
namespace internal {

std::string segmentName(TxnId firstTxnid)
{
    std::string s = std::to_string(firstTxnid);
    return std::string(s.size() < 12 ? 12 - s.size() : 0, '0') + s + ".seg";
}

// ---------------------------------------------------------------- writing ---

ShipWriter::ShipWriter(std::filesystem::path dir, std::size_t pageSize,
                       std::uint64_t segmentBytes, unsigned retain)
    : dir_(std::move(dir)), pageSize_(pageSize), segmentBytes_(segmentBytes), retain_(retain)
{
    std::filesystem::create_directories(dir_);
}

ShipWriter::~ShipWriter()
{
    closeSegment();
}

void ShipWriter::closeSegment() noexcept
{
    os::closeFile(indexFd_);
    indexFd_ = os::kInvalidFile;
    indexOffset_ = 0;
    if (fd_ != os::kInvalidFile) {
        os::closeFile(fd_);
        fd_ = os::kInvalidFile;
    }
    offset_ = 0;
}

void ShipWriter::openSegment(TxnId firstTxnid)
{
    const std::filesystem::path p = dir_ / segmentName(firstTxnid);
    fd_ = os::openFile(p, false, true, false, /*lock=*/false);
    os::resizeFile(fd_, 0);  // a file with this name can only be a leftover

    SegmentHeader h{};
    std::memcpy(h.magic, kSegmentMagic, sizeof h.magic);
    h.version = kReplicationVersion;
    h.pageSize = std::uint32_t(pageSize_);
    h.firstTxnid = firstTxnid;
    os::writeAt(fd_, 0, &h, sizeof h);
    offset_ = sizeof h;
    indexFd_ = os::openFile(p.string() + ".idx", false, true, false, false);
    os::resizeFile(indexFd_, 0);
}

void ShipWriter::pruneOld() noexcept
{
    try {
        std::vector<std::filesystem::path> segs;
        for (const auto& e : std::filesystem::directory_iterator(dir_))
            if (e.is_regular_file() && e.path().extension() == ".seg")
                segs.push_back(e.path());
        if (segs.size() <= retain_)
            return;
        std::sort(segs.begin(), segs.end());
        std::error_code ignored;
        for (std::size_t i = 0, n = segs.size() - retain_; i < n; ++i) {
            std::filesystem::remove(segs[i], ignored);
            std::filesystem::remove(segs[i].string() + ".idx", ignored);
        }
    } catch (...) {
        // Retention is best-effort; failing it must not disturb the commit.
    }
}

void ShipWriter::append(const void* p, std::size_t n)
{
    const auto* bytes = static_cast<const std::byte*>(p);
    checksum_.update(bytes, n);
    constexpr std::size_t capacity = 64u << 10;
    buf_.reserve(capacity);
    while (n != 0) {
        const std::size_t count = std::min(n, capacity - buf_.size());
        buf_.insert(buf_.end(), bytes, bytes + count);
        bytes += count;
        n -= count;
        if (buf_.size() == capacity)
            flushBuffer();
    }
}

void ShipWriter::flushBuffer()
{
    os::writeAt(fd_, offset_, buf_.data(), buf_.size());
    offset_ += buf_.size();
    buf_.clear();
}

void ShipWriter::capture(const Meta& meta, const std::vector<std::pair<PageNo, PageNo>>& runs,
                         const std::byte* mapBase) noexcept
{
    try {
        if (fd_ == os::kInvalidFile || offset_ >= segmentBytes_) {
            closeSegment();
            openSegment(meta.txnid);
            pruneOld();
        }

        std::uint64_t pageCount = 0;
        for (const auto& [a, b] : runs)
            pageCount += b - a + 1;

        buf_.clear();
        checksum_.reset();
        const auto entryOffset = offset_;
        const SegmentEntry entry{meta.txnid, pageCount, segmentEntryBytes(pageCount, pageSize_)};
        append(&entry, sizeof entry);
        for (const auto& [a, b] : runs) {
            for (PageNo p = a; p <= b; ++p) {
                const std::byte* image = mapBase + p * pageSize_;
                const PageRecord rec{p, std::uint32_t(pageSize_), 0, checksum64(image, pageSize_)};
                append(&rec, sizeof rec);
                append(image, pageSize_);
            }
        }
        append(&meta, sizeof meta);
        const Little<std::uint64_t> checksum = checksum_.digest();
        append(&checksum, sizeof checksum);
        flushBuffer();
        SegmentIndexRecord index{};
        index.offset = entryOffset;
        index.pageCount = pageCount;
        index.meta = meta;
        index.frameChecksum = checksum;
        index.checksum = checksum64(&index, offsetof(SegmentIndexRecord, checksum));
        os::writeAt(indexFd_, indexOffset_, &index, sizeof index);
        indexOffset_ += sizeof index;
        captured_.store(meta.txnid);
    } catch (...) {
        // The gap this leaves is what turns into NeedBase on the replica.
        failures_.fetch_add(1);
        closeSegment();
    }
}

}  // namespace internal

// ---------------------------------------------------------------- reading ---

using internal::Checksum64;
using internal::kBundleMagic;
using internal::kReplicationVersion;
using internal::kSegmentMagic;
using internal::Meta;
using internal::metaValid;
using internal::PageNo;
using internal::PageRecord;
using internal::SegmentEntry;
using internal::SegmentHeader;
using internal::TxnId;
namespace os = internal::os;

namespace {

struct Entry
{
    TxnId txnid = 0;
    std::uint64_t recordsAt = 0;  ///< file offset of the first PageRecord
    std::uint64_t pageCount = 0;
    std::uint64_t metaAt = 0;
    std::uint64_t frameChecksum = 0;
};

struct Segment
{
    std::filesystem::path path;
    os::File file;
    std::vector<Entry> entries;
    TxnId first = 0;
    TxnId last = 0;
};

const char* kNotShipDir = "not a shipping directory: ";

/// Parses one segment, stopping at the first entry that is torn, inconsistent
/// or out of sequence. Returns false when the file is not a segment at all.
bool parseSegment(const std::filesystem::path& path, std::uint32_t& pageSize, Segment& out)
{
    os::File f(os::openFile(path, true, false, false, /*lock=*/false));
    const std::uint64_t bytes = os::fileSize(f);
    if (bytes < sizeof(SegmentHeader))
        return false;

    SegmentHeader h{};
    os::readAt(f, 0, &h, sizeof h);
    if (std::memcmp(h.magic, kSegmentMagic, sizeof h.magic) != 0 ||
        h.version != kReplicationVersion)
        return false;
    if (!internal::legalPageSize(h.pageSize))
        return false;
    if (pageSize != 0 && h.pageSize != pageSize)
        throw Error(ErrorCode::Corrupted, "mixed page sizes in " + path.parent_path().string());
    pageSize = h.pageSize;

    const auto indexPath = path.string() + ".idx";
    if (std::filesystem::exists(indexPath)) {
        const os::File index(os::openFile(indexPath, true, false, false, false));
        const auto indexBytes = os::fileSize(index);
        std::uint64_t expected = sizeof h;
        Meta prior{};
        for (std::uint64_t position = 0; position + sizeof(internal::SegmentIndexRecord) <= indexBytes;
             position += sizeof(internal::SegmentIndexRecord)) {
            internal::SegmentIndexRecord row{};
            os::readAt(index, position, &row, sizeof row);
            if (row.checksum != internal::checksum64(&row, offsetof(internal::SegmentIndexRecord, checksum)) ||
                !metaValid(row.meta, pageSize) || row.offset != expected ||
                row.pageCount > (bytes - std::min(bytes, expected)) / (sizeof(PageRecord) + pageSize))
                break;
            const auto length = sizeof(SegmentEntry) + internal::segmentEntryBytes(row.pageCount, pageSize);
            if (length > bytes - expected)
                break;
            if (out.entries.empty() ? row.meta.txnid != h.firstTxnid :
                (row.meta.txnid != prior.txnid + 1 || row.meta.parentId != prior.commitId ||
                 row.meta.storeId != prior.storeId || row.meta.parentChecksum != prior.checksum))
                break;
            out.entries.push_back({row.meta.txnid, expected + sizeof(SegmentEntry), row.pageCount,
                                   expected + sizeof(SegmentEntry) + row.pageCount * (sizeof(PageRecord) + pageSize),
                                   row.frameChecksum});
            prior = row.meta;
            expected += length;
        }
        if (!out.entries.empty() && expected == bytes &&
            out.entries.size() * sizeof(internal::SegmentIndexRecord) == indexBytes) {
            out.path = path;
            out.file = std::move(f);
            out.first = out.entries.front().txnid;
            out.last = out.entries.back().txnid;
            return true;
        }
        out.entries.clear();
    }

    std::uint64_t off = sizeof h;
    std::vector<std::byte> scratch(64u << 10);
    Meta previous{};
    while (off + sizeof(SegmentEntry) <= bytes) {
        SegmentEntry e{};
        os::readAt(f, off, &e, sizeof e);
        if (bytes - off - sizeof e < sizeof(Meta) + sizeof(std::uint64_t) ||
            e.pageCount > (bytes - off - sizeof e - sizeof(Meta) - sizeof(std::uint64_t)) /
                              (sizeof(PageRecord) + h.pageSize))
            break;
        if (e.byteLength != internal::segmentEntryBytes(e.pageCount, h.pageSize))
            break;
        if (off + sizeof e + e.byteLength > bytes)
            break;
        const std::uint64_t metaAt =
            off + sizeof e + e.pageCount * (sizeof(PageRecord) + h.pageSize);
        Meta m{};
        os::readAt(f, metaAt, &m, sizeof m);
        if (!metaValid(m, h.pageSize) || m.txnid != e.txnid)
            break;
        if (out.entries.empty() && e.txnid != h.firstTxnid)
            break;
        if (!out.entries.empty() && (m.storeId != previous.storeId || m.parentId != previous.commitId ||
                                     m.parentChecksum != previous.checksum))
            break;
        Checksum64 checksum;
        for (std::uint64_t position = off; position < metaAt + sizeof(Meta);) {
            const auto count = std::size_t(std::min<std::uint64_t>(scratch.size(), metaAt + sizeof(Meta) - position));
            os::readAt(f, position, scratch.data(), count);
            checksum.update(scratch.data(), count);
            position += count;
        }
        internal::Little<std::uint64_t> recorded = 0;
        os::readAt(f, metaAt + sizeof(Meta), &recorded, sizeof recorded);
        if (checksum.digest() != recorded)
            break;
        if (!out.entries.empty() && e.txnid != out.entries.back().txnid + 1)
            break;
        out.entries.push_back({e.txnid, off + sizeof e, e.pageCount, metaAt, recorded});
        previous = m;
        off += sizeof e + e.byteLength;
    }
    if (out.entries.empty())
        return false;
    out.path = path;
    out.file = std::move(f);
    out.first = out.entries.front().txnid;
    out.last = out.entries.back().txnid;
    return true;
}

/// Every segment in `dir`, ordered by transaction id. With `contiguousOnly`,
/// trimmed to the newest unbroken run: a gap anywhere older is unusable,
/// because a bundle that skipped a transaction would be silently wrong rather
/// than loudly broken.
std::vector<Segment> scanDirectory(const std::filesystem::path& dir, std::uint32_t& pageSize,
                                   bool contiguousOnly = true)
{
    std::vector<std::filesystem::path> paths;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(dir, ec))
        if (e.is_regular_file() && e.path().extension() == ".seg")
            paths.push_back(e.path());
    if (ec)
        throw Error(ErrorCode::NotFound, std::string(kNotShipDir) + dir.string());
    std::sort(paths.begin(), paths.end());

    std::vector<Segment> segs;
    for (const auto& p : paths) {
        Segment s;
        if (parseSegment(p, pageSize, s))
            segs.push_back(std::move(s));
    }
    if (contiguousOnly) {
        std::size_t start = segs.size();
        while (start > 0 && (start == segs.size() || segs[start].first == segs[start - 1].last + 1))
            --start;
        segs.erase(segs.begin(), segs.begin() + std::ptrdiff_t(start));
    }
    return segs;
}

}  // namespace

const char* toString(ShipStatus s) noexcept
{
    switch (s) {
        case ShipStatus::Ok: return "Ok";
        case ShipStatus::UpToDate: return "UpToDate";
        case ShipStatus::NeedBase: return "NeedBase";
    }
    return "?";
}

ShipLog::ShipLog(std::filesystem::path directory) : dir_(std::move(directory)) {}

ShipLog::Range ShipLog::available() const
{
    std::uint32_t pageSize = 0;
    const std::vector<Segment> segs = scanDirectory(dir_, pageSize);
    if (segs.empty())
        return {};
    return {segs.front().first, segs.back().last};
}

std::size_t ShipLog::prune(std::uint64_t keepFrom) const
{
    std::uint32_t pageSize = 0;
    std::size_t removed = 0;
    std::error_code ignored;
    // Never reaches the segment a live primary is appending to: that one ends
    // at the newest transaction, which no sane `keepFrom` is past.
    for (const auto& s : scanDirectory(dir_, pageSize, /*contiguousOnly=*/false)) {
        if (s.last >= keepFrom)
            break;
        removed += std::filesystem::remove(s.path, ignored) ? 1 : 0;
        std::filesystem::remove(s.path.string() + ".idx", ignored);
    }
    return removed;
}

ShipStatus ShipLog::extract(const Checkpoint& base, const std::filesystem::path& bundle,
                            std::uint64_t through, std::size_t memoryBytes) const
{
    if (memoryBytes < (64u << 10))
        throw Error(ErrorCode::InvalidArgument, "coalescing needs at least 64 KiB of working memory");
    std::uint32_t pageSize = 0;
    const std::vector<Segment> segs = scanDirectory(dir_, pageSize);
    if (segs.empty())
        return ShipStatus::NeedBase;

    const TxnId oldest = segs.front().first;
    const TxnId newest = segs.back().last;
    Meta newestMeta{};
    os::readAt(segs.back().file, segs.back().entries.back().metaAt, &newestMeta, sizeof newestMeta);
    if (base.storeId != newestMeta.storeId || base.txnid > newest)
        throw Error(ErrorCode::Incompatible, "checkpoint belongs to another store or history");
    if (base.txnid + 1 < oldest)
        return ShipStatus::NeedBase;
    bool matched = false;
    std::vector<std::byte> frameBuffer(64u << 10);
    const auto verifyFrame = [&](const Segment& segment, const Entry& entry) {
        Checksum64 checksum;
        for (std::uint64_t offset = entry.recordsAt - sizeof(SegmentEntry); offset < entry.metaAt + sizeof(Meta);) {
            const auto length = std::size_t(std::min<std::uint64_t>(frameBuffer.size(), entry.metaAt + sizeof(Meta) - offset));
            os::readAt(segment.file, offset, frameBuffer.data(), length);
            checksum.update(frameBuffer.data(), length);
            offset += length;
        }
        if (checksum.digest() != entry.frameChecksum)
            throw Error(ErrorCode::Corrupted, "segment frame checksum mismatch");
    };
    for (const Segment& segment : segs) {
        for (const Entry& entry : segment.entries) {
            if (entry.txnid != base.txnid && entry.txnid != base.txnid + 1)
                continue;
            verifyFrame(segment, entry);
            Meta meta{};
            os::readAt(segment.file, entry.metaAt, &meta, sizeof meta);
            const bool same = entry.txnid == base.txnid;
            if (base.commitId != (same ? meta.commitId : meta.parentId) ||
                base.metaChecksum != (same ? meta.checksum : meta.parentChecksum))
                throw Error(ErrorCode::Incompatible, "replica checkpoint has diverged");
            matched = true;
        }
    }
    if (!matched)
        return ShipStatus::NeedBase;
    if (base.txnid == newest)
        return ShipStatus::UpToDate;
    if (through != 0 && (through > newest || through <= base.txnid))
        throw Error(ErrorCode::InvalidArgument,
                    "cannot ship through txn " + std::to_string(through) + "; segments hold " +
                        std::to_string(oldest) + ".." + std::to_string(newest));
    const TxnId target = through != 0 ? through : newest;

    // Coalesce: last writer of each page wins. Sound only because the bundle
    // applies as a unit and publishes no intermediate meta.
    internal::AtomicFile indexFile(bundle.string() + ".index");
    indexFile.close();
    Env index = Env::configure().sync(Durability::None).dirtyLimit(memoryBytes)
        .bufferCache(memoryBytes / 4).open(indexFile.path());
    Txn indexWriter = index.writeTxn();
    Db positions = indexWriter.db("pages", DbFlags::Create | DbFlags::IntegerKey);
    const std::size_t batchSize = std::max<std::size_t>(1, memoryBytes / (pageSize * 32));
    std::size_t staged = 0;
    struct Position { internal::Little<std::uint64_t> segment, offset; };
    const Segment* targetSeg = nullptr;
    std::uint64_t targetMetaAt = 0;
    for (std::size_t segmentIndex = 0; segmentIndex < segs.size(); ++segmentIndex) {
        const Segment& s = segs[segmentIndex];
        for (const Entry& e : s.entries) {
            if (e.txnid == base.txnid && base.metaChecksum != 0) {
                Meta m{};
                os::readAt(s.file, e.metaAt, &m, sizeof m);
                if (m.checksum != base.metaChecksum)
                    throw Error(ErrorCode::Incompatible,
                                "replica has diverged: its txn " + std::to_string(base.txnid) +
                                    " is not the one this store committed");
            }
            if (e.txnid <= base.txnid || e.txnid > target)
                continue;
            verifyFrame(s, e);
            std::uint64_t off = e.recordsAt;
            for (std::uint64_t i = 0; i < e.pageCount; ++i) {
                PageRecord rec{};
                os::readAt(s.file, off, &rec, sizeof rec);
                if (rec.byteLength != pageSize || rec.reserved != 0 || off > e.metaAt ||
                    sizeof rec + std::uint64_t(pageSize) > e.metaAt - off)
                    throw Error(ErrorCode::Corrupted, "invalid segment page record");
                const Position position{segmentIndex, off};
                positions.put(Slice::ref(rec.pgno), Slice::ref(position));
                if (++staged == batchSize) {
                    indexWriter.commit();
                    indexWriter = index.writeTxn();
                    positions = indexWriter.db("pages");
                    staged = 0;
                }
                off += sizeof rec + rec.byteLength;
            }
            if (e.txnid == target) {
                targetSeg = &s;
                targetMetaAt = e.metaAt;
            }
        }
    }
    if (targetSeg == nullptr)
        return ShipStatus::NeedBase;
    indexWriter.commit();

    Meta targetMeta{};
    os::readAt(targetSeg->file, targetMetaAt, &targetMeta, sizeof targetMeta);

    Txn indexReader = index.readTxn();
    Db ordered = indexReader.db("pages");

    internal::AtomicFile output(bundle);
    {
        const os::FileHandle out = output.fd();

        internal::BundleHeader h{};
        std::memcpy(h.magic, kBundleMagic, sizeof h.magic);
        h.version = kReplicationVersion;
        h.pageSize = pageSize;
        h.baseTxnid = base.txnid;
        h.baseMetaChecksum = base.metaChecksum;
        h.targetTxnid = target;
        h.pageCount = ordered.count();

        internal::BundleIdentity identity{base.storeId, base.commitId};
        os::writeAt(out, sizeof h, &identity, sizeof identity);
        std::uint64_t off = sizeof h + sizeof identity;
        Checksum64 checksum;
        checksum.update(&h, sizeof h);
        checksum.update(&identity, sizeof identity);
        std::vector<std::byte> image(pageSize);
        for (auto [key, value] : ordered.all()) {
            const auto where = value.as<Position>();
            const auto& segment = segs[std::size_t(where.segment)];
            PageRecord rec{};
            os::readAt(segment.file, where.offset, &rec, sizeof rec);
            if (rec.pgno != internal::readLittle<PageNo>(key.data()))
                throw Error(ErrorCode::Corrupted, "segment page changed during coalescing");
            os::readAt(segment.file, where.offset + sizeof rec, image.data(), image.size());
            checksum.update(&rec, sizeof rec);
            checksum.update(image.data(), image.size());
            const os::Span parts[2] = {{&rec, sizeof rec}, {image.data(), image.size()}};
            os::writeAtV(out, off, parts, 2);
            off += sizeof rec + image.size();
        }
        os::writeAt(out, off, &targetMeta, sizeof targetMeta);
        checksum.update(&targetMeta, sizeof targetMeta);
        h.payloadChecksum = checksum.digest();
        os::writeAt(out, 0, &h, sizeof h);
    }
    output.publish();
    return ShipStatus::Ok;
}

}  // namespace nosql
