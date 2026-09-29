// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Store lifecycle: file creation, geometry, mapping, meta-page selection.

#include <algorithm>
#include <chrono>
#include <cstring>
#include <random>
#include <thread>

#ifndef NOSQL_NO_REPLICATION
#include "nosql/internal/ship_log.hpp"
#endif
#include "nosql/internal/core.hpp"

namespace nosql::internal {

namespace {

/// splitmix64: one multiply-xorshift step per word, good enough for
/// identifiers that only have to be distinct with overwhelming probability.
std::uint64_t splitmix64(std::uint64_t& state) noexcept
{
    std::uint64_t z = (state += 0x9e3779b97f4a7c15ull);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
}

std::uint64_t seedWord() noexcept
{
    try {
        std::random_device source;
        return (std::uint64_t(source()) << 32) | std::uint64_t(source());
    } catch (...) {
        return 0;
    }
}

}  // namespace

Identity newIdentity() noexcept
{
    // Seeded once per thread: two words of real entropy plus the clock and
    // the thread, so two threads (or two processes) never share a stream.
    thread_local std::uint64_t state = [] {
        std::uint64_t s = seedWord() ^ (seedWord() << 1);
        s ^= std::uint64_t(std::chrono::steady_clock::now().time_since_epoch().count());
        s ^= std::hash<std::thread::id>()(std::this_thread::get_id()) * 0x9e3779b97f4a7c15ull;
        return s;
    }();
    Identity result{};
    do {
        result[0] = splitmix64(state);
        result[1] = splitmix64(state);
    } while (result == Identity{});
    return result;
}

EnvImpl::~EnvImpl()
{
    // Pooled transactions hand their buffers back to `buffers`, so they must go
    // first. Any still referenced by a stray handle simply outlives the pool.
    txnPool.clear();
    buffers.purge();
    staleRegions.clear();
    region.reset();
    if (fd != os::kInvalidFile)
        os::closeFile(fd);
}

TxnId EnvImpl::oldestSnapshot() const
{
    if (readers.empty())
        return meta.txnid;
    return std::min<TxnId>(readers.front().first, meta.txnid);
}

/// Reuse a pooled transaction object, or add one to the pool. A pooled entry
/// is available exactly when the pool holds the only reference: a `db` or
/// `cursor` outliving its transaction keeps a reference and so keeps its
/// object out of circulation until it too is gone.
std::shared_ptr<TxnImpl> EnvImpl::acquireTxn()
{
    std::lock_guard<std::mutex> lk(mtx);
    return acquireTxnLocked();
}

std::shared_ptr<TxnImpl> EnvImpl::acquireTxnLocked()
{
    for (auto& slot : txnPool) {
        if (slot.use_count() == 1) {
            slot->reset();
            return slot;
        }
    }
    auto fresh = std::make_shared<TxnImpl>();
    if (txnPool.size() < kTxnPoolCap)
        txnPool.push_back(fresh);
    return fresh;
}

void EnvImpl::readerAcquire(TxnId id)
{
    const auto it =
        std::lower_bound(readers.begin(), readers.end(), id,
                         [](const std::pair<TxnId, unsigned>& e, TxnId v) { return e.first < v; });
    if (it != readers.end() && it->first == id)
        ++it->second;
    else
        readers.insert(it, {id, 1u});
}

void EnvImpl::readerRelease(TxnId id) noexcept
{
    const auto it =
        std::lower_bound(readers.begin(), readers.end(), id,
                         [](const std::pair<TxnId, unsigned>& e, TxnId v) { return e.first < v; });
    if (it == readers.end() || it->first != id)
        return;
    if (--it->second == 0)
        readers.erase(it);
}

/// Extend the file and republish the mapping. Existing transactions keep the
/// mapping they started with, so growing never pulls memory out from under a
/// concurrent reader.
void EnvImpl::growLocked(std::uint64_t needed)
{
    if (needed <= region->m.size)
        return;
    if (needed > sizeUpper)
        throw Error(ErrorCode::MapFull, "store needs " + std::to_string(needed) +
                                            " bytes but maxSize is " + std::to_string(sizeUpper));

    std::uint64_t want = region->m.size ? region->m.size : needed;
    while (want < needed) {
        const std::uint64_t step = std::max<std::uint64_t>(growthStep, want / 8);
        want += std::max<std::uint64_t>(step, pageSize);
    }
    want = std::min(want, sizeUpper);
    want = (want + pageSize - 1) / pageSize * pageSize;
    if (want < needed)
        want = needed;

    const std::uint64_t oldSize = region->m.size;
    os::resizeFile(fd, want);
    if (preallocate)
        os::reserveSpace(fd, oldSize, want - oldSize);

    auto fresh = std::make_shared<MappedRegion>();
    fresh->m = os::mapFile(fd, std::size_t(want), !readOnly);
    // Never unmap here: cursors in the running transaction hold raw pointers
    // into the outgoing mapping, and readers hold whole snapshots of it.
    staleRegions.push_back(std::move(region));
    region = std::move(fresh);
}

/// Drop mappings nobody references any more. Safe only at a transaction
/// boundary, where the last raw pointers into them have gone out of scope.
void EnvImpl::releaseStaleRegions()
{
    staleRegions.erase(
        std::remove_if(staleRegions.begin(), staleRegions.end(),
                       [](const std::shared_ptr<MappedRegion>& r) { return r.use_count() == 1; }),
        staleRegions.end());
}

namespace {

/// Lay down page 0 and page 1 of a brand-new store.
void formatStore(EnvImpl& e)
{
    Meta m{};
    std::memcpy(m.magic, kMagic, sizeof m.magic);
    m.version = kFormatVersion;
    m.pageSize = std::uint32_t(e.pageSize);
    m.lastPgno = 1;  // the two meta pages
    m.fileSize = e.region->m.size;
    m.freeTree = emptyTree();  // 16-byte big-endian keys under byte order
    m.mainTree = emptyTree();
    m.catalogTree = emptyTree();
    m.storeId = newIdentity();
    m.commitId = newIdentity();

    for (unsigned slot = 0; slot < 2; ++slot) {
        Page* p = e.mapPage(*e.region, slot);
        std::memset(p, 0, e.pageSize);
        p->pgno = slot;
        p->flags = P_META;
        // Slot 1 carries txnid 0 as well; both are valid, identical snapshots.
        m.txnid = 0;
        m.checksum = metaChecksum(m);
        *metaOf(p) = m;
    }
    os::flushRange(e.fd, e.region->m, 0, 2 * e.pageSize);
    os::syncFile(e.fd, true);
    e.meta = m;
}

/// Work out the geometry of an existing store before it can be indexed by
/// page. Meta slot 0 always starts at file offset 0; if that slot is the one
/// a crash tore, slot 1 is still there -- but it sits at offset `pageSize`,
/// so each legal page size gets tried in turn. There are only eight.
std::size_t probePageSize(const MappedRegion& r)
{
    if (r.m.size < kMinPageSize)
        throw Error(ErrorCode::Corrupted, "file is too small to be a store");

    const auto at = [&](std::size_t off, std::size_t expect) -> std::size_t {
        if (off + kPageHdr + sizeof(Meta) > r.m.size)
            return 0;
        const Meta* m = metaOf(reinterpret_cast<const Page*>(r.m.base + off));
        if (!metaValid(*m, expect) || !legalPageSize(m->pageSize))
            return 0;
        return m->pageSize;
    };

    if (const std::size_t ps = at(0, 0))
        return ps;
    for (std::size_t ps = kMinPageSize; ps <= kMaxPageSize; ps *= 2)
        if (at(ps, ps))
            return ps;

    // Nothing checksummed. Say something more useful than "corrupted" when the
    // header is readable enough to explain itself.
    const Meta* m = metaOf(reinterpret_cast<const Page*>(r.m.base));
    if (!magicOk(*m))
        throw Error(ErrorCode::Corrupted, "not a libnosql store (bad magic number)");
    if (m->version != kFormatVersion)
        throw Error(ErrorCode::Incompatible, "store format version " + std::to_string(m->version) +
                                                 ", this build speaks " +
                                                 std::to_string(kFormatVersion));
    throw Error(ErrorCode::Corrupted, "both meta pages failed validation");
}

/// Choose the newest meta page that still checksums. A torn write of the
/// newest one simply leaves the previous snapshot in charge: the checksum
/// (not write ordering) is what detects the tear, and the reclamation rule
/// guarantees the older snapshot's pages were never overwritten. See
/// docs/design.md, "The meta page's own durability".
void loadMeta(EnvImpl& e)
{
    const Meta* best = nullptr;
    for (unsigned slot = 0; slot < 2; ++slot) {
        if ((slot + 1) * e.pageSize > e.region->m.size)
            break;
        const Page* p = e.mapPage(*e.region, slot);
        if (p->flags != P_META)
            continue;
        const Meta* m = metaOf(p);
        if (!metaValid(*m, e.pageSize))
            continue;
        if (!best || m->txnid > best->txnid)
            best = m;
    }
    if (!best)
        throw Error(ErrorCode::Corrupted, "no usable meta page in " + e.path.string());
    e.meta = *best;
    if (e.meta.deferredCount > e.meta.deferredPages.size())
        throw Error(ErrorCode::Corrupted, "invalid deferred free-page count");
    e.carryOver.assign(e.meta.deferredPages.begin(),
                      e.meta.deferredPages.begin() + std::ptrdiff_t(e.meta.deferredCount));
    e.commitGen.store(e.meta.txnid, std::memory_order_release);
    if ((e.meta.lastPgno + 1) * e.pageSize > e.region->m.size)
        throw Error(ErrorCode::Corrupted, "meta references pages beyond the end of the file");
}

}  // namespace
}  // namespace nosql::internal

namespace nosql {

Env Env::Options::open(const std::filesystem::path& path) const
{
    using namespace internal;

    if (pageSize_ && !legalPageSize(pageSize_))
        throw Error(ErrorCode::InvalidArgument, "pageSize must be a power of two in [512, 65536]");
    if (maxDbs_ < 1)
        throw Error(ErrorCode::InvalidArgument, "maxDbs must be at least 1");

    auto e = std::make_shared<EnvImpl>();
    e->path = path;
    e->maxDbs = maxDbs_;
    e->readOnly = readOnly_;
    e->dura = durability_;
    e->dirtyLimit = dirtyLimit_;

    e->fd = os::openFile(path, readOnly_, create_, exclusive_, true, false, fileMode_);
    e->preallocate = preallocate_;
    if (!allowNetworkFilesystem_ && os::isNetworkFilesystem(e->fd))
        throw Error(ErrorCode::Unsupported,
                    "the store is on a network or user-space filesystem, where memory-mapped "
                    "access is not dependable; allowNetworkFilesystem() overrides: " +
                        path.string());

    const std::uint64_t existing = os::fileSize(e->fd);
    const bool fresh = existing == 0;
    if (fresh && readOnly_)
        throw Error(ErrorCode::Corrupted, "empty store opened read-only");

    if (fresh) {
        e->pageSize = pageSize_ ? pageSize_ : kDefaultPageSize;
    } else {
        // Learn the geometry from the store itself before mapping it properly.
        auto probe = std::make_shared<MappedRegion>();
        probe->m =
            os::mapFile(e->fd, std::size_t(std::min<std::uint64_t>(existing, 2 * kMaxPageSize)), false);
        e->pageSize = probePageSize(*probe);
        if (pageSize_ && pageSize_ != e->pageSize)
            throw Error(ErrorCode::Incompatible,
                        "store was created with " + std::to_string(e->pageSize) +
                            "-byte pages, but pageSize(" + std::to_string(pageSize_) +
                            ") was requested");
    }

    const std::size_t ps = e->pageSize;
    e->buffers.configure(ps, bufferCache_);
    if (cacheReadChecksums_)
        e->validation.configure(ps, std::max<std::uint64_t>(maxSize_, 16 * ps), revalidateAfter_);
    // One page copy plus its entry list, with room for a recursive split.
    e->writeScratch.setChunkSize(ps * 8);
    e->sizeUpper = std::max<std::uint64_t>(maxSize_, 16 * ps);
    e->growthStep = growth_ ? growth_ : std::max<std::uint64_t>(1u << 20, 64 * ps);

    std::uint64_t want = std::max<std::uint64_t>(initialSize_, 16 * ps);
    want = std::max(want, existing);
    want = std::min(want, e->sizeUpper);
    want = (want + ps - 1) / ps * ps;
    if (want < existing)
        want = existing;

    if (!readOnly_ && want > existing) {
        os::resizeFile(e->fd, want);
        if (preallocate_)
            os::reserveSpace(e->fd, existing, want - existing);
    }
    const std::uint64_t mapped = readOnly_ ? existing : want;

    e->region = std::make_shared<MappedRegion>();
    e->region->m = os::mapFile(e->fd, std::size_t(mapped), !readOnly_);

    if (fresh)
        formatStore(*e);
    else
        loadMeta(*e);

#ifdef NOSQL_NO_REPLICATION
    if (!ship_.empty())
        throw Error(ErrorCode::Unsupported, "built without NOSQL_WITH_REPLICATION");
#else
    if (!ship_.empty() && !readOnly_)
        e->ship = std::make_unique<ShipWriter>(ship_, ps, shipSegment_, shipRetain_);
#endif

    return Env(std::move(e));
}

Env::~Env() = default;

void Env::close()
{
    impl_.reset();
}

Env Env::share() const
{
    internal::openEnv(impl_);  // throws when this handle is closed
    return Env(impl_);
}

const std::filesystem::path& Env::path() const
{
    return internal::openEnv(impl_).path;
}

std::size_t Env::pageSize() const
{
    return internal::openEnv(impl_).pageSize;
}

std::size_t Env::maxKeySize() const
{
    return internal::maxKeySize(pageSize());
}

void Env::invalidateReadCache()
{
    internal::openEnv(impl_).validation.reset();
}

void Env::sync(bool force)
{
    internal::EnvImpl& e = internal::openEnv(impl_);
    if (e.readOnly)
        return;
    std::lock_guard<std::mutex> lk(e.mtx);
    if (force || e.dura != Durability::Safe) {
        // After a failed flush the kernel may have dropped the dirty pages and
        // report success to a retry: the store is finished until reopened from
        // its last checksummed meta page (see EnvImpl::failed).
        try {
            internal::os::flushRange(e.fd, e.region->m, 0, e.region->m.size);
            internal::os::syncFile(e.fd, true);
        } catch (...) {
            e.failed.store(true);
            throw;
        }
    }
}

std::uint64_t Env::commitGeneration() const noexcept
{
    return impl_ ? impl_->commitGen.load(std::memory_order_acquire) : 0;
}

std::uint64_t Env::waitForCommit(std::uint64_t seen, std::chrono::milliseconds timeout) const
{
    internal::EnvImpl& e = internal::openEnv(impl_);
    std::unique_lock<std::mutex> lk(e.mtx);
    e.commitCv.wait_for(lk, timeout,
                        [&] { return e.commitGen.load(std::memory_order_acquire) != seen; });
    return e.commitGen.load(std::memory_order_acquire);
}

const char* version() noexcept
{
    return "0.1.0";
}

const char* toString(ErrorCode c) noexcept
{
    switch (c) {
        case ErrorCode::Ok: return "ok";
        case ErrorCode::NotFound: return "not found";
        case ErrorCode::KeyExists: return "key already exists";
        case ErrorCode::Corrupted: return "store is corrupted";
        case ErrorCode::InvalidArgument: return "invalid argument";
        case ErrorCode::Unsupported: return "unsupported operation";
        case ErrorCode::KeyTooLarge: return "key too large";
        case ErrorCode::ValueTooLarge: return "value too large";
        case ErrorCode::MapFull: return "store reached its maximum size";
        case ErrorCode::TooManyDbs: return "too many sub-databases";
        case ErrorCode::Incompatible: return "incompatible sub-database flags";
        case ErrorCode::BadTransaction: return "invalid transaction state";
        case ErrorCode::ReadOnly: return "store is read-only";
        case ErrorCode::Busy: return "store is busy";
        case ErrorCode::IoError: return "i/o error";
        case ErrorCode::OutOfMemory: return "out of memory";
    }
    return "unknown error";
}

}  // namespace nosql
