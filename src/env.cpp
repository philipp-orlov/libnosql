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
#include "nosql/internal/free_list.hpp"

namespace nosql::internal {

namespace {

/// Whether a writable store reserves address space for maxSize at open. Off
/// only to exercise, on POSIX, the remap-on-growth path that Windows uses.
#ifndef NOSQL_RESERVE_MAPPING
#define NOSQL_RESERVE_MAPPING 1
#endif
constexpr bool kReserveMapping = NOSQL_RESERVE_MAPPING != 0;

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
    if (syncThread.joinable()) {
        {
            std::lock_guard<std::mutex> lk(syncWakeMtx);
            syncStop = true;
        }
        syncWake.notify_all();
        syncThread.join();  // after any sync it is running; the closing one follows
    }
    // Closing syncs a deferred meta. Nothing can report a failure from here;
    // the store then reopens on its last durable meta, as after a crash.
    if (region && !readOnly && deferredMeta() && !failed.load()) {
        try {
            std::lock_guard<std::mutex> sl(syncMtx);
            syncEnv(*this, false);
        } catch (...) {}
    }
    // Pooled transactions hand their buffers back to `buffers`, so they must go
    // first. Any still referenced by a stray handle simply outlives the pool.
    txnPool.clear();
    readerSlots.reset();
    buffers.purge();
    staleRegions.clear();
    region.reset();
    if (fd != os::kInvalidFile)
        os::closeFile(fd);
}

std::int64_t steadyNowNs() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

namespace {

/// One background sync, if commits are waiting and none synced lately. Never
/// throws: a failure poisons the store, which its next transaction reports.
void syncIfDue(EnvImpl& e) noexcept
{
    if (e.failed.load())
        return;
    if (steadyNowNs() - e.lastSyncNs.load(std::memory_order_relaxed) < e.syncIntervalNs)
        return;  // a commit or Env::sync() synced within the interval
    {
        std::lock_guard<std::mutex> lk(e.mtx);
        if (e.meta.txnid <= e.durableTxn)
            return;  // nothing waiting
    }
    // The lock outside the try: `failed` is set while syncMtx is still held, so a writer that
    // takes the lock next sees it.
    std::lock_guard<std::mutex> sl(e.syncMtx);
    try {
        syncEnv(e, false);
    } catch (const Error& err) {
        if (err.code() != ErrorCode::BadTransaction)
            e.failed.store(true);
    } catch (...) {
        e.failed.store(true);
    }
}

/// Options::backgroundSync(): wakes once an interval until the store closes.
void backgroundSyncLoop(EnvImpl* e)
{
    const auto interval = std::chrono::nanoseconds(e->syncIntervalNs);
    std::unique_lock<std::mutex> lk(e->syncWakeMtx);
    while (!e->syncStop) {
        e->syncWake.wait_for(lk, interval);
        if (e->syncStop)
            break;
        lk.unlock();
        syncIfDue(*e);
        lk.lock();
    }
}

}  // namespace

/// The newest free-list entry a writer may recycle: none a live reader's
/// snapshot needs, and none the newest durable snapshot needs. Pages retired
/// by transaction T belong to snapshot T-1, so they are reusable once every
/// snapshot that matters is T or newer. Under Safe the durable snapshot is the
/// newest commit; under the other modes it lags, which is what keeps the
/// snapshot a crash falls back to intact.
TxnId EnvImpl::oldestSnapshot() const
{
    const TxnId durable = std::min<TxnId>(durableTxn, writerMeta.txnid);
    const LiveReaders live = liveReaders();
    if (!live.count)
        return durable;
    return std::min<TxnId>(live.oldest, durable);
}

EnvImpl::LiveReaders EnvImpl::liveReaders() const
{
    LiveReaders live;
    for (const auto& r : readers)
        live.count += r.second;
    if (!readers.empty()) {
        live.oldest = readers.front().first;
        live.since = readerSince.empty() ? 0 : readerSince.front().second;
    }
    const unsigned used = readerSlots ? readerSlotsUsed.load(std::memory_order_seq_cst) : 0;
    for (unsigned i = 0; i < used; ++i) {
        const std::uint64_t word = readerSlots[i].pin.load(std::memory_order_seq_cst);
        if (word <= 1)
            continue;
        // A word its entry no longer carries was written by a reader that has yet to
        // find out the snapshot moved on; it reads nothing and will try again.
        const Published* entry = published[word & 0xff].get();
        if (!entry || entry->word != word)
            continue;
        const TxnId id = entry->meta.txnid;
        const std::int64_t began = readerSlots[i].since.load(std::memory_order_relaxed);
        if (!live.count || id < live.oldest) {
            live.oldest = id;
            live.since = began;
        } else if (id == live.oldest && began < live.since) {
            live.since = began;
        }
        ++live.count;
    }
    return live;
}

void EnvImpl::publishSnapshotLocked() noexcept
{
    if (!readerSlots)
        return;
    const std::uint64_t current = pubWord.load(std::memory_order_seq_cst);
    const unsigned used = readerSlotsUsed.load(std::memory_order_seq_cst);
    const auto pinned = [&](std::uint64_t word) {
        for (unsigned i = 0; i < used; ++i)
            if (readerSlots[i].pin.load(std::memory_order_seq_cst) == word)
                return true;
        return false;
    };
    for (unsigned i = 0; i < kPublished; ++i) {
        if (current && (current & 0xff) == i)
            continue;
        if (!published[i]) {
            try {
                published[i] = std::make_unique<Published>();
            } catch (const std::bad_alloc&) {
                break;
            }
        } else if (pinned(published[i]->word)) {
            continue;
        }
        Published& entry = *published[i];
        entry.meta = meta;
        entry.region = region;
        entry.word = (++pubCount << 8) | i;
        pubWord.store(entry.word, std::memory_order_seq_cst);
        return;
    }
    // No entry to be had: what readers would take is out of date, so they must not.
    pubWord.store(0, std::memory_order_seq_cst);
}

void EnvImpl::dropIdleReaderObjects() noexcept
{
    if (!readerSlots)
        return;
    const unsigned used = readerSlotsUsed.load(std::memory_order_seq_cst);
    for (unsigned i = 0; i < used; ++i) {
        std::uint64_t expected = 0;
        if (!readerSlots[i].pin.compare_exchange_strong(expected, 1, std::memory_order_seq_cst))
            continue;  // in use: its reader does this when it ends
        // Out of the slot before it goes: letting go of the last reference to the store
        // from inside the slot would destroy the slot under its own member.
        std::shared_ptr<TxnImpl> gone = std::move(readerSlots[i].txn);
        readerSlots[i].pin.store(0, std::memory_order_seq_cst);
    }
}

EnvImpl::ReaderSlot* EnvImpl::claimReaderSlot(int& index) noexcept
{
    if (!readerSlots)
        return nullptr;
    // A thread comes back to the slot it used last: its transaction object is there, and
    // the slot's cache line is already this core's.
    static thread_local unsigned hint = 0;
    const auto take = [&](unsigned i) {
        std::uint64_t expected = 0;
        return readerSlots[i].pin.load(std::memory_order_relaxed) == 0 &&
               readerSlots[i].pin.compare_exchange_strong(expected, 1, std::memory_order_seq_cst);
    };
    unsigned i = hint < kReaderSlots ? hint : 0;
    if (!take(i)) {
        for (i = 0; i < kReaderSlots && !take(i); ++i) {}
        if (i == kReaderSlots)
            return nullptr;
        hint = i;
    }
    unsigned used = readerSlotsUsed.load(std::memory_order_relaxed);
    while (used <= i &&
           !readerSlotsUsed.compare_exchange_weak(used, i + 1, std::memory_order_seq_cst)) {}
    index = int(i);
    return &readerSlots[i];
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

namespace {
using SnapshotCounts = std::vector<std::pair<TxnId, unsigned>>;

void countAcquire(SnapshotCounts& counts, TxnId id)
{
    const auto it =
        std::lower_bound(counts.begin(), counts.end(), id,
                         [](const std::pair<TxnId, unsigned>& e, TxnId v) { return e.first < v; });
    if (it != counts.end() && it->first == id)
        ++it->second;
    else
        counts.insert(it, {id, 1u});
}

void countRelease(SnapshotCounts& counts, TxnId id) noexcept
{
    const auto it =
        std::lower_bound(counts.begin(), counts.end(), id,
                         [](const std::pair<TxnId, unsigned>& e, TxnId v) { return e.first < v; });
    if (it == counts.end() || it->first != id)
        return;
    if (--it->second == 0)
        counts.erase(it);
}
}  // namespace

void EnvImpl::readerAcquire(TxnId id)
{
    // The step that can throw (an allocation) goes first and is undone if the second one throws, so the two
    // tables never disagree about which snapshots are live.
    const auto since = [](const std::pair<TxnId, std::int64_t>& e, TxnId v) { return e.first < v; };
    const auto it = std::lower_bound(readerSince.begin(), readerSince.end(), id, since);
    bool inserted = false;
    if (it == readerSince.end() || it->first != id) {
        readerSince.insert(it, {id, steadyNowNs()});
        inserted = true;
    }
    try {
        countAcquire(readers, id);
    } catch (...) {
        if (inserted)
            readerSince.erase(std::lower_bound(readerSince.begin(), readerSince.end(), id, since));
        throw;
    }
}

void EnvImpl::readerRelease(TxnId id) noexcept
{
    countRelease(readers, id);
    const auto still = std::lower_bound(readers.begin(), readers.end(), id,
                                        [](const std::pair<TxnId, unsigned>& e, TxnId v) { return e.first < v; });
    if (still != readers.end() && still->first == id)
        return;
    const auto it = std::lower_bound(readerSince.begin(), readerSince.end(), id,
                                     [](const std::pair<TxnId, std::int64_t>& e, TxnId v) { return e.first < v; });
    if (it != readerSince.end() && it->first == id)
        readerSince.erase(it);
}

void EnvImpl::occPinAcquire(TxnId id)
{
    countAcquire(occPins, id);
}

void EnvImpl::occPinRelease(TxnId id) noexcept
{
    countRelease(occPins, id);
}

/// Extend the file and the mapping. Where the mapping has its address range
/// reserved (POSIX) it grows in place; otherwise a new mapping is published
/// and existing transactions keep the ones they have seen, so growing never
/// pulls memory out from under a concurrent reader or the writer itself.
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
    const auto extend = [&](std::uint64_t target) {
        try {
            os::resizeFile(fd, target);
            if (preallocate)
                os::reserveSpace(fd, oldSize, target - oldSize);
        } catch (...) {
            // Never leave a tail no block backs: a later open would map it, and a write there on a full disk is SIGBUS.
            try { os::resizeFile(fd, oldSize); } catch (...) {}
            throw;
        }
    };
    const std::uint64_t exact = (needed + pageSize - 1) / pageSize * pageSize;
    // The generous step may not fit on the disk while what this commit needs does: halve down to the exact size.
    for (;;) {
        try {
            extend(want);
            break;
        } catch (const std::exception&) {
            if (want <= exact)
                throw;
            want = std::max(exact, (oldSize + (want - oldSize) / 2) / pageSize * pageSize);
        }
    }

    // Inside the address range reserved at open, the new extent is mapped in
    // place: every pointer handed out so far stays valid and the pages
    // already faulted in stay mapped.
    if (os::extendMapping(fd, region->m, std::size_t(want), !readOnly))
        return;

    auto fresh = std::make_shared<MappedRegion>();
    fresh->m = os::mapFile(fd, std::size_t(want), !readOnly);
    // Never unmap here: cursors in the running transaction hold raw pointers
    // into the outgoing mapping, and readers hold whole snapshots of it.
    staleRegions.push_back(std::move(region));
    region = std::move(fresh);
    publishSnapshotLocked();  // new readers map through the new one
}

/// Cut the file to `bytes` (Env::shrink()). A reserved mapping keeps its
/// address range, and its extent past the new end is mapped again in place
/// when the file grows; a plain one is unmapped by its size, so it is
/// replaced by a mapping of the new size and kept for whoever still holds it.
void EnvImpl::shrinkLocked(std::uint64_t bytes)
{
    if (bytes >= region->m.size)
        return;
    os::resizeFile(fd, bytes);
    if (region->m.reserved) {
        region->m.size = std::size_t(bytes);
        return;
    }
    auto fresh = std::make_shared<MappedRegion>();
    fresh->m = os::mapFile(fd, std::size_t(bytes), !readOnly);
    staleRegions.push_back(std::move(region));
    region = std::move(fresh);
    publishSnapshotLocked();
}

/// Drop mappings nobody references any more. Safe only at a transaction
/// boundary, where the last raw pointers into them have gone out of scope.
void EnvImpl::releaseStaleRegions()
{
    if (staleRegions.empty())
        return;
    // Entries of the ring no reader is on keep the mapping they were published with.
    if (readerSlots) {
        const std::uint64_t current = pubWord.load(std::memory_order_seq_cst);
        const unsigned used = readerSlotsUsed.load(std::memory_order_seq_cst);
        for (auto& entry : published) {
            if (!entry || entry->word == current || !entry->region)
                continue;
            bool pinned = false;
            for (unsigned i = 0; i < used && !pinned; ++i)
                pinned = readerSlots[i].pin.load(std::memory_order_seq_cst) == entry->word;
            if (!pinned)
                entry->region.reset();
        }
    }
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
    // The oldest version that reads it, until it holds a PrefixKeys tree.
    m.version = NOSQL_PREFIX_KEYS_DEFAULT ? kFormatVersion : kOldestFormatVersion;
    m.pageSize = std::uint32_t(e.pageSize);
    m.lastPgno = 1;  // the two meta pages
    m.fileSize = e.region->m.size;
    m.freeTree = emptyTree();  // 16-byte big-endian keys under byte order
    m.mainTree = emptyTree(NOSQL_PREFIX_KEYS_DEFAULT ? std::uint32_t(DbFlags::PrefixKeys) : 0u);
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
        os::recordMappedWrite(e.fd, std::uint64_t(slot) * e.pageSize, p, e.pageSize);
    }
    os::flushRange(e.fd, e.region->m, 0, 2 * e.pageSize);
    os::syncFile(e.fd, true);
    e.meta = m;
    e.writerMeta = m;
    e.durableTxn = 0;
    e.durableSlot = 0;
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
    if (m->version < kOldestFormatVersion || m->version > kFormatVersion)
        throw Error(ErrorCode::Incompatible, "store format version " + std::to_string(m->version) +
                                                 ", this build speaks " +
                                                 std::to_string(kOldestFormatVersion) + " to " +
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
    unsigned bestSlot = 0;
    for (unsigned slot = 0; slot < 2; ++slot) {
        if ((slot + 1) * e.pageSize > e.region->m.size)
            break;
        const Page* p = e.mapPage(*e.region, slot);
        if (p->flags != P_META)
            continue;
        const Meta* m = metaOf(p);
        if (!metaValid(*m, e.pageSize))
            continue;
        if (!best || m->txnid > best->txnid) {
            best = m;
            bestSlot = slot;
        }
    }
    if (!best)
        throw Error(ErrorCode::Corrupted, "no usable meta page in " + e.path.string());
    e.meta = *best;
    e.writerMeta = *best;
    e.durableTxn = best->txnid;
    e.durableSlot = bestSlot;
    if (std::uint64_t(e.meta.deferredCount) + e.meta.inlineCount > e.meta.deferredPages.size())
        throw Error(ErrorCode::Corrupted, "invalid deferred free-page count");
    for (std::size_t i = 0; i < std::size_t(e.meta.deferredCount) + e.meta.inlineCount; ++i)
        if (e.meta.deferredPages[i] < 2 || e.meta.deferredPages[i] > e.meta.lastPgno)
            throw Error(ErrorCode::Corrupted, "meta names a free page outside the store");
    e.carryOver.assign(e.meta.deferredPages.begin(),
                      e.meta.deferredPages.begin() + std::ptrdiff_t(e.meta.deferredCount));
    e.commitGen.store(e.meta.txnid, std::memory_order_release);
    if ((e.meta.lastPgno + 1) * e.pageSize > e.region->m.size)
        throw Error(ErrorCode::Corrupted, "meta references pages beyond the end of the file");
}

/// Pages the free tree of the snapshot just loaded lists: one walk at open,
/// so that EnvStats::freePages never needs one.
std::uint64_t countFreeTreePages(EnvImpl& e)
{
    if (e.meta.freeTree.root == kInvalidPage)
        return 0;
    TxnImpl t;
    t.env = &e;
    t.readOnly = true;
    t.loadSnapshot(e.meta, e.region);
    CursorImpl c;
    c.txn = &t;
    c.dbi = kFreeDbi;
    std::uint64_t pages = 0;
    for (bool ok = cursorFirst(c); ok; ok = cursorNext(c)) {
        std::size_t n = 0;
        if (!forEachFreePage(cursorValue(c), freeChunkLimit(e.pageSize), n, [](PageNo) {}))
            throw Error(ErrorCode::Corrupted, "invalid free-list value");
        pages += n;
    }
    t.releaseResources();
    t.env = nullptr;
    return pages;
}

}  // namespace
}  // namespace nosql::internal

namespace nosql {

Env Env::Options::open(const std::filesystem::path& path) const
{
    using namespace internal;

    if (pageSize_ && !legalPageSize(pageSize_))
        throw Error(ErrorCode::InvalidArgument, "pageSize must be a power of two in [1024, 65536]");
    if (maxDbs_ < 1)
        throw Error(ErrorCode::InvalidArgument, "maxDbs must be at least 1");

    auto e = std::make_shared<EnvImpl>();
    e->path = path;
    e->maxDbs = maxDbs_;
    e->readOnly = readOnly_;
    e->dura = durability_;
    e->pipelined = pipelineCommits_ && durability_ == Durability::Safe;
    e->occSpins = commitSpins_;
    e->syncIntervalNs = std::chrono::nanoseconds(syncInterval_).count();
    e->lagAgeNs = std::chrono::nanoseconds(lagAge_).count();
    e->lagIntervalNs = std::chrono::nanoseconds(lagInterval_).count();
    e->lagCallback = lagCallback_;
    e->lastSyncNs.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count(),
                        std::memory_order_relaxed);
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
    // A spilled buffer gives its memory back only as whole system pages.
    e->spill = spillDirtyPages_ && ps >= os::systemPageSize();
    if (cacheReadChecksums_)
        e->validation.configure(ps, std::max<std::uint64_t>(maxSize_, 16 * ps), revalidateAfter_);
    e->writerCache = writerUsesReadCache_ && cacheReadChecksums_;
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

    // Reserve the address range for the largest file this open may grow to,
    // so the mapping never has to move (see EnvImpl::growLocked).
    const std::uint64_t reserve =
        readOnly_ || !kReserveMapping ? mapped : std::max(mapped, e->sizeUpper);
    e->region = std::make_shared<MappedRegion>();
    e->region->m =
        os::mapFileReserved(e->fd, std::size_t(mapped), std::size_t(reserve), !readOnly_);

    if (fresh)
        formatStore(*e);
    else
        loadMeta(*e);
    e->freeTreePages = countFreeTreePages(*e);
    e->readerSlots = std::make_unique<EnvImpl::ReaderSlot[]>(EnvImpl::kReaderSlots);
    e->publishSnapshotLocked();
    // A tail left by a growth that failed, or by a build without this check, may have no blocks behind it.
    if (!fresh && !readOnly_ && preallocate_) {
        const std::uint64_t used = (e->meta.lastPgno + 1) * std::uint64_t(ps);
        if (mapped > used) {
            try {
                os::reserveSpace(e->fd, used, mapped - used);
            } catch (const std::exception&) {
                std::lock_guard<std::mutex> lk(e->mtx);
                e->shrinkLocked(used);   // growth then asks the disk again, and fails cleanly
            }
        }
    }

#ifdef NOSQL_NO_REPLICATION
    if (!ship_.empty())
        throw Error(ErrorCode::Unsupported, "built without NOSQL_WITH_REPLICATION");
#else
    if (!ship_.empty() && !readOnly_)
        e->ship = std::make_unique<ShipWriter>(ship_, ps, shipSegment_, shipRetain_);
#endif

    if (backgroundSync_ && durability_ == Durability::Lazy && !readOnly_ && e->syncIntervalNs > 0)
        e->syncThread = std::thread(backgroundSyncLoop, e.get());
    return Env(std::move(e));
}

// Handles are counted apart from the references transactions and their handles hold: when
// the last one goes, the transaction objects idle reader slots keep, each with a reference
// to the store of its own (TxnImpl::envKeep), are let go, so the store closes once the
// transactions still running have ended.
Env::Env(std::shared_ptr<internal::EnvImpl> e) noexcept : impl_(std::move(e))
{
    if (impl_)
        impl_->handles.fetch_add(1, std::memory_order_seq_cst);
}

void Env::release() noexcept
{
    if (!impl_)
        return;
    if (impl_->handles.fetch_sub(1, std::memory_order_seq_cst) == 1)
        impl_->dropIdleReaderObjects();
    impl_.reset();
}

Env::~Env()
{
    release();
}

Env& Env::operator=(Env&& other) noexcept
{
    if (this != &other) {
        release();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

void Env::close()
{
    release();
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
    // syncMtx, not mtx: readers and the writer carry on during the flush.
    std::lock_guard<std::mutex> sl(e.syncMtx);
    // After a failed flush the kernel may have dropped the dirty pages and
    // report success to a retry: the store is finished until reopened from
    // its last checksummed meta page (see EnvImpl::failed).
    try {
        internal::syncEnv(e, force);
    } catch (const Error& err) {
        if (err.code() != ErrorCode::BadTransaction)
            e.failed.store(true);
        throw;
    } catch (...) {
        e.failed.store(true);
        throw;
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
        case ErrorCode::Conflict: return "optimistic transaction conflict";
    }
    return "unknown error";
}

}  // namespace nosql
