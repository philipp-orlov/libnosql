// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Transactions: Page allocation, the reclaim (GC) list, and commit.
//
// Commit is shadow paging, not logging. Dirty pages are copied into the map at
// page numbers nothing older references, flushed, and only then does a new
// meta page point at them. A torn meta write leaves the previous one intact,
// which is why the reclaim rule never touches pages the previous snapshot
// still needs.

#include <algorithm>
#include <chrono>
#include <cstring>

#ifndef NOSQL_NO_REPLICATION
#include "nosql/internal/ship_log.hpp"
#endif
#include "nosql/internal/io_observer.hpp"
#include "nosql/internal/core.hpp"
#include "nosql/internal/free_list.hpp"

namespace nosql::internal {

TxnImpl::~TxnImpl()
{
    releaseResources();
}

/// Give back everything heavy once the transaction is over, so a handle that
/// outlives it keeps only enough state to report ErrorCode::BadTransaction.
void TxnImpl::releaseResources()
{
    // Buffers go back to the env's recycler rather than to the system
    // allocator: page-sized blocks churning through malloc for months is what
    // fragments a long-running process.
    // Every owned buffer came from env->buffers, so `owned` is empty
    // whenever `env` is not set. One lock and one counter update for the
    // whole list: a large commit hands back tens of thousands at once.
    if (!owned.empty() && env) {
        std::uint64_t bytes = 0;
        for (const auto& [buf, npages] : owned)
            bytes += env->buffers.allocationSize(npages);
        env->buffers.release(owned.data(), owned.size());
        env->dirtyBytes.fetch_sub(bytes - spilledBytes, std::memory_order_relaxed);
    }
    owned.clear();
    spilledBytes = 0;
    if (!spilled.empty())
        spilled.release();  // a spilling transaction's table is not worth keeping
    spillPool.clear();
    spillList.clear();
    // `pool` only ever holds buffers that are also in `owned`, so dropping the
    // references here is enough.
    pool.clear();
    dirty.clear();
    loose.clear();
    retiredLocal.clear();
    pending.clear();
    flushList.clear();
    flushRuns.clear();
    flushSpans.clear();
    gcKeys.clear();
    gcAdd.clear();
    gcAcc.clear();
    gcPinned.clear();
    gcExempt.clear();
    carryNext.clear();
    reclaim.clear();
    consumed.clear();
    occWritten.clear();
    std::size_t remaining = kRetainedScratchBytes;
    if (dirty.retainedBytes() > remaining)
        dirty.release();
    else
        remaining -= dirty.retainedBytes();
    const auto trim = [&remaining](auto& values) {
        const std::size_t bytes = values.capacity() *
            sizeof(typename std::decay_t<decltype(values)>::value_type);
        if (bytes > remaining)
            std::decay_t<decltype(values)>().swap(values);
        else
            remaining -= bytes;
    };
    trim(owned);
    trim(pool);
    trim(loose);
    trim(pending);
    trim(retiredLocal);
    trim(flushList);
    trim(flushRuns);
    trim(flushSpans);
    trim(gcKeys);
    trim(gcAdd);
    trim(gcAcc);
    trim(gcPinned);
    trim(gcExempt);
    trim(reclaim);
    trim(consumed);
    trim(carryNext);
    trim(occWritten);
    trim(spillPool);
    trim(spillList);
    region.reset();
    seen.clear();
    parent = nullptr;
    child = nullptr;
}

/// Hand a recycled object back in the state a fresh one would be in. Every
/// container keeps the capacity it earned, which is the whole point.
void TxnImpl::reset()
{
    releaseResources();
    resizeDbs(0);
    meta = Meta{};
    env = nullptr;
    readOnly = false;
    finished = false;
    txnid = 0;
    lastPgno = 0;
    namedDbs = 0;
    reclaimPos = 0;
    reclaimNext = 0;
    reclaimNextChunk = 0;
    oldest = 0;
    readersOldest = 0;
    exemptEpoch = 0;
    freeTreeDelta = 0;
    reclaimDrained = false;
    inlineAbsorbed = false;
    inlineOurs = false;
    inlinePages.clear();
    noAbsorb = false;
    noSpend = false;
    occTrack = false;
    occUnknown = false;
    occLastDbi = ~0u;
    prefixFormat = false;
    seq = 1;
}

void TxnImpl::loadSnapshot(const Meta& snapshot, std::shared_ptr<MappedRegion> mapping)
{
    meta = snapshot;
    region = std::move(mapping);
    lastPgno = meta.lastPgno;
    namedDbs = meta.namedDbs;
    // Resize in place rather than assign from a temporary: a recycled object
    // already holds three empty slots, and this touches only the trees.
    resizeDbs(kCatalogDbi + 1);
    for (DbSlot& slot : dbs) {
        slot.name.clear();
        slot.dirty = 0;
    }
    dbs[kFreeDbi].tree = meta.freeTree;
    dbs[kMainDbi].tree = meta.mainTree;
    dbs[kCatalogDbi].tree = meta.catalogTree;
    dbs[kFreeDbi].loaded = dbs[kMainDbi].loaded = dbs[kCatalogDbi].loaded = 1;
}

namespace {

/// Refuses `bytes` more of dirty buffers past the budget. A budget, not a
/// synchronization point: relaxed is enough.
void chargeDirty(const EnvImpl* env, std::uint64_t bytes)
{
    const auto current = env->dirtyBytes.load(std::memory_order_relaxed);
    if (env->dirtyLimit && !env->commitOverBudget &&
        (bytes > env->dirtyLimit || current > env->dirtyLimit - bytes))
        throw Error(ErrorCode::OutOfMemory,
                    "dirty page budget exceeded; reduce the batch or raise dirtyLimit");
}

}  // namespace

Page* TxnImpl::allocBuffer(PageNo p, std::uint16_t flags, unsigned npages)
{
    Page* buf = nullptr;
    if (npages == 1 && !pool.empty()) {
        // Freed earlier in this same transaction, so it is already in `owned`.
        buf = pool.back();
        pool.pop_back();
    } else if (npages == 1 && !spillPool.empty()) {
        // Also in `owned`, but its memory went back to the OS: counted again.
        const auto bytes = env->buffers.allocationSize(1);
        chargeDirty(env, bytes);
        buf = spillPool.back();
        spillPool.pop_back();
        spilledBytes -= bytes;
        env->dirtyBytes.fetch_add(bytes, std::memory_order_relaxed);
    } else {
        const auto bytes = env->buffers.allocationSize(npages);
        chargeDirty(env, bytes);
        buf = env->buffers.acquire(npages);
        try {
            owned.emplace_back(buf, npages);
        } catch (...) {
            env->buffers.release(buf, npages);
            throw;
        }
        env->dirtyBytes.fetch_add(bytes, std::memory_order_relaxed);
    }
    buf->pgno = p;
    buf->flags = flags;
    dirty.set(p, buf);
    if (!(flags & P_BRANCH))
        env->spillExhausted = false;  // something a spill could write again
    return buf;
}

namespace {

/// Merges the sorted tail reclaim[joined..] into the sorted unspent part
/// reclaim[reclaimPos..joined), dropping the spent prefix. Through the
/// retained gcAcc rather than std::inplace_merge, which takes a temporary
/// buffer from the heap whenever both parts are non-empty.
void mergeReclaimed(TxnImpl* t, std::size_t joined)
{
    std::vector<PageNo>& rc = t->reclaim;
    std::vector<PageNo>& out = t->gcAcc;
    out.resize(rc.size() - t->reclaimPos);
    std::merge(rc.begin() + std::ptrdiff_t(t->reclaimPos), rc.begin() + std::ptrdiff_t(joined),
               rc.begin() + std::ptrdiff_t(joined), rc.end(), out.begin());
    rc.swap(out);
    t->reclaimPos = 0;
}

/// Whether this writer may recycle the GC entry (key, chunk). Beyond
/// `oldest` (Lazy and None, pinned by the last sync) only exempt chunks
/// written under the current durable commit qualify.
bool reclaimable(const TxnImpl* t, TxnId key, std::uint64_t chunk) noexcept
{
    if (key <= t->oldest)
        return true;
    return key <= t->readersOldest && isExemptChunk(chunk) &&
           exemptEpochOf(chunk) == t->exemptEpoch;
}

/// `p`, read from the free list, checked before the allocator may hand it out:
/// a writer that took a damaged number would write outside the file it maps.
PageNo freePageNumber(const TxnImpl* t, PageNo p)
{
    if (p < 2 || p > t->lastPgno)
        throw Error(ErrorCode::Corrupted, "free list names a page outside the store");
    return p;
}

/// Absorb the next GC entry into the reclaim list. Only entries retired by a
/// transaction no live snapshot can still see are eligible.
bool reclaimMore(TxnImpl* t)
{
    if (t->noAbsorb)
        return false;
    // The snapshot's inline entry first: absorbing it costs no tree access.
    if (!t->inlineAbsorbed && t->meta.inlineCount && t->meta.inlineTxn <= t->oldest) {
        const std::size_t d = t->meta.deferredCount, n = t->meta.inlineCount;
        if (d + n > t->meta.deferredPages.size())
            throw Error(ErrorCode::Corrupted, "invalid inline free-list count");
        const std::size_t joined = t->reclaim.size();
        t->reclaim.resize(joined + n);
        for (std::size_t i = 0; i < n; ++i)
            t->reclaim[joined + i] = freePageNumber(t, t->meta.deferredPages[d + i]);
        std::sort(t->reclaim.begin() + std::ptrdiff_t(joined), t->reclaim.end());
        mergeReclaimed(t, joined);
        t->inlineAbsorbed = true;
        return true;
    }
    if (t->reclaimDrained)
        return false;
    CursorImpl c;
    c.txn = t;
    c.dbi = kFreeDbi;
    const FreeKey from = freeKey(t->reclaimNext, t->reclaimNextChunk);
    if (!cursorSeek(c, from.slice())) {
        t->reclaimDrained = true;
        return false;
    }
    TxnId key;
    std::uint64_t chunk;
    for (;;) {
        const Slice k = cursorKey(c);
        if (k.size() != kFreeKeyBytes)
            throw Error(ErrorCode::Corrupted, "invalid free-list key length");
        key = freeKeyTxn(k);
        chunk = freeKeyChunk(k);
        if (key > t->readersOldest) {
            t->reclaimDrained = true;
            return false;
        }
        if (reclaimable(t, key, chunk))
            break;
        // Waits for the next sync; this transaction passes over it.
        if (!cursorNext(c)) {
            t->reclaimDrained = true;
            return false;
        }
    }
    const Slice v = cursorValue(c);
    std::size_t n = 0;
    const std::size_t joined = t->reclaim.size();
    if (!forEachFreePage(v, freeChunkLimit(t->pageSize()), n, [](PageNo) {}))
        throw Error(ErrorCode::Corrupted, "invalid free-list value");
    t->reclaim.resize(joined + n);
    std::size_t index = joined;
    forEachFreePage(v, n, n, [&](PageNo p) { t->reclaim[index++] = freePageNumber(t, p); });
    // Keep the unspent part fully ordered, not just the new tail: allocating a
    // multi-page overflow run scans it for a contiguous stretch.
    std::sort(t->reclaim.begin() + std::ptrdiff_t(joined), t->reclaim.end());
    mergeReclaimed(t, joined);
    t->consumed.emplace_back(key, chunk);
    t->freeTreeDelta -= std::int64_t(n);
    // Resume after this chunk; past the last chunk number, at the next id.
    t->reclaimNext = chunk == kInlineChunk ? key + 1 : key;
    t->reclaimNextChunk = chunk == kInlineChunk ? 0 : chunk + 1;
    return n > 0;
}

/// Moves the transaction and its ancestors onto the mapping a growth just
/// published (only where the mapping cannot grow in place). The root keeps
/// every mapping the tree has used until it ends: slices handed to the caller
/// and pointers the writer still holds address them.
void refreshRegion(TxnImpl* t, const std::shared_ptr<MappedRegion>& r)
{
    TxnImpl* root = t->root();
    for (TxnImpl* x = t; x; x = x->parent) {
        if (x->region == r)
            continue;
        root->seen.push_back(std::move(x->region));
        x->region = r;
    }
}

}  // namespace

PageNo TxnImpl::allocPages(unsigned count)
{
    if (count == 1 && !loose.empty()) {
        const PageNo p = loose.back();
        loose.pop_back();
        return p;
    }

    TxnImpl* r = root();
    // Scanning for a contiguous run is linear in the reclaim list, so cap how
    // much a single multi-page request may absorb-and-rescan. Falling
    // through to the end of the file is always correct, and those pages come
    // back through the free list like any others.
    constexpr int kRunScanBudget = 4;
    for (int attempt = 0;; ++attempt) {
        auto& rc = r->reclaim;
        std::size_t& pos = r->reclaimPos;
        if (!r->noSpend && pos < rc.size()) {
            if (count == 1) {
                const PageNo p = rc[pos++];
                if (env->deferredMeta())
                    env->noteBorn(p, 1);
                return p;
            }
            // The list is sorted, so a contiguous stretch of `count` pages shows
            // up as a matching first and last page number.
            for (std::size_t i = pos; i + count <= rc.size(); ++i) {
                if (rc[i] + count - 1 == rc[i + count - 1]) {
                    const PageNo p = rc[i];
                    rc.erase(rc.begin() + std::ptrdiff_t(i),
                             rc.begin() + std::ptrdiff_t(i + count));
                    if (env->deferredMeta())
                        env->noteBorn(p, count);
                    return p;
                }
            }
        }
        // Counted in chunks' worth of pages scanned, so a free list of many
        // small entries gets the same search as one of a few full ones.
        if (count > 1 && attempt >= kRunScanBudget &&
            (rc.size() - pos >= kRunScanBudget * freeChunkPages(pageSize()) ||
             attempt >= kRunScanBudget * 16))
            break;
        if (!reclaimMore(r))
            break;
    }

    const PageNo p = lastPgno + 1;
    if (p + count - 1 > kMaxPgno)
        throw Error(ErrorCode::MapFull, "page number space exhausted");
    const std::uint64_t need = (p + count) * std::uint64_t(pageSize());
    if (need > region->m.size) {
        std::lock_guard<std::mutex> lk(env->mtx);
        env->growLocked(need);
        if (env->region != region)
            refreshRegion(this, env->region);
    }
    if (env->deferredMeta())
        env->noteBorn(p, count);
    lastPgno = p + count - 1;
    return p;
}

void TxnImpl::freePage(PageNo p, unsigned count)
{
    if (Page* buf = dirty.find(p)) {
        // Ours: nothing committed points at it, so it is reusable right away.
        dirty.erase(p);
        pool.push_back(buf);
        for (unsigned k = 0; k < count; ++k)
            loose.push_back(p + k);
        return;
    }
    if (Page* buf = spilled.find(p)) {
        // Ours as well; only the bytes went to the file early.
        spilled.erase(p);
        if (count == 1)
            spillPool.push_back(buf);
        for (unsigned k = 0; k < count; ++k)
            loose.push_back(p + k);
        return;
    }
    for (TxnImpl* a = parent; a; a = a->parent) {
        if (a->dirty.find(p) || a->spilled.find(p)) {
            // An ancestor's private page: only reusable once we commit into it.
            for (unsigned k = 0; k < count; ++k)
                retiredLocal.push_back(p + k);
            return;
        }
    }
    // Part of a committed snapshot: has to go through the GC tree.
    for (unsigned k = 0; k < count; ++k)
        pending.push_back(p + k);
}

// ---------------------------------------------------------------- commit ---

namespace {

/// vector::assign that grows the capacity geometrically. assign() allocates
/// exactly what it is given, so a list whose high-water mark creeps up, as
/// the free-list remainder does with the timing of Lazy syncs, would
/// allocate again at every new mark.
template <class V, class It>
void assignGrowing(V& v, It first, It last)
{
    const auto n = std::size_t(last - first);
    if (n > v.capacity())
        v.reserve(std::max(n, 2 * v.capacity()));
    v.assign(first, last);
}

void flushNamedTrees(TxnImpl* t)
{
    for (unsigned i = kCatalogDbi + 1; i < t->dbs.size(); ++i) {
        if (!t->dbs[i].dirty)
            continue;
        const DbSlot& slot = t->dbs[i];
        if (!slot.loaded)
            continue;
        treeFlushSubdb(t, Slice(slot.name), slot.tree);
        t->dbs[i].dirty = 0;
    }
}

/// Records sorted, distinct `pages` under free-list key `key`: as runs where
/// `runs` allows them (the store is at format 8) and they take less room
/// (free_list.hpp), else as an array.
void putFreeChunk(CursorImpl& c, Slice key, const PageNo* pages, std::size_t n, bool runs)
{
    const std::size_t packed = runs ? freeRunsBytes(pages, n) : 0;
    WritableSlice w;
    if (packed) {
        treePut(c, key, Slice(nullptr, packed), PutMode::Upsert, &w);
        writeFreeRuns(w.data(), packed, pages, n);
        return;
    }
    treePut(c, key, Slice(nullptr, n * sizeof(PageNo)), PutMode::Upsert, &w);
    for (std::size_t index = 0; index < n; ++index)
        writeLittle(w.data() + index * sizeof(PageNo), pages[index]);
}

/// Fold this transaction's retired pages into the GC tree and drop the
/// entries it consumed. Reclamation is switched off for the duration so the
/// procedure cannot feed on its own output.
void updateFreelist(TxnImpl* t)
{
    // Keep room to copy a free-tree path and split it before absorption is
    // disabled -- and, for a commit that will write chunks of its own rather
    // than an inline entry, about half a page per chunk. Otherwise small
    // commits can spend the inline free entry, then extend the file for GC
    // bookkeeping while older free chunks go unused.
    if (t->treeAt(kFreeDbi).root != kInvalidPage) {
        const std::size_t chunkPages = freeChunkPages(t->pageSize());
        const std::size_t retired = t->pending.size() + t->loose.size();
        const std::size_t chunkReserve =
            retired > 16 ? (retired + chunkPages - 1) / chunkPages / 2 + 1 : 0;
        const std::size_t reserve = 2 * (std::size_t(t->treeAt(kFreeDbi).depth) + 1) + chunkReserve;
        while (t->reclaim.size() - t->reclaimPos + t->loose.size() < reserve)
            if (!reclaimMore(t))
                break;
    }

    // Stop absorbing new GC entries -- the delete pass below has already run,
    // so a newly absorbed key would have nobody to remove it -- but keep
    // spending the ones already absorbed. Without that, a commit would have to
    // take its own bookkeeping pages from the end of the file, and the store
    // would grow by a few pages on every single commit, for ever.
    t->noAbsorb = true;

    // The environment's list stays as it is until this commit is published.
    t->pending.insert(t->pending.end(), t->env->carryOver.begin(), t->env->carryOver.end());

    CursorImpl c;
    c.txn = t;
    c.dbi = kFreeDbi;

    // Absorbed chunks must go, and they must go now: their pages have been
    // handed out or are about to be re-listed, so a surviving key would list
    // them twice.
    t->gcKeys.clear();
    t->gcKeys.swap(t->consumed);
    for (const TxnImpl::GcKey& key : t->gcKeys)
        treeDel(c, freeKey(key.first, key.second).slice());

    // An inline entry a reader still pins moves into the tree as an ordinary
    // entry, freeing the inline slot for ours. Its transaction may have tree
    // chunks of its own (0, 1, ...), so it takes a chunk number no commit
    // writes.
    if (!t->inlineAbsorbed && t->meta.inlineCount) {
        const std::size_t d = t->meta.deferredCount, n = t->meta.inlineCount;
        if (d + n > t->meta.deferredPages.size())
            throw Error(ErrorCode::Corrupted, "invalid inline free-list count");
        WritableSlice w;
        if (!treePut(c, freeKey(t->meta.inlineTxn, kInlineChunk).slice(), Slice(nullptr, n * sizeof(PageNo)),
                     PutMode::InsertUnique, &w))
            throw Error(ErrorCode::Corrupted, "inline free-list entry is already in the tree");
        for (std::size_t i = 0; i < n; ++i)
            writeLittle(w.data() + i * sizeof(PageNo), PageNo(t->meta.deferredPages[d + i]));
        t->inlineAbsorbed = true;
        t->freeTreeDelta += std::int64_t(n);
    }
    t->inlineOurs = false;
    t->inlinePages.clear();

    // Record, under this transaction's id, everything it retired plus whatever
    // it absorbed and did not spend, in chunks small enough to sit inline in a
    // leaf. Both sets move while we write them -- writing the entries
    // allocates and frees pages of its own -- so rewrite until a pass changes
    // neither, at which point the entries are exact.
    std::vector<PageNo>& freed = t->gcAdd;
    std::vector<PageNo>& acc = t->gcAcc;
    freed.clear();
    const std::size_t chunkPages = freeChunkPages(t->pageSize());
    // Runs only where every build that opens the store reads them.
    const bool runs = t->meta.version >= kFormatVersion || t->prefixFormat;
    std::size_t written = 0;  ///< chunks currently recorded under our id
    constexpr int kMaxRounds = 8;
    // At most this many of our retired pages go inline, leaving room in the
    // meta page's shared array for pages a commit frees too late (deferred).
    constexpr std::size_t kInlineMax = 16;
    bool inlineFreed = false;
    std::vector<PageNo>& remainder = t->gcRemainder;
    // Under Lazy and None, what this commit absorbed and did not spend goes
    // back under `oldest`, which no absorbed entry is newer than, instead of
    // under our own id: there it would wait for the next sync although no
    // snapshot that matters names it. The chunk numbers carry our id, so no
    // other commit writes the same keys (see remainderChunk).
    const bool keepReusable = t->env->deferredMeta() && t->txnid < kRemainderTxnLimit;
    std::size_t writtenOld = 0;     ///< chunks currently recorded under `oldest`
    std::size_t writtenExempt = 0;  ///< exempt chunks currently recorded under our id
    std::size_t listed = 0;         ///< pages the chunks written so far list
    static const std::vector<PageNo> kNone;
    // Of what this commit retired, the pages handed out since the last sync
    // are named by no durable snapshot: they go into exempt chunks, which only
    // readers hold back. The rest waits for the next sync.
    std::vector<PageNo>& pinned = t->gcPinned;
    std::vector<PageNo>& exempt = t->gcExempt;
    const std::vector<PageNo>* retired = &freed;
    for (int round = 0;; ++round) {
        freed.insert(freed.end(), t->pending.begin(), t->pending.end());
        t->pending.clear();
        freed.insert(freed.end(), t->loose.begin(), t->loose.end());
        t->loose.clear();

        const std::size_t spent = t->reclaimPos;
        const std::size_t left = t->reclaim.size();

        // `freed` is short and unordered; the unspent reclaim list is already
        // sorted, so one merge yields the sorted, duplicate-free union.
        std::sort(freed.begin(), freed.end());
        freed.erase(std::unique(freed.begin(), freed.end()), freed.end());
        const auto remainderBegin = t->reclaim.begin() + std::ptrdiff_t(t->reclaimPos);
        acc.resize(freed.size() + std::size_t(t->reclaim.end() - remainderBegin));
        const auto accEnd = std::set_union(freed.begin(), freed.end(), remainderBegin,
                                           t->reclaim.end(), acc.begin());
        acc.resize(std::size_t(accEnd - acc.begin()));

        // Last chance: freeze the reclaim list so this write is describing a set
        // that cannot shift under it.
        if (round + 1 >= kMaxRounds)
            t->noSpend = true;

        // The pages this transaction retired go into the meta page when there
        // are few of them -- every commit writes that page anyway -- and only
        // the unspent remainder of what it absorbed goes into the tree. The
        // next small commit then absorbs the inline entry, needs nothing from
        // the tree, and writes no free-list page at all.
        assignGrowing(remainder, remainderBegin, t->reclaim.end());
        const std::size_t oldChunks = (remainder.size() + chunkPages - 1) / chunkPages;
        const bool useOldKeys = keepReusable && oldChunks <= kRemainderChunks;
        pinned.clear();
        exempt.clear();
        if (useOldKeys) {
            for (const PageNo p : freed)
                (t->env->isBorn(p) ? exempt : pinned).push_back(p);
        } else {
            assignGrowing(pinned, freed.begin(), freed.end());
        }
        retired = useOldKeys ? &pinned : &freed;
        inlineFreed = !retired->empty() && retired->size() <= kInlineMax;
        const std::vector<PageNo>& record = useOldKeys    ? (inlineFreed ? kNone : pinned)
                                            : inlineFreed ? remainder
                                                          : acc;
        const std::size_t exemptChunks =
            useOldKeys ? (exempt.size() + chunkPages - 1) / chunkPages : 0;
        for (std::size_t i = 0; i < exemptChunks; ++i) {
            const std::size_t first = i * chunkPages;
            const std::size_t n = std::min(chunkPages, exempt.size() - first);
            putFreeChunk(c, freeKey(t->txnid, exemptChunk(t->exemptEpoch, i)).slice(),
                         exempt.data() + first, n, runs);
        }
        for (std::size_t i = exemptChunks; i < writtenExempt; ++i)
            treeDel(c, freeKey(t->txnid, exemptChunk(t->exemptEpoch, i)).slice());
        writtenExempt = exemptChunks;
        const std::size_t keptOld = useOldKeys ? oldChunks : 0;
        for (std::size_t i = 0; i < keptOld; ++i) {
            const std::size_t first = i * chunkPages;
            const std::size_t n = std::min(chunkPages, remainder.size() - first);
            putFreeChunk(c, freeKey(t->oldest, remainderChunk(t->txnid, i)).slice(),
                         remainder.data() + first, n, runs);
        }
        for (std::size_t i = keptOld; i < writtenOld; ++i)
            treeDel(c, freeKey(t->oldest, remainderChunk(t->txnid, i)).slice());
        writtenOld = keptOld;

        const std::size_t chunks = (record.size() + chunkPages - 1) / chunkPages;
        for (std::size_t i = 0; i < chunks; ++i) {
            const std::size_t first = i * chunkPages;
            const std::size_t n = std::min(chunkPages, record.size() - first);
            putFreeChunk(c, freeKey(t->txnid, i).slice(), record.data() + first, n, runs);
        }
        // A previous round may have needed more chunks than this one.
        for (std::size_t i = chunks; i < written; ++i)
            treeDel(c, freeKey(t->txnid, i).slice());
        written = chunks;
        listed = record.size() + (useOldKeys ? remainder.size() + exempt.size() : 0);
        if (acc.empty())
            break;

        const bool settled = t->pending.empty() && t->loose.empty() && t->reclaimPos == spent &&
                             t->reclaim.size() == left;
        if (settled || round + 1 >= kMaxRounds)
            break;
    }
    t->freeTreeDelta += std::int64_t(listed);
    if (inlineFreed) {
        assignGrowing(t->inlinePages, retired->begin(), retired->end());
        t->inlineOurs = true;
    }

    // Pages freed too late to make it into the entry above roll into the next
    // writer, which records them then. Until then they are referenced by
    // nothing and listed nowhere, which is safe -- nothing can hand them out.
    auto& carry = t->carryNext;
    carry.clear();
    carry.insert(carry.end(), t->pending.begin(), t->pending.end());
    carry.insert(carry.end(), t->loose.begin(), t->loose.end());
    // Both lists share the meta page's array: if they would not fit
    // together, our retired pages join the deferred ones instead (the next
    // writer records them under its own id, which is only more cautious).
    if (t->inlineOurs && carry.size() + t->inlinePages.size() > t->meta.deferredPages.size()) {
        carry.insert(carry.end(), t->inlinePages.begin(), t->inlinePages.end());
        t->inlinePages.clear();
        t->inlineOurs = false;
    }
    t->pending.clear();
    t->loose.clear();
    t->reclaim.clear();
    t->reclaimPos = 0;
    t->noAbsorb = false;
    t->noSpend = false;
}

/// How committed pages reach the file. Through the mapping, a page the kernel
/// has already cleaned -- every page, once a commit has been fsynced -- costs a
/// write-protect fault that on a journaling filesystem runs into the
/// microseconds, and a page evicted from the cache is read back before it can
/// be overwritten. A positional write of whole pages needs neither; it costs a
/// system call per contiguous run instead.
#ifndef NOSQL_WRITE_THROUGH_MAPPING
#define NOSQL_WRITE_THROUGH_MAPPING 1
#endif
constexpr bool kWriteThroughMapping = NOSQL_WRITE_THROUGH_MAPPING != 0;

/// Readies a dirty buffer's bytes for the file; returns its length in pages.
unsigned sealForFile(Page* buf, std::size_t ps)
{
    const bool big = isOverflow(buf);
    const unsigned n = big ? ovPages(buf) : 1u;
    if (!big) {
        // Only a slotted page has a gap; an overflow page reuses those header
        // words for its run length, and its tail was zeroed when it was made.
        // Buffers are recycled, and node deletion compacts by moving bytes
        // down and leaving the originals behind, so without this one page's
        // leftovers would travel inside another page's free space and the same
        // logical content would not produce the same bytes on disk.
        std::memset(reinterpret_cast<std::byte*>(buf) + buf->lower, 0, pageUpper(buf) - buf->lower);
    }
    sealPage(buf, std::size_t(n) * ps);
    return n;
}

/// Write dirty pages into the file and record the runs they occupy. Nothing
/// is flushed here: writeMeta() decides, by durability mode, whether this
/// commit flushes the data before its meta page.
void writePages(TxnImpl* t)
{
    EnvImpl* e = t->env;
    const std::size_t ps = e->pageSize;

    {
        std::lock_guard<std::mutex> lk(e->mtx);
        e->growLocked((t->lastPgno + 1) * std::uint64_t(ps));
        if (e->region != t->region)
            refreshRegion(t, e->region);
    }
    std::byte* base = t->region->m.base;

    // Collect (Page number, buffer) once and sort by page number: the writes
    // below then walk the file forwards, and the flush ranges coalesce.
    std::vector<std::pair<PageNo, Page*>>& ids = t->flushList;
    ids.clear();
    ids.reserve(t->dirty.size() + t->spilled.size());
    t->dirty.forEach([&](PageNo pn, Page* buf) { ids.emplace_back(pn, buf); });
    // Spilled pages are in the file already, but the flush, the validation
    // cache and the ship log need their runs like any other's.
    t->spilled.forEach([&](PageNo pn, Page*) { ids.emplace_back(pn, nullptr); });
    std::sort(ids.begin(), ids.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    std::vector<std::pair<PageNo, PageNo>>& runs = t->flushRuns;  // inclusive
    runs.clear();
    std::vector<os::Span>& spans = t->flushSpans;
    spans.clear();
    const auto writeSpans = [&] {
        if (spans.empty())
            return;
        os::writeAtV(e->fd, runs.back().first * std::uint64_t(ps), spans.data(),
                     unsigned(spans.size()));
        spans.clear();
    };
    bool runSpilled = false;
    for (const auto& [p, buf] : ids) {
        if (!buf) {
            const Page* written = e->mapPage(*t->region, p);
            const unsigned n = isOverflow(written) ? ovPages(written) : 1u;
            e->validation.invalidate(p, n);
            if (!runs.empty() && runs.back().second + 1 == p &&
                (kWriteThroughMapping || runSpilled)) {
                runs.back().second = p + n - 1;
            } else {
                if (!kWriteThroughMapping)
                    writeSpans();
                runs.emplace_back(p, p + n - 1);
                runSpilled = true;
            }
            continue;
        }
        const unsigned n = sealForFile(buf, ps);
        // Before the meta that publishes these bytes: a reader that can see
        // them must find them unverified.
        e->validation.invalidate(p, n);
        // Spans gather only behind the run's first page, so without the
        // mapping a written page never joins a spilled page's run.
        if (!runs.empty() && runs.back().second + 1 == p && (kWriteThroughMapping || !runSpilled)) {
            runs.back().second = p + n - 1;
        } else {
            if (!kWriteThroughMapping)
                writeSpans();
            runs.emplace_back(p, p + n - 1);
            runSpilled = false;
        }
        if (kWriteThroughMapping) {
            os::observeIo(os::IoEvent::Write, std::size_t(n) * ps);
            std::memcpy(base + p * ps, buf, std::size_t(n) * ps);
            os::recordMappedWrite(e->fd, p * std::uint64_t(ps), base + p * ps, std::size_t(n) * ps);
            os::observeIo(os::IoEvent::WriteProgress, std::size_t(n) * ps);
        } else {
            spans.push_back(os::Span{buf, std::size_t(n) * ps});
        }
    }
    if (!kWriteThroughMapping)
        writeSpans();
}

std::int64_t steadyNs() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

/// Writes `m` into the meta slot that does not hold the newest durable meta,
/// so a torn write leaves that one in charge. Caller holds syncMtx.
unsigned storeMeta(EnvImpl& e, const os::Mapping& view, const Meta& m)
{
    const unsigned slot = 1u - e.durableSlot;
    Page* mp = reinterpret_cast<Page*>(view.base + slot * e.pageSize);
    mp->pgno = slot;
    mp->flags = P_META;
    mp->nkeys = 0;
    *metaOf(mp) = m;
    os::recordMappedWrite(e.fd, std::uint64_t(slot) * e.pageSize, mp, e.pageSize);
    return slot;
}

void markDurable(EnvImpl& e, TxnId id, unsigned slot)
{
    {
        std::lock_guard<std::mutex> lk(e.mtx);
        e.durableTxn = id;
    }
    e.durableSlot = slot;
}

/// The deferred-meta sync: every data page of every commit up to `m`, then
/// its meta page, each flushed before the next step. Caller holds syncMtx.
void syncTo(EnvImpl& e, const os::Mapping& view, const Meta& m)
{
    e.syncRuns.assign(1, {0, m.lastPgno});
    os::flushData(e.fd, view, e.syncRuns, e.pageSize);
    const unsigned slot = storeMeta(e, view, m);
    os::flushRange(e.fd, view, std::size_t(slot) * e.pageSize, e.pageSize);
    markDurable(e, m.txnid, slot);
    e.lastSyncNs.store(steadyNs(), std::memory_order_relaxed);
}

/// Makes a commit visible to new readers and to commit waiters. Caller holds mtx.
void publishToReadersLocked(EnvImpl& e, const Meta& m)
{
    e.meta = m;
    e.commitGen.store(m.txnid, std::memory_order_release);
}

/// Makes a commit the one the next writer builds on. Caller holds mtx.
void publishToWritersLocked(EnvImpl& e, TxnImpl* t, const Meta& m)
{
    e.writerMeta = m;
    e.carryOver.swap(t->carryNext);
    e.freeTreePages = std::uint64_t(std::int64_t(e.freeTreePages) + t->freeTreeDelta);
    e.occLog.record(m.txnid, t->occTrack && !t->occUnknown ? &t->occWritten : nullptr);
    // Every snapshot still validated against, a reader's or an optimistic
    // commit's, is at or after the oldest one held, and a new one starts at
    // the newest readers may see; entries at or before that can no longer
    // conflict with anything.
    TxnId oldest = e.readers.empty() ? TxnId(e.meta.txnid) : TxnId(e.readers.front().first);
    if (!e.occPins.empty())
        oldest = std::min(oldest, TxnId(e.occPins.front().first));
    e.occLog.trim(oldest);
}

/// Makes the commit visible to everyone at once: every mode but pipelined Safe.
void publish(EnvImpl& e, TxnImpl* t, const Meta& m)
{
    std::lock_guard<std::mutex> lk(e.mtx);
    publishToReadersLocked(e, m);
    publishToWritersLocked(e, t, m);
}

/// A pipelined commit's meta has reached the device, by a later commit's data
/// flush or by its own: readers may see it now, and its committer may return.
/// Caller holds syncMtx.
void landPipelined(EnvImpl& e, bool carried)
{
    e.lastMetaCarried.store(carried, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(e.mtx);
        e.durableTxn = e.unflushedTxn;
        publishToReadersLocked(e, e.unflushedMeta);
    }
    e.durableSlot = e.unflushedSlot;
    e.unflushedTxn = 0;
    e.commitCv.notify_all();
}

/// Flushes pipelined commit `id`'s meta page itself, unless a later commit's
/// data flush carried it already.
void flushOwnMeta(EnvImpl& e, TxnId id)
{
    std::lock_guard<std::mutex> sl(e.syncMtx);
    if (e.unflushedTxn != id || e.failed.load())
        return;
    // The mapping by value, under mtx: a writer growing the file updates the
    // region's mapping in place, under mtx, while this runs without it.
    std::shared_ptr<MappedRegion> keep;  // where the mapping can move, this one stays mapped
    os::Mapping view;
    {
        std::lock_guard<std::mutex> lk(e.mtx);
        keep = e.region;
        view = e.region->m;
    }
    try {
        os::flushRange(e.fd, view, std::size_t(e.unflushedSlot) * e.pageSize, e.pageSize);
    } catch (...) {
        // The kernel may drop the page after a failed flush; see Failure.
        e.failed.store(true);
        e.commitCv.notify_all();
        throw;
    }
    landPipelined(e, false);
}

/// Builds the new meta and publishes it. What reaches the file, and when,
/// depends on the durability mode; in every mode a meta page is written only
/// after the data it names has been flushed, so it can never reach stable
/// storage before those pages. Torn writes of the meta page itself are
/// handled by the two-slot checksum scheme in loadMeta() (env.cpp), not by
/// write ordering.
TxnId writeMeta(TxnImpl* t)
{
    EnvImpl* e = t->env;
    Meta m = t->meta;
    std::memcpy(m.magic, kMagic, sizeof m.magic);
    // A store's version only rises: to 8 when it first holds P_PREFIX leaves.
    m.version =
        std::max<std::uint32_t>(m.version, t->prefixFormat ? kFormatVersion : kOldestFormatVersion);
    m.pageSize = std::uint32_t(e->pageSize);
    m.txnid = t->txnid;
    m.lastPgno = t->lastPgno;
    m.namedDbs = t->namedDbs;
    m.fileSize = t->region->m.size;
    m.freeTree = t->dbs[kFreeDbi].tree;
    m.mainTree = t->dbs[kMainDbi].tree;
    m.catalogTree = t->dbs[kCatalogDbi].tree;
    m.parentId = t->meta.commitId;
    m.parentChecksum = t->meta.checksum;
    m.commitId = newIdentity();
    // The inline slot: ours, or still the snapshot's entry if nothing
    // absorbed or moved it (then it carries forward unchanged), or empty.
    // Both share the array with the deferred pages, which go first.
    const std::vector<PageNo>& carry = t->carryNext;
    const std::size_t d = t->meta.deferredCount;
    const std::size_t inlineCount = t->inlineOurs        ? t->inlinePages.size()
                                    : !t->inlineAbsorbed ? std::size_t(t->meta.inlineCount)
                                                         : 0;
    if (carry.size() + inlineCount > m.deferredPages.size())
        throw Error(ErrorCode::MapFull,
                    "deferred free-page budget exceeded; reduce transaction size");
    m.deferredPages = {};
    std::copy(carry.begin(), carry.end(), m.deferredPages.begin());
    TxnId inlineTxn = 0;
    if (t->inlineOurs) {
        std::copy(t->inlinePages.begin(), t->inlinePages.end(),
                  m.deferredPages.begin() + std::ptrdiff_t(carry.size()));
        inlineTxn = t->txnid;
    } else if (inlineCount) {
        for (std::size_t i = 0; i < inlineCount; ++i)
            m.deferredPages[carry.size() + i] = t->meta.deferredPages[d + i];
        inlineTxn = t->meta.inlineTxn;
    }
    m.deferredCount = std::uint32_t(carry.size());
    m.inlineCount = std::uint32_t(inlineCount);
    m.inlineTxn = inlineTxn;
    m.checksum = metaChecksum(m);

    if (e->pipelined) {
        std::lock_guard<std::mutex> sl(e->syncMtx);
        // One barrier for this commit's data and for the previous commit's
        // meta, still waiting for one; the runs name that page too, for the
        // platforms that flush by range. Then this meta goes in without a
        // barrier of its own, and the writer slot goes to the next writer,
        // which builds on it. Readers see it once it lands.
        assignGrowing(e->syncRuns, t->flushRuns.begin(), t->flushRuns.end());
        if (e->unflushedTxn)
            e->syncRuns.insert(e->syncRuns.begin(), {e->unflushedSlot, e->unflushedSlot});
        const std::int64_t began = steadyNs();
        os::flushData(e->fd, t->region->m, e->syncRuns, e->pageSize);
        e->dataFlushNs.store(steadyNs() - began, std::memory_order_relaxed);
        if (e->unflushedTxn)
            landPipelined(*e, true);
        const unsigned slot = storeMeta(*e, t->region->m, m);
        e->unflushedTxn = m.txnid;
        e->unflushedSlot = slot;
        e->unflushedMeta = m;
        std::lock_guard<std::mutex> lk(e->mtx);
        publishToWritersLocked(*e, t, m);
        return m.txnid;
    }

    const bool lazyDue =
        e->dura == Durability::Lazy &&
        steadyNs() - e->lastSyncNs.load(std::memory_order_relaxed) >= e->syncIntervalNs;
    if (e->dura == Durability::Safe || e->dura == Durability::NoMetaSync) {
        std::lock_guard<std::mutex> sl(e->syncMtx);
        // A barrier, not a hint: it blocks until the data is off every
        // write-back cache in the stack, so the meta write below cannot race
        // it even if the OS, controller or drive reorder writes freely on
        // either side. See docs/design.md, "Durability and the memory map".
        os::flushData(e->fd, t->region->m, t->flushRuns, e->pageSize);
        // The same barrier carried the previous NoMetaSync meta to the device.
        if (e->unflushedTxn) {
            markDurable(*e, e->unflushedTxn, e->unflushedSlot);
            e->unflushedTxn = 0;
        }
        const unsigned slot = storeMeta(*e, t->region->m, m);
        if (e->dura == Durability::Safe) {
            os::flushRange(e->fd, t->region->m, std::size_t(slot) * e->pageSize, e->pageSize);
            markDurable(*e, m.txnid, slot);
        } else {
            e->unflushedTxn = m.txnid;
            e->unflushedSlot = slot;
        }
        publish(*e, t, m);
    } else if (lazyDue) {
        std::lock_guard<std::mutex> sl(e->syncMtx);
        syncTo(*e, t->region->m, m);
        publish(*e, t, m);
    } else {
        // Lazy before its interval, and None: the meta lives in memory only.
        // Readers take their snapshot from e->meta, never from the file.
        publish(*e, t, m);
    }
    // After the flush, so a woken waiter's snapshot always contains the commit.
    e->commitCv.notify_all();
    return 0;
}

void commitNested(TxnImpl* t)
{
    TxnImpl* p = t->parent;

    t->dirty.forEach([&](PageNo pgno, Page* buf) { p->dirty.set(pgno, buf); });
    t->spilled.forEach([&](PageNo pgno, Page* buf) { p->spilled.set(pgno, buf); });
    p->owned.insert(p->owned.end(), t->owned.begin(), t->owned.end());
    t->owned.clear();
    p->pool.insert(p->pool.end(), t->pool.begin(), t->pool.end());
    p->spillPool.insert(p->spillPool.end(), t->spillPool.begin(), t->spillPool.end());
    p->spilledBytes += t->spilledBytes;
    t->spilledBytes = 0;

    // Pages the child copied away from. One of the parent's own private pages
    // is now unreachable everywhere and can be reused at once. A page private
    // to an older ancestor is still reachable from that ancestor -- the parent
    // may yet abort -- so it stays retired until the parent commits into the
    // level that owns it.
    const std::vector<PageNo>& retired = t->retiredLocal;
    for (std::size_t i = 0; i < retired.size(); ++i) {
        const PageNo pgno = retired[i];
        if (Page* buf = p->dirty.find(pgno)) {
            // An overflow run is retired page by page but held under its first
            // page: the whole run is free, and the rest of it follows here.
            const unsigned n = isOverflow(buf) ? ovPages(buf) : 1u;
            p->pool.push_back(buf);
            p->dirty.erase(pgno);
            for (unsigned k = 0; k < n; ++k)
                p->loose.push_back(pgno + k);
            while (i + 1 < retired.size() && retired[i + 1] > pgno && retired[i + 1] < pgno + n)
                ++i;
        } else if (Page* gone = p->spilled.find(pgno)) {
            // Likewise, but the buffer's memory went back: the file has the header.
            const Page* written = p->env->mapPage(*p->region, pgno);
            const unsigned n = isOverflow(written) ? ovPages(written) : 1u;
            if (n == 1)
                p->spillPool.push_back(gone);
            p->spilled.erase(pgno);
            for (unsigned k = 0; k < n; ++k)
                p->loose.push_back(pgno + k);
            while (i + 1 < retired.size() && retired[i + 1] > pgno && retired[i + 1] < pgno + n)
                ++i;
        } else {
            p->retiredLocal.push_back(pgno);
        }
    }
    p->loose.insert(p->loose.end(), t->loose.begin(), t->loose.end());
    p->pending.insert(p->pending.end(), t->pending.begin(), t->pending.end());
    p->prefixFormat = p->prefixFormat || t->prefixFormat;
    // The child's writes become the parent's; an aborted child's never do.
    if (p->occTrack) {
        p->occUnknown = p->occUnknown || t->occUnknown;
        if (!p->occUnknown)
            p->occWritten.insert(p->occWritten.end(), t->occWritten.begin(), t->occWritten.end());
    }

    p->lastPgno = t->lastPgno;
    p->namedDbs = t->namedDbs;
    // Fold the child's view of every sub-database back into the parent, but
    // keep the parent's own "needs writing back" bits: it may have dirtied a
    // tree before the child ever opened it.
    p->resizeDbs(t->dbs.size());
    for (std::size_t i = 0; i < t->dbs.size(); ++i) {
        const std::uint8_t wasDirty = p->dbs[i].dirty;
        p->dbs[i] = t->dbs[i];
        p->dbs[i].dirty |= wasDirty;
    }
    ++p->root()->seq;
}

/// The last step of every transaction: mark it done and hand its memory back.
void finish(TxnImpl* t)
{
    t->finished = true;
    t->releaseResources();
}

/// Ends a read transaction (nested or not): unpins its snapshot so pages it
/// protected can be reclaimed and stale mappings released.
void finishReader(TxnImpl* t)
{
    EnvImpl* e = t->env;
    t->region.reset();
    std::lock_guard<std::mutex> lk(e->mtx);
    e->readerRelease(t->txnid);
    e->releaseStaleRegions();
    if (t->parent)
        t->parent->child = nullptr;
    finish(t);
}

/// Ends the root write transaction, handing the writer slot back.
void finishWriter(TxnImpl* t)
{
    EnvImpl* e = t->env;
    finish(t);
    {
        std::lock_guard<std::mutex> lk(e->mtx);
        e->releaseStaleRegions();
    }
    e->writersPending.fetch_sub(1, std::memory_order_relaxed);
    e->writeMtx.unlock();
}

void abortNested(TxnImpl* t)
{
    TxnImpl* p = t->parent;

    // Hand the buffers up rather than releasing them: a cursor belonging to the
    // parent may still hold a pointer into one until it next re-anchors.
    p->owned.insert(p->owned.end(), t->owned.begin(), t->owned.end());
    t->owned.clear();
    t->dirty.forEach([&](PageNo pgno, Page* buf) {
        const unsigned n = isOverflow(buf) ? ovPages(buf) : 1u;
        for (unsigned k = 0; k < n; ++k)
            p->loose.push_back(pgno + k);
    });
    // Spilled ones too; their bytes in the file are as dead as a buffer's.
    t->spilled.forEach([&](PageNo pgno, Page*) {
        const Page* written = t->env->mapPage(*t->region, pgno);
        const unsigned n = isOverflow(written) ? ovPages(written) : 1u;
        for (unsigned k = 0; k < n; ++k)
            p->loose.push_back(pgno + k);
    });
    p->spilledBytes += t->spilledBytes;
    t->spilledBytes = 0;
    p->loose.insert(p->loose.end(), t->loose.begin(), t->loose.end());
    // Never rewind the frontier: pages handed back above may sit beyond it.
    p->lastPgno = std::max(p->lastPgno, t->lastPgno);

    ++p->root()->seq;
}

}  // namespace

void txnCommit(TxnImpl* t, TxnId* awaitLater)
{
    if (awaitLater)
        *awaitLater = 0;
    EnvImpl* e = t->env;
    if (t->child)
        throw Error(ErrorCode::BadTransaction, "commit the nested transaction first");
    if (t->readOnly) {
        finishReader(t);
        return;
    }
    if (t->parent) {
        commitNested(t);
        t->parent->child = nullptr;
        finish(t);
        return;
    }

    // Anything that throws part-way through leaves the store on its previous
    // meta page -- nothing is published until writeMeta succeeds -- but the
    // writer slot still has to be handed back.
    //
    // Any failure before the first page is written (MapFull, or an IoError
    // such as ENOSPC from growing the file) is a plain refusal rather than a
    // fault: no page a snapshot names has been touched, so the store stays
    // usable and only this transaction is lost. Everything after that point
    // poisons the handle (see Env::writeTxn).
    bool untouched = true;
    TxnId pending = 0;  // a pipelined commit whose meta still waits for its flush
    if (t->occTrack && !t->occUnknown) {
        // Sorted now, so publish() can hand it to the log under the lock as is.
        std::sort(t->occWritten.begin(), t->occWritten.end());
        t->occWritten.erase(std::unique(t->occWritten.begin(), t->occWritten.end()),
                            t->occWritten.end());
    }
    try {
        e->commitOverBudget = e->spill;
        flushNamedTrees(t);
        updateFreelist(t);
        e->commitOverBudget = false;
        {
            std::lock_guard<std::mutex> lk(e->mtx);
            e->growLocked((t->lastPgno + 1) * std::uint64_t(e->pageSize));
        }
        untouched = false;
        writePages(t);
        pending = writeMeta(t);
        // Reads the bytes back out of the mapping writePages() just wrote
        // through, so a segment carries what the store holds rather than what
        // the dirty buffers held. Cannot fail the commit: see ShipWriter.
        if (e->ship)
            e->ship->capture(e->writerMeta, t->flushRuns, t->region->m.base);
    } catch (...) {
        e->commitOverBudget = false;
        if (!untouched)
            e->failed.store(true);
        finishWriter(t);
        throw;
    }
    finishWriter(t);  // the next writer may start while this one's meta waits
    if (pending) {
        if (awaitLater)
            *awaitLater = pending;
        else
            awaitDurable(*e, pending);
    }
}

void awaitDurable(EnvImpl& e, TxnId id)
{
    // Only a pipelined commit waits for its own meta. Under Lazy and None the
    // durable commit lags on purpose, and no flush here would move it.
    if (!e.pipelined)
        return;
    // Worth waiting for a successor to carry the meta when one is visible, or
    // when the last meta was carried and so commits are arriving as a stream;
    // for about two data flushes at most, about what flushing it here costs.
    // A lone writer flushes at once and pays no wait.
    constexpr std::int64_t kMinPatienceNs = 50'000;
    const std::int64_t patience =
        std::max(kMinPatienceNs, 2 * e.dataFlushNs.load(std::memory_order_relaxed));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::nanoseconds(patience);
    std::unique_lock<std::mutex> lk(e.mtx);
    while (e.durableTxn < id) {
        if (e.failed.load())
            throw Error(ErrorCode::IoError,
                        "a flush failed before this commit's meta reached the device; close and "
                        "reopen the environment");
        const bool successor = e.writersPending.load(std::memory_order_relaxed) != 0 ||
                               e.occWaiting.load(std::memory_order_relaxed) != 0 ||
                               e.lastMetaCarried.load(std::memory_order_relaxed);
        if (!successor || std::chrono::steady_clock::now() >= deadline) {
            lk.unlock();
            flushOwnMeta(e, id);
            lk.lock();
            continue;
        }
        e.commitCv.wait_until(lk, deadline);
    }
}

void syncEnv(EnvImpl& e, bool force)
{
    Meta m;
    os::Mapping view;
    std::shared_ptr<MappedRegion> keep;
    TxnId durable;
    {
        std::lock_guard<std::mutex> lk(e.mtx);
        m = e.meta;
        keep = e.region;  // where the mapping can move, this one stays mapped
        view = e.region->m;
        durable = e.durableTxn;
    }
    if (e.deferredMeta()) {
        // A failed flush may have lost pages the in-memory meta names; it
        // must never be written. The store reopens on its last durable meta.
        if (e.failed.load())
            throw Error(ErrorCode::BadTransaction,
                        "commit failed; close and reopen the environment");
        if (m.txnid > durable)
            syncTo(e, view, m);
        else if (force)
            os::syncFile(e.fd, true);
        return;
    }
    if (!force && e.dura == Durability::Safe)
        return;
    os::flushRange(e.fd, view, 0, view.size);
    os::syncFile(e.fd, true);
    if (e.unflushedTxn) {
        if (e.pipelined) {
            landPipelined(e, false);
        } else {
            markDurable(e, e.unflushedTxn, e.unflushedSlot);
            e.unflushedTxn = 0;
        }
    }
}

void txnAbort(TxnImpl* t)
{
    while (t->child)
        txnAbort(t->child);
    if (t->readOnly) {
        finishReader(t);
        return;
    }
    if (t->parent) {
        abortNested(t);
        t->parent->child = nullptr;
        finish(t);
        return;
    }
    finishWriter(t);
}

// ---------------------------------------------------------------- shrink ---

void absorbFreeList(TxnImpl* t)
{
    while (reclaimMore(t)) {}
}

PageNo shrinkLimit(const TxnImpl* t, std::uint64_t slack)
{
    // Live pages from T up must fit the free pages below T, with `slack`
    // over. The free pages from T up count on both sides, so what remains is
    // T >= lastPgno + 1 + slack - free: the file shrinks to its live pages
    // and the slack.
    const std::uint64_t free = t->reclaim.size() - t->reclaimPos;
    const std::uint64_t end = std::uint64_t(t->lastPgno) + 1 + slack;
    return end > free + 2 ? PageNo(end - free) : PageNo(2);
}

std::size_t freeBelow(const TxnImpl* t, PageNo limit)
{
    const auto begin = t->reclaim.begin() + std::ptrdiff_t(t->reclaimPos);
    return std::size_t(std::lower_bound(begin, t->reclaim.end(), limit) - begin);
}

PageNo allocBelow(TxnImpl* t, unsigned count, PageNo limit)
{
    auto& rc = t->reclaim;
    if (t->noSpend)
        return kInvalidPage;
    for (std::size_t i = t->reclaimPos; i + count <= rc.size() && rc[i] + count - 1 < limit; ++i) {
        if (rc[i] + count - 1 == rc[i + count - 1]) {
            const PageNo p = rc[i];
            if (i == t->reclaimPos && count == 1)
                ++t->reclaimPos;
            else
                rc.erase(rc.begin() + std::ptrdiff_t(i), rc.begin() + std::ptrdiff_t(i + count));
            if (t->env->deferredMeta())
                t->env->noteBorn(p, count);
            return p;
        }
    }
    return kInvalidPage;
}

std::uint64_t cutFreeTail(TxnImpl* t)
{
    auto& rc = t->reclaim;
    std::size_t k = 0;
    while (k < rc.size() - t->reclaimPos && rc[rc.size() - 1 - k] == t->lastPgno - k &&
           t->lastPgno - k > 1)
        ++k;
    if (!k)
        return 0;
    rc.resize(rc.size() - k);
    t->lastPgno -= k;
    // Nothing names them now; whatever takes their numbers again is new.
    t->env->validation.invalidate(t->lastPgno + 1, unsigned(k));
    return k;
}

// ----------------------------------------------------------------- spill ---

void TxnImpl::spill()
{
    const std::uint64_t target = env->dirtyLimit / 2;
    const std::size_t ps = env->pageSize;
    // Coldest first, from this transaction and its ancestors alike: a page
    // that has not been touched for a while is the least likely to be soon,
    // so the least likely to be read back. Branch pages stay: nearly every
    // write descends through them, and they are a small share of any tree.
    std::vector<SpillCandidate>& list = spillList;
    list.clear();
    const std::uint32_t now = env->touchClock;
    for (TxnImpl* a = this; a; a = a->parent)
        a->dirty.forEach([&](PageNo pn, Page* buf) {
            if (isBranch(buf))
                return;
            const unsigned n = isOverflow(buf) ? ovPages(buf) : 1u;
            const auto stamp = readLittle<std::uint32_t>(reinterpret_cast<std::byte*>(buf) +
                                                         std::size_t(n) * ps - kPageChecksumBytes);
            list.push_back({now - stamp, pn, buf, a});  // modular: the clock may wrap
        });
    // Only as many as the target needs, then in file order, which the file
    // system writes best.
    std::uint64_t over = env->dirtyBytes.load(std::memory_order_relaxed);
    over = over > target ? over - target : 0;
    std::sort(list.begin(), list.end(), [](const SpillCandidate& x, const SpillCandidate& y) {
        return x.age > y.age || (x.age == y.age && x.pgno < y.pgno);
    });
    std::size_t take = 0;
    for (std::uint64_t freed = 0; take < list.size() && freed < over; ++take)
        freed +=
            env->buffers.allocationSize(isOverflow(list[take].buf) ? ovPages(list[take].buf) : 1u);
    list.resize(take);
    std::sort(list.begin(), list.end(),
              [](const SpillCandidate& x, const SpillCandidate& y) { return x.pgno < y.pgno; });
    std::uint64_t pages = 0;
    for (const SpillCandidate& s : list) {
        TxnImpl* a = s.owner;
        Page* buf = s.buf;
        const PageNo p = s.pgno;
        std::byte* base = a->region->m.base;
        const unsigned n = sealForFile(buf, ps);
        const std::size_t bytes = std::size_t(n) * ps;
        env->validation.invalidate(p, n);
        // Where it will commit: a page number no snapshot, durable or
        // visible, refers to, so the write is as safe now as at commit.
        if (kWriteThroughMapping) {
            std::memcpy(base + p * ps, buf, bytes);
            os::recordMappedWrite(env->fd, p * std::uint64_t(ps), base + p * ps, bytes);
        } else {
            os::writeAt(env->fd, p * std::uint64_t(ps), buf, bytes);
        }
        const std::size_t held = env->buffers.allocationSize(n);
        os::discardMemory(buf, held);
        a->dirty.erase(p);
        a->spilled.set(p, buf);
        a->spilledBytes += held;
        env->dirtyBytes.fetch_sub(held, std::memory_order_relaxed);
        pages += n;
    }
    list.clear();
    env->spilledPages.fetch_add(pages, std::memory_order_relaxed);
    // Everything it could write is written: the next spill would find only
    // what writes make dirty from here on.
    env->spillExhausted = env->dirtyBytes.load(std::memory_order_relaxed) > target;
    // Cursors re-anchor rather than read a page they saw in a buffer.
    ++root()->seq;
}

Page* TxnImpl::unspill(PageNo p)
{
    Page* buf = spilled.find(p);
    if (!buf)
        return nullptr;
    // Checked like any page read back from the file.
    const Page* written = getPage(p, true, PageKind::Either);
    const unsigned n = isOverflow(written) ? ovPages(written) : 1u;
    const std::size_t held = env->buffers.allocationSize(n);
    chargeDirty(env, held);
    std::memcpy(buf, written, std::size_t(n) * env->pageSize);
    spilled.erase(p);
    dirty.set(p, buf);
    spilledBytes -= held;
    env->dirtyBytes.fetch_add(held, std::memory_order_relaxed);
    env->unspilledPages.fetch_add(n, std::memory_order_relaxed);
    env->spillExhausted = false;
    touch(buf, n);
    return buf;
}

}  // namespace nosql::internal
