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
#include <cstring>

#ifndef NOSQL_NO_REPLICATION
#include "nosql/internal/ship_log.hpp"
#endif
#include "nosql/internal/io_observer.hpp"
#include "nosql/internal/core.hpp"

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
    if (!owned.empty()) {
        if (env)
            for (const auto& [buf, npages] : owned)
            {
                env->dirtyBytes.fetch_sub(env->buffers.allocationSize(npages));
                env->buffers.release(buf, npages);
            }
        else
            for (const auto& [buf, npages] : owned)
                os::freePage(buf);
        owned.clear();
    }
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
    reclaim.clear();
    consumed.clear();
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
    trim(reclaim);
    trim(consumed);
    region.reset();
    parent = nullptr;
    child = nullptr;
}

/// Hand a recycled object back in the state a fresh one would be in. Every
/// container keeps the capacity it earned, which is the whole point.
void TxnImpl::reset()
{
    releaseResources();
    dbs.clear();
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
    reclaimDrained = false;
    noAbsorb = false;
    noSpend = false;
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
    dbs.resize(kCatalogDbi + 1);
    for (DbSlot& slot : dbs) {
        slot.name.clear();
        slot.dirty = 0;
    }
    dbs[kFreeDbi].tree = meta.freeTree;
    dbs[kMainDbi].tree = meta.mainTree;
    dbs[kCatalogDbi].tree = meta.catalogTree;
    dbs[kFreeDbi].loaded = dbs[kMainDbi].loaded = dbs[kCatalogDbi].loaded = 1;
}

Page* TxnImpl::allocBuffer(PageNo p, std::uint16_t flags, unsigned npages)
{
    Page* buf = nullptr;
    if (npages == 1 && !pool.empty()) {
        // Freed earlier in this same transaction, so it is already in `owned`.
        buf = pool.back();
        pool.pop_back();
    } else {
        const auto bytes = env->buffers.allocationSize(npages);
        const auto current = env->dirtyBytes.load();
        if (env->dirtyLimit && (bytes > env->dirtyLimit || current > env->dirtyLimit - bytes))
            throw Error(ErrorCode::OutOfMemory, "dirty page budget exceeded; reduce the batch or raise dirtyLimit");
        buf = env->buffers.acquire(npages);
        try {
            owned.emplace_back(buf, npages);
        } catch (...) {
            env->buffers.release(buf, npages);
            throw;
        }
        env->dirtyBytes.fetch_add(bytes);
    }
    buf->pgno = p;
    buf->flags = flags;
    dirty.set(p, buf);
    return buf;
}

namespace {

/// Absorb the next GC entry into the reclaim list. Only entries retired by a
/// transaction no live snapshot can still see are eligible.
bool reclaimMore(TxnImpl* t)
{
    if (t->reclaimDrained || t->noAbsorb)
        return false;
    CursorImpl c;
    c.txn = t;
    c.dbi = kFreeDbi;
    const FreeKey from = freeKey(t->reclaimNext, t->reclaimNextChunk);
    if (!cursorSeek(c, from.slice())) {
        t->reclaimDrained = true;
        return false;
    }
    const Slice k = cursorKey(c);
    if (k.size() != kFreeKeyBytes)
        throw Error(ErrorCode::Corrupted, "invalid free-list key length");
    const TxnId key = freeKeyTxn(k);
    const std::uint64_t chunk = freeKeyChunk(k);
    if (key > t->oldest) {
        t->reclaimDrained = true;
        return false;
    }
    const Slice v = cursorValue(c);
    const std::size_t n = v.size() / sizeof(PageNo);
    if (v.size() % sizeof(PageNo) != 0)
        throw Error(ErrorCode::Corrupted, "invalid free-list value length");
    const std::size_t joined = t->reclaim.size();
    t->reclaim.resize(joined + n);
    for (std::size_t index = 0; index < n; ++index)
        t->reclaim[joined + index] = readLittle<PageNo>(v.data() + index * sizeof(PageNo));
    // Keep the unspent part fully ordered, not just the new tail: allocating a
    // multi-page overflow run scans it for a contiguous stretch.
    std::sort(t->reclaim.begin() + std::ptrdiff_t(joined), t->reclaim.end());
    std::inplace_merge(t->reclaim.begin() + std::ptrdiff_t(t->reclaimPos),
                       t->reclaim.begin() + std::ptrdiff_t(joined), t->reclaim.end());
    t->consumed.emplace_back(key, chunk);
    t->reclaimNext = key;
    t->reclaimNextChunk = chunk + 1;
    return n > 0;
}

void refreshRegion(TxnImpl* t, std::shared_ptr<MappedRegion> r)
{
    for (TxnImpl* x = t; x; x = x->parent)
        x->region = r;
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
    // many times a single multi-page request may absorb-and-rescan. Falling
    // through to the end of the file is always correct, and those pages come
    // back through the free list like any others.
    constexpr int kRunScanBudget = 4;
    for (int attempt = 0;; ++attempt) {
        auto& rc = r->reclaim;
        std::size_t& pos = r->reclaimPos;
        if (!r->noSpend && pos < rc.size()) {
            if (count == 1)
                return rc[pos++];
            if (attempt < kRunScanBudget) {
                // The list is sorted, so a contiguous stretch of `count` pages shows
                // up as a matching first and last page number.
                for (std::size_t i = pos; i + count <= rc.size(); ++i) {
                    if (rc[i] + count - 1 == rc[i + count - 1]) {
                        const PageNo p = rc[i];
                        rc.erase(rc.begin() + std::ptrdiff_t(i),
                                 rc.begin() + std::ptrdiff_t(i + count));
                        return p;
                    }
                }
            }
        }
        if (count > 1 && attempt >= kRunScanBudget)
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
        refreshRegion(this, env->region);
    }
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
    for (TxnImpl* a = parent; a; a = a->parent) {
        if (a->dirty.find(p)) {
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

void flushNamedTrees(TxnImpl* t)
{
    for (unsigned i = kCatalogDbi + 1; i < t->dbs.size(); ++i) {
        if (!t->dbs[i].dirty)
            continue;
        const DbSlot& slot = t->dbs[i];
        if (!slot.loaded)
            continue;
        treePutSubdb(t, Slice(slot.name), slot.tree);
        t->dbs[i].dirty = 0;
    }
}

/// Fold this transaction's retired pages into the GC tree and drop the
/// entries it consumed. Reclamation is switched off for the duration so the
/// procedure cannot feed on its own output.
void updateFreelist(TxnImpl* t)
{
    // Stop absorbing new GC entries -- the delete pass below has already run,
    // so a newly absorbed key would have nobody to remove it -- but keep
    // spending the ones already absorbed. Without that, a commit would have to
    // take its own bookkeeping pages from the end of the file, and the store
    // would grow by a few pages on every single commit, for ever.
    t->noAbsorb = true;

    t->pending.insert(t->pending.end(), t->env->carryOver.begin(), t->env->carryOver.end());
    t->env->carryOver.clear();

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

    // Record, under this transaction's id, everything it retired plus whatever
    // it absorbed and did not spend, in chunks small enough to sit inline in a
    // leaf. Both sets move while we write them -- writing the entries
    // allocates and frees pages of its own -- so rewrite until a pass changes
    // neither, at which point the entries are exact.
    std::vector<PageNo>& freed = t->gcAdd;
    std::vector<PageNo>& acc = t->gcAcc;
    freed.clear();
    const std::size_t chunkPages = freeChunkPages(t->pageSize());
    std::size_t written = 0;  ///< chunks currently recorded under our id
    constexpr int kMaxRounds = 8;
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

        const std::size_t chunks = (acc.size() + chunkPages - 1) / chunkPages;
        for (std::size_t i = 0; i < chunks; ++i) {
            const std::size_t first = i * chunkPages;
            const std::size_t n = std::min(chunkPages, acc.size() - first);
            WritableSlice w;
            treePut(c, freeKey(t->txnid, i).slice(), Slice(nullptr, n * sizeof(PageNo)),
                    PutMode::Upsert, &w);
            for (std::size_t index = 0; index < n; ++index)
                writeLittle(w.data() + index * sizeof(PageNo), acc[first + index]);
        }
        // A previous round may have needed more chunks than this one.
        for (std::size_t i = chunks; i < written; ++i)
            treeDel(c, freeKey(t->txnid, i).slice());
        written = chunks;
        if (acc.empty())
            break;

        const bool settled = t->pending.empty() && t->loose.empty() && t->reclaimPos == spent &&
                             t->reclaim.size() == left;
        if (settled || round + 1 >= kMaxRounds)
            break;
    }

    // Pages freed too late to make it into the entry above roll into the next
    // writer, which records them then. Until then they are referenced by
    // nothing and listed nowhere, which is safe -- nothing can hand them out.
    auto& carry = t->env->carryOver;
    carry.insert(carry.end(), t->pending.begin(), t->pending.end());
    carry.insert(carry.end(), t->loose.begin(), t->loose.end());
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

/// Write dirty pages into the file, then flush exactly the ranges we touched.
/// This flush is a barrier, not a hint: it blocks until the data is off every
/// write-back cache in the stack, so writeMeta() below cannot race it even if
/// the OS/controller/drive reorder writes freely on either side of the call.
/// See docs/design.md, "Durability and the memory map".
void writePages(TxnImpl* t)
{
    EnvImpl* e = t->env;
    const std::size_t ps = e->pageSize;

    {
        std::lock_guard<std::mutex> lk(e->mtx);
        e->growLocked((t->lastPgno + 1) * std::uint64_t(ps));
        refreshRegion(t, e->region);
    }
    std::byte* base = t->region->m.base;

    // Collect (Page number, buffer) once and sort by page number: the writes
    // below then walk the file forwards, and the flush ranges coalesce.
    std::vector<std::pair<PageNo, Page*>>& ids = t->flushList;
    ids.clear();
    ids.reserve(t->dirty.size());
    t->dirty.forEach([&](PageNo pn, Page* buf) { ids.emplace_back(pn, buf); });
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
    for (const auto& [p, buf] : ids) {
        const bool big = isOverflow(buf);
        const unsigned n = big ? ovPages(buf) : 1u;
        if (!big) {
            // Only a slotted page has a gap; an overflow page reuses those header
            // words for its run length, and its tail was zeroed when it was made.
            // Buffers are recycled, and node deletion compacts by moving bytes
            // down and leaving the originals behind, so without this one page's
            // leftovers would travel inside another page's free space and the same
            // logical content would not produce the same bytes on disk.
            std::memset(reinterpret_cast<std::byte*>(buf) + buf->lower, 0,
                        pageUpper(buf) - buf->lower);
        }
        sealPage(buf, std::size_t(n) * ps);
        // Before the meta that publishes these bytes: a reader that can see
        // them must find them unverified.
        e->validation.invalidate(p, n);
        if (!runs.empty() && runs.back().second + 1 == p) {
            runs.back().second = p + n - 1;
        } else {
            if (!kWriteThroughMapping)
                writeSpans();
            runs.emplace_back(p, p + n - 1);
        }
        if (kWriteThroughMapping) {
            os::observeIo(os::IoEvent::Write, std::size_t(n) * ps);
            std::memcpy(base + p * ps, buf, std::size_t(n) * ps);
            os::observeIo(os::IoEvent::WriteProgress, std::size_t(n) * ps);
        } else {
            spans.push_back(os::Span{buf, std::size_t(n) * ps});
        }
    }
    if (!kWriteThroughMapping)
        writeSpans();

    if (e->dura != Durability::None)
        os::flushData(e->fd, t->region->m, runs, ps);
}

/// Runs only after writePages()'s flush has returned, so the new meta page
/// can never reach stable storage before the pages it points to. Torn writes
/// of this page specifically are handled by the two-slot checksum scheme in
/// loadMeta() (Env.cpp), not by write ordering.
void writeMeta(TxnImpl* t)
{
    EnvImpl* e = t->env;
    Meta m = t->meta;
    std::memcpy(m.magic, kMagic, sizeof m.magic);
    m.version = kFormatVersion;
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
    if (e->carryOver.size() > m.deferredPages.size())
        throw Error(ErrorCode::MapFull, "deferred free-page budget exceeded; reduce transaction size");
    m.deferredCount = e->carryOver.size();
    m.deferredPages = {};
    std::copy(e->carryOver.begin(), e->carryOver.end(), m.deferredPages.begin());
    m.checksum = metaChecksum(m);

    const unsigned slot = metaSlotFor(t->txnid);
    Page* mp = e->mapPage(*t->region, slot);
    mp->pgno = slot;
    mp->flags = P_META;
    mp->nkeys = 0;
    *metaOf(mp) = m;

    if (e->dura == Durability::Safe)
        os::flushRange(e->fd, t->region->m, std::size_t(slot * e->pageSize), e->pageSize);

    {
        std::lock_guard<std::mutex> lk(e->mtx);
        e->meta = m;
        e->commitGen.store(m.txnid, std::memory_order_release);
    }
    // After the flush, so a woken waiter's snapshot always contains the commit.
    e->commitCv.notify_all();
}

void commitNested(TxnImpl* t)
{
    TxnImpl* p = t->parent;

    t->dirty.forEach([&](PageNo pgno, Page* buf) { p->dirty.set(pgno, buf); });
    p->owned.insert(p->owned.end(), t->owned.begin(), t->owned.end());
    t->owned.clear();
    p->pool.insert(p->pool.end(), t->pool.begin(), t->pool.end());

    // Ancestor pages the child copied away from are now unreachable everywhere.
    for (PageNo pgno : t->retiredLocal) {
        for (TxnImpl* a = p; a; a = a->parent) {
            if (Page* buf = a->dirty.find(pgno)) {
                a->pool.push_back(buf);
                a->dirty.erase(pgno);
                break;
            }
        }
        p->loose.push_back(pgno);
    }
    p->loose.insert(p->loose.end(), t->loose.begin(), t->loose.end());
    p->pending.insert(p->pending.end(), t->pending.begin(), t->pending.end());

    p->lastPgno = t->lastPgno;
    p->namedDbs = t->namedDbs;
    // Fold the child's view of every sub-database back into the parent, but
    // keep the parent's own "needs writing back" bits: it may have dirtied a
    // tree before the child ever opened it.
    p->dbs.resize(t->dbs.size());
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
    p->loose.insert(p->loose.end(), t->loose.begin(), t->loose.end());
    // Never rewind the frontier: pages handed back above may sit beyond it.
    p->lastPgno = std::max(p->lastPgno, t->lastPgno);

    ++p->root()->seq;
}

}  // namespace


void txnCommit(TxnImpl* t)
{
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
    // Running out of room (MapFull) before the first page is written is the
    // one failure that is a plain refusal rather than a fault: the file has
    // not been touched, so the store stays usable and only this transaction is
    // lost. Everything else poisons the handle (see Env::writeTxn).
    bool untouched = true;
    try {
        flushNamedTrees(t);
        updateFreelist(t);
        {
            std::lock_guard<std::mutex> lk(e->mtx);
            e->growLocked((t->lastPgno + 1) * std::uint64_t(e->pageSize));
        }
        untouched = false;
        writePages(t);
        writeMeta(t);
        // Reads the bytes back out of the mapping writePages() just wrote
        // through, so a segment carries what the store holds rather than what
        // the dirty buffers held. Cannot fail the commit: see ShipWriter.
        if (e->ship)
            e->ship->capture(e->meta, t->flushRuns, t->region->m.base);
    } catch (const Error& err) {
        if (!(untouched && err.code() == ErrorCode::MapFull))
            e->failed.store(true);
        finishWriter(t);
        throw;
    } catch (...) {
        e->failed.store(true);
        finishWriter(t);
        throw;
    }
    finishWriter(t);
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

}  // namespace nosql::internal
