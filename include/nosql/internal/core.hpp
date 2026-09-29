// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "nosql/internal/arena.hpp"
#include "nosql/internal/buffer_pool.hpp"
#include "nosql/internal/format.hpp"
#include "nosql/internal/os.hpp"
#include "nosql/internal/page_table.hpp"
#include "nosql/internal/validation_cache.hpp"
#include "nosql/nosql.hpp"

namespace nosql::internal {

using CompareFn = int (*)(Slice, Slice) noexcept;

int cmpLexicographic(Slice, Slice) noexcept;
int cmpReverse(Slice, Slice) noexcept;
int cmpInteger(Slice, Slice) noexcept;
CompareFn comparatorFor(std::uint32_t DbFlags) noexcept;

#ifdef NOSQL_NO_REPLICATION
/// Stand-in when change shipping is compiled out: never instantiated (shipTo
/// is refused), present so the commit path needs no conditionals.
class ShipWriter
{
public:
    std::size_t failures() const { return 0; }
    std::uint64_t capturedTxnid() const { return 0; }
    template <class... Args>
    void capture(Args&&...)
    {}
};
#else
class ShipWriter;
#endif

/// Reserved sub-database handles.
inline constexpr unsigned kFreeDbi = 0;  ///< txnid -> array of reclaimable pgno
inline constexpr unsigned kMainDbi = 1;  ///< user keys
inline constexpr unsigned kCatalogDbi = 2;

/// A whole-file mapping, kept alive by every transaction that observed it.
/// Growing the store publishes a new one; stale ones die with their readers.
struct MappedRegion
{
    os::Mapping m;
    ~MappedRegion() { os::unmap(m); }
};

struct TxnImpl;

/// A transaction's private view of one sub-database.
struct DbSlot
{
    Tree tree = emptyTree();
    std::string name;
    std::uint8_t dirty = 0;   ///< the root record needs writing back at commit
    std::uint8_t loaded = 0;  ///< materialised in this transaction
};

struct EnvImpl
{
    os::FileHandle fd = os::kInvalidFile;
    std::filesystem::path path;
    std::size_t pageSize = kDefaultPageSize;
    std::uint64_t sizeUpper = 0;
    std::uint64_t growthStep = 0;
    unsigned maxDbs = 128;
    bool readOnly = false;
    bool preallocate = true;  ///< reserve disk blocks when the file grows
    Durability dura = Durability::Safe;
    std::atomic<bool> failed{false};
    std::uint64_t dirtyLimit = 256ull << 20;
    std::atomic<std::uint64_t> dirtyBytes{0};

    std::shared_ptr<MappedRegion> region;  ///< current mapping
    /// Mappings replaced by a growth. Page pointers handed out before the
    /// growth still point into these, so they are held until every transaction
    /// that could have seen one has finished.
    std::vector<std::shared_ptr<MappedRegion>> staleRegions;
    Meta meta{};  ///< newest committed snapshot

    /// Guards `meta`, `region` and the reader table.
    mutable std::mutex mtx;
    /// Serializes write transactions (one writer at a time, in-process).
    std::mutex writeMtx;

    /// Id of the newest published commit, mirrored out of `meta` so a waiter
    /// can sample it without taking `mtx`. Waiters block on `mtx`, never on
    /// `writeMtx`: waiting on the writer slot would deadlock against the very
    /// commit being waited for.
    std::atomic<TxnId> commitGen{0};
    mutable std::condition_variable commitCv;

    /// scratch for page splits and rebalances. It lives on the env rather than
    /// on the transaction because there is only ever one writer, and because a
    /// per-transaction arena would hand the allocator a fresh multi-page chunk
    /// on every commit -- exactly what it exists to avoid. See internal/Arena.hpp.
    Arena writeScratch;

    /// Page buffers recycled across transactions. Carries its own lock: it is
    /// touched from transaction teardown, which can run after writeMtx has
    /// already been handed to the next writer.
    BufferPool buffers;

    /// Committed pages whose checksum a read transaction has already verified,
    /// when Options::cacheReadChecksums() asked for it. Empty (disabled)
    /// otherwise. See internal/validation_cache.hpp.
    ValidationCache validation;

    /// Live read snapshots as a sorted (txnid, count) histogram; the front
    /// entry is the one pinning reclamation. A sorted vector rather than a map
    /// because there are only ever a handful of distinct snapshots, and this
    /// way the table stops allocating after the first few transactions.
    std::vector<std::pair<TxnId, unsigned>> readers;
    void readerAcquire(TxnId id);           // caller holds mtx
    void readerRelease(TxnId id) noexcept;  // caller holds mtx
    unsigned readerCount() const noexcept
    {  // caller holds mtx
        unsigned n = 0;
        for (const auto& r : readers)
            n += r.second;
        return n;
    }

    /// Transaction objects held for reuse. Beginning a transaction otherwise
    /// means a dozen small allocations for its containers; recycling the whole
    /// object keeps those capacities and makes a steady-state transaction
    /// allocation-free. An entry is reusable exactly when the pool holds the
    /// only reference to it. Declared after `buffers` so it is destroyed first.
    std::vector<std::shared_ptr<TxnImpl>> txnPool;
    static constexpr std::size_t kTxnPoolCap = 32;
    std::shared_ptr<TxnImpl> acquireTxn();        // caller must not hold mtx
    std::shared_ptr<TxnImpl> acquireTxnLocked();  // caller holds mtx

    /// Pages freed by an earlier commit that did not fit in that commit's
    /// free-list update; folded into the next write transaction.
    std::vector<PageNo> carryOver;

    /// Replication capture, present only when Options::shipTo() was set.
    /// Touched by the commit path alone, which serialises on writeMtx.
    std::unique_ptr<ShipWriter> ship;

    ~EnvImpl();

    Page* mapPage(const MappedRegion& r, PageNo p) const noexcept
    {
        return reinterpret_cast<Page*>(r.m.base + p * pageSize);
    }
    TxnId oldestSnapshot() const;  // caller holds mtx
    void growLocked(std::uint64_t neededBytes);
    void releaseStaleRegions();  // caller holds mtx
};

/// The environment behind a public handle, or ErrorCode::InvalidArgument once
/// it has been closed.
inline EnvImpl& openEnv(const std::shared_ptr<EnvImpl>& impl)
{
    if (!impl)
        throw Error(ErrorCode::InvalidArgument, "environment is closed");
    return *impl;
}

/// One transaction. Read transactions own nothing but a snapshot; write
/// transactions own a dirty-page set that is folded into the file at commit.
struct TxnImpl
{
    /// Raw on purpose: every public handle that can reach a transaction also
    /// holds a reference to the environment, so this can never dangle, and
    /// keeping it raw avoids a reference cycle with the transaction pool.
    EnvImpl* env = nullptr;
    TxnImpl* parent = nullptr;
    TxnImpl* child = nullptr;
    bool readOnly = false;
    bool finished = false;

    TxnId txnid = 0;
    std::shared_ptr<MappedRegion> region;
    Meta meta{};  ///< snapshot for readers; the target meta for writers
    PageNo lastPgno = 0;
    std::uint64_t namedDbs = 0;
    static constexpr std::size_t kRetainedScratchBytes = 2u << 20;

    /// Pages this transaction copied or created, by page number. Buffers are
    /// owned; freed ones go back to `pool` rather than to the allocator, so a
    /// stale cursor pointer can never dangle into released memory.
    PageTable dirty;
    std::vector<Page*> pool;  ///< recycled single-page buffers
    /// Every buffer this transaction allocated, with its Page count (needed to
    /// free/return it correctly since a page's own header is not trusted once
    /// it is idle). Buffers return to the environment's bounded recycler when
    /// the transaction ends, including cacheable overflow runs.
    std::vector<std::pair<Page*, unsigned>> owned;

    /// Commit-path scratch, kept on the transaction so a commit allocates
    /// nothing it has not already allocated once.
    std::vector<std::pair<PageNo, Page*>> flushList;
    std::vector<std::pair<PageNo, PageNo>> flushRuns;
    std::vector<os::Span> flushSpans;

    std::vector<PageNo> loose;         ///< ours, freed, immediately reusable
    std::vector<PageNo> retiredLocal;  ///< an ancestor's dirty page we replaced
    std::vector<PageNo> pending;       ///< committed-snapshot pages we retired

    /// Per-sub-database state, indexed by handle. One vector instead of three
    /// keeps a transaction's set-up to a single allocation and the three fields
    /// on the same cache line.
    std::vector<DbSlot> dbs;

    /// A free-list chunk: the transaction that retired its pages, and its
    /// number within that transaction's entries.
    using GcKey = std::pair<TxnId, std::uint64_t>;

    /// Free-list update scratch, kept here so a commit reuses its capacity.
    std::vector<GcKey> gcKeys;
    std::vector<PageNo> gcAdd, gcAcc;

    // --- reclaim state, root write transaction only ---
    std::vector<PageNo> reclaim;  ///< sorted free pages pulled from the GC tree
    std::size_t reclaimPos = 0;   ///< how far into `reclaim` we have handed out
    std::vector<GcKey> consumed;  ///< GC chunks absorbed into `reclaim`
    TxnId reclaimNext = 0;        ///< scan cursor into the GC tree ...
    std::uint64_t reclaimNextChunk = 0;  ///< ... and the chunk to resume at
    TxnId oldest = 0;             ///< newest GC key we may touch
    bool reclaimDrained = false;
    /// Set while the commit rewrites the GC tree. It stops the allocator
    /// absorbing *new* GC entries (which would need deleting after the delete
    /// pass has already run), but pages already absorbed stay spendable -- if
    /// they did not, every commit would have to extend the file for its own
    /// bookkeeping and the free list would grow for ever.
    bool noAbsorb = false;
    /// Set for the final free-list write, which must describe a set that can no
    /// longer change under it.
    bool noSpend = false;

    /// Bumped by every structural mutation; cursors use it to detect that they
    /// must re-anchor before touching a page pointer again.
    std::uint64_t seq = 1;

    ~TxnImpl();

    /// Give back everything heavy once the transaction is over. Handles that
    /// outlive it keep only a husk, enough to report ErrorCode::BadTransaction.
    /// Container capacities survive, ready for the next user of this object.
    void releaseResources();
    /// Prepare a recycled object for a new transaction.
    void reset();
    /// Point the transaction at a committed snapshot: its meta, mapping and
    /// the three reserved trees (free list, main, catalog).
    void loadSnapshot(const Meta& snapshot, std::shared_ptr<MappedRegion> mapping);

    Arena& scratch() const noexcept { return env->writeScratch; }

    TxnImpl* root() noexcept
    {
        TxnImpl* t = this;
        while (t->parent)
            t = t->parent;
        return t;
    }
    std::size_t pageSize() const noexcept { return env->pageSize; }

    Page* getPage(PageNo p, bool forceValidation = false) const
    {
        if (Page* d = dirty.find(p))
            return d;
        for (const TxnImpl* a = parent; a; a = a->parent)
            if (Page* d = a->dirty.find(p))
                return d;
        // One comparison guards every descent against a garbage child pointer
        // turning into a wild read; it costs far less than the lookup above.
        if (p > lastPgno)
            throw Error(ErrorCode::Corrupted, "page number out of range");
        Page* page = env->mapPage(*region, p);
        // Readers may trust a check this process already made since the page
        // was last written; writers always re-verify what they are about to
        // copy forward, so damage can never be sealed under a fresh checksum.
        if (!forceValidation && readOnly && env->validation.enabled()) {
            if (env->validation.isValidated(p))
                return page;
            validatePage(page, pageSize(), lastPgno - p + 1);
            env->validation.markValidated(p);
            return page;
        }
        validatePage(page, pageSize(), lastPgno - p + 1);
        return page;
    }
    bool isOwnDirty(PageNo p) const noexcept { return dirty.find(p) != nullptr; }

    /// Allocate (or recycle) a dirty buffer of `npages` contiguous pages.
    Page* allocBuffer(PageNo p, std::uint16_t flags, unsigned npages);
    PageNo allocPages(unsigned count);
    void freePage(PageNo p, unsigned count = 1);
    void markDirty(unsigned dbi) noexcept { dbs[dbi].dirty = 1; }
    Tree& treeAt(unsigned dbi) noexcept { return dbs[dbi].tree; }
    /// Make room for a handle that was registered mid-transaction.
    void ensureDbs(unsigned dbi)
    {
        if (dbs.size() <= dbi)
            dbs.resize(dbi + 1);
    }
};

// ------------------------------------------------------------- btree API ---

/// A descent path. `pg[0]` is the root, `pg[top]` the leaf.
struct CursorImpl
{
    TxnImpl* txn = nullptr;
    unsigned dbi = 0;
    int top = -1;
    bool positioned = false;
    /// Deliberately uninitialised: zeroing 480 bytes on every point lookup is
    /// pure overhead, and `top` bounds every read of these.
    Page* pg[kMaxDepth];
    std::uint16_t idx[kMaxDepth];

    /// Snapshot of the anchor key, so the cursor can re-find its place after a
    /// split, merge or page relocation performed through the same transaction.
    std::uint64_t seqSeen = 0;
    std::vector<std::byte> anchor;
    /// Second buffer the anchor is swapped into while re-searching, so
    /// re-anchoring never has to give a buffer back to the allocator.
    std::vector<std::byte> anchorSpare;
    /// Set for cursors handed to callers, so the transaction -- and the store
    /// under it -- outlive them.
    std::shared_ptr<TxnImpl> keepalive;
    std::shared_ptr<EnvImpl> envKeep;
    /// Set only by the sub-database bookkeeping, which is allowed to read and
    /// write the N_SUBDB records that user code must not touch.
    bool allowSubdb = false;
    /// Transient cursors (single lookups) skip anchor bookkeeping entirely.
    bool track = false;

    Tree& tree() const
    {
        if (dbi >= txn->dbs.size() || !txn->dbs[dbi].loaded)
            throw Error(ErrorCode::BadTransaction, "database handle was dropped");
        return txn->dbs[dbi].tree;
    }
    CompareFn cmp() const { return comparatorFor(tree().flags); }
    void unposition()
    {
        positioned = false;
        top = -1;
        anchor.clear();
    }
};

/// Descend to the leaf that would hold `key`. Sets `*exact` when the key is
/// present. Returns false only for an empty tree.
bool treeSearch(CursorImpl& c, Slice key, bool* exact);
/// Position on the first key >= `key` / on `key` exactly.
bool cursorSeek(CursorImpl& c, Slice key);
bool cursorSeekExact(CursorImpl& c, Slice key);
bool cursorFirst(CursorImpl& c);
bool cursorLast(CursorImpl& c);
bool cursorNext(CursorImpl& c);
bool cursorPrev(CursorImpl& c);
/// Re-anchor after a mutation if the transaction moved on without us.
void cursorRevalidate(CursorImpl& c);

Slice cursorKey(CursorImpl& c);
Slice cursorValue(CursorImpl& c);
Slice leafValue(TxnImpl* t, const Page* p, unsigned i);

/// Insert or overwrite. `out` (optional) receives the writable value area,
/// which is what powers Db::reserve. Returns false for the benign
/// insertUnique/updateOnly misses.
bool treePut(CursorImpl& c, Slice key, Slice value, PutMode mode, WritableSlice* out);
bool treeDel(CursorImpl& c, Slice key);
/// Erase the entry the cursor sits on; the cursor ends up unpositioned.
void treeDelCurrent(CursorImpl& c);
/// Release every page of a tree; `t` is reset to an empty tree.
void treeDrop(TxnImpl* txn, unsigned dbi);

/// Named database descriptors live in the catalog.
bool treeGetSubdb(TxnImpl* t, Slice name, Tree* out);
void treePutSubdb(TxnImpl* t, Slice name, const Tree& tr);
bool treeDelSubdb(TxnImpl* t, Slice name);
std::vector<std::string> treeListSubdbs(TxnImpl* t);

}  // namespace nosql::internal
