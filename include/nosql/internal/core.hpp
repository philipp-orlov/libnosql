// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <string>
#include <vector>

#include "nosql/internal/arena.hpp"
#include "nosql/internal/buffer_pool.hpp"
#include "nosql/internal/format.hpp"
#include "nosql/internal/key_prefix.hpp"
#include "nosql/internal/occ_log.hpp"
#include "nosql/internal/os.hpp"
#include "nosql/internal/page_table.hpp"
#include "nosql/internal/validation_cache.hpp"
#include "nosql/internal/writer_slot.hpp"
#include "nosql/nosql.hpp"

namespace nosql::internal {

using CompareFn = int (*)(Slice, Slice) noexcept;

int cmpLexicographic(Slice, Slice) noexcept;
int cmpReverse(Slice, Slice) noexcept;
int cmpInteger(Slice, Slice) noexcept;
CompareFn comparatorFor(std::uint32_t DbFlags) noexcept;

/// The shortest key that can separate two adjacent children in byte order: any
/// s with left < s <= right does, because keys routed left sort below s and keys
/// routed right sort at or above it. A prefix of `right` is the shortest such
/// key, and it is never empty. Only valid for cmpLexicographic trees.
inline Slice shortestSeparator(Slice left, Slice right) noexcept
{
    const std::size_t n = std::min(left.size(), right.size());
    std::size_t d = 0;
    while (d < n && left.data()[d] == right.data()[d])
        ++d;
    return Slice(right.data(), std::min(right.size(), d + 1));
}

/// shortestSeparator for cmpReverse trees, which compare from the last byte
/// back: there a suffix of `right` is the shortest separator. Never empty.
inline Slice shortestSeparatorReverse(Slice left, Slice right) noexcept
{
    const std::size_t n = std::min(left.size(), right.size());
    std::size_t d = 0;
    while (d < n && left.data()[left.size() - 1 - d] == right.data()[right.size() - 1 - d])
        ++d;
    const std::size_t len = std::min(right.size(), d + 1);
    return Slice(right.data() + right.size() - len, len);
}

/// The separator to store between two adjacent leaves, `left`'s last key and
/// `right`'s first: shortened under byte and reverse order, whole under
/// integer order, where a shorter key has a different width.
inline Slice separatorFor(CompareFn cmp, Slice left, Slice right) noexcept
{
    if (cmp == &cmpLexicographic)
        return shortestSeparator(left, right);
    if (cmp == &cmpReverse)
        return shortestSeparatorReverse(left, right);
    return right;
}

/// An optimistic commit waiting in EnvImpl::occQueue; defined in optimistic.cpp.
struct OccPending;

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
/// Where the address range is reserved (POSIX) there is one for the life of
/// the environment and growth extends it in place. Elsewhere growing the
/// store publishes a new one; stale ones die with their transactions.
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
    /// Write a transaction's pages out early rather than fail at dirtyLimit
    /// (TxnImpl::spill). Off where a page is smaller than a system page: its
    /// buffer's memory could not be handed back on its own.
    bool spill = true;
    std::atomic<std::uint64_t> dirtyBytes{0};
    std::atomic<std::uint64_t> spilledPages{0};    ///< EnvStats::spilledPages
    std::atomic<std::uint64_t> unspilledPages{0};  ///< EnvStats::unspilledPages
    // The writer's alone, across its nested transactions:
    std::uint32_t touchClock = 0;  ///< TxnImpl::touch()
    /// The last spill wrote every leaf and overflow page it could and still
    /// left more than it aimed for; no other is tried until a write makes
    /// such a page dirty again, since it could only find what that made.
    /// Nothing else clears it: a new transaction has nothing to spill until
    /// it makes such a page, and that clears it.
    bool spillExhausted = false;
    /// A commit is writing its catalog and free-list entries. Those are no
    /// batch the caller can reduce, and spilling cannot run under them, so
    /// with spilling on they may take the budget past dirtyLimit: a
    /// transaction whose writes fit is not lost at commit.
    bool commitOverBudget = false;

    std::shared_ptr<MappedRegion> region;  ///< current mapping
    /// Mappings replaced by a growth (only where the mapping cannot grow in
    /// place). Page pointers handed out before the growth still point into
    /// these, so they are held until every transaction that could have seen
    /// one has finished.
    std::vector<std::shared_ptr<MappedRegion>> staleRegions;
    /// Newest snapshot readers may take: the newest commit, or under
    /// pipelined commits the newest durable one.
    Meta meta{};
    /// Newest commit, which the next writer builds on. Ahead of `meta` only
    /// while a pipelined commit's meta waits for its flush. Guarded by `mtx`.
    Meta writerMeta{};

    /// Newest meta known to be on stable storage (guarded by `mtx`) and the
    /// slot holding it (guarded by `syncMtx`). The next meta page written goes
    /// into the other slot, so a torn write always leaves this one, and
    /// reclamation never recycles a page this snapshot still names.
    TxnId durableTxn = 0;
    unsigned durableSlot = 0;
    /// NoMetaSync and pipelined Safe: a meta written into the mapping but not
    /// flushed. The next data flush carries it to the device; a pipelined one
    /// is published to readers then (`unflushedMeta`). Guarded by `syncMtx`.
    TxnId unflushedTxn = 0;
    unsigned unflushedSlot = 0;
    Meta unflushedMeta{};
    /// Safe with Options::pipelineCommits(): see writeMeta() in txn.cpp.
    bool pipelined = false;
    /// Writers holding or waiting for the writer slot, and how long the last
    /// data flush took: what a pipelined commit judges whether to wait for a
    /// successor to carry its meta by.
    std::atomic<unsigned> writersPending{0};
    std::atomic<std::int64_t> dataFlushNs{0};
    /// Whether the last pipelined meta reached the device with a later commit's
    /// data rather than by its own flush: whether commits are arriving as a
    /// stream, so that the next is worth waiting for even before it shows.
    std::atomic<bool> lastMetaCarried{false};
    /// Serializes every meta-page write and flush: commits that flush and
    /// Env::sync(). Taken before `mtx`, never while holding it, and never
    /// held across anything but the sync itself.
    std::mutex syncMtx;
    /// Durability::Lazy: minimum time between syncs, and when the last one
    /// finished (steady clock, nanoseconds).
    std::int64_t syncIntervalNs = 0;
    std::atomic<std::int64_t> lastSyncNs{0};
    /// Options::backgroundSync(): the thread that syncs an idle Lazy store once
    /// an interval, and what stops it. It holds a raw pointer, never an owning
    /// one, so the store's destructor never runs on it; the destructor stops
    /// and joins it before anything else.
    std::thread syncThread;
    std::mutex syncWakeMtx;
    std::condition_variable syncWake;
    bool syncStop = false;  // guarded by syncWakeMtx
    /// When a reader last checked the file against the mapping (steady
    /// clock, nanoseconds; see checkFileIntact in api.cpp).
    std::atomic<std::int64_t> lastIntactCheckNs{0};
    /// Lazy and None, writer only: a bit per page handed out while
    /// `bornEpoch` was the newest durable commit. That snapshot never names
    /// such a page, so once retired it waits for readers, not for a sync.
    std::vector<std::uint64_t> born;
    TxnId bornEpoch = 0;
    void noteBorn(PageNo first, unsigned count)
    {
        const std::size_t need = std::size_t((first + count - 1) / 64 + 1);
        if (born.size() < need)
            born.resize(std::max(need, born.size() * 2));
        for (PageNo p = first; p < first + count; ++p)
            born[std::size_t(p / 64)] |= std::uint64_t(1) << (p % 64);
    }
    bool isBorn(PageNo p) const noexcept
    {
        const std::size_t w = std::size_t(p / 64);
        return w < born.size() && ((born[w] >> (p % 64)) & 1u);
    }
    /// The one-run page list a whole-store data flush passes to os::flushData.
    std::vector<std::pair<std::uint64_t, std::uint64_t>> syncRuns;

    /// Named sub-database roots as of one committed snapshot. A snapshot's
    /// directory never changes, so read transactions on the same snapshot
    /// share their lookups instead of each searching the directory tree --
    /// one search per sub-database per snapshot, not per transaction. Valid
    /// only for readers, which pin the snapshot and so its pages.
    struct SubdbCache
    {
        std::mutex mtx;
        TxnId txnid = 0;
        /// The first `used` entries are this snapshot's; the rest keep their
        /// name buffers for the next snapshot to reuse.
        std::vector<std::pair<std::string, Tree>> trees;
        std::size_t used = 0;
        static constexpr std::size_t kMax = 256;
    } subdbs;

    /// Guards `meta`, `durableTxn`, `region`, `carryOver` and the reader table.
    mutable std::mutex mtx;
    /// Serializes write transactions (one writer at a time, in-process), and
    /// keeps any from waiting without bound. Timed, so a writer can give up
    /// after Env::writeTxn(timeout).
    WriterSlot writeMtx;

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

    /// Keys written by recent commits, for validating optimistic transactions;
    /// see occ_log.hpp. Touched only under writeMtx.
    OccLog occLog;
    /// Optimistic transactions alive now. While any are, write transactions
    /// record the keys they write, so a commit conflicts only on those.
    std::atomic<unsigned> occActive{0};
    /// Group commit: optimistic commits queue here, and whichever committer
    /// finds no leader running becomes one and applies a batch of them in a
    /// single write transaction. Guarded by occMtx.
    std::mutex occMtx;
    std::vector<OccPending*> occQueue;
    std::atomic<unsigned> occWaiting{0};  ///< occQueue.size(), readable without occMtx
    std::atomic<unsigned> occInCommit{0};  ///< commits queued or being applied, for stats()
    /// How many times a waiting optimistic committer yields before it sleeps
    /// (Options::commitSpins). Set at open.
    unsigned occSpins = 256;
    bool occLeader = false;
    /// The transaction holding back every other optimistic commit, after losing
    /// too many conflicts (see OptimisticTxn); null when there is none. Commits
    /// and would-be exclusive transactions wait on occTurn. Guarded by occMtx.
    const void* occExclusive = nullptr;
    std::condition_variable occTurn;
    /// The leader's scratch, kept for its capacity. Only the leader touches it.
    std::vector<OccPending*> occBatch;
    std::vector<std::uint64_t> occBatchWritten;
    std::vector<std::uint64_t> occMergeScratch;

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
    /// Snapshots that optimistic commits still validate against after giving
    /// back their reader slot: they hold back trimming of occLog, not page
    /// reclamation. The same histogram as `readers`.
    std::vector<std::pair<TxnId, unsigned>> occPins;
    void occPinAcquire(TxnId id);           // caller holds mtx
    void occPinRelease(TxnId id) noexcept;  // caller holds mtx
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
    /// Each pooled object may keep up to TxnImpl::kRetainedScratchBytes of
    /// container capacity, so the cap bounds that retention at 16 MiB.
    std::vector<std::shared_ptr<TxnImpl>> txnPool;
    static constexpr std::size_t kTxnPoolCap = 8;
    std::shared_ptr<TxnImpl> acquireTxn();        // caller must not hold mtx
    std::shared_ptr<TxnImpl> acquireTxnLocked();  // caller holds mtx

    /// Pages freed by an earlier commit that did not fit in that commit's
    /// free-list update; folded into the next write transaction.
    std::vector<PageNo> carryOver;
    /// Pages the newest snapshot's free tree lists: counted once at open,
    /// then kept up to date by each commit, so EnvStats::freePages needs no
    /// walk. Guarded by `mtx`.
    std::uint64_t freeTreePages = 0;

    /// Replication capture, present only when Options::shipTo() was set.
    /// Touched by the commit path alone, which serialises on writeMtx.
    std::unique_ptr<ShipWriter> ship;

    ~EnvImpl();

    /// Lazy and None keep the newest meta in memory until a sync.
    bool deferredMeta() const noexcept
    {
        return dura == Durability::Lazy || dura == Durability::None;
    }

    Page* mapPage(const MappedRegion& r, PageNo p) const noexcept
    {
        return reinterpret_cast<Page*>(r.m.base + p * pageSize);
    }
    TxnId oldestSnapshot() const;  // caller holds mtx
    void growLocked(std::uint64_t neededBytes);
    void shrinkLocked(std::uint64_t bytes);  // caller holds mtx and the write slot
    void releaseStaleRegions();  // caller holds mtx
};

/// Make the newest commit durable (see Env::sync). Caller holds `syncMtx`.
void syncEnv(EnvImpl& e, bool force);

/// Commits a top-level or nested transaction. A pipelined top-level commit
/// returns once its meta is written, not flushed; it then waits for that here,
/// or, with `awaitLater`, leaves the id to wait on there (zero for none).
void txnCommit(TxnImpl* t, TxnId* awaitLater = nullptr);
/// Returns once commit `id` is durable: carried to the device by a later
/// commit's data flush when one follows soon, or flushed here.
void awaitDurable(EnvImpl& e, TxnId id);

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
    /// Root write transaction only: mappings this transaction tree moved off
    /// after a growth that had to remap. Slices and page pointers taken
    /// before the growth still address them, so they live until it ends.
    std::vector<std::shared_ptr<MappedRegion>> seen;
    Meta meta{};  ///< snapshot for readers; the target meta for writers
    PageNo lastPgno = 0;
    std::uint64_t namedDbs = 0;
    static constexpr std::size_t kRetainedScratchBytes = 2u << 20;

    /// Pages this transaction copied or created, by page number. Buffers are
    /// owned; freed ones go back to `pool` rather than to the allocator, so a
    /// stale cursor pointer can never dangle into released memory.
    PageTable dirty;
    std::vector<Page*> pool;  ///< recycled single-page buffers
    /// Recycled overflow-run buffers, with their page counts, for a run of
    /// the same size class. Counted in dirtyBytes like `pool`, until a spill
    /// hands both back (spill()).
    std::vector<std::pair<Page*, unsigned>> runPool;
    /// A buffer of this transaction's that no page uses any more, kept for
    /// the next allocation of its size.
    void recycle(Page* buf, unsigned npages)
    {
        if (npages == 1)
            pool.push_back(buf);
        else
            runPool.emplace_back(buf, npages);
    }
    /// Every buffer this transaction allocated, with its Page count (needed to
    /// free/return it correctly since a page's own header is not trusted once
    /// it is idle). Buffers return to the environment's bounded recycler when
    /// the transaction ends, including cacheable overflow runs.
    std::vector<std::pair<Page*, unsigned>> owned;
    /// Pages written to the file before commit to stay within dirtyLimit, by
    /// page number, with the buffer they left. Its memory went back to the OS
    /// but its address stays in `owned`, so a stale pointer still lands on
    /// mapped memory; touching the page again brings it back (unspill).
    PageTable spilled;
    std::vector<Page*> spillPool;    ///< spilled single-page buffers whose page was freed
    std::uint64_t spilledBytes = 0;  ///< of `owned`, not counted in dirtyBytes
    struct SpillCandidate
    {
        std::uint32_t age;  ///< touches since this buffer's last
        PageNo pgno;
        Page* buf;
        TxnImpl* owner;  ///< this transaction or an ancestor
    };
    std::vector<SpillCandidate> spillList;  ///< spill() scratch

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
    std::vector<std::string> spareNames;  ///< see resizeDbs

    /// A free-list chunk: the transaction that retired its pages, and its
    /// number within that transaction's entries.
    using GcKey = std::pair<TxnId, std::uint64_t>;

    /// Free-list update scratch, kept here so a commit reuses its capacity.
    std::vector<GcKey> gcKeys;
    std::vector<PageNo> gcRemainder;  ///< updateFreelist scratch, kept between commits
    std::vector<PageNo> gcAdd, gcAcc;
    std::vector<PageNo> gcPinned, gcExempt;  ///< `gcAdd` split by EnvImpl::isBorn
    /// The environment's next `carryOver`, built by the commit and swapped in
    /// only once it is published: a commit refused part-way leaves the
    /// environment's list as it was.
    std::vector<PageNo> carryNext;
    /// Root write transaction: pages its free tree lists minus the snapshot's.
    std::int64_t freeTreeDelta = 0;

    // --- reclaim state, root write transaction only ---
    std::vector<PageNo> reclaim;  ///< sorted free pages pulled from the GC tree
    std::size_t reclaimPos = 0;   ///< how far into `reclaim` we have handed out
    std::vector<GcKey> consumed;  ///< GC chunks absorbed into `reclaim`
    TxnId reclaimNext = 0;        ///< scan cursor into the GC tree ...
    std::uint64_t reclaimNextChunk = 0;  ///< ... and the chunk to resume at
    TxnId oldest = 0;             ///< newest GC key we may touch
    /// Lazy and None: newest GC key readers alone allow, and the durable
    /// commit that exempt chunks must name to be reusable up to it.
    TxnId readersOldest = 0;
    TxnId exemptEpoch = 0;
    bool reclaimDrained = false;
    /// The snapshot's inline free-list entry (Meta::inlineTxn) went into
    /// `reclaim`, so the slot is free for this commit's own entry.
    bool inlineAbsorbed = false;
    /// This commit's entry went inline: these pages under our txnid.
    bool inlineOurs = false;
    std::vector<PageNo> inlinePages;
    /// Set while the commit rewrites the GC tree. It stops the allocator
    /// absorbing *new* GC entries (which would need deleting after the delete
    /// pass has already run), but pages already absorbed stay spendable -- if
    /// they did not, every commit would have to extend the file for its own
    /// bookkeeping and the free list would grow for ever.
    bool noAbsorb = false;
    /// Set for the final free-list write, which must describe a set that can no
    /// longer change under it.
    bool noSpend = false;

    /// Keys this write transaction wrote, for validating optimistic ones
    /// (occ_log.hpp). Kept only while optimistic transactions exist, which
    /// beginWriter samples; without that, or after a clear or a drop, the
    /// commit is recorded as having written keys nobody can name.
    bool occTrack = false;
    bool occUnknown = false;
    std::vector<std::uint64_t> occWritten;
    unsigned occLastDbi = ~0u;  // sub-database whose occDbKey was recorded last
    static constexpr std::size_t kOccTrackLimit = 1u << 20;

    /// It created a PrefixKeys sub-database: the commit raises the store to the
    /// format version that reads P_PREFIX leaves.
    bool prefixFormat = false;
    /// One of Env::shrink()'s: nothing in it is the caller's to reduce, so
    /// its commit may pass the dirty budget with spilling off too.
    bool maintenance = false;

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

    /// What a getPage() caller follows the page number to: a branch or leaf,
    /// or the first page of an overflow run. A page of the other kind is
    /// damage, however good its checksum.
    enum class PageKind
    {
        Tree,
        Overflow,
        Either
    };

    Page* getPage(PageNo p, bool forceValidation = false, PageKind kind = PageKind::Tree) const
    {
        if (Page* d = dirty.find(p))
            return d;
        for (const TxnImpl* a = parent; a; a = a->parent)
            if (Page* d = a->dirty.find(p))
                return d;
        // One comparison guards every descent against a garbage child pointer
        // turning into a wild read; it costs far less than the lookup above.
        if (p > lastPgno || p < 2)
            throw Error(ErrorCode::Corrupted, "page number out of range");
        Page* page = env->mapPage(*region, p);
        // The kind every time, the cached check or not: a page checked as one
        // kind may be reached as the other through a damaged pointer.
        const bool overflow = isOverflow(page);
        if ((kind == PageKind::Tree && overflow) || (kind == PageKind::Overflow && !overflow))
            throw Error(ErrorCode::Corrupted, "page is not of the kind that points to it");
        // Readers may trust a check this process already made since the page
        // was last written; writers always re-verify what they are about to
        // copy forward, so damage can never be sealed under a fresh checksum.
        if (!forceValidation && readOnly && env->validation.enabled()) {
            if (env->validation.isValidated(p))
                return page;
            // Checked once for many reads: every node, not just the header.
            validateFetched(page, p);
            if (!overflow)
                validateNodes(page, pageSize());
            env->validation.markValidated(p);
            return page;
        }
        validateFetched(page, p);
        return page;
    }
    /// The first page of the overflow run holding `vsize` value bytes.
    Page* getOverflow(PageNo p, std::uint64_t vsize, bool forceValidation = false) const
    {
        Page* run = getPage(p, forceValidation, PageKind::Overflow);
        if (vsize > std::uint64_t(ovPages(run)) * pageSize() - kPageHdr - kPageChecksumBytes)
            throw Error(ErrorCode::Corrupted, "large value is longer than its overflow run");
        return run;
    }
    void validateFetched(const Page* page, PageNo p) const
    {
        validatePage(page, pageSize(), lastPgno - p + 1);
        if (!isOverflow(page))
            validateHeader(page, pageSize());
    }
    bool isOwnDirty(PageNo p) const noexcept { return dirty.find(p) != nullptr; }

    /// Allocate (or recycle) a dirty buffer of `npages` contiguous pages.
    Page* allocBuffer(PageNo p, std::uint16_t flags, unsigned npages);
    Page* reuseRun(unsigned npages);  ///< from runPool, or null
    /// Whether `bytes` more of dirty buffers would pass the budget, spill or not.
    bool overBudget(std::uint64_t bytes) const noexcept
    {
        const std::uint64_t limit = env->dirtyLimit;
        return limit && env->dirtyBytes.load(std::memory_order_relaxed) + bytes > limit;
    }
    /// Whether a write that may dirty `bytes` more should spill() first.
    bool wantsSpill(std::uint64_t bytes) const noexcept
    {
        const std::uint64_t limit = env->dirtyLimit;
        return limit && env->spill && !env->spillExhausted &&
               env->dirtyBytes.load(std::memory_order_relaxed) + bytes > limit;
    }
    /// Stamps dirty buffer `buf` of `npages` with the touch clock, in the
    /// checksum trailer that sealing the page overwrites, for spill() to send
    /// the coldest pages first. Only with a budget: nothing reads it otherwise.
    void touch(Page* buf, unsigned npages = 1) noexcept
    {
        if (env->dirtyLimit)
            writeLittle<std::uint32_t>(reinterpret_cast<std::byte*>(buf) +
                                           std::size_t(npages) * env->pageSize - kPageChecksumBytes,
                                       ++env->touchClock);
    }
    /// Writes this transaction's and its ancestors' leaf and overflow pages to
    /// the file, at the page numbers they will commit at, until half the dirty
    /// budget is free and `room` bytes more fit, and hands their buffers'
    /// memory back. Only between operations: no page pointer of an internal
    /// descent may be live.
    void spill(std::uint64_t room);
    /// The buffer of spilled page or overflow run `p`, read back in and dirty
    /// again; null if `p` is not one this transaction spilled.
    Page* unspill(PageNo p);
    PageNo allocPages(unsigned count);
    void freePage(PageNo p, unsigned count = 1);
    void markDirty(unsigned dbi) noexcept { dbs[dbi].dirty = 1; }
    Tree& treeAt(unsigned dbi) noexcept { return dbs[dbi].tree; }
    /// Make room for a handle that was registered mid-transaction.
    void ensureDbs(unsigned dbi)
    {
        if (dbs.size() <= dbi)
            resizeDbs(dbi + 1);
    }
    /// Resize `dbs`, parking the names of slots that go and handing them to
    /// slots that come, so a recycled transaction reuses their buffers.
    void resizeDbs(std::size_t n)
    {
        while (dbs.size() > n) {
            spareNames.push_back(std::move(dbs.back().name));
            dbs.pop_back();
        }
        while (dbs.size() < n) {
            dbs.emplace_back();
            if (!spareNames.empty()) {
                dbs.back().name = std::move(spareNames.back());
                dbs.back().name.clear();
                spareNames.pop_back();
            }
        }
    }
    /// dbs = from, through resizeDbs.
    void copyDbs(const std::vector<DbSlot>& from)
    {
        resizeDbs(from.size());
        for (std::size_t i = 0; i < from.size(); ++i)
            dbs[i] = from[i];
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
    /// Cursor::erase's copy of the key it erases, kept for its capacity.
    std::vector<std::byte> erasedKey;
    /// The key last read from a P_PREFIX leaf, assembled whole (leafKeyAt).
    std::vector<std::byte> keyBuf;
    /// The leaf whose every node Cursor::next's fast path has checked, so the
    /// rest of a scan through it reads them unchecked. Only that path, which
    /// runs where the page cannot change under the cursor, trusts it.
    const Page* scannedLeaf = nullptr;
    /// Set for cursors handed to callers, so the transaction -- and the store
    /// under it -- outlive them.
    std::shared_ptr<TxnImpl> keepalive;
    std::shared_ptr<EnvImpl> envKeep;
    /// Set only by the sub-database bookkeeping, which is allowed to read and
    /// write the N_SUBDB records that user code must not touch.
    bool allowSubdb = false;
    /// Transient cursors (single lookups) skip anchor bookkeeping entirely.
    bool track = false;
    /// Seeks within the current leaf (b_tree.cpp, seekInLeaf): consecutive
    /// misses, and how many seeks to skip trying after a run of them.
    std::uint8_t hintMisses = 0;
    std::uint8_t hintSkip = 0;

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

// Env::shrink(), for a top-level writer.

/// Absorbs every free-list entry no snapshot or pending sync holds back.
void absorbFreeList(TxnImpl* t);
/// The lowest page number T such that the free pages already absorbed below
/// it hold every live page from it up, with `slack` to spare.
PageNo shrinkLimit(const TxnImpl* t, std::uint64_t slack);
/// Absorbed free pages below `limit`.
std::size_t freeBelow(const TxnImpl* t, PageNo limit);
/// `count` contiguous absorbed free pages all below `limit`, taken from the
/// reclaim list; kInvalidPage if there are none.
PageNo allocBelow(TxnImpl* t, unsigned count, PageNo limit);
/// Drops the absorbed free pages that end the file from the free list and
/// lowers the allocation frontier past them; returns how many.
std::uint64_t cutFreeTail(TxnImpl* t);
/// Moves every page of tree `dbi` from `limit` up, tree and overflow pages,
/// to free pages below it while there are any; returns pages moved.
std::uint64_t treeRelocate(TxnImpl* t, unsigned dbi, PageNo limit);

/// Descend to the leaf that would hold `key`. Sets `*exact` when the key is
/// present. Returns false only for an empty tree.
bool treeSearch(CursorImpl& c, Slice key, bool* exact);
/// Entries whose keys sort before `key`.
std::uint64_t treeRank(CursorImpl& c, Slice key);
/// Positions on the entry with zero-based rank `rank`; false past the end.
bool treeSelect(CursorImpl& c, std::uint64_t rank);
/// Rank of the entry the cursor is on.
std::uint64_t cursorRank(CursorImpl& c);
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
/// Leaf `p`'s key `i` whole: in place, or on a leaf with shared bytes
/// assembled in the cursor, where it lasts until the cursor reads another.
Slice leafKeyAssembled(CursorImpl& c, const Page* p, unsigned i);
inline Slice leafKeyAt(CursorImpl& c, const Page* p, unsigned i)
{
    if (p->flags & P_SHARED)
        return leafKeyAssembled(c, p, i);
    return checkedLeafKey(p, i, c.txn->pageSize() - kPageChecksumBytes);
}
Slice cursorValue(CursorImpl& c);
/// The value of leaf node `i`; out of line only for overflow values.
Slice leafValueOverflow(TxnImpl* t, const Page* p, unsigned i);
inline Slice leafValue(TxnImpl* t, const Page* p, unsigned i)
{
    const LeafNode* n = checkedLeafNode(p, i, nodeTop(p, t->pageSize()));
    if (!(n->flags & N_BIGDATA))
        return Slice(reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode) + n->ksize, n->vsize);
    return leafValueOverflow(t, p, i);
}

/// Leaf node `i`'s key and value at once, the node checked once: what a
/// cursor step hands back.
NOSQL_ALWAYS_INLINE void leafEntryAt(CursorImpl& c, const Page* p, unsigned i, Slice& key,
                                     Slice& value)
{
    const LeafNode* n = checkedLeafNode(p, i, nodeTop(p, c.txn->pageSize()));
    const auto* body = reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode);
    key = (p->flags & P_SHARED) ? leafKeyAssembled(c, p, i) : Slice(body, n->ksize);
    value =
        (n->flags & N_BIGDATA) ? leafValueOverflow(c.txn, p, i) : Slice(body + n->ksize, n->vsize);
}

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
/// Commit-time write of a named tree's record: into its meta root slot
/// when it has or can take one, else into the catalog.
void treeFlushSubdb(TxnImpl* t, Slice name, const Tree& tr);
/// The meta root slot holding `name`, if any.
RootSlot* findRootSlot(Meta& m, Slice name) noexcept;
bool treeDelSubdb(TxnImpl* t, Slice name);
std::vector<std::string> treeListSubdbs(TxnImpl* t);

}  // namespace nosql::internal
