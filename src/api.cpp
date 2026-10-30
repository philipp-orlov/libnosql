// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The public surface: env / txn / db / cursor, mapped onto the internals.

#include <algorithm>
#include <chrono>
#include <cstring>

#include "nosql/internal/core.hpp"
#include "nosql/internal/atomic_file.hpp"
#ifndef NOSQL_NO_REPLICATION
#include "nosql/internal/ship_log.hpp"
#endif
#include "nosql/internal/bulk_builder.hpp"

namespace nosql {

using namespace internal;

namespace internal {
void txnAbort(TxnImpl*);
}  // namespace internal

namespace {

TxnImpl* live(TxnImpl* t)
{
    if (!t || t->finished)
        throw Error(ErrorCode::BadTransaction, "transaction is not usable");
    if (t->child)
        throw Error(ErrorCode::BadTransaction, "a nested transaction is still open");
    return t;
}

TxnImpl* writable(TxnImpl* t)
{
    live(t);
    if (t->readOnly)
        throw Error(ErrorCode::ReadOnly, "this is a read transaction");
    return t;
}

/// Before a write: spills when the dirty budget may not hold it (a descent's
/// path split at every level, a sibling, and the value's buffer). The caller's key and
/// value may point into the very pages that go, so they are copied into
/// `keep` first and redirected there.
void makeRoom(TxnImpl* t, unsigned dbi, Slice& key, Slice& value, std::string& keep)
{
    const std::size_t ps = t->pageSize();
    const std::uint64_t depth =
        dbi < t->dbs.size() && t->dbs[dbi].loaded ? std::uint64_t(t->dbs[dbi].tree.depth) : 0;
    // A value of more than a page takes a buffer of its size class.
    const std::uint64_t valueBytes = t->env->buffers.allocationSize(
        unsigned((kPageHdr + value.size() + kPageChecksumBytes + ps - 1) / ps));
    if (!t->wantsSpill((depth + 2) * 3 * ps + valueBytes))
        return;
    keep.reserve(key.size() + (value.data() ? value.size() : 0));
    if (key.size())
        keep.assign(reinterpret_cast<const char*>(key.data()), key.size());
    if (value.data())
        keep.append(reinterpret_cast<const char*>(value.data()), value.size());
    t->spill((depth + 2) * ps + valueBytes);  // what the write takes unless it splits
    key = Slice(keep.data(), key.size());
    if (value.data())
        value = Slice(keep.data() + key.size(), value.size());
}

/// A throwaway descent used by single-shot lookups; no anchor bookkeeping.
CursorImpl probe(TxnImpl* t, unsigned dbi)
{
    CursorImpl c;
    c.txn = t;
    c.dbi = dbi;
    return c;
}

/// Public cursors are recycled per thread rather than allocated: a range is a
/// cursor, and a nested-loop join opens one per outer row, which is exactly
/// the ~600-byte churn a long-lived process should not hand its allocator.
/// A pooled object holds no transaction or store reference, so the pool is
/// safe to drain at thread exit whatever else has been destroyed.
struct CursorPool
{
    static constexpr std::size_t kCap = 16;
    CursorImpl* items[kCap] = {};
    std::size_t count = 0;

    ~CursorPool()
    {
        for (std::size_t i = 0; i < count; ++i)
            delete items[i];
    }

    CursorImpl* take()
    {
        if (count)
            return items[--count];
        return new CursorImpl();
    }

    void give(CursorImpl* c) noexcept
    {
        c->keepalive.reset();
        c->envKeep.reset();
        c->scannedLeaf = nullptr;
        c->txn = nullptr;
        c->dbi = 0;
        c->top = -1;
        c->positioned = false;
        c->seqSeen = 0;
        c->anchor.clear();
        c->allowSubdb = false;
        c->track = false;
        c->hintMisses = 0;
        c->hintSkip = 0;
        if (count < kCap)
            items[count++] = c;
        else
            delete c;
    }
};

CursorPool& cursorPool()
{
    thread_local CursorPool pool;
    return pool;
}

void releaseCursor(CursorImpl* c) noexcept
{
    if (c)
        cursorPool().give(c);
}

/// Records a key a write transaction wrote, while optimistic transactions need
/// to know (see TxnImpl::occTrack).
void noteWrite(TxnImpl* t, unsigned dbi, Slice key)
{
    if (!t->occTrack || t->occUnknown)
        return;
    if (t->occWritten.size() >= TxnImpl::kOccTrackLimit) {
        t->occUnknown = true;  // too many to list: conflict like an untracked commit
        t->occWritten.clear();
        return;
    }
    const std::string& name = t->dbs[dbi].name;
    t->occWritten.push_back(occKey(name, key.view()));
    if (dbi != t->occLastDbi) {  // once per run of writes to one sub-database
        t->occWritten.push_back(occDbKey(name));
        t->occLastDbi = dbi;
    }
}

/// A write that removes keys nobody listed: the commit conflicts with every
/// optimistic transaction that began before it.
void noteUnknown(TxnImpl* t) noexcept
{
    if (t->occTrack)
        t->occUnknown = true;
}

void dropNamed(TxnImpl* t, unsigned dbi)
{
    noteUnknown(t);
    const std::string name = t->dbs[dbi].name;
    treeDrop(t, dbi);
    treeDelSubdb(t, Slice(name));
    t->dbs[dbi].dirty = 0;
    t->dbs[dbi].loaded = 0;
    if (t->namedDbs)
        --t->namedDbs;
}

}  // namespace

// ------------------------------------------------------------------ env ---

namespace {
/// The map is only as big as the file was when it was made: a file another
/// process shortened would turn the next access to the lost pages into SIGBUS.
/// Checked as a writer begins and, at most every kIntactCheckNs, as a reader
/// begins, which cannot close the race but turns the persistent case into an
/// error. `mapped` is read before the file size: growth extends the file
/// before the mapping, so a growth in between cannot look like a truncation.
void checkFileIntact(const EnvImpl& e, std::uint64_t mapped)
{
    if (os::fileSize(e.fd) < mapped)
        throw Error(ErrorCode::IoError, "the store file was truncated behind the process: " +
                                            e.path.string());
}

constexpr std::int64_t kIntactCheckNs = 100'000'000;

std::int64_t nowNs() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
}  // namespace

Txn Env::writeTxn()
{
    EnvImpl* e = &openEnv(impl_);
    if (e->readOnly)
        throw Error(ErrorCode::ReadOnly, "store was opened read-only");
    e->writersPending.fetch_add(1, std::memory_order_relaxed);  // until finishWriter
    e->writeMtx.lock();
    return beginWriter();
}

Txn Env::writeTxn(std::chrono::milliseconds timeout)
{
    EnvImpl* e = &openEnv(impl_);
    if (e->readOnly)
        throw Error(ErrorCode::ReadOnly, "store was opened read-only");
    e->writersPending.fetch_add(1, std::memory_order_relaxed);  // until finishWriter
    if (!e->writeMtx.try_lock_for(timeout)) {
        e->writersPending.fetch_sub(1, std::memory_order_relaxed);
        throw Error(ErrorCode::Busy, "another write transaction held the store for " +
                                         std::to_string(timeout.count()) + " ms");
    }
    return beginWriter();
}

Txn Env::beginWriter()
{
    EnvImpl* e = &openEnv(impl_);
    std::shared_ptr<TxnImpl> t;
    try {
        if (e->failed.load())
            throw Error(ErrorCode::BadTransaction, "commit failed; close and reopen the environment");
        {
            std::lock_guard<std::mutex> lk(e->mtx);
            checkFileIntact(*e, e->region->m.size);
            t = e->acquireTxnLocked();
            t->env = e;
            t->readOnly = false;
            t->loadSnapshot(e->writerMeta, e->region);
            // Under mtx, which every optimistic snapshot is taken under too.
            t->occTrack = e->occActive.load(std::memory_order_relaxed) != 0;
            t->oldest = e->oldestSnapshot();
            t->readersOldest = t->oldest;
            t->exemptEpoch = 0;
            if (e->deferredMeta()) {
                const TxnId newest = e->writerMeta.txnid;
                t->readersOldest =
                    e->readers.empty() ? newest : std::min<TxnId>(e->readers.front().first, newest);
                t->exemptEpoch = std::min<TxnId>(e->durableTxn, newest);
            }
        }
        // Pages handed out under an older durable commit may be named by the
        // current one: start the record afresh (writer only, under writeMtx).
        if (e->deferredMeta() && e->bornEpoch != t->exemptEpoch) {
            std::fill(e->born.begin(), e->born.end(), 0);
            e->bornEpoch = t->exemptEpoch;
        }
        // Pre-size the hot per-commit containers so a typical transaction fills
        // them without any incremental reallocation.
        t->dirty.reserve(64);
        t->pool.reserve(16);
        t->owned.reserve(16);
        t->loose.reserve(16);
        t->txnid = t->meta.txnid + 1;
    } catch (...) {
        e->writersPending.fetch_sub(1, std::memory_order_relaxed);
        e->writeMtx.unlock();
        throw;
    }
    return Txn(impl_, std::move(t));
}

Txn Env::readTxn()
{
    EnvImpl* e = &openEnv(impl_);
    if (e->failed.load())
        throw Error(ErrorCode::BadTransaction, "commit failed; close and reopen the environment");
    e->validation.maybeReset();
    // An fstat per reader would cost a system call per request: readers check
    // now and then, outside the lock.
    const std::int64_t now = nowNs();
    std::int64_t last = e->lastIntactCheckNs.load(std::memory_order_relaxed);
    if (now - last >= kIntactCheckNs &&
        e->lastIntactCheckNs.compare_exchange_strong(last, now, std::memory_order_relaxed)) {
        try {
            // Under the lock, with the size: Env::shrink() truncates the file
            // and the mapping together under it.
            std::lock_guard<std::mutex> lk(e->mtx);
            checkFileIntact(*e, e->region->m.size);
        } catch (...) {
            e->lastIntactCheckNs.store(0, std::memory_order_relaxed);  // next reader checks too
            throw;
        }
    }
    std::shared_ptr<TxnImpl> t;
    {
        // One critical section for the pool, the snapshot and the reader
        // table: a request-per-transaction caller begins thousands of these a
        // second from several threads.
        std::lock_guard<std::mutex> lk(e->mtx);
        t = e->acquireTxnLocked();
        t->env = e;
        t->readOnly = true;
        t->loadSnapshot(e->meta, e->region);
        e->readerAcquire(t->meta.txnid);
    }
    t->txnid = t->meta.txnid;
    return Txn(impl_, std::move(t));
}

EnvStats Env::stats() const
{
    EnvImpl* e = &openEnv(impl_);

    EnvStats s;
    {
        std::lock_guard<std::mutex> lk(e->mtx);
        s.pageSize = std::uint32_t(e->pageSize);
        s.fileSize = os::fileSize(e->fd);
        s.mapSize = e->region->m.size;
        s.usedPages = e->meta.lastPgno + 1;
        s.lastTxn = e->meta.txnid;
        s.readers = e->readerCount();
        s.oldestReader = e->readers.empty() ? 0 : e->readers.front().first;
        s.bufferBytes = e->buffers.retainedBytes();
        s.dirtyBytes = e->dirtyBytes.load();
        s.optimisticCommits = e->occInCommit.load(std::memory_order_relaxed);
        s.optimisticQueued = e->occWaiting.load(std::memory_order_relaxed);
        s.spilledPages = e->spilledPages.load(std::memory_order_relaxed);
        s.unspilledPages = e->unspilledPages.load(std::memory_order_relaxed);
        s.durableTxn = std::min<std::uint64_t>(e->durableTxn, e->meta.txnid);
        if (e->ship) {
            s.captureFailures = e->ship->failures();
            s.capturedTxnid = e->ship->capturedTxnid();
        }
        // Pages a commit freed too late to list (the next writer records
        // them), the free-list entry kept in the meta page, and the tree: all
        // of the commit writers build on, a pipelined one not yet durable too.
        s.freePages = e->carryOver.size() + e->writerMeta.inlineCount + e->freeTreePages;
    }
    return s;
}

// ------------------------------------------------------------------ txn ---

Txn::Txn(Txn&& o) noexcept : env_(std::move(o.env_)), impl_(std::move(o.impl_)) {}

Txn& Txn::operator=(Txn&& o) noexcept
{
    if (this != &o) {
        abort();
        env_ = std::move(o.env_);
        impl_ = std::move(o.impl_);
    }
    return *this;
}

Txn::~Txn()
{
    abort();
}

bool Txn::isReadOnly() const
{
    if (!impl_)
        throw Error(ErrorCode::BadTransaction, "transaction is not usable");
    return impl_->readOnly;
}

bool Txn::belongsTo(const Env& env) const noexcept
{
    return impl_ && !impl_->finished && env_ == env.impl_;
}

std::uint64_t Txn::id() const
{
    if (!impl_)
        throw Error(ErrorCode::BadTransaction, "transaction is not usable");
    return impl_->txnid;
}

void Txn::commit()
{
    if (!impl_)
        return;
    // Precondition failures leave the transaction usable, so check before
    // taking ownership away from the handle.
    if (!impl_->finished && impl_->child)
        throw Error(ErrorCode::BadTransaction, "commit the nested transaction first");
    const std::shared_ptr<TxnImpl> held = std::move(impl_);
    const std::shared_ptr<EnvImpl> keep = std::move(env_);
    if (!held->finished)
        txnCommit(held.get());
}

void Txn::commit(std::uint64_t* awaitLater)
{
    *awaitLater = 0;
    if (!impl_)
        return;
    if (!impl_->finished && impl_->child)
        throw Error(ErrorCode::BadTransaction, "commit the nested transaction first");
    const std::shared_ptr<TxnImpl> held = std::move(impl_);
    const std::shared_ptr<EnvImpl> keep = std::move(env_);
    if (!held->finished) {
        TxnId pending = 0;
        txnCommit(held.get(), &pending);
        *awaitLater = pending;
    }
}

void Txn::abort()
{
    if (!impl_)
        return;
    const std::shared_ptr<TxnImpl> held = std::move(impl_);
    const std::shared_ptr<EnvImpl> keep = std::move(env_);
    if (!held->finished)
        txnAbort(held.get());
}

Txn Txn::nested()
{
    TxnImpl* p = writable(impl_.get());
    auto t = p->env->acquireTxn();
    t->env = p->env;
    t->parent = p;
    t->readOnly = false;
    t->dirty.reserve(32);
    t->pool.reserve(8);
    t->owned.reserve(8);
    t->loose.reserve(8);
    t->txnid = p->txnid;
    t->region = p->region;
    t->meta = p->meta;
    t->oldest = p->oldest;
    t->lastPgno = p->lastPgno;
    t->namedDbs = p->namedDbs;
    t->occTrack = p->occTrack;
    t->copyDbs(p->dbs);
    for (DbSlot& d : t->dbs)
        d.dirty = 0;
    p->child = t.get();
    ++p->root()->seq;
    return Txn(env_, std::move(t));
}

Db Txn::db(Slice name, DbFlags flags)
{
    TxnImpl* t = live(impl_.get());
    if (name.empty())
        return nosql::Db(env_, impl_, kMainDbi);
    if (name.size() > maxKeySize(t->pageSize()))
        throw Error(ErrorCode::KeyTooLarge, "sub-database name is too long");

    EnvImpl* e = t->env;
    unsigned dbi = 0;
    const std::uint32_t persist = std::uint32_t(flags) & (std::uint32_t(DbFlags::IntegerKey) |
                                                          std::uint32_t(DbFlags::ReverseKey) |
                                                          std::uint32_t(DbFlags::PrefixKeys));
    const std::uint32_t order = persist & ~std::uint32_t(DbFlags::PrefixKeys);
    if (order == (std::uint32_t(DbFlags::IntegerKey) | std::uint32_t(DbFlags::ReverseKey)))
        throw Error(ErrorCode::InvalidArgument, "conflicting key order flags");
    // A shared prefix bounds a page's keys in byte order, a shared suffix in
    // reverse order; integers share nothing worth storing once.
    if ((flags & DbFlags::PrefixKeys) && (order & std::uint32_t(DbFlags::IntegerKey)))
        throw Error(ErrorCode::InvalidArgument, "PrefixKeys needs byte- or reverse-ordered keys");
    // The order must match; PrefixKeys only when asked for, since a tree may
    // compress without it (NOSQL_PREFIX_KEYS_DEFAULT).
    const std::uint32_t compared =
        (flags & DbFlags::PrefixKeys) ? 0xffffu : 0xffffu & ~std::uint32_t(DbFlags::PrefixKeys);
    for (unsigned index = kCatalogDbi + 1; index < t->dbs.size(); ++index) {
        const auto& slot = t->dbs[index];
        if (slot.loaded && slot.name == name.view()) {
            if (persist && (slot.tree.flags & compared) != persist)
                throw Error(ErrorCode::Incompatible, "database ordering differs");
            return nosql::Db(env_, impl_, index);
        }
    }

    Tree tr{};
    bool created = false;
    bool found = false;
    if (t->readOnly) {
        auto& cache = e->subdbs;
        std::lock_guard<std::mutex> lk(cache.mtx);
        if (cache.txnid == t->txnid) {
            for (std::size_t i = 0; i < cache.used; ++i) {
                const auto& entry = cache.trees[i];
                if (entry.first == name.view()) {
                    tr = entry.second;
                    found = true;
                    break;
                }
            }
        }
    }
    if (!found && treeGetSubdb(t, name, &tr)) {
        found = true;
        if (t->readOnly) {
            auto& cache = e->subdbs;
            std::lock_guard<std::mutex> lk(cache.mtx);
            if (cache.txnid < t->txnid) {
                // A newer snapshot: the old entries describe the past.
                cache.txnid = t->txnid;
                cache.used = 0;
            }
            if (cache.txnid == t->txnid && cache.used < cache.kMax) {
                if (cache.used == cache.trees.size())
                    cache.trees.emplace_back();
                cache.trees[cache.used].first.assign(name.chars(), name.size());
                cache.trees[cache.used].second = tr;
                ++cache.used;
            }
        }
    }
    if (!found) {
        if (!(flags & DbFlags::Create))
            throw Error(ErrorCode::NotFound, "no sub-database named '" + name.string() + "'");
        if (t->readOnly)
            throw Error(ErrorCode::ReadOnly, "cannot create a sub-database in a read transaction");
        if (t->namedDbs >= e->maxDbs)
            throw Error(ErrorCode::TooManyDbs, "maximum number of live databases reached");
        tr = emptyTree(persist |
                       (NOSQL_PREFIX_KEYS_DEFAULT && !(order & std::uint32_t(DbFlags::IntegerKey))
                            ? std::uint32_t(DbFlags::PrefixKeys)
                            : 0u));
        created = true;
        if (tr.flags & std::uint32_t(DbFlags::PrefixKeys))
            t->prefixFormat = true;
    } else if (persist && (tr.flags & compared) != persist) {
        throw Error(ErrorCode::Incompatible,
                    "sub-database '" + name.string() + "' was created with different key flags");
    }

    dbi = unsigned(t->dbs.size());
    t->ensureDbs(dbi);
    t->dbs[dbi].name.assign(name.chars(), name.size());
    t->dbs[dbi].tree = tr;
    t->dbs[dbi].loaded = 1;
    if (created) {
        // Publish the directory entry now rather than at commit, so the rest of
        // this transaction -- and any nested one -- can see the sub-database.
        treePutSubdb(t, name, tr);
        t->dbs[dbi].dirty = 1;
        ++t->namedDbs;
    }
    return nosql::Db(env_, impl_, dbi);
}

bool Txn::hasDb(Slice name) const
{
    TxnImpl* t = live(impl_.get());
    if (name.empty())
        return true;
    Tree tr{};
    return treeGetSubdb(t, name, &tr);
}

std::vector<std::string> Txn::listDbs() const
{
    return treeListSubdbs(live(impl_.get()));
}

void Txn::dropDb(Slice name)
{
    TxnImpl* t = writable(impl_.get());
    if (name.empty())
        throw Error(ErrorCode::InvalidArgument, "the main tree cannot be dropped");
    dropNamed(t, db(name).handle());
}

// ------------------------------------------------------------------- db ---

const std::string& Db::name() const
{
    live(txn_.get());
    if (dbi_ >= txn_->dbs.size() || !txn_->dbs[dbi_].loaded)
        throw Error(ErrorCode::BadTransaction, "database handle was dropped");
    return txn_->dbs[dbi_].name;
}

std::optional<Slice> Db::get(Slice key) const
{
    TxnImpl* t = live(txn_.get());
    CursorImpl c = probe(t, dbi_);
    if (!cursorSeekExact(c, key))
        return std::nullopt;
    return cursorValue(c);
}

Slice Db::at(Slice key) const
{
    auto v = get(key);
    if (!v)
        throw Error(ErrorCode::NotFound, "no such key");
    return *v;
}

bool Db::contains(Slice key) const
{
    return get(key).has_value();
}

std::uint64_t Db::count() const
{
    TxnImpl* t = live(txn_.get());
    return t->dbs[dbi_].tree.entries;
}

std::uint64_t Db::countRange(Slice lo, Slice hi) const
{
    TxnImpl* t = live(txn_.get());
    CursorImpl c = probe(t, dbi_);
    const std::uint64_t total = t->dbs[dbi_].tree.entries;
    const std::uint64_t from = lo.empty() ? 0 : treeRank(c, lo);
    const std::uint64_t to = hi.empty() ? total : treeRank(c, hi);
    return to > from ? to - from : 0;
}

TreeStats Db::stats() const
{
    TxnImpl* t = live(txn_.get());
    const Tree& tr = t->dbs[dbi_].tree;
    TreeStats s;
    s.pageSize = std::uint32_t(t->pageSize());
    s.depth = tr.depth;
    s.branchPages = tr.branchPages;
    s.leafPages = tr.leafPages;
    s.overflowPages = tr.overflowPages;
    s.entries = count();
    return s;
}

bool Db::put(Slice key, Slice value, PutMode mode) const
{
    TxnImpl* t = writable(txn_.get());
    std::string keep;
    makeRoom(t, dbi_, key, value, keep);
    CursorImpl c = probe(t, dbi_);
    const bool placed = treePut(c, key, value, mode, nullptr);
    noteWrite(t, dbi_, key);
    return placed;
}

WritableSlice Db::reserve(Slice key, std::size_t size, PutMode mode) const
{
    TxnImpl* t = writable(txn_.get());
    std::string keep;
    Slice value(nullptr, size);
    makeRoom(t, dbi_, key, value, keep);
    CursorImpl c = probe(t, dbi_);
    WritableSlice w;
    if (!treePut(c, key, value, mode, &w))
        throw Error(mode == PutMode::InsertUnique ? ErrorCode::KeyExists : ErrorCode::NotFound,
                    "reserve did not place a value");
    noteWrite(t, dbi_, key);
    return w;
}

bool Db::erase(Slice key) const
{
    TxnImpl* t = writable(txn_.get());
    std::string keep;
    Slice none;
    makeRoom(t, dbi_, key, none, keep);
    CursorImpl c = probe(t, dbi_);
    const bool erased = treeDel(c, key);
    noteWrite(t, dbi_, key);
    return erased;
}

void Db::clear() const
{
    TxnImpl* t = writable(txn_.get());
    noteUnknown(t);
    treeDrop(t, dbi_);
}

void Db::drop() const
{
    TxnImpl* t = writable(txn_.get());
    if (dbi_ == kMainDbi) {
        clear();
        return;
    }
    dropNamed(t, dbi_);
}

DbFlags Db::flags() const
{
    live(txn_.get());
    return DbFlags(txn_->dbs[dbi_].tree.flags & 0xffffu);
}

bool Cursor::boundTo(const Db& db) const noexcept
{
    // The cursor keeps its transaction alive, so the object cannot have been
    // recycled for another transaction while the pointers match.
    return impl_ && db.txn_ && impl_->txn == db.txn_.get() && impl_->dbi == db.dbi_ && !impl_->txn->finished;
}

Cursor Db::cursor() const
{
    TxnImpl* t = live(txn_.get());
    CursorImpl* c = cursorPool().take();
    c->txn = t;
    c->keepalive = txn_;  // the cursor may outlive every other handle
    c->envKeep = env_;
    c->dbi = dbi_;
    c->track = true;
    c->seqSeen = t->root()->seq;
    return nosql::Cursor(c);
}

Db::Range Db::all() const
{
    return Range(cursor(), std::nullopt, std::nullopt, std::nullopt);
}
Db::Range Db::from(Slice lo) const
{
    return Range(cursor(), lo.string(), std::nullopt, std::nullopt);
}
Db::Range Db::upto(Slice hi) const
{
    return Range(cursor(), std::nullopt, hi.string(), std::nullopt);
}
Db::Range Db::between(Slice lo, Slice hi) const
{
    return Range(cursor(), lo.string(), hi.string(), std::nullopt);
}
Db::Range Db::prefix(Slice p) const
{
    return Range(cursor(), p.string(), std::nullopt, p.string());
}

// --------------------------------------------------------------- cursor ---

Cursor::Cursor(Cursor&& o) noexcept : impl_(o.impl_)
{
    o.impl_ = nullptr;
}

Cursor& Cursor::operator=(Cursor&& o) noexcept
{
    if (this != &o) {
        releaseCursor(impl_);
        impl_ = o.impl_;
        o.impl_ = nullptr;
    }
    return *this;
}

Cursor::~Cursor()
{
    releaseCursor(impl_);
}

namespace {

/// The cursor's implementation, once it is attached to a usable transaction.
CursorImpl& attached(CursorImpl* impl, bool forWrite = false)
{
    if (!impl)
        throw Error(ErrorCode::BadTransaction, "cursor is not attached");
    if (forWrite)
        writable(impl->txn);
    else
        live(impl->txn);
    return *impl;
}

}  // namespace

Db Cursor::db() const
{
    if (!impl_)
        throw Error(ErrorCode::BadTransaction, "cursor is not attached");
    return nosql::Db(impl_->envKeep, impl_->keepalive, impl_->dbi);
}

bool Cursor::valid() const
{
    // A cursor can outlive its transaction; that is not an error, it just means
    // it is no longer sitting on anything.
    if (!impl_ || !impl_->txn || impl_->txn->finished)
        return false;
    cursorRevalidate(*impl_);
    return impl_->positioned;
}

Slice Cursor::key() const
{
    return cursorKey(attached(impl_));
}

Slice Cursor::value() const
{
    return cursorValue(attached(impl_));
}

bool Cursor::first()
{
    return cursorFirst(attached(impl_));
}

bool Cursor::last()
{
    return cursorLast(attached(impl_));
}

bool Cursor::next()
{
    return cursorNext(attached(impl_));
}

namespace {

bool landed(CursorImpl& c, bool ok, Slice& key, Slice& value)
{
    if (!ok)
        return false;
    leafEntryAt(c, c.pg[c.top], c.idx[c.top], key, value);
    return true;
}

}  // namespace

bool Cursor::first(Slice& key, Slice& value)
{
    CursorImpl& c = attached(impl_);
    return landed(c, cursorFirst(c), key, value);
}

bool Cursor::next(Slice& key, Slice& value)
{
    // The step a scan takes almost every time -- to the next slot of the same
    // leaf, in a usable transaction nothing has changed under -- without the
    // checks and re-anchoring the general path does.
    CursorImpl* c = impl_;
    if (c && c->positioned && c->top >= 0 && c->txn && !c->txn->finished && !c->txn->child &&
        (c->txn->readOnly || !c->track) && c->seqSeen == c->txn->root()->seq) {
        const Page* p = c->pg[c->top];
        const unsigned i = c->idx[c->top] + 1u;
        if (i < p->nkeys) {
            c->idx[c->top] = std::uint16_t(i);
            // A scan reads every node of the leaf: check them all once.
            if (c->scannedLeaf != p) {
                validateNodes(p, c->txn->pageSize());
                c->scannedLeaf = p;
            }
            const LeafNode* n = lnode(p, i);
            const auto* body = reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode);
            key = (p->flags & P_SHARED) ? leafKeyAssembled(*c, p, i) : Slice(body, n->ksize);
            value = (n->flags & N_BIGDATA) ? leafValueOverflow(c->txn, p, i)
                                           : Slice(body + n->ksize, n->vsize);
            return true;
        }
    }
    CursorImpl& ci = attached(impl_);
    return landed(ci, cursorNext(ci), key, value);
}

bool Cursor::skip(std::uint64_t n)
{
    CursorImpl& c = attached(impl_);
    if (n == 0) {
        cursorRevalidate(c);
        return c.positioned;
    }
    const std::uint64_t at = cursorRank(c);
    if (at + n < at) {
        c.unposition();
        return false;
    }
    return treeSelect(c, at + n);
}

bool Cursor::seekExact(Slice k, Slice& value)
{
    CursorImpl& c = attached(impl_);
    if (!cursorSeekExact(c, k))
        return false;
    value = leafValue(c.txn, c.pg[c.top], c.idx[c.top]);
    return true;
}

bool Cursor::seek(Slice k, Slice& key, Slice& value)
{
    CursorImpl& c = attached(impl_);
    return landed(c, cursorSeek(c, k), key, value);
}

bool Cursor::prev()
{
    return cursorPrev(attached(impl_));
}

bool Cursor::seek(Slice k)
{
    return cursorSeek(attached(impl_), k);
}

bool Cursor::seekExact(Slice k)
{
    return cursorSeekExact(attached(impl_), k);
}

bool Cursor::put(Slice k, Slice v, PutMode mode)
{
    CursorImpl& c = attached(impl_, true);
    std::string keep;
    makeRoom(c.txn, c.dbi, k, v, keep);
    const bool placed = treePut(c, k, v, mode, nullptr);
    noteWrite(c.txn, c.dbi, k);
    return placed;
}

bool Cursor::erase()
{
    CursorImpl& c = attached(impl_, true);
    std::string keep;
    Slice none;
    Slice probeKey;
    makeRoom(c.txn, c.dbi, probeKey, none, keep);
    cursorRevalidate(c);
    if (!c.positioned)
        return false;
    const Slice k = cursorKey(c);
    std::vector<std::byte>& saved = c.erasedKey;
    saved.assign(k.data(), k.data() + k.size());
    noteWrite(c.txn, c.dbi, k);
    treeDelCurrent(c);
    // The key is gone, so seeking to it lands on whatever came next.
    return cursorSeek(c, Slice(saved.data(), saved.size()));
}

// ---------------------------------------------------------------- Range ---

namespace {

/// Window test, honouring the sub-database's own key ordering.
bool inside(CursorImpl& c, const std::optional<std::string>& hi,
            const std::optional<std::string>& pfx)
{
    const Slice k = leafKeyAt(c, c.pg[c.top], c.idx[c.top]);
    // Under ReverseKey the keys that share a prefix in the tree's order, the
    // ones next to each other, are those that end the same.
    if (pfx && !((c.tree().flags & std::uint32_t(DbFlags::ReverseKey)) ? k.endsWith(Slice(*pfx))
                                                                       : k.startsWith(Slice(*pfx))))
        return false;
    if (hi && c.cmp()(k, Slice(*hi)) >= 0)
        return false;
    return true;
}

}  // namespace

bool Db::Range::rewind()
{
    const bool ok = lo_ ? cur_.seek(Slice(*lo_)) : cur_.first();
    return ok && inside(*cur_.impl_, hi_, prefix_);
}

bool Db::Range::step()
{
    return cur_.next() && inside(*cur_.impl_, hi_, prefix_);
}

Db::Range::Entry Db::Range::current() const
{
    return {cursorKey(*cur_.impl_), cursorValue(*cur_.impl_)};
}

// --------------------------------------------------------------- shrink ---

std::uint64_t Env::shrink()
{
    EnvImpl* e = &openEnv(impl_);
    if (e->readOnly)
        throw Error(ErrorCode::ReadOnly, "store was opened read-only");
    const std::uint64_t before = os::fileSize(e->fd);
    const std::size_t ps = e->pageSize;
    auto treesOf = [](Txn& txn) {
        std::vector<unsigned> dbis{kMainDbi, kCatalogDbi, kFreeDbi};
        for (const std::string& name : txn.listDbs())
            dbis.push_back(txn.db(Slice(name)).handle());
        return dbis;
    };

    // Move what lies near the end down into free pages, as low as they go.
    // The free list grows by what moved, and the writes of this commit take
    // their pages too: the limit keeps room for every copied path's branch
    // pages and for the commit's free-list chunks.
    {
        Txn txn = writeTxn();
        TxnImpl* t = txn.impl_.get();
        t->maintenance = true;
        const std::vector<unsigned> dbis = treesOf(txn);
        absorbFreeList(t);
        std::uint64_t slack = 64 + (std::uint64_t(t->lastPgno) + 1) / freeChunkPages(ps);
        for (const unsigned dbi : dbis)
            slack += t->dbs[dbi].tree.branchPages;
        const PageNo limit = shrinkLimit(t, slack);
        if (limit <= t->lastPgno)
            for (const unsigned dbi : dbis)
                treeRelocate(t, dbi, limit);
        txn.commit();
    }
    // Then cut what is free at the end, once what moved is free in every
    // snapshot. A round's own writes may free more of the tail: a few rounds.
    for (int round = 0; round < 4; ++round) {
        sync(true);
        Txn txn = writeTxn();
        TxnImpl* t = txn.impl_.get();
        t->maintenance = true;
        absorbFreeList(t);
        const std::uint64_t cut = cutFreeTail(t);
        txn.commit();
        if (!cut)
            break;
    }
    // The cut is durable before the file loses the pages: a store recovered
    // from an older meta would list them as free. Truncated under the write
    // slot, so no commit takes a page there in between, and under the lock
    // that readers check the size under.
    sync(true);
    {
        Txn txn = writeTxn();
        TxnImpl* t = txn.impl_.get();
        t->maintenance = true;
#ifndef _WIN32
        {
            std::lock_guard<std::mutex> lk(e->mtx);
            e->shrinkLocked((std::uint64_t(t->lastPgno) + 1) * ps);
            if (t->region != e->region) {
                t->seen.push_back(std::move(t->region));
                t->region = e->region;
            }
        }
#else
        (void)t;
#endif
        txn.commit();  // its meta records the new size, for replicas
    }
    sync(true);
    const std::uint64_t after = os::fileSize(e->fd);
    return before > after ? before - after : 0;
}

// -------------------------------------------------------------- compact ---

void compact(const std::filesystem::path& src, const std::filesystem::path& dst)
{
    if (std::filesystem::exists(dst))
        throw Error(ErrorCode::InvalidArgument, "compaction destination already exists");
    Env s = Env::configure().readOnly(true).open(src);
    const EnvStats st = s.stats();
    AtomicFile output(dst);
    BulkFile destination(output.fd(), st.pageSize);
    Txn rt = s.readTxn();
    bool prefixed = false;  // the copy holds P_PREFIX leaves: format version 8
    auto copyTree = [&](nosql::Db from) {
        prefixed = prefixed || (from.flags() & DbFlags::PrefixKeys);
        BulkTree builder(destination, std::uint32_t(from.flags()));
        auto cur = from.cursor();
        for (bool ok = cur.first(); ok; ok = cur.next()) {
            builder.add(cur.key(), cur.value());
        }
        return builder.finish();
    };
    Meta meta{};
    std::memcpy(meta.magic, kMagic, sizeof meta.magic);
    meta.version = kFormatVersion;
    meta.pageSize = st.pageSize;
    meta.txnid = 1;
    meta.storeId = newIdentity();
    meta.commitId = newIdentity();
    meta.freeTree = emptyTree();
    meta.mainTree = copyTree(rt.mainDb());
    BulkTree catalog(destination, 0);
    for (const std::string& name : rt.listDbs()) {
        nosql::Db from = rt.db(Slice(name));
        const Tree tree = copyTree(from);
        catalog.add(name, Slice::ref(tree), N_SUBDB);
        ++meta.namedDbs;
    }
    meta.catalogTree = catalog.finish();
    meta.version = prefixed ? kFormatVersion : kOldestFormatVersion;
    destination.finish(meta);
    rt.abort();
    output.close();
    {
        Env check = Env::configure().readOnly().open(output.path());
        check.read([](Txn& txn) { checkIntegrity(txn); });
    }
    output.publish(false);
}

}  // namespace nosql
