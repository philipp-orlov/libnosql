// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The public surface: env / txn / db / cursor, mapped onto the internals.

#include <algorithm>
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
void txnCommit(TxnImpl*);
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
        c->txn = nullptr;
        c->dbi = 0;
        c->top = -1;
        c->positioned = false;
        c->seqSeen = 0;
        c->anchor.clear();
        c->allowSubdb = false;
        c->track = false;
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

void dropNamed(TxnImpl* t, unsigned dbi)
{
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
/// Checked as a transaction begins (caller holds e.mtx), which cannot close the
/// race but turns the persistent case into an error.
void checkFileIntact(const EnvImpl& e)
{
    if (os::fileSize(e.fd) < e.region->m.size)
        throw Error(ErrorCode::IoError, "the store file was truncated behind the process: " +
                                            e.path.string());
}
}  // namespace

Txn Env::writeTxn()
{
    EnvImpl* e = &openEnv(impl_);
    if (e->readOnly)
        throw Error(ErrorCode::ReadOnly, "store was opened read-only");

    e->writeMtx.lock();
    std::shared_ptr<TxnImpl> t;
    try {
        if (e->failed.load())
            throw Error(ErrorCode::BadTransaction, "commit failed; close and reopen the environment");
        {
            std::lock_guard<std::mutex> lk(e->mtx);
            checkFileIntact(*e);
            t = e->acquireTxnLocked();
            t->env = e;
            t->readOnly = false;
            t->loadSnapshot(e->meta, e->region);
            t->oldest = e->oldestSnapshot();
        }
        // Pre-size the hot per-commit containers so a typical transaction fills
        // them without any incremental reallocation.
        t->dirty.reserve(64);
        t->pool.reserve(16);
        t->owned.reserve(16);
        t->loose.reserve(16);
        t->txnid = t->meta.txnid + 1;
    } catch (...) {
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
    std::shared_ptr<TxnImpl> t;
    {
        // One critical section for the pool, the snapshot and the reader
        // table: a request-per-transaction caller begins thousands of these a
        // second from several threads.
        std::lock_guard<std::mutex> lk(e->mtx);
        checkFileIntact(*e);
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
        if (e->ship) {
            s.captureFailures = e->ship->failures();
            s.capturedTxnid = e->ship->capturedTxnid();
        }
        // Pages a commit freed too late to list; the next writer records them.
        s.freePages = e->carryOver.size();
    }

    Txn rt = const_cast<Env*>(this)->readTxn();
    CursorImpl c;
    c.txn = rt.impl_.get();
    c.dbi = kFreeDbi;
    for (bool ok = cursorFirst(c); ok; ok = cursorNext(c))
        s.freePages += cursorValue(c).size() / sizeof(PageNo);
    rt.abort();
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
    t->dbs = p->dbs;
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
                                                          std::uint32_t(DbFlags::ReverseKey));
    if (persist == (std::uint32_t(DbFlags::IntegerKey) | std::uint32_t(DbFlags::ReverseKey)))
        throw Error(ErrorCode::InvalidArgument, "conflicting key order flags");
    for (unsigned index = kCatalogDbi + 1; index < t->dbs.size(); ++index) {
        const auto& slot = t->dbs[index];
        if (slot.loaded && slot.name == name.view()) {
            if (persist && (slot.tree.flags & 0xffffu) != persist)
                throw Error(ErrorCode::Incompatible, "database ordering differs");
            return nosql::Db(env_, impl_, index);
        }
    }

    Tree tr{};
    bool created = false;
    if (!treeGetSubdb(t, name, &tr)) {
        if (!(flags & DbFlags::Create))
            throw Error(ErrorCode::NotFound, "no sub-database named '" + name.string() + "'");
        if (t->readOnly)
            throw Error(ErrorCode::ReadOnly, "cannot create a sub-database in a read transaction");
        if (t->namedDbs >= e->maxDbs)
            throw Error(ErrorCode::TooManyDbs, "maximum number of live databases reached");
        tr = emptyTree(persist);
        created = true;
    } else if (persist && (tr.flags & 0xffffu) != persist) {
        throw Error(ErrorCode::Incompatible,
                    "sub-database '" + name.string() + "' was created with different key ordering");
    }

    dbi = unsigned(t->dbs.size());
    t->ensureDbs(dbi);
    t->dbs[dbi].name = name.string();
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
    CursorImpl c = probe(t, dbi_);
    return treePut(c, key, value, mode, nullptr);
}

WritableSlice Db::reserve(Slice key, std::size_t size, PutMode mode) const
{
    TxnImpl* t = writable(txn_.get());
    CursorImpl c = probe(t, dbi_);
    WritableSlice w;
    if (!treePut(c, key, Slice(nullptr, size), mode, &w))
        throw Error(mode == PutMode::InsertUnique ? ErrorCode::KeyExists : ErrorCode::NotFound,
                    "reserve did not place a value");
    return w;
}

bool Db::erase(Slice key) const
{
    TxnImpl* t = writable(txn_.get());
    CursorImpl c = probe(t, dbi_);
    return treeDel(c, key);
}

void Db::clear() const
{
    TxnImpl* t = writable(txn_.get());
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
    return treePut(attached(impl_, true), k, v, mode, nullptr);
}

bool Cursor::erase()
{
    CursorImpl& c = attached(impl_, true);
    cursorRevalidate(c);
    if (!c.positioned)
        return false;
    const Slice k = cursorKey(c);
    const std::vector<std::byte> saved(k.data(), k.data() + k.size());
    treeDelCurrent(c);
    // The key is gone, so seeking to it lands on whatever came next.
    return cursorSeek(c, Slice(saved.data(), saved.size()));
}

// ---------------------------------------------------------------- Range ---

namespace {

/// Window test, honouring the sub-database's own key ordering.
bool inside(const CursorImpl& c, const std::optional<std::string>& hi,
            const std::optional<std::string>& pfx)
{
    const Slice k = nodeKey(c.pg[c.top], c.idx[c.top]);
    if (pfx && !k.startsWith(Slice(*pfx)))
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
    auto copyTree = [&](nosql::Db from) {
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
