// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/optimistic.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iterator>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "nosql/internal/core.hpp"
#include "nosql/internal/occ_log.hpp"

namespace nosql {

using internal::EnvImpl;
using internal::TxnId;

struct OptimisticTxn::Impl
{
    /// One buffered write; its bytes live in `bytes`.
    struct Op
    {
        std::uint64_t hash;
        std::uint32_t db, dbLen, key, keyLen, value, valueLen;
        bool erase;
    };

    std::shared_ptr<EnvImpl> env;
    /// Pins the snapshot every read comes from, and its reader slot.
    Txn snapshot;
    TxnId snapshotId = 0;
    unsigned attempt = 0;    // conflicts this transaction has already lost
    bool exclusive = false;  // holds back the other optimistic commits
    bool pinned = false;     // snapshot released, its id pinned in the log

    std::string bytes;
    std::vector<Op> ops;                 // in the order they were made
    std::vector<std::uint64_t> touched;  // every key read or written, and scanned dbs
    std::vector<std::uint64_t> written;  // every key written, and its db's occDbKey
    std::vector<std::uint32_t> scanOps;  // scratch for scan(): buffered ops in range
    /// The group this thread led, while it waits for the group's commit to be
    /// durable after handing the lead on. Kept for its capacity.
    std::vector<internal::OccPending*> ledGroup;
    bool scanning = false;
    /// Latest op per key hash, built once a transaction has kIndexAfter writes;
    /// below that a backward scan is faster.
    std::unordered_map<std::uint64_t, std::uint32_t> index;
    static constexpr std::size_t kIndexAfter = 32;

    /// Snapshot sub-database handles by name. Only the first dbCount entries are
    /// live; the names keep their capacity across reuse.
    std::vector<std::string> dbNames;
    std::vector<Db> dbHandles;
    std::size_t dbCount = 0;

    Slice slice(std::uint32_t off, std::uint32_t len) const noexcept
    {
        return Slice(bytes.data() + off, len);
    }

    bool matches(const Op& op, Slice db, Slice key) const noexcept
    {
        return slice(op.db, op.dbLen).view() == db.view() &&
               slice(op.key, op.keyLen).view() == key.view();
    }

    const Op* find(std::uint64_t h, Slice db, Slice key) const noexcept
    {
        if (!index.empty()) {
            const auto it = index.find(h);
            if (it == index.end())
                return nullptr;
            if (matches(ops[it->second], db, key))
                return &ops[it->second];
            // Another key with the same hash was written later: scan for ours.
        }
        for (std::size_t i = ops.size(); i-- > 0;)
            if (ops[i].hash == h && matches(ops[i], db, key))
                return &ops[i];
        return nullptr;
    }

    /// The snapshot's handle on `name`, or null if it has no such sub-database.
    const Db* snapshotDb(Slice name)
    {
        for (std::size_t i = 0; i < dbCount; ++i)
            if (dbNames[i] == name.view())
                return dbHandles[i] ? &dbHandles[i] : nullptr;
        Db d;
        if (name.empty())
            d = snapshot.mainDb();
        else if (snapshot.hasDb(name))
            d = snapshot.db(name);
        if (dbCount == dbNames.size()) {
            dbNames.emplace_back();
            dbHandles.emplace_back();
        }
        dbNames[dbCount].assign(name.view());
        dbHandles[dbCount] = d;
        ++dbCount;
        return d ? &dbHandles[dbCount - 1] : nullptr;
    }

    void add(Slice db, Slice key, Slice value, bool erase)
    {
        if (scanning)
            throw Error(ErrorCode::BadTransaction,
                        "cannot write during a scan of this transaction");
        if (key.empty() || key.size() > internal::maxKeySize(env->pageSize))
            throw Error(ErrorCode::KeyTooLarge, key.empty() ? "empty keys are not allowed"
                                                            : "key exceeds the page-size limit");
        if (value.size() > UINT32_MAX)
            throw Error(ErrorCode::ValueTooLarge, "value exceeds 4 GiB");
        if (bytes.size() + db.size() + key.size() + value.size() > UINT32_MAX)
            throw Error(ErrorCode::ValueTooLarge, "optimistic transaction buffers exceed 4 GiB");
        Op op;
        op.hash = internal::occKey(db.view(), key.view());
        op.erase = erase;
        op.db = std::uint32_t(bytes.size());
        op.dbLen = std::uint32_t(db.size());
        bytes.append(db.view());
        op.key = std::uint32_t(bytes.size());
        op.keyLen = std::uint32_t(key.size());
        bytes.append(key.view());
        op.value = std::uint32_t(bytes.size());
        op.valueLen = std::uint32_t(value.size());
        bytes.append(value.view());
        ops.push_back(op);
        touched.push_back(op.hash);
        written.push_back(op.hash);
        written.push_back(internal::occDbKey(db.view()));  // sortUnique drops repeats
        if (!index.empty()) {
            index[op.hash] = std::uint32_t(ops.size() - 1);
        } else if (ops.size() >= kIndexAfter) {
            for (std::size_t i = 0; i < ops.size(); ++i)
                index[ops[i].hash] = std::uint32_t(i);
        }
    }

    void dropHandles() noexcept
    {
        for (std::size_t i = 0; i < dbCount; ++i)
            dbHandles[i] = Db();
        dbCount = 0;
    }

    /// At commit the reads are done, and validation needs only the snapshot's
    /// id: trade the reader slot, which holds back page reclamation while the
    /// commit waits for its group, for a pin that holds back only the log.
    void unpinPages()
    {
        dropHandles();
        {
            std::lock_guard<std::mutex> lk(env->mtx);
            env->occPinAcquire(snapshotId);
        }
        pinned = true;
        try {
            snapshot.abort();
        } catch (...) {}
    }

    /// Back to an empty, unbound transaction. Keeps the capacity a typical
    /// transaction needs and gives back what a large one grew.
    void reset() noexcept
    {
        dropHandles();
        try {
            snapshot.abort();
        } catch (...) {}
        if (pinned) {
            std::lock_guard<std::mutex> lk(env->mtx);
            env->occPinRelease(snapshotId);
            pinned = false;
        }
        env.reset();
        bytes.clear();
        ops.clear();
        touched.clear();
        written.clear();
        scanOps.clear();
        scanning = false;
        index.clear();
        constexpr std::size_t kRetainBytes = 1u << 20, kRetainOps = 4096;
        if (bytes.capacity() > kRetainBytes)
            std::string().swap(bytes);
        if (ops.capacity() > kRetainOps)
            std::vector<Op>().swap(ops);
        if (touched.capacity() > 2 * kRetainOps)
            std::vector<std::uint64_t>().swap(touched);
        if (written.capacity() > kRetainOps)
            std::vector<std::uint64_t>().swap(written);
        if (index.bucket_count() > kRetainOps)
            std::unordered_map<std::uint64_t, std::uint32_t>().swap(index);
    }
};

namespace internal {
struct OccPending
{
    OptimisticTxn::Impl* txn = nullptr;
    /// Set by the leader under occMtx, after it has notified `cv`; a spinning
    /// waiter reads it without the lock and may return at once.
    std::atomic<bool> done{false};
    bool conflict = false;
    std::exception_ptr error;
    /// Each waiter sleeps on its own condition, so a finished group wakes its
    /// members and the next leader, not every thread in the queue.
    std::condition_variable cv;
};
}  // namespace internal

namespace {

/// One spare Impl per thread. A thread that runs one transaction at a time,
/// the usual pattern, reuses the same buffers every time.
thread_local std::unique_ptr<OptimisticTxn::Impl> spare;

/// Most transactions one group commit applies, and roughly most bytes; past
/// either the rest wait for the next group, which keeps a group well inside the
/// dirty-page budget.
constexpr std::size_t kMaxBatch = 256;
constexpr std::size_t kMaxBatchBytes = 8u << 20;

void sortUnique(std::vector<std::uint64_t>& v)
{
    std::sort(v.begin(), v.end());
    v.erase(std::unique(v.begin(), v.end()), v.end());
}

/// dst = dst ∪ src, both sorted and unique; `scratch` keeps its capacity.
void mergeInto(std::vector<std::uint64_t>& dst, const std::vector<std::uint64_t>& src,
               std::vector<std::uint64_t>& scratch)
{
    scratch.clear();
    std::set_union(dst.begin(), dst.end(), src.begin(), src.end(), std::back_inserter(scratch));
    dst.swap(scratch);
}

/// Applies one transaction's writes in order. Consecutive writes to the same
/// sub-database share a handle.
void replay(Txn& w, const OptimisticTxn::Impl& t)
{
    Db d;
    std::string_view current;
    bool bound = false;
    for (const OptimisticTxn::Impl::Op& op : t.ops) {
        const Slice name = t.slice(op.db, op.dbLen);
        if (!bound || name.view() != current) {
            bound = true;
            current = name.view();
            if (name.empty())
                d = w.mainDb();
            else if (!op.erase)
                d = w.db(name, DbFlags::Create);
            else
                d = w.hasDb(name) ? w.db(name) : Db();
        } else if (!d && !op.erase) {
            d = w.db(name, DbFlags::Create);  // an erase found it absent; this put creates it
        }
        const Slice key = t.slice(op.key, op.keyLen);
        if (op.erase) {
            if (d)
                d.erase(key);
        } else {
            d.put(key, t.slice(op.value, op.valueLen));
        }
    }
}

/// Thrown inside a group when one member's writes fail to apply.
struct RetryWithout
{};

/// Moves the front of the queue, up to the group limits, into the batch.
void takeBatch(EnvImpl& e)
{
    std::size_t n = 0, bytes = 0;
    while (n < e.occQueue.size() && n < kMaxBatch) {
        const std::size_t b = e.occQueue[n]->txn->bytes.size();
        if (n && bytes + b > kMaxBatchBytes)
            break;
        bytes += b;
        ++n;
    }
    e.occBatch.assign(e.occQueue.begin(), e.occQueue.begin() + std::ptrdiff_t(n));
    e.occQueue.erase(e.occQueue.begin(), e.occQueue.begin() + std::ptrdiff_t(n));
    e.occWaiting.store(unsigned(e.occQueue.size()), std::memory_order_relaxed);
}

}  // namespace

OptimisticTxn::OptimisticTxn(Env& env, unsigned attempt)
{
    EnvImpl& e = internal::openEnv(env.impl_);  // throws when the handle is closed
    std::unique_ptr<Impl> t = spare ? std::move(spare) : std::make_unique<Impl>();
    t->env = env.impl_;
    t->attempt = attempt;
    impl_ = std::move(t);  // from here on finish() undoes whatever was taken
    e.occActive.fetch_add(1, std::memory_order_relaxed);  // before the snapshot below
    try {
        if (attempt >= kExclusiveAfter) {
            // Wait for any other exclusive transaction, claim the turn, then let
            // the commits already queued drain; new ones wait for this to finish.
            std::unique_lock<std::mutex> lk(e.occMtx);
            e.occTurn.wait(lk, [&] { return e.occExclusive == nullptr; });
            e.occExclusive = impl_.get();
            impl_->exclusive = true;
            e.occTurn.wait(lk, [&] { return e.occQueue.empty() && !e.occLeader; });
        }
        if (impl_->exclusive && e.pipelined) {
            // A drained group may still wait for its meta to land under
            // pipelined commits; the snapshot must include it, or that commit
            // could still win this transaction's keys.
            TxnId newest;
            {
                std::lock_guard<std::mutex> lk(e.mtx);
                newest = e.writerMeta.txnid;
            }
            internal::awaitDurable(e, newest);
        }
        impl_->snapshot = env.readTxn();
        impl_->snapshotId = impl_->snapshot.id();
    } catch (...) {
        finish();  // a constructor that throws gets no destructor
        throw;
    }
}

OptimisticTxn::~OptimisticTxn()
{
    finish();
}

OptimisticTxn::OptimisticTxn(OptimisticTxn&&) noexcept = default;

OptimisticTxn& OptimisticTxn::operator=(OptimisticTxn&& other) noexcept
{
    if (this != &other) {
        finish();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

OptimisticTxn::Impl& OptimisticTxn::active()
{
    if (!impl_)
        throw Error(ErrorCode::BadTransaction, "optimistic transaction already finished");
    return *impl_;
}

void OptimisticTxn::finish() noexcept
{
    if (!impl_)
        return;
    impl_->env->occActive.fetch_sub(1, std::memory_order_relaxed);
    if (impl_->exclusive) {
        EnvImpl& e = *impl_->env;
        std::lock_guard<std::mutex> lk(e.occMtx);
        e.occExclusive = nullptr;
        impl_->exclusive = false;
        e.occTurn.notify_all();
    }
    impl_->reset();
    if (!spare)
        spare = std::move(impl_);
    else
        impl_.reset();
}

std::optional<Slice> OptimisticTxn::view(Slice db, Slice key)
{
    Impl& t = active();
    const std::uint64_t h = internal::occKey(db.view(), key.view());
    t.touched.push_back(h);
    if (const Impl::Op* op = t.find(h, db, key)) {
        if (op->erase)
            return std::nullopt;
        return t.slice(op->value, op->valueLen);
    }
    const Db* d = t.snapshotDb(db);
    if (!d)
        return std::nullopt;
    return d->get(key);
}

std::optional<std::string> OptimisticTxn::get(Slice db, Slice key)
{
    const std::optional<Slice> v = view(db, key);
    if (!v)
        return std::nullopt;
    return v->string();
}

void OptimisticTxn::scanImpl(Slice db, Slice lo, Slice hi, bool (*call)(void*, Slice, Slice),
                             void* ctx)
{
    Impl& t = active();
    if (t.scanning)
        throw Error(ErrorCode::BadTransaction, "scans of one transaction cannot nest");
    t.touched.push_back(internal::occDbKey(db.view()));
    const Db* d = t.snapshotDb(db);
    const internal::CompareFn cmp =
        d ? internal::comparatorFor(std::uint32_t(d->flags())) : &internal::cmpLexicographic;
    const auto inRange = [&](Slice k) {
        return (lo.empty() || cmp(k, lo) >= 0) && (hi.empty() || cmp(k, hi) < 0);
    };

    const auto keyAt = [&](std::uint32_t op) { return t.slice(t.ops[op].key, t.ops[op].keyLen); };

    // This transaction's writes in range, in key order, the latest per key last.
    std::vector<std::uint32_t>& own = t.scanOps;
    own.clear();
    for (std::uint32_t i = 0; i < t.ops.size(); ++i) {
        const Impl::Op& op = t.ops[i];
        if (t.slice(op.db, op.dbLen).view() == db.view() && inRange(keyAt(i)))
            own.push_back(i);
    }
    std::sort(own.begin(), own.end(), [&](std::uint32_t a, std::uint32_t b) {
        const int c = cmp(keyAt(a), keyAt(b));
        return c < 0 || (c == 0 && a < b);
    });

    struct ScanGuard
    {
        bool& flag;
        ~ScanGuard() { flag = false; }
    } guard{t.scanning};
    t.scanning = true;

    Cursor c;
    Slice sk, sv;
    bool more = false;
    if (d) {
        c = d->cursor();
        more = lo.empty() ? c.first(sk, sv) : c.seek(lo, sk, sv);
        if (more && !hi.empty() && cmp(sk, hi) >= 0)
            more = false;
    }
    std::size_t i = 0;
    while (more || i < own.size()) {
        // The latest of this transaction's writes to the next key it wrote.
        std::size_t last = i;
        while (last + 1 < own.size() && cmp(keyAt(own[last + 1]), keyAt(own[i])) == 0)
            ++last;
        // Negative: the snapshot's key comes first; positive: ours; zero: same key.
        int order;
        if (!more)
            order = 1;
        else if (i >= own.size())
            order = -1;
        else
            order = cmp(sk, keyAt(own[i]));
        if (order < 0) {  // only the snapshot has this key
            if (!call(ctx, sk, sv))
                return;
        } else {  // this transaction wrote it, over the snapshot's value if any
            const Impl::Op& op = t.ops[own[last]];
            if (!op.erase && !call(ctx, keyAt(own[last]), t.slice(op.value, op.valueLen)))
                return;
            i = last + 1;
        }
        if (order <= 0 && more) {
            more = c.next(sk, sv);
            if (more && !hi.empty() && cmp(sk, hi) >= 0)
                more = false;
        }
    }
}

void OptimisticTxn::put(Slice db, Slice key, Slice value)
{
    active().add(db, key, value, false);
}

void OptimisticTxn::erase(Slice db, Slice key)
{
    active().add(db, key, Slice(), true);
}

void OptimisticTxn::abort()
{
    finish();
}

std::uint64_t OptimisticTxn::applyBatch(EnvImpl& e) noexcept
{
    std::vector<internal::OccPending*>& batch = e.occBatch;
    if (batch.empty())
        return 0;
    Env env(batch.front()->txn->env);
    // Validate the transactions that have lost most often first, keeping
    // arrival order otherwise. Without this the leader's own thread, which
    // never has to be woken, starts its next transaction first and can win a
    // hot key every time. Insertion sort: groups are small, and it allocates
    // nothing.
    for (std::size_t i = 1; i < batch.size(); ++i) {
        internal::OccPending* p = batch[i];
        std::size_t j = i;
        for (; j > 0 && batch[j - 1]->txn->attempt < p->txn->attempt; --j)
            batch[j] = batch[j - 1];
        batch[j] = p;
    }
    // A member whose writes fail to apply is taken out and the group tried
    // again without it, so one bad transaction never sinks the others. Flags
    // set before the failure stay right: a conflict was with the log or with a
    // member that is still in the group.
    for (;;) {
        try {
            Txn w = env.writeTxn();
            e.occBatchWritten.clear();
            bool wrote = false;
            for (internal::OccPending* p : batch) {
                if (p->conflict || p->error)
                    continue;  // refused in an earlier round
                const Impl& t = *p->txn;
                if (e.occLog.conflicts(t.snapshotId, t.touched) ||
                    internal::occIntersects(e.occBatchWritten, t.touched)) {
                    p->conflict = true;
                    continue;
                }
                if (t.ops.empty())
                    continue;  // read-only: validated, nothing to apply
                try {
                    replay(w, t);
                } catch (...) {
                    p->error = std::current_exception();
                    throw RetryWithout{};
                }
                mergeInto(e.occBatchWritten, t.written, e.occMergeScratch);
                wrote = true;
            }
            if (!wrote)
                return 0;  // nothing to publish; the writer aborts
            // The writer recorded the keys it replayed, which is what the log
            // keeps for this group (an optimistic transaction is alive, this
            // one, so it was tracking). A pipelined commit returns before its
            // meta is flushed; the leader waits for that after handing on.
            std::uint64_t pending = 0;
            w.commit(&pending);
            return pending;
        } catch (const RetryWithout&) {
        } catch (...) {
            // The writer slot or the commit failed: nothing in the group was
            // applied, and every member not already refused reports it.
            const std::exception_ptr err = std::current_exception();
            for (internal::OccPending* p : batch)
                if (!p->conflict && !p->error)
                    p->error = err;
            return 0;
        }
    }
}

void OptimisticTxn::commit()
{
    Impl& t = active();
    sortUnique(t.touched);
    sortUnique(t.written);
    t.unpinPages();
    EnvImpl& e = *t.env;
    internal::OccPending me;
    me.txn = &t;
    struct Leaving
    {
        std::atomic<unsigned>* count = nullptr;
        ~Leaving()
        {
            if (count)
                count->fetch_sub(1, std::memory_order_relaxed);
        }
    } leaving;
    {
        std::unique_lock<std::mutex> lk(e.occMtx);
        // While another transaction has the turn, its commit goes alone.
        e.occTurn.wait(lk, [&] { return e.occExclusive == nullptr || e.occExclusive == &t; });
        e.occQueue.push_back(&me);
        e.occWaiting.store(unsigned(e.occQueue.size()), std::memory_order_relaxed);
        e.occInCommit.fetch_add(1, std::memory_order_relaxed);
        leaving.count = &e.occInCommit;
        // Lead only when there is work queued: under pipelined commits this
        // commit may already sit in a group whose leader has handed the lead on
        // and waits for the group's meta to land; that leader wakes it.
        const auto mustWait = [&] { return e.occLeader || e.occQueue.empty(); };
        while (!me.done.load(std::memory_order_relaxed)) {
            if (mustWait()) {
                // Without device flushes a group takes microseconds, about what
                // sleeping and waking costs, so look a few times before sleeping.
                lk.unlock();
                for (unsigned i = 0; i < e.occSpins && !me.done.load(std::memory_order_acquire);
                     ++i)
                    std::this_thread::yield();
                lk.lock();
                if (!me.done.load(std::memory_order_relaxed) && mustWait())
                    me.cv.wait(lk);
                continue;
            }
            // No group is being applied: lead the next one. The leader's own
            // commit may be past the group limit, in which case it leads again.
            e.occLeader = true;
            takeBatch(e);
            lk.unlock();
            const std::uint64_t pending = applyBatch(e);
            lk.lock();
            // Hand the lead on first: under pipelined commits the next group's
            // data flush is what carries this group's meta to the device.
            std::vector<internal::OccPending*>& group = t.ledGroup;
            group.swap(e.occBatch);
            e.occLeader = false;
            if (!e.occQueue.empty())
                e.occQueue.front()->cv.notify_one();  // the next leader
            else if (e.occExclusive)
                e.occTurn.notify_all();  // the queue it waits on has drained
            if (pending) {
                lk.unlock();
                std::exception_ptr lost;
                try {
                    internal::awaitDurable(e, pending);
                } catch (...) {
                    lost = std::current_exception();
                }
                lk.lock();
                if (lost)
                    for (internal::OccPending* p : group)
                        if (!p->conflict && !p->error)
                            p->error = lost;
            }
            // Notified under the lock, and before `done`: a member may return
            // and destroy its condition as soon as it can see `done`.
            for (internal::OccPending* p : group) {
                if (p != &me)
                    p->cv.notify_one();
                p->done.store(true, std::memory_order_release);  // last touch of *p
            }
            group.clear();
        }
    }
    const bool conflict = me.conflict;
    const std::exception_ptr error = me.error;
    finish();  // release the snapshot and reader slot before reporting
    if (error)
        std::rethrow_exception(error);
    if (conflict)
        throw Error(ErrorCode::Conflict,
                    "a commit after this transaction's snapshot wrote a key it used");
}

}  // namespace nosql
