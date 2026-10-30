// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// A durable message queue on the store: ready / lease / dead trees keyed by a
// monotonic sequence, peek-lock delivery, and group commit.
//
// Correctness rests on one property: every state change a message makes is a
// delete plus an insert inside a single transaction, so a message is always in
// exactly one tree and a crash can neither duplicate nor drop it.

#include <algorithm>
#include <atomic>
#include <cstring>
#include <mutex>

#include "nosql/internal/mq_record.hpp"
#include "nosql/internal/mq_writer.hpp"
#include "nosql/message_queue.hpp"
#include "nosql/internal/format.hpp"

namespace nosql {

using internal::kMqDeadHdr;
using internal::kMqExternalBody;
using internal::kMqLeaseHdr;
using internal::mqDecode;
using internal::mqEncode;
using internal::MqRecord;
using internal::mqUnwrap;
using internal::mqLeaseDeadline;
using internal::mqLeaseToken;
using internal::mqSeqKey;
using internal::mqWrapDead;
using internal::mqWrapLease;

namespace {

constexpr const char* kNextSeq = "nextSeq";
constexpr const char* kNextToken = "nextToken";

/// Long enough for anything descriptive, short enough that the derived tar
/// member name for an external body stays inside ustar's 100-byte limit.
constexpr std::size_t kMaxQueueName = 48;

std::uint64_t nowNanos()
{
    return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count());
}

std::chrono::system_clock::time_point timePointOf(std::uint64_t nanos)
{
    using D = std::chrono::system_clock::duration;
    return std::chrono::system_clock::time_point(
        std::chrono::duration_cast<D>(std::chrono::nanoseconds(nanos)));
}

std::uint64_t nanosOf(std::chrono::milliseconds d)
{
    return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(d).count());
}

internal::Little<std::uint64_t> seqKey(std::uint64_t seq) noexcept
{
    return mqSeqKey(seq);
}

std::uint64_t scalar(const Db& meta, const char* key, std::uint64_t dflt = 0)
{
    if (auto v = meta.get(key))
        return v->as<internal::Little<std::uint64_t>>();
    return dflt;
}

void setScalar(const Db& meta, const char* key, std::uint64_t v)
{
    meta.put(key, Slice::ref(internal::Little<std::uint64_t>(v)));
}

/// The four trees, reopened per transaction because a Db handle is scoped to
/// the transaction that produced it.
struct MqTrees
{
    Db ready, lease, dead, meta;
};

}  // namespace

struct MessageQueue::Impl
{
    Env* env = nullptr;
    std::string name;
    std::string readyDb, leaseDb, deadDb, metaDb;
    MessageQueue::Options opt;
    std::optional<BlobStorage> blobs;
    internal::Identity queueId{};
    std::atomic<std::uint64_t> blobSeq{0};
    std::uint64_t stamp = 0;
    /// Declared last so its thread is joined before anything it touches goes.
    std::unique_ptr<internal::MqWriter> writer;

    MqTrees open(Txn& t) const
    {
        if (!t.belongsTo(*env))
            throw Error(ErrorCode::BadTransaction, "queue transaction belongs to another environment");
        const auto seqFlags = DbFlags::Create | DbFlags::IntegerKey;
        return MqTrees{t.db(readyDb, seqFlags), t.db(leaseDb, seqFlags), t.db(deadDb, seqFlags),
                       t.db(metaDb, DbFlags::Create)};
    }

    std::uint64_t ttlNanos() const { return nanosOf(opt.ttl_); }
    std::uint64_t leaseNanos() const { return nanosOf(opt.lease_); }

    /// Appends one already-encoded body. Returns the sequence it landed on.
    std::uint64_t append(const MqTrees& tr, std::uint64_t seq, Slice body, std::string_view cid,
                         bool external) const
    {
        MqRecord r;
        r.enqueuedAtNanos = nowNanos();
        r.expiresAtNanos = ttlNanos() ? r.enqueuedAtNanos + ttlNanos() : 0;
        r.flags = external ? kMqExternalBody : 0;
        r.correlationId = cid;
        r.body = body;
        // Strictly ascending, so this takes the dense bulk-load path rather
        // than splitting the right-most leaf in half on every message.
        tr.ready.put(Slice::ref(seqKey(seq)), mqEncode(r), PutMode::Append);
        return seq;
    }

    /// Writes a large body to the side-car archive and returns its key.
    ///
    /// This commits on its own, before the queue transaction, because
    /// BlobStorage takes the store's write lock itself and the store's writer
    /// is not reentrant. A crash in between leaves an unreferenced blob --
    /// archive garbage, which is what a repack is for -- never a message
    /// pointing at bytes that are not there.
    std::string stashBlob(Slice body)
    {
        char buf[128];
        std::snprintf(buf, sizeof buf, "%s-%llu-%llu", name.c_str(), (unsigned long long)stamp,
                      (unsigned long long)blobSeq.fetch_add(1, std::memory_order_relaxed));
        std::string key = buf;
        blobs->put(Slice(key), key, body);
        return key;
    }

    bool isExternal(Slice body) const
    {
        return opt.externalAt_ && body.size() >= opt.externalAt_ && blobs.has_value();
    }

    Message decode(std::uint64_t seq, Slice wire) const
    {
        const MqRecord r = mqDecode(wire);
        Message m;
        m.sequence = seq;
        m.deliveryCount = r.deliveryCount;
        m.enqueuedAt = timePointOf(r.enqueuedAtNanos);
        m.expiresAt = timePointOf(r.expiresAtNanos);
        m.correlationId = r.correlationId;
        if (r.flags & kMqExternalBody)
            m.blobKey = r.body.string();
        else
            m.body = r.body.string();
        return m;
    }

    void toDead(const MqTrees& tr, std::uint64_t seq, Slice wire, DeadReason why,
                std::uint64_t at) const
    {
        tr.dead.put(Slice::ref(seqKey(seq)), mqWrapDead(std::uint8_t(why), at, wire));
        tr.ready.erase(Slice::ref(seqKey(seq)));
    }

    /// Leases the head message, skipping (and retiring) anything undeliverable.
    /// Sets `changed` if the transaction was modified, which can happen even
    /// when nothing is returned: retiring the last message is a write.
    Receipt take(const MqTrees& tr, bool& changed)
    {
        for (;;) {
            std::uint64_t seq = 0;
            std::string wire;
            {
                Cursor c = tr.ready.cursor();
                if (!c.first())
                    return Receipt{};
                seq = c.key().as<internal::Little<std::uint64_t>>();
                wire = c.value().string();
            }

            const std::uint64_t now = nowNanos();
            MqRecord r = mqDecode(wire);
            if (r.expiresAtNanos && r.expiresAtNanos <= now) {
                toDead(tr, seq, wire, DeadReason::Expired, now);
                changed = true;
                continue;
            }
            if (r.deliveryCount + 1 > opt.maxDeliveries_) {
                toDead(tr, seq, wire, DeadReason::TooManyDeliveries, now);
                changed = true;
                continue;
            }

            ++r.deliveryCount;
            const std::string rec = mqEncode(r);
            const std::uint64_t token = scalar(tr.meta, kNextToken, 1);
            if (token == UINT64_MAX)
                throw Error(ErrorCode::MapFull, "queue delivery token space exhausted");
            setScalar(tr.meta, kNextToken, token + 1);
            tr.lease.put(Slice::ref(seqKey(seq)), mqWrapLease(now + leaseNanos(), token, rec));
            tr.ready.erase(Slice::ref(seqKey(seq)));
            changed = true;

            Receipt out;
            out.msg_ = decode(seq, rec);
            out.token_ = token;
            out.queueId_ = queueId;
            out.env_ = env;
            return out;
        }
    }

    bool settle(const MqTrees& tr, std::uint64_t seq, std::uint64_t token, bool requeue,
                bool deadLetter) const
    {
        const auto leased = tr.lease.get(Slice::ref(seqKey(seq)));
        if (!leased)
            return false;
        const Slice wire = mqUnwrap(*leased, kMqLeaseHdr);
        if (mqLeaseToken(*leased) != token)
            return false;
        if (deadLetter) {
            tr.dead.put(Slice::ref(seqKey(seq)), mqWrapDead(std::uint8_t(DeadReason::Rejected), nowNanos(),
                                                    wire));
        } else if (requeue) {
            // Back under its original sequence, so a message that keeps failing
            // cannot overtake fresh traffic on every retry.
            tr.ready.put(Slice::ref(seqKey(seq)), wire);
        }
        tr.lease.erase(Slice::ref(seqKey(seq)));
        return true;
    }

    void validate(const Receipt& receipt) const
    {
        if (receipt.queueId_ != queueId || receipt.env_ != env)
            throw Error(ErrorCode::BadTransaction, "receipt belongs to another queue or environment");
    }

    /// Takes a receipt out of the caller's hands: validates it, returns the
    /// (sequence, token) pair to settle, and leaves the receipt invalid.
    std::pair<std::uint64_t, std::uint64_t> claim(Receipt&& receipt) const
    {
        validate(receipt);
        const std::pair<std::uint64_t, std::uint64_t> out{receipt.sequence(), receipt.token_};
        receipt = Receipt{};
        return out;
    }
};

// ------------------------------------------------------------------ open ---

MessageQueue::MessageQueue(MessageQueue&&) noexcept = default;
MessageQueue& MessageQueue::operator=(MessageQueue&&) noexcept = default;
MessageQueue::~MessageQueue() = default;

MessageQueue MessageQueue::open(Env& env, Slice name, const Options& options)
{
    if (options.lease_.count() <= 0 || options.ttl_.count() < 0 || options.maxDeliveries_ == 0 ||
        options.linger_.count() < 0)
        throw Error(ErrorCode::InvalidArgument, "invalid queue lease, TTL, delivery limit, or linger");
    if (name.empty() || name.size() > kMaxQueueName)
        throw Error(ErrorCode::InvalidArgument, "queue name must be 1.." +
                                                    std::to_string(kMaxQueueName) + " bytes");
    if (name.view().find('/') != std::string_view::npos)
        throw Error(ErrorCode::InvalidArgument, "queue name must not contain '/'");

    auto im = std::make_unique<Impl>();
    im->env = &env;
    im->name = name.string();
    im->opt = options;
    im->stamp = nowNanos();
    const std::string base = "mq/" + im->name + "/";
    im->readyDb = base + "ready";
    im->leaseDb = base + "lease";
    im->deadDb = base + "dead";
    im->metaDb = base + "meta";

    if (options.externalAt_)
        // The index name carries '/' separators, which would send the sidecar
        // archive into a directory that does not exist; name it explicitly.
        im->blobs = BlobStorage::open(
            env, Slice(base + "blobs"),
            BlobStorage::Options().syncOnAppend(true).fileName(env.path().stem().string() + "-mq-" + im->name +
                                            ".tar"));

    env.write([&](Txn& t) {
        MqTrees tr = im->open(t);
        if (!tr.meta.contains(kNextSeq))
            setScalar(tr.meta, kNextSeq, 1);
        if (auto identity = tr.meta.get("queueId")) {
            if (identity->size() != sizeof(internal::Identity))
                throw Error(ErrorCode::Corrupted, "invalid queue identity");
            im->queueId = identity->as<internal::Identity>();
        } else {
            im->queueId = internal::newIdentity();
            tr.meta.put("queueId", Slice::ref(im->queueId));
        }
        if (!tr.meta.contains(kNextToken)) {
            std::uint64_t next = 1;
            for (auto [key, value] : tr.lease.all()) {
                (void)key;
                mqUnwrap(value, kMqLeaseHdr);
                const auto token = mqLeaseToken(value);
                if (token == UINT64_MAX)
                    throw Error(ErrorCode::MapFull, "queue token space exhausted");
                next = std::max(next, token + 1);
            }
            setScalar(tr.meta, kNextToken, next);
        }
        tr.meta.erase("readyCount");
        tr.meta.erase("leaseCount");
        tr.meta.erase("deadCount");
    });

    if (options.batch_) {
        Impl* raw = im.get();
        im->writer = std::make_unique<internal::MqWriter>(
            [raw](const std::vector<std::shared_ptr<internal::MqWriter::Item>>& batch) {
                raw->env->write([&](Txn& t) {
                    MqTrees tr = raw->open(t);
                    std::uint64_t seq = scalar(tr.meta, kNextSeq, 1);
                    for (const auto& item : batch)
                        item->sequence = raw->append(tr, seq++, Slice(item->body),
                                                     item->correlationId, item->external);
                    setScalar(tr.meta, kNextSeq, seq);
                });
            },
            options.batch_, options.linger_, options.intakeLimit_);
    }

    MessageQueue q;
    q.impl_ = std::move(im);
    return q;
}

MessageQueue MessageQueue::open(Env& env, Slice name)
{
    return open(env, name, Options());
}

const std::string& MessageQueue::name() const
{
    if (!impl_)
        throw Error(ErrorCode::InvalidArgument, "queue is closed");
    return impl_->name;
}

Env& MessageQueue::env() const
{
    if (!impl_)
        throw Error(ErrorCode::InvalidArgument, "queue is closed");
    return *impl_->env;
}

// --------------------------------------------------------------- produce ---

std::uint64_t MessageQueue::send(Slice body, std::string_view correlationId)
{
    Impl& im = *impl_;
    std::string blobKey;
    const bool external = im.isExternal(body);
    if (external)
        blobKey = im.stashBlob(body);
    const Slice stored = external ? Slice(blobKey) : body;

    if (im.writer)
        return im.writer->submit(stored.string(), std::string(correlationId), external);

    return im.env->write([&](Txn& t) {
        MqTrees tr = im.open(t);
        const std::uint64_t seq = scalar(tr.meta, kNextSeq, 1);
        im.append(tr, seq, stored, correlationId, external);
        setScalar(tr.meta, kNextSeq, seq + 1);
        return seq;
    });
}

std::uint64_t MessageQueue::send(Txn& txn, Slice body, std::string_view correlationId)
{
    Impl& im = *impl_;
    if (im.isExternal(body))
        throw Error(ErrorCode::Unsupported,
                    "external bodies need their own transaction; use send(body)");
    MqTrees tr = im.open(txn);
    const std::uint64_t seq = scalar(tr.meta, kNextSeq, 1);
    im.append(tr, seq, body, correlationId, false);
    setScalar(tr.meta, kNextSeq, seq + 1);
    return seq;
}

std::vector<std::uint64_t> MessageQueue::sendBatch(const Slice* bodies, std::size_t count)
{
    Impl& im = *impl_;
    std::vector<std::string> keys;
    std::vector<Slice> stored;
    if (count)
        stored.assign(bodies, bodies + count);
    std::vector<bool> external(count, false);
    if (im.opt.externalAt_) {
        keys.resize(count);
        for (std::size_t i = 0; i < count; ++i)
            if (im.isExternal(bodies[i])) {
                keys[i] = im.stashBlob(bodies[i]);
                stored[i] = Slice(keys[i]);
                external[i] = true;
            }
    }

    return im.env->write([&](Txn& t) {
        MqTrees tr = im.open(t);
        std::uint64_t seq = scalar(tr.meta, kNextSeq, 1);
        std::vector<std::uint64_t> out;
        out.reserve(count);
        for (std::size_t i = 0; i < stored.size(); ++i)
            out.push_back(im.append(tr, seq++, stored[i], {}, external[i]));
        setScalar(tr.meta, kNextSeq, seq);
        return out;
    });
}

// --------------------------------------------------------------- consume ---

std::optional<Message> MessageQueue::peek() const
{
    Impl& im = *impl_;
    return im.env->read([&](Txn& t) -> std::optional<Message> {
        MqTrees tr = im.open(t);
        Cursor c = tr.ready.cursor();
        if (!c.first())
            return std::nullopt;
        return im.decode(c.key().as<internal::Little<std::uint64_t>>(), c.value());
    });
}

Receipt MessageQueue::tryReceive()
{
    Impl& im = *impl_;
    // Probe under a read snapshot first: readers never block, so consumers
    // that lose the race cost nothing, while reaching straight for the write
    // lock would serialise every one of them behind an empty queue.
    if (readyCount() == 0)
        return Receipt{};

    Txn t = im.env->writeTxn();
    MqTrees tr = im.open(t);
    bool changed = false;
    Receipt r = im.take(tr, changed);
    // An attempt that changed nothing must not commit: a commit bumps the
    // generation and would wake every other waiter for nothing.
    if (changed)
        t.commit();
    else
        t.abort();
    return r;
}

Receipt MessageQueue::receive(Txn& txn)
{
    Impl& im = *impl_;
    bool changed = false;
    return im.take(im.open(txn), changed);
}

Receipt MessageQueue::receive(std::chrono::milliseconds timeout)
{
    Impl& im = *impl_;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        // Sampled before the attempt, so a send landing between the two does
        // not get slept through.
        const std::uint64_t seen = im.env->commitGeneration();
        if (Receipt r = tryReceive(); r.valid())
            return r;
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
            return Receipt{};
        im.env->waitForCommit(seen,
                              std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
    }
}

std::vector<Receipt> MessageQueue::receiveBatch(std::size_t max, std::chrono::milliseconds timeout)
{
    Impl& im = *impl_;
    std::vector<Receipt> out;
    if (max == 0)
        return out;

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const std::uint64_t seen = im.env->commitGeneration();
        if (readyCount() != 0) {
            Txn t = im.env->writeTxn();
            MqTrees tr = im.open(t);
            bool changed = false;
            while (out.size() < max) {
                Receipt r = im.take(tr, changed);
                if (!r.valid())
                    break;
                out.push_back(std::move(r));
            }
            if (changed)
                t.commit();
            else
                t.abort();
            if (!out.empty())
                return out;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
            return out;
        im.env->waitForCommit(seen,
                              std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
    }
}

// ---------------------------------------------------------------- settle ---

void MessageQueue::ack(Receipt&& r)
{
    nack(std::move(r), false, /*requeue=*/false);
}

void MessageQueue::ack(Txn& txn, Receipt&& r)
{
    if (!r.valid())
        return;
    Impl& im = *impl_;
    const auto [seq, token] = im.claim(std::move(r));
    if (!im.settle(im.open(txn), seq, token, false, false)) {
        txn.abort();
        throw Error(ErrorCode::BadTransaction, "stale receipt cannot acknowledge transactional effects");
    }
}

void MessageQueue::ackAll(std::vector<Receipt>&& rs)
{
    if (rs.empty())
        return;
    Impl& im = *impl_;
    std::vector<std::pair<std::uint64_t, std::uint64_t>> settle;
    settle.reserve(rs.size());
    for (Receipt& r : rs)
        if (r.valid())
            settle.push_back(im.claim(std::move(r)));
    rs.clear();
    if (settle.empty())
        return;
    im.env->write([&](Txn& t) {
        MqTrees tr = im.open(t);
        for (const auto& [seq, token] : settle)
            im.settle(tr, seq, token, false, false);
    });
}

void MessageQueue::nack(Receipt&& r, bool deadLetter)
{
    nack(std::move(r), deadLetter, /*requeue=*/true);
}

/// ack() and nack() differ only in whether the message goes back to the ready
/// tree; a stale receipt (lease expired and swept) settles nothing.
void MessageQueue::nack(Receipt&& r, bool deadLetter, bool requeue)
{
    if (!r.valid())
        return;
    Impl& im = *impl_;
    const auto [seq, token] = im.claim(std::move(r));
    Txn txn = im.env->writeTxn();
    if (im.settle(im.open(txn), seq, token, requeue, deadLetter))
        txn.commit();
    else
        txn.abort();
}

// -------------------------------------------------------------- operate ---

std::uint64_t MessageQueue::readyCount() const
{
    Impl& im = *impl_;
    return im.env->read([&](Txn& t) { return im.open(t).ready.count(); });
}

std::uint64_t MessageQueue::leasedCount() const
{
    Impl& im = *impl_;
    return im.env->read([&](Txn& t) { return im.open(t).lease.count(); });
}

std::uint64_t MessageQueue::deadCount() const
{
    Impl& im = *impl_;
    return im.env->read([&](Txn& t) { return im.open(t).dead.count(); });
}

std::uint64_t MessageQueue::sweep()
{
    Impl& im = *impl_;
    const std::uint64_t now = nowNanos();

    // Collect under a read snapshot so a sweep with nothing to do costs no
    // commit -- a periodic sweeper that committed every tick would wake every
    // waiter in the process forever.
    struct Candidate { std::uint64_t sequence, token, deadline; };
    std::vector<Candidate> due;
    im.env->read([&](Txn& t) {
        for (auto [key, value] : im.open(t).lease.all()) {
            mqUnwrap(value, kMqLeaseHdr);
            const auto deadline = mqLeaseDeadline(value);
            if (deadline <= now)
                due.push_back({key.as<internal::Little<std::uint64_t>>(), mqLeaseToken(value), deadline});
            if (due.size() == 256)
                break;
        }
    });
    if (due.empty())
        return 0;

    return im.env->write([&](Txn& t) {
        MqTrees tr = im.open(t);
        std::uint64_t moved = 0;
        for (const auto& [seq, token, deadline] : due) {
            // Re-check inside the write transaction: the consumer may have
            // acked between the scan and here.
            const auto current = tr.lease.get(Slice::ref(seqKey(seq)));
            if (!current)
                continue;
            const Slice wire = mqUnwrap(*current, kMqLeaseHdr);
            if (mqLeaseDeadline(*current) != deadline || mqLeaseToken(*current) != token)
                continue;
            tr.ready.put(Slice::ref(seqKey(seq)), wire);
            tr.lease.erase(Slice::ref(seqKey(seq)));
            ++moved;
        }
        return moved;
    });
}

std::vector<Message> MessageQueue::deadLetters(std::size_t max) const
{
    Impl& im = *impl_;
    return im.env->read([&](Txn& t) {
        std::vector<Message> out;
        for (auto [k, v] : im.open(t).dead.all()) {
            if (out.size() >= max)
                break;
            out.push_back(im.decode(k.as<internal::Little<std::uint64_t>>(), mqUnwrap(v, kMqDeadHdr)));
        }
        return out;
    });
}

std::uint64_t MessageQueue::purgeDeadLetters()
{
    Impl& im = *impl_;
    return im.env->write([&](Txn& t) {
        MqTrees tr = im.open(t);
        const std::uint64_t n = tr.dead.count();
        tr.dead.clear();
        return n;
    });
}

Blob MessageQueue::fetchBody(const Message& m) const
{
    Impl& im = *impl_;
    if (!m.hasExternalBody() || !im.blobs)
        return Blob{};
    return im.blobs->find(Slice(m.blobKey));
}

const char* toString(DeadReason r) noexcept
{
    switch (r) {
        case DeadReason::Expired: return "expired";
        case DeadReason::TooManyDeliveries: return "too many deliveries";
        case DeadReason::Rejected: return "rejected";
    }
    return "unknown";
}

}  // namespace nosql
