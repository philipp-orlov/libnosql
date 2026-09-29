// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// A durable, MSMQ-shaped message queue on top of the store.
//
// Messages live in B+trees keyed by a monotonic sequence number, so delivery
// is FIFO and enqueue lands on the ascending-key fast path. Receive is
// peek-lock: a message moves to a lease tree and comes back automatically if
// the consumer dies before acking, so delivery is at-least-once.
//
// Durable commits are storage-bound at roughly a hundred per second, which is
// two to four orders of magnitude below what the tree itself can absorb. Batch
// or turn on group commit; a queue that commits once per message is measuring
// the disk, not the queue. See docs/mq.md.
#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "nosql/blob_storage.hpp"
#include "nosql/nosql.hpp"
#include "nosql/slice.hpp"

namespace nosql {

/// Why a message ended up in the dead-letter tree.
enum class DeadReason : std::uint8_t
{
    Expired = 0,        ///< its time-to-live ran out before it was delivered
    TooManyDeliveries,  ///< redelivered past Options::maxDeliveries
    Rejected,           ///< a consumer nacked it with deadLetter = true
};

const char* toString(DeadReason) noexcept;

/// One message, owning its own bytes.
///
/// Unlike a Slice read from a transaction, a Message stays valid after the
/// transaction that produced it has ended -- receive commits before returning,
/// so a borrowed view would have to pin a snapshot, and a queue consumer
/// holding a snapshot open blocks page reclamation on the whole store. The
/// copy is the price of not having that footgun.
///
/// Bodies above Options::externalBodyThreshold are the exception: they live in
/// a side-car blob archive, `body` is empty, and MessageQueue::fetchBody()
/// returns them zero-copy.
struct Message
{
    std::uint64_t sequence = 0;
    std::uint32_t deliveryCount = 0;  ///< 1 on first delivery
    std::chrono::system_clock::time_point enqueuedAt{};
    std::chrono::system_clock::time_point expiresAt{};  ///< epoch = never
    std::string correlationId;
    std::string body;
    std::string blobKey;  ///< non-empty when the body is external

    bool hasExternalBody() const noexcept { return !blobKey.empty(); }
};

/// An outstanding lease, i.e. a message that has been delivered but not yet
/// acknowledged.
///
/// Dropping a Receipt without ack() or nack() is not a loss: the lease expires
/// and MessageQueue::sweep() puts the message back. It is a redelivery, which
/// is exactly what happens when a consumer process dies.
class Receipt
{
public:
    Receipt() = default;
    Receipt(Receipt&&) noexcept = default;
    Receipt& operator=(Receipt&&) noexcept = default;
    Receipt(const Receipt&) = delete;
    Receipt& operator=(const Receipt&) = delete;

    bool valid() const noexcept { return msg_.sequence != 0; }
    explicit operator bool() const noexcept { return valid(); }
    const Message& message() const noexcept { return msg_; }
    std::uint64_t sequence() const noexcept { return msg_.sequence; }

private:
    friend class MessageQueue;
    Message msg_;
    /// Identifies this particular delivery. A receipt whose lease expired and
    /// was swept back to the queue carries a stale token, and settling it must
    /// not disturb whoever holds the message now.
    std::uint64_t token_ = 0;
    std::array<std::uint64_t, 2> queueId_{};
    const Env* env_ = nullptr;
};

/// A named queue inside a store.
///
/// Threading follows the store's model: any number of threads may read, and
/// writes serialise on the store's single writer. Move-only, and it borrows
/// the Env, which must outlive it.
class MessageQueue
{
public:
    class Options
    {
    public:
        /// How long a delivered message stays leased before sweep() returns it
        /// to the ready tree. Make it comfortably longer than the slowest
        /// expected handler; too short means duplicate delivery of messages
        /// that are still being processed.
        Options& leaseDuration(std::chrono::milliseconds d)
        {
            lease_ = d;
            return *this;
        }
        /// Deliveries before a message is dead-lettered. Bounds the damage a
        /// message that crashes its consumer can do.
        Options& maxDeliveries(std::uint32_t n)
        {
            maxDeliveries_ = n;
            return *this;
        }
        /// Time-to-live stamped onto each message at send. Zero means never.
        /// Enforced when the message reaches the head of the queue, so an
        /// expired message is never delivered, but it does occupy space until
        /// a consumer walks past it.
        Options& messageTtl(std::chrono::milliseconds d)
        {
            ttl_ = d;
            return *this;
        }
        /// Bodies at or above this size go to a side-car blob archive instead
        /// of into the tree. Zero (the default) keeps every body inline.
        ///
        /// Copy-on-write copies every page on the path from root to leaf, so a
        /// multi-megabyte inline value allocates and frees a matching overflow
        /// chain per message. Above a few hundred kilobytes the archive is
        /// cheaper. The trade is that blob bytes are never reclaimed by
        /// acking -- see the note on fetchBody().
        Options& externalBodyThreshold(std::size_t bytes)
        {
            externalAt_ = bytes;
            return *this;
        }
        /// Batch sends behind one transaction on a dedicated writer thread.
        /// `maxBatch` bounds transaction size, `linger` bounds latency when
        /// traffic is thin. Off by default.
        ///
        /// This is the difference between a hundred and a hundred thousand
        /// messages a second: the fsync is per transaction, not per message.
        Options& groupCommit(std::size_t maxBatch, std::chrono::microseconds linger)
        {
            batch_ = maxBatch;
            linger_ = linger;
            return *this;
        }
        Options& intakeLimit(std::size_t bytes)
        {
            intakeLimit_ = bytes;
            return *this;
        }

    private:
        friend class MessageQueue;
        std::chrono::milliseconds lease_{30000};
        std::chrono::milliseconds ttl_{0};
        std::uint32_t maxDeliveries_ = 10;
        std::size_t externalAt_ = 0;
        std::size_t batch_ = 0;
        std::chrono::microseconds linger_{1000};
        std::size_t intakeLimit_ = 16u << 20;
    };

    MessageQueue() = default;
    MessageQueue(MessageQueue&&) noexcept;
    MessageQueue& operator=(MessageQueue&&) noexcept;
    MessageQueue(const MessageQueue&) = delete;
    MessageQueue& operator=(const MessageQueue&) = delete;
    ~MessageQueue();

    /// Opens (creating if needed) the queue called `name`. Its trees are
    /// sub-databases called `mq/<name>/ready`, `/lease` and `/dead`, plus a
    /// `/meta` holds queue identity and sequence/delivery counters; tree counts are authoritative.
    ///
    /// That is four sub-database handles per queue against a default
    /// maxDbs of 128; raise Env::Options::maxDbs for many queues.
    static MessageQueue open(Env& env, Slice name, const Options& options);
    static MessageQueue open(Env& env, Slice name);

    bool valid() const noexcept { return impl_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }
    const std::string& name() const;
    /// The store the queue lives in, for opening the transaction that spans a
    /// receive, the work it triggers and its acknowledgement.
    Env& env() const;

    // --- produce ---

    /// Appends one message and returns its sequence number. Commits before
    /// returning, or hands the message to the group-commit writer and waits
    /// for that batch to commit.
    std::uint64_t send(Slice body, std::string_view correlationId = {});
    /// Appends inside a transaction the caller owns, so the message becomes
    /// visible exactly when the caller's other writes do. Never batched.
    std::uint64_t send(Txn& txn, Slice body, std::string_view correlationId = {});
    /// Appends many messages in one transaction: one fsync for the batch.
    std::vector<std::uint64_t> sendBatch(const Slice* bodies, std::size_t count);
    template <class Container>
    std::vector<std::uint64_t> sendBatch(const Container& bodies)
    {
        return sendBatch(std::data(bodies), std::size(bodies));
    }

    // --- consume ---

    /// Looks at the head without leasing or hiding it. Costs a read snapshot,
    /// never the write lock, so it is the cheap "is there anything there"
    /// probe.
    std::optional<Message> peek() const;

    /// Leases the head message, or returns an invalid Receipt when the queue
    /// is empty. Expired and over-delivered messages are moved aside on the
    /// way, in the same transaction.
    Receipt tryReceive();
    /// tryReceive(), then wait for a commit and retry until `timeout` elapses.
    /// Woken by any commit to the store, so it re-probes with a read snapshot
    /// before reaching for the write lock.
    Receipt receive(std::chrono::milliseconds timeout);
    /// Up to `max` messages leased in one transaction. The throughput path:
    /// one fsync for the whole batch.
    std::vector<Receipt> receiveBatch(std::size_t max, std::chrono::milliseconds timeout);
    /// Leases inside a transaction the caller owns. Aborting that transaction
    /// puts the message back as though it was never delivered, which is how a
    /// consumer makes its side effects and its dequeue atomic.
    Receipt receive(Txn& txn);

    // --- settle ---

    /// Done with it: drops the lease and the message.
    void ack(Receipt&& r);
    /// A stale receipt aborts the caller's transaction and raises BadTransaction.
    void ack(Txn& txn, Receipt&& r);
    /// One transaction for the whole batch. Acking one at a time is
    /// fsync-bound at the same rate committing one at a time is.
    void ackAll(std::vector<Receipt>&& rs);
    /// Puts the message back at its original sequence number, so a poison
    /// message cannot overtake fresh traffic. With `deadLetter` it goes
    /// straight to the dead-letter tree instead.
    void nack(Receipt&& r, bool deadLetter = false);

    // --- operate ---

    std::uint64_t readyCount() const;
    std::uint64_t leasedCount() const;
    std::uint64_t deadCount() const;

    /// Returns expired leases to the ready tree and reports how many moved.
    /// Scans leases and revalidates at most 256 expired deliveries per call.
    std::uint64_t sweep();

    std::vector<Message> deadLetters(std::size_t max) const;
    /// Drops every dead letter. Returns how many.
    std::uint64_t purgeDeadLetters();

    /// The bytes of an external body, zero-copy out of the blob archive.
    /// Invalid for a message whose body is inline.
    ///
    /// Acking an external-bodied message does not reclaim its blob: the
    /// archive is append-only and only a repack shrinks it. Treat external
    /// bodies as a space-for-speed trade with a compaction chore attached.
    Blob fetchBody(const Message& m) const;

private:
    struct Impl;
    void nack(Receipt&& r, bool deadLetter, bool requeue);
    std::unique_ptr<Impl> impl_;
};

}  // namespace nosql
