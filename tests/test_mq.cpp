// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Message queue semantics: FIFO delivery, peek-lock, lease expiry,
// dead-lettering, notification, group commit and transactional receive.
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "nosql/message_queue.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
using namespace std::chrono_literals;

namespace {

MessageQueue::Options fastLease(std::chrono::milliseconds d)
{
    return MessageQueue::Options().leaseDuration(d);
}

}  // namespace

TEST(sendsAreDeliveredInOrderWithAscendingSequences)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    for (int i = 0; i < 50; ++i)
        CHECK_EQ(q.send(tst::keyOf(std::uint64_t(i), "body")), std::uint64_t(i + 1));
    CHECK_EQ(q.readyCount(), std::uint64_t(50));

    for (int i = 0; i < 50; ++i) {
        Receipt r = q.tryReceive();
        CHECK(r.valid());
        CHECK_EQ(r.sequence(), std::uint64_t(i + 1));
        CHECK_EQ(r.message().body, tst::keyOf(std::uint64_t(i), "body"));
        CHECK_EQ(r.message().deliveryCount, 1u);
        q.ack(std::move(r));
    }
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
    CHECK(!q.tryReceive().valid());
}

TEST(countersAndSequencesSurviveReopen)
{
    tst::Scratch s("mq");
    std::uint64_t lastSeq = 0;
    {
        Env e = Env::configure().open(s.file());
        MessageQueue q = MessageQueue::open(e, "orders");
        for (int i = 0; i < 5; ++i)
            lastSeq = q.send("x");
        Receipt r = q.tryReceive();  // leave one leased across the reopen
        CHECK(r.valid());
        CHECK_EQ(q.readyCount(), std::uint64_t(4));
        CHECK_EQ(q.leasedCount(), std::uint64_t(1));
    }
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");
    CHECK_EQ(q.readyCount(), std::uint64_t(4));
    CHECK_EQ(q.leasedCount(), std::uint64_t(1));
    CHECK_EQ(q.send("y"), lastSeq + 1);
}

TEST(peekDoesNotConsumeOrLease)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");
    CHECK(!q.peek().has_value());

    q.send("first", "corr-1");
    q.send("second");
    for (int i = 0; i < 3; ++i) {
        auto m = q.peek();
        CHECK(m.has_value());
        CHECK_EQ(m->sequence, std::uint64_t(1));
        CHECK_EQ(m->body, std::string("first"));
        CHECK_EQ(m->correlationId, std::string("corr-1"));
    }
    CHECK_EQ(q.readyCount(), std::uint64_t(2));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
}

TEST(anEmptyReceiveDoesNotCommit)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    // A poll that finds nothing must not bump the commit generation: it would
    // wake every other waiter in the process for no reason.
    const std::uint64_t gen = e.commitGeneration();
    for (int i = 0; i < 20; ++i)
        CHECK(!q.tryReceive().valid());
    CHECK_EQ(e.commitGeneration(), gen);
}

TEST(nackRedeliversUnderTheOriginalSequence)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    q.send("poison");
    q.send("fresh");

    Receipt r = q.tryReceive();
    CHECK_EQ(r.sequence(), std::uint64_t(1));
    q.nack(std::move(r));
    CHECK_EQ(q.readyCount(), std::uint64_t(2));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));

    // Still ahead of the message sent after it, and its delivery count stuck.
    Receipt again = q.tryReceive();
    CHECK_EQ(again.sequence(), std::uint64_t(1));
    CHECK_EQ(again.message().deliveryCount, 2u);
}

TEST(nackWithDeadLetterSkipsTheReadyTree)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    q.send("bad");
    q.nack(q.tryReceive(), true);
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
    CHECK_EQ(q.deadCount(), std::uint64_t(1));

    const auto dead = q.deadLetters(10);
    CHECK_EQ(dead.size(), std::size_t(1));
    CHECK_EQ(dead[0].body, std::string("bad"));
    CHECK_EQ(q.purgeDeadLetters(), std::uint64_t(1));
    CHECK_EQ(q.deadCount(), std::uint64_t(0));
}

TEST(maxDeliveriesDeadLettersThePoisonMessage)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders", MessageQueue::Options().maxDeliveries(3));

    q.send("poison");
    q.send("good");
    for (int i = 1; i <= 3; ++i) {
        Receipt r = q.tryReceive();
        CHECK_EQ(r.sequence(), std::uint64_t(1));
        CHECK_EQ(r.message().deliveryCount, std::uint32_t(i));
        q.nack(std::move(r));
    }

    // The fourth attempt retires it and moves on to the message behind it, all
    // in one transaction.
    Receipt r = q.tryReceive();
    CHECK_EQ(r.sequence(), std::uint64_t(2));
    CHECK_EQ(q.deadCount(), std::uint64_t(1));
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
}

TEST(expiredMessagesAreNeverDelivered)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders", MessageQueue::Options().messageTtl(20ms));

    q.send("stale-1");
    q.send("stale-2");
    std::this_thread::sleep_for(60ms);
    q.send("fresh");

    Receipt r = q.tryReceive();
    CHECK(r.valid());
    CHECK_EQ(r.message().body, std::string("fresh"));
    CHECK_EQ(q.deadCount(), std::uint64_t(2));
}

TEST(anAbandonedLeaseComesBackAfterItExpires)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders", fastLease(30ms));

    q.send("work");
    {
        Receipt r = q.tryReceive();  // dropped without ack, as a dead consumer would
        CHECK(r.valid());
    }
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.sweep(), std::uint64_t(0));  // not due yet

    std::this_thread::sleep_for(60ms);
    CHECK_EQ(q.sweep(), std::uint64_t(1));
    CHECK_EQ(q.readyCount(), std::uint64_t(1));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
    CHECK_EQ(q.tryReceive().message().deliveryCount, 2u);
}

TEST(sweepWithNothingDueCostsNoCommit)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");
    q.send("work");

    const std::uint64_t gen = e.commitGeneration();
    for (int i = 0; i < 10; ++i)
        CHECK_EQ(q.sweep(), std::uint64_t(0));
    CHECK_EQ(e.commitGeneration(), gen);
}

TEST(aBlockedConsumerIsWokenByASend)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    std::thread producer([&] {
        std::this_thread::sleep_for(30ms);
        q.send("late");
    });
    const auto t0 = std::chrono::steady_clock::now();
    Receipt r = q.receive(5s);
    const auto waited = std::chrono::steady_clock::now() - t0;
    producer.join();

    CHECK(r.valid());
    CHECK_EQ(r.message().body, std::string("late"));
    CHECK(waited < 2s);  // woken, not timed out
}

TEST(receiveReturnsAnInvalidReceiptOnTimeout)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    const auto t0 = std::chrono::steady_clock::now();
    CHECK(!q.receive(60ms).valid());
    CHECK(std::chrono::steady_clock::now() - t0 >= 50ms);
}

TEST(noWakeupIsLostWhenTheSendRacesTheReceive)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    // The producer sends exactly as often as the consumer receives, with no
    // handshake between them. A lost wakeup shows up as the consumer timing
    // out while a message sits in the queue.
    constexpr int kRounds = 500;
    std::atomic<bool> go{false};
    std::thread producer([&] {
        while (!go.load(std::memory_order_acquire)) {
        }
        for (int i = 0; i < kRounds; ++i)
            q.send(tst::keyOf(std::uint64_t(i)));
    });

    go.store(true, std::memory_order_release);
    for (int i = 0; i < kRounds; ++i) {
        Receipt r = q.receive(10s);
        CHECK(r.valid());
        q.ack(std::move(r));
    }
    producer.join();
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
}

TEST(receiveBatchTakesManyMessagesInOneTransaction)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    std::vector<std::string> bodies;
    std::vector<Slice> slices;
    for (int i = 0; i < 64; ++i)
        bodies.push_back(tst::keyOf(std::uint64_t(i)));
    for (const auto& b : bodies)
        slices.emplace_back(b);

    const auto seqs = q.sendBatch(slices);
    CHECK_EQ(seqs.size(), std::size_t(64));
    CHECK_EQ(seqs.front(), std::uint64_t(1));
    CHECK_EQ(seqs.back(), std::uint64_t(64));

    const std::uint64_t before = e.commitGeneration();
    auto batch = q.receiveBatch(64, 1s);
    CHECK_EQ(batch.size(), std::size_t(64));
    CHECK_EQ(e.commitGeneration(), before + 1);
    for (std::size_t i = 0; i < batch.size(); ++i)
        CHECK_EQ(batch[i].message().body, bodies[i]);

    q.ackAll(std::move(batch));
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
}

TEST(sendBatchAcceptsCxx17ContainersAndEmptyRanges)
{
    tst::Scratch scratch("mq-batch");
    auto env = Env::configure().sync(Durability::None).open(scratch.file());
    auto queue = MessageQueue::open(env, "orders");
    CHECK(queue.sendBatch(nullptr, 0).empty());
    CHECK(queue.sendBatch(std::vector<Slice>{}).empty());
    CHECK(queue.sendBatch(std::array<Slice, 0>{}).empty());

    const Slice raw[] = {"first", "second"};
    const std::array<Slice, 2> array = {"third", "fourth"};
    CHECK_EQ(queue.sendBatch(raw).front(), 1u);
    CHECK_EQ(queue.sendBatch(array).front(), 3u);
    CHECK_EQ(queue.sendBatch(raw + 1, 1).front(), 5u);
    auto receipts = queue.receiveBatch(5, 0ms);
    CHECK_EQ(receipts.size(), 5u);
    const char* expected[] = {"first", "second", "third", "fourth", "second"};
    for (std::size_t index = 0; index < receipts.size(); ++index)
        CHECK_EQ(receipts[index].message().body, std::string(expected[index]));
    queue.ackAll(std::move(receipts));
}

TEST(receiveBatchStopsAtWhatIsAvailable)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");
    q.send("a");
    q.send("b");

    auto got = q.receiveBatch(10, 50ms);
    CHECK_EQ(got.size(), std::size_t(2));
    CHECK(q.receiveBatch(10, 50ms).empty());
}

TEST(abortingTheCallersTransactionUndoesTheReceive)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");
    q.send("unit-of-work");

    {
        Txn t = e.writeTxn();
        Receipt r = q.receive(t);
        CHECK(r.valid());
        t.db("side-effects", DbFlags::Create).put("k", "v");
        t.abort();
    }
    CHECK_EQ(q.readyCount(), std::uint64_t(1));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));

    {
        Txn t = e.writeTxn();
        Receipt r = q.receive(t);
        q.ack(t, std::move(r));
        t.db("side-effects", DbFlags::Create).put("k", "v");
        t.commit();
    }
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
    e.read([](Txn& t) { CHECK(t.db("side-effects").contains("k")); });
}

TEST(sendInsideACallersTransactionIsAtomicWithIt)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders");

    {
        Txn t = e.writeTxn();
        q.send(t, "rolled-back");
        t.abort();
    }
    CHECK_EQ(q.readyCount(), std::uint64_t(0));

    e.write([&](Txn& t) { q.send(t, "committed"); });
    CHECK_EQ(q.readyCount(), std::uint64_t(1));
    CHECK_EQ(q.peek()->body, std::string("committed"));
}

TEST(groupCommitPreservesEverySendAndTheirOrder)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q =
        MessageQueue::open(e, "orders", MessageQueue::Options().groupCommit(256, 200us));

    constexpr int kThreads = 4;
    constexpr int kEach = 250;
    std::vector<std::thread> producers;
    std::atomic<std::uint64_t> sum{0};
    for (int p = 0; p < kThreads; ++p)
        producers.emplace_back([&, p] {
            for (int i = 0; i < kEach; ++i)
                sum.fetch_add(q.send(tst::keyOf(std::uint64_t(p * kEach + i))));
        });
    for (auto& t : producers)
        t.join();

    const std::uint64_t n = kThreads * kEach;
    CHECK_EQ(q.readyCount(), n);
    // Sequences are handed out once each, so they must be exactly 1..n.
    CHECK_EQ(sum.load(), n * (n + 1) / 2);

    std::uint64_t expect = 1;
    for (;;) {
        auto batch = q.receiveBatch(128, 1s);
        if (batch.empty())
            break;
        for (const Receipt& r : batch)
            CHECK_EQ(r.sequence(), expect++);
        q.ackAll(std::move(batch));
    }
    CHECK_EQ(expect, n + 1);
}

TEST(externalBodiesRoundTripThroughTheBlobArchive)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q =
        MessageQueue::open(e, "images", MessageQueue::Options().externalBodyThreshold(4096));

    const std::string small = tst::blob(1000, 1);
    const std::string large = tst::blob(200000, 2);
    q.send(small);
    q.send(large);

    Receipt a = q.tryReceive();
    CHECK(!a.message().hasExternalBody());
    CHECK_EQ(a.message().body, small);
    CHECK(!q.fetchBody(a.message()).valid());
    q.ack(std::move(a));

    Receipt b = q.tryReceive();
    CHECK(b.message().hasExternalBody());
    CHECK(b.message().body.empty());
    Blob body = q.fetchBody(b.message());
    CHECK(body.valid());
    CHECK_EQ(body.size(), std::uint64_t(large.size()));
    CHECK_EQ(body.data().string(), large);
    CHECK(body.verify());
    q.ack(std::move(b));
}

TEST(twoQueuesInOneStoreDoNotSeeEachOther)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue a = MessageQueue::open(e, "alpha");
    MessageQueue b = MessageQueue::open(e, "beta");

    a.send("for-alpha");
    CHECK_EQ(a.readyCount(), std::uint64_t(1));
    CHECK_EQ(b.readyCount(), std::uint64_t(0));
    CHECK(!b.tryReceive().valid());
    CHECK_EQ(a.tryReceive().message().body, std::string("for-alpha"));
}

TEST(badQueueNamesAreRejected)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    CHECK_THROWS(MessageQueue::open(e, ""), ErrorCode::InvalidArgument);
    CHECK_THROWS(MessageQueue::open(e, "a/b"), ErrorCode::InvalidArgument);
    CHECK_THROWS(MessageQueue::open(e, std::string(200, 'q')), ErrorCode::InvalidArgument);
}

TEST(ackAndNackOfASweptLeaseAreHarmless)
{
    tst::Scratch s("mq");
    Env e = Env::configure().open(s.file());
    MessageQueue q = MessageQueue::open(e, "orders", fastLease(10ms));

    q.send("work");
    Receipt r = q.tryReceive();
    std::this_thread::sleep_for(40ms);
    CHECK_EQ(q.sweep(), std::uint64_t(1));

    // The lease is gone; settling it must not resurrect or double-count it.
    q.ack(std::move(r));
    CHECK_EQ(q.readyCount(), std::uint64_t(1));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
}

TEST(staleTransactionalAcknowledgementAbortsEffects)
{
    tst::Scratch scratch("stale-effects");
    Env env = Env::configure().open(scratch.file());
    MessageQueue queue = MessageQueue::open(env, "jobs");
    queue.send("work");
    Receipt stale = queue.tryReceive();
    env.write([](Txn& txn) { txn.db("mq/jobs/lease").clear(); });
    Txn txn = env.writeTxn();
    txn.mainDb().put("effect", "must-not-commit");
    CHECK_THROWS(queue.ack(txn, std::move(stale)), ErrorCode::BadTransaction);
    env.read([](Txn& reader) { CHECK(!reader.mainDb().contains("effect")); });
}

TEST(queueRejectsForeignTransactionsAndReceipts)
{
    tst::Scratch scratch("affinity");
    Env env = Env::configure().open(scratch.file());
    Env foreign = Env::configure().open(scratch.file("foreign"));
    auto first = MessageQueue::open(env, "first");
    auto second = MessageQueue::open(env, "second");
    Txn txn = foreign.writeTxn();
    CHECK_THROWS(first.send(txn, "wrong"), ErrorCode::BadTransaction);
    txn.abort();
    first.send("work");
    Receipt receipt = first.tryReceive();
    CHECK_THROWS(second.ack(std::move(receipt)), ErrorCode::BadTransaction);
    first.ack(std::move(receipt));
}

TEST(groupCommitIntakeHasAByteBudget)
{
    tst::Scratch scratch("intake");
    Env env = Env::configure().open(scratch.file());
    auto queue = MessageQueue::open(env, "jobs",
        MessageQueue::Options().groupCommit(16, 0us).intakeLimit(8));
    CHECK_THROWS(queue.send("larger-than-eight"), ErrorCode::Busy);
    queue.send("small");
    CHECK_EQ(queue.readyCount(), 1u);
}

int main()
{
    return tst::runAll("mq");
}
