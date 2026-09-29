// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The queue under sustained load: nothing is lost or duplicated when many
// producers and consumers race, and a queue that is drained as fast as it is
// filled does not grow the file.
#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "nosql/message_queue.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
using namespace std::chrono_literals;

namespace {

std::uint64_t idOf(const std::string& body)
{
    return std::stoull(body);
}

std::string bodyOf(std::uint64_t id)
{
    return std::to_string(id);
}

std::uint64_t usedPages(Env& e)
{
    return e.stats().usedPages;
}

}  // namespace

TEST(nothingIsLostOrDeliveredTwiceUnderConcurrency)
{
    tst::Scratch s("mqsteady");
    // Durability is covered by TestMq; here the point is volume, and 149
    // fsyncs a second would make these runs take minutes.
    Env e = Env::configure().sync(Durability::None).open(s.file());
    // A lease long enough that none expires during the run, so any second
    // delivery of a message would be an outright bug rather than a recovery.
    MessageQueue q = MessageQueue::open(e, "work", MessageQueue::Options().leaseDuration(5min));

    constexpr std::uint64_t kProducers = 4;
    constexpr std::uint64_t kEach = 2000;
    constexpr std::uint64_t kTotal = kProducers * kEach;
    constexpr int kConsumers = 4;

    std::vector<std::atomic<int>> acked(kTotal);
    std::atomic<std::uint64_t> done{0};

    std::vector<std::thread> threads;
    for (std::uint64_t p = 0; p < kProducers; ++p)
        threads.emplace_back([&, p] {
            for (std::uint64_t i = 0; i < kEach; ++i)
                q.send(bodyOf(p * kEach + i));
        });

    for (int c = 0; c < kConsumers; ++c)
        threads.emplace_back([&] {
            while (done.load(std::memory_order_relaxed) < kTotal) {
                auto batch = q.receiveBatch(32, 200ms);
                for (const Receipt& r : batch) {
                    CHECK_EQ(r.message().deliveryCount, 1u);
                    const int seen = acked[idOf(r.message().body)].fetch_add(1);
                    CHECK_EQ(seen, 0);
                }
                done.fetch_add(batch.size(), std::memory_order_relaxed);
                q.ackAll(std::move(batch));
            }
        });

    for (auto& t : threads)
        t.join();

    CHECK_EQ(done.load(), kTotal);
    for (std::uint64_t i = 0; i < kTotal; ++i)
        CHECK_EQ(acked[i].load(), 1);
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
    CHECK_EQ(q.deadCount(), std::uint64_t(0));
}

TEST(abandonedAndRejectedWorkIsAlwaysRecovered)
{
    tst::Scratch s("mqsteady");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    MessageQueue q = MessageQueue::open(
        e, "flaky", MessageQueue::Options().leaseDuration(50ms).maxDeliveries(1000));

    constexpr std::uint64_t kTotal = 1500;
    for (std::uint64_t i = 0; i < kTotal; ++i)
        q.send(bodyOf(i));

    std::vector<std::atomic<int>> acked(kTotal);
    std::atomic<std::uint64_t> done{0};
    std::atomic<bool> stop{false};

    std::thread sweeper([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            q.sweep();
            std::this_thread::sleep_for(10ms);
        }
    });

    std::vector<std::thread> consumers;
    for (int c = 0; c < 3; ++c)
        consumers.emplace_back([&, c] {
            std::mt19937_64 rng(1234 + c);
            while (done.load(std::memory_order_relaxed) < kTotal) {
                Receipt r = q.receive(200ms);
                if (!r.valid())
                    continue;
                const std::uint64_t id = idOf(r.message().body);
                switch (rng() % 4) {
                    case 0:  // crash: drop the receipt and let the lease lapse
                        continue;
                    case 1:  // transient failure: hand it straight back
                        q.nack(std::move(r));
                        continue;
                    default:
                        // A message may be delivered many times, but only the
                        // delivery that acks it removes it, so exactly one ack
                        // per message must succeed.
                        if (acked[id].fetch_add(1) == 0)
                            done.fetch_add(1, std::memory_order_relaxed);
                        q.ack(std::move(r));
                }
            }
        });

    for (auto& t : consumers)
        t.join();
    stop.store(true);
    sweeper.join();

    for (std::uint64_t i = 0; i < kTotal; ++i)
        CHECK_EQ(acked[i].load(), 1);
    q.sweep();
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.leasedCount(), std::uint64_t(0));
    CHECK_EQ(q.deadCount(), std::uint64_t(0));
}

TEST(aDrainedQueueDoesNotGrowTheFile)
{
    tst::Scratch s("mqsteady");
    Env e = Env::configure().pageSize(4096).sync(Durability::None).open(s.file());
    MessageQueue q = MessageQueue::open(e, "steady");

    // Prime the queue with a working set, then run for many rounds keeping it
    // at exactly that depth. Pages freed by the acks must be reused, so the
    // file has to plateau rather than track total throughput.
    constexpr std::size_t kDepth = 500;
    constexpr int kRounds = 400;
    const std::string payload(200, 'p');
    std::vector<Slice> batchIn(kDepth, Slice(payload));

    q.sendBatch(batchIn);

    std::uint64_t settled = 0;
    for (int round = 0; round < kRounds; ++round) {
        auto batch = q.receiveBatch(kDepth, 1s);
        CHECK_EQ(batch.size(), kDepth);
        q.ackAll(std::move(batch));
        q.sendBatch(batchIn);

        if (round == kRounds / 4)
            settled = usedPages(e);
    }

    const std::uint64_t after = usedPages(e);
    CHECK(settled != 0);
    // Some slack for freelist churn, but nowhere near the 400x that a file
    // growing with throughput would show.
    CHECK(after <= settled * 2);
    CHECK_EQ(q.readyCount(), kDepth);

    e.read([](Txn& t) { checkIntegrity(t); });
}

TEST(deadLettersAccumulateAndPurgeCleanly)
{
    tst::Scratch s("mqsteady");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    MessageQueue q = MessageQueue::open(e, "poison", MessageQueue::Options().maxDeliveries(2));

    constexpr std::uint64_t kTotal = 500;
    for (std::uint64_t i = 0; i < kTotal; ++i)
        q.send(bodyOf(i));

    // Every message fails twice, so every message ends up dead-lettered.
    while (q.readyCount() != 0) {
        Receipt r = q.tryReceive();
        if (!r.valid())
            break;
        q.nack(std::move(r));
    }
    CHECK_EQ(q.deadCount(), kTotal);
    CHECK_EQ(q.readyCount(), std::uint64_t(0));
    CHECK_EQ(q.deadLetters(10).size(), std::size_t(10));

    CHECK_EQ(q.purgeDeadLetters(), kTotal);
    CHECK_EQ(q.deadCount(), std::uint64_t(0));
    CHECK(q.deadLetters(10).empty());
    e.read([](Txn& t) { checkIntegrity(t); });
}

int main()
{
    return tst::runAll("mqsteady");
}
