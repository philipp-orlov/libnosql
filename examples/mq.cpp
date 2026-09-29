// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// A durable work queue: producers hand out jobs, workers take them under a
// lease, and anything a worker drops is redelivered rather than lost.
//
//   ./example_mq [path]

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "nosql/message_queue.hpp"

using namespace std::chrono_literals;

namespace {

constexpr int kJobs = 500;
constexpr int kWorkers = 3;

// Produced jobs land in one queue; every job a worker finishes is reported on
// a second queue, which is how a real pipeline chains stages together.
void produce(nosql::MessageQueue& jobs)
{
    for (int i = 0; i < kJobs; ++i) {
        const std::string body = "job-" + std::to_string(i);
        // Group commit is on, so this blocks only until the batch containing
        // this job is durable -- not until a commit of its own.
        jobs.send(body, "batch-" + std::to_string(i / 100));
    }
}

void work(int id, nosql::MessageQueue& jobs, nosql::MessageQueue& results,
          std::atomic<int>& completed, std::atomic<int>& retried, std::atomic<bool>& draining)
{
    std::mt19937_64 rng(id * 7919 + 1);

    for (;;) {
        // Blocks until something arrives or the timeout elapses; it is woken
        // by the producer's commit, not by polling.
        nosql::Receipt r = jobs.receive(250ms);
        if (!r) {
            if (draining.load(std::memory_order_acquire))
                return;
            continue;
        }

        const nosql::Message& m = r.message();

        // Pretend one job in twenty fails. Handing it back makes it available
        // again immediately, with its delivery count bumped.
        if (rng() % 20 == 0 && m.deliveryCount < 3) {
            retried.fetch_add(1, std::memory_order_relaxed);
            jobs.nack(std::move(r));
            continue;
        }

        // The interesting case: the result and the acknowledgement land in the
        // same transaction. Either the work is recorded and the job is gone,
        // or neither happened. No two-phase commit, no outbox table.
        jobs.env().write([&](nosql::Txn& t) {
            results.send(t, "done:" + m.body, m.correlationId);
            jobs.ack(t, std::move(r));
        });
        completed.fetch_add(1, std::memory_order_relaxed);
    }
}

}  // namespace

int main(int argc, char** argv)
{
    const std::filesystem::path path = argc > 1 ? argv[1] : "mq.db";

    // Four sub-databases per queue, so leave room for both plus whatever else
    // the application keeps in the same file.
    nosql::Env store = nosql::Env::configure().pageSize(4096).maxDbs(32).open(path);

    nosql::MessageQueue jobs = nosql::MessageQueue::open(
        store, "jobs",
        nosql::MessageQueue::Options()
            .leaseDuration(5s)        // a worker that dies gets swept after this
            .maxDeliveries(5)         // then dead-lettered rather than retried forever
            .groupCommit(256, 500us)  // amortise fsync across concurrent producers
    );
    nosql::MessageQueue results = nosql::MessageQueue::open(store, "results");

    std::printf("queue depth at start: ready=%llu leased=%llu dead=%llu\n",
                (unsigned long long)jobs.readyCount(), (unsigned long long)jobs.leasedCount(),
                (unsigned long long)jobs.deadCount());

    std::atomic<int> completed{0}, retried{0};
    std::atomic<bool> draining{false};

    const auto t0 = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < kWorkers; ++i)
        threads.emplace_back(work, i, std::ref(jobs), std::ref(results), std::ref(completed),
                             std::ref(retried), std::ref(draining));
    std::thread producer(produce, std::ref(jobs));

    producer.join();
    // Workers keep going until the queue stays empty for a full receive
    // timeout, which is the usual way to drain one cleanly.
    draining.store(true, std::memory_order_release);
    for (auto& t : threads)
        t.join();

    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0);

    std::printf("%d jobs through %d workers in %.2fs (%.0f/s), %d retried\n", completed.load(),
                kWorkers, elapsed.count(), completed.load() / elapsed.count(), retried.load());
    std::printf("jobs:    ready=%llu leased=%llu dead=%llu\n",
                (unsigned long long)jobs.readyCount(), (unsigned long long)jobs.leasedCount(),
                (unsigned long long)jobs.deadCount());
    std::printf("results: ready=%llu\n", (unsigned long long)results.readyCount());

    // Peek at the head of the results queue without consuming it. Restarting
    // this program would find these still there -- the queue is the file.
    if (auto head = results.peek())
        std::printf("next result: %s (correlation %s, enqueued %llu)\n", head->body.c_str(),
                    head->correlationId.c_str(), (unsigned long long)head->sequence);

    // A worker that crashes leaves its lease behind. Somebody has to notice;
    // a periodic sweep is all it takes.
    if (const std::uint64_t recovered = jobs.sweep())
        std::printf("swept %llu abandoned leases back onto the queue\n",
                    (unsigned long long)recovered);

    for (const nosql::Message& m : jobs.deadLetters(5))
        std::printf("dead letter %llu after %u deliveries: %s\n", (unsigned long long)m.sequence,
                    m.deliveryCount, m.body.c_str());

    return 0;
}
