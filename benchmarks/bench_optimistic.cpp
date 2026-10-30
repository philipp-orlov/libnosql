// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Single-writer transactions against optimistic ones, on the same workload.
//
// Each transaction reads one key, does `work` microseconds of application work
// on the value, and writes the key back. Under Env::write the whole body runs
// holding the writer slot. Under optimistic() only the commit does, so the work
// of different threads overlaps and a conflict costs a retry.
//
// usage: bench_optimistic [txns-per-thread] [keys] [work-us] [none|safe] [pipelined]

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "nosql/optimistic.hpp"

using namespace nosql;
using Clock = std::chrono::steady_clock;

namespace {

void spinFor(std::chrono::microseconds d)
{
    const auto end = Clock::now() + d;
    while (Clock::now() < end) {}
}

std::string keyOf(std::uint64_t i)
{
    char buf[24];
    std::snprintf(buf, sizeof buf, "k%010llu", static_cast<unsigned long long>(i));
    return buf;
}

struct Result
{
    double seconds = 0;
    std::uint64_t retries = 0;
    std::uint64_t commits = 0;    // store commits; fewer than transactions when grouped
    std::uint64_t usedPages = 0;  // pages in use at the end, free list included
};

Result run(const std::filesystem::path& file, Durability dura, bool pipelined, bool optimisticMode,
           unsigned threads, unsigned txnsPerThread, unsigned keys, unsigned workUs)
{
    std::filesystem::remove(file);
    std::filesystem::remove(file.string() + ".lck");
    Env e = Env::configure().sync(dura).pipelineCommits(pipelined).open(file);
    e.write([&](Txn& t) {
        for (unsigned i = 0; i < keys; ++i)
            t.mainDb().put(keyOf(i), "0");
    });

    std::atomic<std::uint64_t> attempts{0};
    auto body = [&](unsigned seed) {
        std::mt19937_64 rng(seed);
        for (unsigned n = 0; n < txnsPerThread; ++n) {
            const std::string k = keyOf(rng() % keys);
            if (optimisticMode) {
                optimistic(
                    e,
                    [&](OptimisticTxn& t) {
                        ++attempts;
                        const std::string v = *t.get("", k);
                        spinFor(std::chrono::microseconds(workUs));
                        t.put("", k, std::to_string(std::stoull(v) + 1));
                    },
                    1u << 20);
            } else {
                e.write([&](Txn& t) {
                    const std::string v = t.mainDb().at(k).string();
                    spinFor(std::chrono::microseconds(workUs));
                    t.mainDb().put(k, std::to_string(std::stoull(v) + 1));
                });
            }
        }
    };

    std::vector<std::thread> workers;
    const std::uint64_t gen0 = e.commitGeneration();
    const auto t0 = Clock::now();
    for (unsigned i = 0; i < threads; ++i)
        workers.emplace_back(body, 1000 + i);
    for (auto& w : workers)
        w.join();
    Result r;
    r.commits = e.commitGeneration() - gen0;
    r.usedPages = e.stats().usedPages;
    r.seconds = std::chrono::duration<double>(Clock::now() - t0).count();
    if (optimisticMode)
        r.retries = attempts.load() - std::uint64_t(threads) * txnsPerThread;
    return r;
}

}  // namespace

int main(int argc, char** argv)
{
    const unsigned txnsPerThread = argc > 1 ? unsigned(std::atoi(argv[1])) : 5000;
    const unsigned keys = argc > 2 ? unsigned(std::atoi(argv[2])) : 1000;
    const unsigned workUs = argc > 3 ? unsigned(std::atoi(argv[3])) : 50;
    const Durability dura =
        (argc > 4 && std::string(argv[4]) == "safe") ? Durability::Safe : Durability::None;
    const bool pipelined = argc > 5 && std::string(argv[5]) == "pipelined";
    const std::filesystem::path file =
        std::filesystem::temp_directory_path() / "nosql-bench-optimistic.db";

    std::printf("txns/thread=%u keys=%u work=%uus durability=%s%s\n", txnsPerThread, keys, workUs,
                dura == Durability::Safe ? "safe" : "none", pipelined ? " pipelined" : "");
    std::printf("%8s %12s %14s %14s %10s %12s %10s\n", "threads", "mode", "txn/s", "elapsed (s)",
                "retries", "txns/commit", "pages");
    for (unsigned threads : {1u, 2u, 4u, 8u}) {
        for (bool opt : {false, true}) {
            const Result r = run(file, dura, pipelined, opt, threads, txnsPerThread, keys, workUs);
            const double total = double(threads) * txnsPerThread;
            std::printf("%8u %12s %14.0f %14.3f %10llu %12.1f %10llu\n", threads,
                        opt ? "optimistic" : "writeTxn", total / r.seconds, r.seconds,
                        static_cast<unsigned long long>(r.retries),
                        r.commits ? total / double(r.commits) : 0.0,
                        static_cast<unsigned long long>(r.usedPages));
        }
    }
    std::filesystem::remove(file);
    std::filesystem::remove(file.string() + ".lck");
    return 0;
}
