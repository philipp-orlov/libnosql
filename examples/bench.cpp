// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// A small, honest micro-benchmark. Not a comparison with anything -- just a
// way to see how the store behaves on your machine and to spot regressions.
//
//   ./example_bench [entries] [valueBytes] [pageSize] [every-read|transaction]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/checksum.hpp"
#include "examples/scratch.hpp"

using namespace nosql;
using Clock = std::chrono::steady_clock;

namespace {

struct Timer
{
    Clock::time_point t0 = Clock::now();
    double ms() const
    {
        return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
    }
};

void report(const char* label, double ms, std::size_t n, std::size_t bytes = 0)
{
    std::printf("  %-30s %9.1f ms  %10.0f op/s", label, ms, n / (ms / 1000.0));
    if (bytes)
        std::printf("  %7.1f MiB/s", bytes / (1024.0 * 1024.0) / (ms / 1000.0));
    std::printf("\n");
}

std::string keyFor(std::uint64_t i)
{
    char b[24];
    std::snprintf(b, sizeof b, "%016llx", (unsigned long long)i);
    return b;
}

}  // namespace

int main(int argc, char** argv)
{
    const std::size_t n = argc > 1 ? std::stoul(argv[1]) : 500000;
    const std::size_t vsize = argc > 2 ? std::stoul(argv[2]) : 100;
    const std::size_t psize = argc > 3 ? std::stoul(argv[3]) : 4096;
    const std::string validation = argc > 4 ? argv[4] : "every-read";
    if (validation != "every-read" && validation != "transaction") {
        std::fprintf(stderr, "Validation must be every-read or transaction\n");
        return 1;
    }

    Scratch scratch("bench");
    const std::filesystem::path path = scratch.file("bench.db");

    std::printf("libnosql %s: %zu entries x %zu B values, %zu B pages\n\n", version(), n, vsize,
                psize);
    std::printf("  checksum backend: %s (read validation: %s)\n\n",
                internal::checksumBackend(), validation.c_str());

    Env store = Env::configure()
                    .cacheReadChecksums(validation == "transaction")
                    .pageSize(psize)
                    .maxSize(16ull << 30)
                    .sync(Durability::None)  // bulk-load mode; see README
                    .open(path);

    const std::string value(vsize, 'x');
    std::vector<std::uint64_t> order(n);
    for (std::size_t i = 0; i < n; ++i)
        order[i] = i;
    std::shuffle(order.begin(), order.end(), std::mt19937_64(42));

    {  // Sequential bulk load, one transaction, append mode.
        Timer t;
        store.write([&](Txn& tx) {
            auto d = tx.mainDb();
            for (std::size_t i = 0; i < n; ++i)
                d.put(keyFor(i), value, PutMode::Append);
        });
        report("bulk append (1 txn)", t.ms(), n, n * vsize);
    }

    {  // Random point lookups.
        Timer t;
        store.read([&](Txn& tx) {
            auto d = tx.mainDb();
            std::size_t hits = 0;
            for (std::uint64_t i : order)
                hits += d.get(keyFor(i)).has_value();
            if (hits != n)
                std::printf("  !! %zu misses\n", n - hits);
        });
        report("random get", t.ms(), n, n * vsize);
    }

    {  // Full ordered scan.
        Timer t;
        std::size_t seen = 0, bytes = 0;
        store.read([&](Txn& tx) {
            for (auto [k, v] : tx.mainDb().all()) {
                bytes += k.size() + v.size();
                ++seen;
            }
        });
        report("ordered scan", t.ms(), seen, bytes);
    }

    {  // Random overwrites in one transaction.
        Timer t;
        store.write([&](Txn& tx) {
            auto d = tx.mainDb();
            for (std::size_t i = 0; i < n / 4; ++i)
                d.put(keyFor(order[i]), value);
        });
        report("random overwrite (1 txn)", t.ms(), n / 4, (n / 4) * vsize);
    }

    {  // Small transactions, each durably committed.
        Env syncStore = std::move(store);
        const std::size_t batches = 200, per = 50;
        Timer t;
        for (std::size_t b = 0; b < batches; ++b)
            syncStore.write([&](Txn& tx) {
                auto d = tx.mainDb();
                for (std::size_t i = 0; i < per; ++i)
                    d.put(keyFor(n + b * per + i), value);
            });
        report("small txns (no fsync)", t.ms(), batches * per);
        store = std::move(syncStore);
    }

    {  // Random deletes.
        Timer t;
        store.write([&](Txn& tx) {
            auto d = tx.mainDb();
            for (std::size_t i = 0; i < n / 4; ++i)
                d.erase(keyFor(order[i]));
        });
        report("random erase (1 txn)", t.ms(), n / 4);
    }

    const EnvStats s = store.stats();
    std::printf("\n  file %.1f MiB, %llu pages used, %llu free, depth ", s.fileSize / 1048576.0,
                (unsigned long long)s.usedPages, (unsigned long long)s.freePages);
    store.read([](Txn& t) { std::printf("%u\n", t.mainDb().stats().depth); });

    {  // Durable commits, to show what an fsync actually costs.
        Env durable = Env::configure()
                          .pageSize(psize)
                          .maxSize(1ull << 30)
                          .sync(Durability::Safe)
                          .open(scratch.file("bench-sync.db"));
        Timer t;
        for (int i = 0; i < 200; ++i)
            durable.write([&](Txn& tx) { tx.mainDb().put(keyFor(i), value); });
        report("durable commits (fsync each)", t.ms(), 200);
    }

    return 0;
}
