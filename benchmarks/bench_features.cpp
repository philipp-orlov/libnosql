// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// What the optional features cost and buy, each against the same work without
// it: DbFlags::PrefixKeys on keys with a long shared prefix, and spilling a
// transaction several times dirtyLimit against one held in memory.
//
//   bench_features [--entries N] [--value B] [--reps R] [--only prefix,spill]

#include "bench_util.hpp"

#include <algorithm>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"

using namespace nosql;

namespace {

/// Keys in the shape PrefixKeys is for: a tenant, a table, a date, an id.
std::vector<std::string> prefixedKeys(std::uint64_t n)
{
    std::vector<std::string> keys(n);
    for (std::uint64_t i = 0; i < n; ++i) {
        char b[96];
        std::snprintf(b, sizeof b, "tenant/%04llu/orders/2026-10-%02llu/%012llu",
                      (unsigned long long)(i % 3), (unsigned long long)(i % 28 + 1),
                      (unsigned long long)i);
        keys[i] = b;
    }
    std::sort(keys.begin(), keys.end());
    return keys;
}

std::vector<std::uint32_t> shuffled(std::uint64_t n, std::uint64_t seed)
{
    std::vector<std::uint32_t> order(n);
    for (std::uint64_t i = 0; i < n; ++i)
        order[i] = std::uint32_t(i);
    std::shuffle(order.begin(), order.end(), std::mt19937_64(seed));
    return order;
}

}  // namespace

int main(int argc, char** argv)
{
    bench::Args args(argc, argv);
    std::uint64_t entries = args.getU("--entries", 300000);
    const std::uint64_t valueBytes = args.getU("--value", 48);
    const int reps = int(args.getU("--reps", 3));
    const std::string only = args.get("--only", "");
    const auto wants = [&](const char* name) {
        return only.empty() || only.find(name) != std::string::npos;
    };

    std::printf("libnosql %s bench_features: %llu entries x %llu B, reps %d\n\n", version(),
                (unsigned long long)entries, (unsigned long long)valueBytes, reps);
    bench::Report report("nosql-features");
    if (const std::string j = args.get("--json", ""); !j.empty())
        report.jsonPath(j);
    report.header();
    bench::Scratch scratch("nosql-bench-features");
    const std::string value(valueBytes, 'v');

    if (wants("prefix")) {
        const std::vector<std::string> keys = prefixedKeys(entries);
        const std::vector<std::uint32_t> order = shuffled(entries, 7);
        for (const bool prefixed : {false, true}) {
            const std::string tag = prefixed ? "prefix keys" : "plain keys";
            const auto path = scratch.file(prefixed ? "prefixed.db" : "plain.db");
            Env env = Env::configure().sync(Durability::None).open(path);
            const DbFlags flags =
                DbFlags::Create | (prefixed ? DbFlags::PrefixKeys : DbFlags::None);
            report.run(tag + ": insert random (batched)", entries, entries * valueBytes, 1, [&] {
                for (std::uint64_t at = 0; at < entries;) {
                    env.write([&](Txn& tx) {
                        Db d = tx.db("orders", flags);
                        for (std::uint64_t k = 0; k < 1000 && at < entries; ++k, ++at)
                            d.put(keys[order[at]], value);
                    });
                }
            });
            std::uint64_t leaves = 0, branches = 0;
            env.read([&](Txn& tx) {
                const TreeStats s = tx.db("orders").stats();
                leaves = s.leafPages;
                branches = s.branchPages;
            });
            std::printf("    %s: %llu leaf and %llu branch pages\n", tag.c_str(),
                        (unsigned long long)leaves, (unsigned long long)branches);
            report.run(tag + ": get random (1 txn)", entries, entries * valueBytes, reps, [&] {
                env.read([&](Txn& tx) {
                    Db d = tx.db("orders");
                    std::uint64_t hit = 0;
                    for (std::uint32_t i : order)
                        hit += d.get(keys[i]).has_value();
                    if (hit != entries)
                        std::abort();
                });
            });
            report.run(tag + ": scan ordered (cursor)", entries, entries * valueBytes, reps, [&] {
                env.read([&](Txn& tx) {
                    Cursor c = tx.db("orders").cursor();
                    Slice k, v;
                    std::uint64_t seen = 0;
                    for (bool ok = c.first(k, v); ok; ok = c.next(k, v))
                        seen += k.size() != 0;
                    if (seen != entries)
                        std::abort();
                });
            });
        }
    }

    if (wants("spill")) {
        // One transaction four times the budget, against the same with none.
        const std::uint64_t budget = 16ull << 20;
        const std::uint64_t n = std::max<std::uint64_t>(entries, 4 * budget / (valueBytes + 40));
        std::vector<std::uint32_t> order = shuffled(n, 11);
        for (const bool spill : {false, true}) {
            const std::string tag = spill ? "spilling, 16 MiB budget" : "in memory, no budget";
            const auto path = scratch.file(spill ? "spill.db" : "memory.db");
            std::uint64_t peak = 0;
            long long rss0 = 0, rss = 0;
            report.run(tag + ": one txn, random keys", n, n * valueBytes, 1, [&] {
                Env env = Env::configure()
                              .sync(Durability::None)
                              .dirtyLimit(spill ? budget : 0)
                              .open(path);
                rss0 = bench::residentBytes();
                env.write([&](Txn& tx) {
                    Db d = tx.mainDb();
                    char b[24];
                    for (std::uint64_t i = 0; i < n; ++i) {
                        std::snprintf(b, sizeof b, "%016llx", (unsigned long long)order[i]);
                        d.put(Slice(b, 16), value);
                        if (i % 4096 == 0) {
                            peak = std::max<std::uint64_t>(peak, env.stats().dirtyBytes);
                            rss = std::max(rss, bench::residentBytes());
                        }
                    }
                });
            });
            // Resident memory includes the file's pages in the mapping, which
            // the kernel may keep or drop as it likes; the dirty buffers are ours.
            std::printf("    %s: peak dirty buffers %.1f MiB, resident growth %.1f MiB\n",
                        tag.c_str(), double(peak) / (1 << 20), double(rss - rss0) / (1 << 20));
        }
    }
    report.finish();
    return 0;
}
