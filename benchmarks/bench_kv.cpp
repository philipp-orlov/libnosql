// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The key/value engine under a fixed set of workloads, reported as ns/op with
// the allocations each repetition made. Keys are generated ahead of time so
// the loops time the store and nothing else.
//
//   bench_kv [--entries N] [--value B] [--page P] [--reps R] [--batch B]
//            [--cache] [--sync safe|nometa|none] [--threads T]
//            [--json out.json] [--csv out.csv] [--quick] [--only name,name]

#define BENCH_DEFINE_ALLOC_HOOKS
#include "bench_util.hpp"

#include <atomic>
#include <cstring>
#include <thread>

#include "nosql/nosql.hpp"
#include "nosql/internal/checksum.hpp"

using namespace nosql;

namespace {

struct Config
{
    std::uint64_t entries = 500000;
    std::uint64_t valueBytes = 100;
    std::uint64_t pageSize = 4096;
    std::uint64_t batch = 1000;
    int reps = 3;
    bool cache = false;
    Durability sync = Durability::None;
    unsigned threads = 4;
    std::vector<std::string> only;

    bool wants(const char* name) const
    {
        if (only.empty())
            return true;
        for (const std::string& o : only)
            if (o == name)
                return true;
        return false;
    }
};

std::vector<std::string> makeKeys(std::uint64_t n)
{
    std::vector<std::string> keys(n);
    for (std::uint64_t i = 0; i < n; ++i) {
        char b[24];
        std::snprintf(b, sizeof b, "%016llx", (unsigned long long)i);
        keys[i].assign(b, 16);
    }
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

Env openStore(const Config& c, const std::filesystem::path& path)
{
    return Env::configure()
        .pageSize(c.pageSize)
        .maxSize(64ull << 30)
        .sync(c.sync)
        .cacheReadChecksums(c.cache)
        .open(path);
}

void bulkLoad(Env& store, const std::vector<std::string>& keys, const std::string& value)
{
    store.write([&](Txn& tx) {
        Db d = tx.mainDb();
        for (const std::string& k : keys)
            d.put(k, value, PutMode::Append);
    });
}

}  // namespace

int main(int argc, char** argv)
{
    bench::Args args(argc, argv);
    Config c;
    if (args.has("--quick")) {
        c.entries = 100000;
        c.reps = 2;
    }
    c.entries = args.getU("--entries", c.entries);
    c.valueBytes = args.getU("--value", c.valueBytes);
    c.pageSize = args.getU("--page", c.pageSize);
    c.batch = args.getU("--batch", c.batch);
    c.reps = int(args.getU("--reps", std::uint64_t(c.reps)));
    c.cache = args.has("--cache");
    c.threads = unsigned(args.getU("--threads", c.threads));
    const std::string sync = args.get("--sync", "none");
    c.sync = sync == "safe" ? Durability::Safe
             : sync == "nometa" ? Durability::NoMetaSync
                                : Durability::None;
    {
        std::string list = args.get("--only", "");
        while (!list.empty()) {
            const std::size_t comma = list.find(',');
            c.only.push_back(list.substr(0, comma));
            list = comma == std::string::npos ? std::string() : list.substr(comma + 1);
        }
    }

    std::printf("libnosql %s bench_kv: %llu entries x %llu B, %llu B pages, batch %llu, reps %d, "
                "sync %s, read cache %s\n",
                version(), (unsigned long long)c.entries, (unsigned long long)c.valueBytes,
                (unsigned long long)c.pageSize, (unsigned long long)c.batch, c.reps, sync.c_str(),
                c.cache ? "on" : "off");
    std::printf("  checksum backend: %s\n\n", internal::checksumBackend());

    bench::Report report("nosql-kv");
    if (const std::string j = args.get("--json", ""); !j.empty())
        report.jsonPath(j);
    if (const std::string v = args.get("--csv", ""); !v.empty())
        report.csvPath(v);
    report.header();

    bench::Scratch scratch("nosql-bench");
    const std::vector<std::string> keys = makeKeys(c.entries);
    const std::vector<std::uint32_t> order = shuffled(c.entries, 42);
    const std::string value(c.valueBytes, 'x');
    const std::string value2(c.valueBytes, 'y');
    const std::uint64_t n = c.entries;

    // --- bulk append: N entries, one transaction -----------------------------
    if (c.wants("append")) {
        Env store;
        report.run("append (1 txn)", n, n * c.valueBytes, c.reps,
                   [&] {
                       store.close();
                       std::filesystem::remove(scratch.file("append.db"));
                       store = openStore(c, scratch.file("append.db"));
                   },
                   [&] { bulkLoad(store, keys, value); });
    }

    // --- random inserts in batches --------------------------------------------
    if (c.wants("insert")) {
        Env store;
        report.run("insert random (batched)", n, n * c.valueBytes, c.reps,
                   [&] {
                       store.close();
                       std::filesystem::remove(scratch.file("insert.db"));
                       store = openStore(c, scratch.file("insert.db"));
                   },
                   [&] {
                       for (std::uint64_t at = 0; at < n; at += c.batch) {
                           const std::uint64_t end = std::min(n, at + c.batch);
                           store.write([&](Txn& tx) {
                               Db d = tx.mainDb();
                               for (std::uint64_t i = at; i < end; ++i)
                                   d.put(keys[order[i]], value);
                           });
                       }
                   });
    }

    // The remaining workloads share one loaded store.
    Env store = openStore(c, scratch.file("main.db"));
    bulkLoad(store, keys, value);

    if (c.wants("get")) {
        report.run("get random (1 txn)", n, n * c.valueBytes, c.reps, [&] {
            std::size_t hits = 0;
            store.read([&](Txn& tx) {
                Db d = tx.mainDb();
                for (std::uint32_t i : order)
                    hits += d.get(keys[i]).has_value();
            });
            if (hits != n)
                std::printf("  !! %zu misses\n", std::size_t(n - hits));
        });

        // One transaction per lookup: what a request-per-statement caller does.
        const std::uint64_t m = std::min<std::uint64_t>(n, 200000);
        report.run("get random (txn each)", m, m * c.valueBytes, c.reps, [&] {
            std::size_t hits = 0;
            for (std::uint64_t k = 0; k < m; ++k) {
                Txn tx = store.readTxn();
                hits += tx.mainDb().get(keys[order[k]]).has_value();
                tx.abort();
            }
            if (hits != m)
                std::printf("  !! %zu misses\n", std::size_t(m - hits));
        });

        // Keys that are not there: the whole path is walked and nothing found.
        report.run("get missing (1 txn)", m, 0, c.reps, [&] {
            std::size_t hits = 0;
            store.read([&](Txn& tx) {
                Db d = tx.mainDb();
                std::string k;
                for (std::uint64_t i = 0; i < m; ++i) {
                    k = keys[order[i]];
                    k[0] = 'z';
                    hits += d.get(k).has_value();
                }
            });
            if (hits)
                std::printf("  !! %zu unexpected hits\n", hits);
        });
    }

    if (c.wants("scan")) {
        report.run("scan ordered (cursor)", n, n * (16 + c.valueBytes), c.reps, [&] {
            std::size_t seen = 0;
            store.read([&](Txn& tx) {
                Cursor cur = tx.mainDb().cursor();
                for (bool ok = cur.first(); ok; ok = cur.next())
                    seen += cur.value().size() ? 1 : 0;
            });
            if (seen != n)
                std::printf("  !! saw %zu\n", seen);
        });
        report.run("scan ordered (range)", n, n * (16 + c.valueBytes), c.reps, [&] {
            std::size_t seen = 0;
            store.read([&](Txn& tx) {
                for (auto [k, v] : tx.mainDb().all())
                    seen += v.size() ? 1 : 0;
            });
            if (seen != n)
                std::printf("  !! saw %zu\n", seen);
        });

        // Short windows: 100 entries starting at a random key, the shape of a
        // secondary-index probe or a paged listing.
        const std::uint64_t windows = std::min<std::uint64_t>(n, 100000);
        report.run("range 100 entries (txn each)", windows, windows * 100 * (16 + c.valueBytes),
                   c.reps, [&] {
                       std::size_t seen = 0;
                       for (std::uint64_t w = 0; w < windows; ++w) {
                           const std::uint64_t start = order[w] % (n > 100 ? n - 100 : 1);
                           Txn tx = store.readTxn();
                           for (auto [k, v] : tx.mainDb().between(keys[start], keys[start + 100]))
                               seen += v.size() ? 1 : 0;
                           tx.abort();
                       }
                       if (seen != windows * 100)
                           std::printf("  !! saw %zu\n", seen);
                   });
    }

    if (c.wants("overwrite")) {
        const std::uint64_t m = n / 4;
        int flip = 0;
        report.run("overwrite random (batched)", m, m * c.valueBytes, c.reps, [&] {
            const std::string& v = (flip++ & 1) ? value : value2;
            for (std::uint64_t at = 0; at < m; at += c.batch) {
                const std::uint64_t end = std::min(m, at + c.batch);
                store.write([&](Txn& tx) {
                    Db d = tx.mainDb();
                    for (std::uint64_t i = at; i < end; ++i)
                        d.put(keys[order[i]], v);
                });
            }
        });
    }

    if (c.wants("commit")) {
        const std::uint64_t txns = std::min<std::uint64_t>(n / 10, 20000);
        std::uint64_t round = 0;
        report.run("commit 10 puts (no fsync)", txns, 0, c.reps, [&] {
            Env quick = Env::configure()
                            .pageSize(c.pageSize)
                            .sync(Durability::None)
                            .cacheReadChecksums(c.cache)
                            .open(scratch.file("commit.db"));
            for (std::uint64_t t = 0; t < txns; ++t) {
                quick.write([&](Txn& tx) {
                    Db d = tx.mainDb();
                    for (int i = 0; i < 10; ++i)
                        d.put(keys[(round + t * 10 + std::uint64_t(i)) % n], value);
                });
            }
            round += txns * 10;
        });
        report.run("commit 1 put (no fsync)", txns, 0, c.reps, [&] {
            Env quick = Env::configure()
                            .pageSize(c.pageSize)
                            .sync(Durability::None)
                            .cacheReadChecksums(c.cache)
                            .open(scratch.file("commit1.db"));
            for (std::uint64_t t = 0; t < txns; ++t)
                quick.write([&](Txn& tx) { tx.mainDb().put(keys[(round + t) % n], value); });
            round += txns;
        });
        const std::uint64_t durable = 200;
        report.run("commit 1 put (fsync, Safe)", durable, 0, std::min(c.reps, 2), [&] {
            Env safe = Env::configure()
                           .pageSize(c.pageSize)
                           .sync(Durability::Safe)
                           .open(scratch.file("durable.db"));
            for (std::uint64_t t = 0; t < durable; ++t)
                safe.write([&](Txn& tx) { tx.mainDb().put(keys[(round + t) % n], value); });
            round += durable;
        });
    }

    if (c.wants("overflow")) {
        const std::uint64_t big = 64 * 1024;
        const std::uint64_t m = 2000;
        const std::string payload(big, 'b');
        Env ov = Env::configure()
                     .pageSize(c.pageSize)
                     .sync(Durability::None)
                     .cacheReadChecksums(c.cache)
                     .open(scratch.file("overflow.db"));
        report.run("overflow put 64 KiB (batched)", m, m * big, c.reps, [&] {
            for (std::uint64_t at = 0; at < m; at += 100) {
                ov.write([&](Txn& tx) {
                    Db d = tx.mainDb();
                    for (std::uint64_t i = at; i < at + 100; ++i)
                        d.put(keys[i], payload);
                });
            }
        });
        report.run("overflow get 64 KiB (1 txn)", m, m * big, c.reps, [&] {
            std::size_t bytes = 0;
            ov.read([&](Txn& tx) {
                Db d = tx.mainDb();
                for (std::uint64_t i = 0; i < m; ++i)
                    bytes += d.at(keys[i]).size();
            });
            if (bytes != m * big)
                std::printf("  !! read %zu bytes\n", bytes);
        });
    }

    if (c.wants("erase")) {
        const std::uint64_t m = n / 4;
        report.run("erase+reinsert random (batched)", m, 0, c.reps, [&] {
            for (std::uint64_t at = 0; at < m; at += c.batch) {
                const std::uint64_t end = std::min(m, at + c.batch);
                store.write([&](Txn& tx) {
                    Db d = tx.mainDb();
                    for (std::uint64_t i = at; i < end; ++i)
                        d.erase(keys[order[i]]);
                });
            }
            for (std::uint64_t at = 0; at < m; at += c.batch) {
                const std::uint64_t end = std::min(m, at + c.batch);
                store.write([&](Txn& tx) {
                    Db d = tx.mainDb();
                    for (std::uint64_t i = at; i < end; ++i)
                        d.put(keys[order[i]], value);
                });
            }
        });
    }

    if (c.wants("commit") || c.wants("loaded")) {
        // The same single-put commit, but on the loaded store after the batched
        // churn above: its free list holds thousands of pages, and a commit that
        // rewrites the whole list to draw one page from it shows up here.
        const std::uint64_t txns = 5000;
        std::uint64_t round = 0;
        report.run("commit 1 put (loaded store, big free list)", txns, 0, c.reps, [&] {
            for (std::uint64_t t = 0; t < txns; ++t)
                store.write([&](Txn& tx) { tx.mainDb().put(keys[(round + t) % n], value2); });
            round += txns;
        });
    }

    if (c.wants("readers") && c.threads > 0) {
        // Readers hammer random gets while the writer commits small batches.
        // Reports aggregate reader throughput; the writer's commits are the
        // interference.
        const std::uint64_t perThread = std::min<std::uint64_t>(n, 200000);
        std::atomic<std::uint64_t> writerCommits{0};
        report.run("get random, " + std::to_string(c.threads) + " readers + writer",
                   perThread * c.threads, 0, c.reps, [&] {
                       std::atomic<bool> stop{false};
                       std::thread writer([&] {
                           std::uint64_t k = 0;
                           while (!stop.load(std::memory_order_relaxed)) {
                               store.write([&](Txn& tx) {
                                   Db d = tx.mainDb();
                                   for (int i = 0; i < 100; ++i)
                                       d.put(keys[order[(k++) % n]], value);
                               });
                               writerCommits.fetch_add(1, std::memory_order_relaxed);
                           }
                       });
                       std::vector<std::thread> readers;
                       for (unsigned t = 0; t < c.threads; ++t) {
                           readers.emplace_back([&, t] {
                               std::size_t hits = 0;
                               const std::uint64_t from = (t * 7919) % n;
                               for (std::uint64_t i = 0; i < perThread; i += 1000) {
                                   Txn tx = store.readTxn();
                                   Db d = tx.mainDb();
                                   for (std::uint64_t j = i; j < i + 1000 && j < perThread; ++j)
                                       hits += d.get(keys[order[(from + j) % n]]).has_value();
                                   tx.abort();
                               }
                               if (hits != perThread)
                                   std::printf("  !! reader %u saw %zu hits\n", t, hits);
                           });
                       }
                       for (std::thread& r : readers)
                           r.join();
                       stop.store(true);
                       writer.join();
                   });
        std::printf("      (writer committed %llu batches of 100 across all repetitions)\n",
                    (unsigned long long)writerCommits.load());
    }

    if (c.wants("reopen")) {
        report.run("close + reopen", 1, 0, c.reps, [&] {
            store.close();
            store = openStore(c, scratch.file("main.db"));
        });
    }

    const EnvStats s = store.stats();
    std::printf("\n  store: file %s, %llu pages used, %llu free, buffers retained %s\n",
                bench::humanBytes(static_cast<long long>(s.fileSize)).c_str(),
                (unsigned long long)s.usedPages, (unsigned long long)s.freePages,
                bench::humanBytes(static_cast<long long>(s.bufferBytes)).c_str());
    std::printf("  process: rss %s, peak rss %s, allocator footprint %s\n",
                bench::humanBytes(bench::residentBytes()).c_str(),
                bench::humanBytes(bench::peakResidentBytes()).c_str(),
                bench::humanBytes(bench::heapBytes()).c_str());
    report.finish();
    return 0;
}
