// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Snapshot isolation: readers see the store as of the commit they started on,
// and keep seeing it while the writer moves on.
#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

static void checkFrozenSnapshot(bool cached)
{
    tst::Scratch s("mvcc");
    Env e = Env::configure().cacheReadChecksums(cached).open(s.file());
    e.write([](Txn& t) { t.mainDb().put("v", "first"); });

    Txn reader = e.readTxn();
    CHECK_EQ(reader.mainDb().at("v").string(), std::string("first"));

    e.write([](Txn& t) { t.mainDb().put("v", "second"); });
    e.write([](Txn& t) { t.mainDb().put("v", "third"); });

    CHECK_EQ(reader.mainDb().at("v").string(), std::string("first"));
    reader.abort();
    e.read([](Txn& t) { CHECK_EQ(t.mainDb().at("v").string(), std::string("third")); });
}

TEST(readerSeesAFrozenSnapshot)
{
    checkFrozenSnapshot(false);
    checkFrozenSnapshot(true);
}

TEST(readerDoesNotSeeUncommittedWrites)
{
    tst::Scratch s("mvcc");
    Env e = Env::configure().open(s.file());
    Txn w = e.writeTxn();
    w.mainDb().put("pending", "1");
    e.read([](Txn& r) { CHECK(!r.mainDb().contains("pending")); });
    w.commit();
    e.read([](Txn& r) { CHECK(r.mainDb().contains("pending")); });
}

TEST(transactionIdsAdvanceByOnePerCommit)
{
    tst::Scratch s("mvcc");
    Env e = Env::configure().open(s.file());
    std::uint64_t prev = e.stats().lastTxn;
    for (int i = 0; i < 5; ++i) {
        e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(i), "v"); });
        CHECK_EQ(e.stats().lastTxn, prev + 1);
        prev = e.stats().lastTxn;
    }
}

static void checkConcurrentReaders(bool cached)
{
    tst::Scratch s("mvcc");
    Env e = Env::configure().cacheReadChecksums(cached).pageSize(1024).maxSize(1ull << 30).open(s.file());
    constexpr int kKeys = 2000;

    // Invariant every reader checks: all values in a snapshot carry the same
    // generation number, because each writer commits a whole sweep at once.
    auto writeGeneration = [&](int gen) {
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < kKeys; ++i)
                d.put(tst::keyOf(i), std::to_string(gen) + ":" + tst::blob(60, gen * 1000 + i));
        });
    };
    writeGeneration(0);

    std::atomic<bool> stop{false};
    std::atomic<int> checks{0};
    std::string errorText;
    std::mutex errorMtx;

    std::vector<std::thread> readers;
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&] {
            try {
                while (!stop.load(std::memory_order_relaxed)) {
                    Txn t = e.readTxn();
                    auto d = t.mainDb();
                    CHECK_EQ(d.count(), std::uint64_t(kKeys));
                    std::string gen;
                    int seen = 0;
                    for (auto [k, v] : d.all()) {
                        (void)k;
                        const std::string cur = v.string().substr(0, v.string().find(':'));
                        if (seen == 0)
                            gen = cur;
                        else if (cur != gen)
                            throw std::runtime_error("torn snapshot: saw generations " + gen +
                                                     " and " + cur);
                        ++seen;
                    }
                    CHECK_EQ(seen, kKeys);
                    t.abort();
                    checks.fetch_add(1);
                }
            } catch (const std::exception& ex) {
                std::lock_guard<std::mutex> lk(errorMtx);
                if (errorText.empty())
                    errorText = ex.what();
                stop.store(true);
            }
        });
    }

    for (int gen = 1; gen <= 12 && !stop.load(); ++gen)
        writeGeneration(gen);
    stop.store(true);
    for (auto& th : readers)
        th.join();

    CHECK(errorText.empty());
    if (!errorText.empty())
        std::printf("        reader said: %s\n", errorText.c_str());
    CHECK(checks.load() > 0);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(concurrentReadersWhileOneWriterChurns)
{
    checkConcurrentReaders(false);
    checkConcurrentReaders(true);
}

TEST(writeTransactionsSerialise)
{
    tst::Scratch s("mvcc");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("counter", std::string(sizeof(std::uint64_t), '\0')); });

    constexpr int kThreads = 8, kPerThread = 40;
    std::vector<std::thread> ws;
    for (int i = 0; i < kThreads; ++i)
        ws.emplace_back([&] {
            for (int n = 0; n < kPerThread; ++n)
                e.write([](Txn& t) {
                    auto d = t.mainDb();
                    std::uint64_t v = d.at("counter").as<std::uint64_t>();
                    ++v;
                    d.put("counter", Slice::ref(v));
                });
        });
    for (auto& th : ws)
        th.join();

    e.read([&](Txn& t) {
        CHECK_EQ(t.mainDb().at("counter").as<std::uint64_t>(),
                 std::uint64_t(kThreads * kPerThread));
    });
}

static void checkReadersAcrossGrowth(bool cached)
{
    tst::Scratch s("mvcc");
    // A tiny initial file guarantees several remaps during the run.
    Env e = Env::configure()
                .cacheReadChecksums(cached)
                .pageSize(1024)
                .initialSize(8192)
                .growthStep(4096)
                .maxSize(1ull << 28)
                .open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 200; ++i)
            d.put(tst::keyOf(i), tst::blob(60, i));
    });

    Txn reader = e.readTxn();
    auto cur = reader.mainDb().cursor();
    CHECK(cur.first());
    const std::string firstKey = cur.key().string();

    for (int round = 0; round < 40; ++round)
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 200; ++i)
                d.put(tst::keyOf(1000 + round * 200 + i), tst::blob(120, i));
        });

    // The parked cursor still points into the mapping it started with.
    CHECK_EQ(cur.key().string(), firstKey);
    int n = 0;
    for (bool ok = cur.first(); ok; ok = cur.next())
        ++n;
    CHECK_EQ(n, 200);
    reader.abort();
}

TEST(readersStayValidAcrossAStoreGrowth)
{
    checkReadersAcrossGrowth(false);
    checkReadersAcrossGrowth(true);
}

namespace {

/// Small commits under now-and-then pinned readers, optionally with large
/// commits mixed in; integrity checked as it goes and after a reopen.
/// Returns the store's page count after warm-up and at the end.
std::pair<std::uint64_t, std::uint64_t> smallCommitWorkload(bool largeToo)
{
    tst::Scratch s("mvcc");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        Db d = t.mainDb();
        for (int i = 0; i < 3000; ++i)
            d.put(tst::keyOf(i), std::string(40, 'v'));
    });
    std::vector<Txn> pinned;
    std::uint64_t usedAfterWarmup = 0;
    for (int round = 0; round < 3000; ++round) {
        // Now and then readers hold old snapshots for a while, so a commit
        // finds the inline entry pinned and has to move it into the tree.
        if (round % 97 == 0)
            pinned.push_back(e.readTxn());
        if (round % 97 == 13)
            pinned.clear();
        e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(round % 3000), "u" + std::to_string(round)); });
        // A large commit leaves a big chunk in the free tree for the small
        // ones after it to work through.
        if (largeToo && round % 311 == 7)
            e.write([&](Txn& t) {
                Db d = t.mainDb();
                for (int i = 0; i < 1500; ++i)
                    d.put(tst::keyOf((i * 7) % 3000), std::string(40, char('a' + round % 26)));
            });
        if (round % 250 == 0) {
            Txn r = e.readTxn();
            checkIntegrity(r);
        }
        if (round == 500)
            usedAfterWarmup = e.stats().usedPages;
    }
    pinned.clear();
    {
        Txn r = e.readTxn();
        checkIntegrity(r);
        CHECK_EQ(r.mainDb().get(tst::keyOf(2999)).value().string(), std::string("u2999"));
    }
    const std::uint64_t usedAtEnd = e.stats().usedPages;
    e.close();
    Env again = Env::configure().open(s.file());
    Txn r = again.readTxn();
    checkIntegrity(r);
    return {usedAfterWarmup, usedAtEnd};
}

}  // namespace

TEST(smallCommitsKeepTheirFreeListInTheMetaPage)
{
    // Space is reused: steady small commits do not grow the store.
    const auto [warm, end] = smallCommitWorkload(false);
    CHECK(end <= warm + 16);
}

TEST(smallCommitsAfterLargeOnesStayConsistent)
{
    smallCommitWorkload(true);
}

int main()
{
    return tst::runAll("mvcc");
}
