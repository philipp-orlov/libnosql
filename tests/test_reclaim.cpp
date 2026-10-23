// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The free list is what keeps a copy-on-write store from growing for ever.
#include <random>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/free_list.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

/// A free-list chunk as runs: what it lists comes back, it is never longer
/// than the array, and a value that is neither form, or lists more than the
/// limit, is refused before any page is handed out.
TEST(freeListRunsRoundTripAndRefuseNonsense)
{
    using namespace nosql::internal;
    std::mt19937_64 rng(8);
    for (int round = 0; round < 2000; ++round) {
        std::vector<PageNo> pages;
        PageNo p = 2 + rng() % 1000;
        const std::size_t n = rng() % 300;
        for (std::size_t i = 0; i < n; ++i) {
            pages.push_back(p);
            p += rng() % 4 ? 1 : 1 + rng() % (round % 3 ? 50 : 1ull << 40);
        }
        const std::size_t bytes = freeRunsBytes(pages.data(), pages.size());
        if (!bytes)
            continue;  // the array is no longer
        CHECK(bytes < pages.size() * sizeof(PageNo));
        CHECK(bytes % sizeof(PageNo) != 0);
        std::vector<std::byte> v(bytes);
        writeFreeRuns(v.data(), bytes, pages.data(), pages.size());
        std::vector<PageNo> back;
        std::size_t count = 0;
        CHECK(forEachFreePage(Slice(v.data(), v.size()), 300, count,
                              [&](PageNo q) { back.push_back(q); }));
        CHECK_EQ(count, pages.size());
        CHECK(back == pages);
        // Below the count it lists: refused, and nothing handed out.
        if (!pages.empty()) {
            std::size_t calls = 0;
            CHECK(!forEachFreePage(Slice(v.data(), v.size()), pages.size() - 1, count,
                                   [&](PageNo) { ++calls; }));
            CHECK_EQ(calls, 0u);
        }
        // Cut short, or with a byte too many: neither form.
        CHECK(!forEachFreePage(Slice(v.data(), v.size() - 1), 300, count, [](PageNo) {}) ||
              (v.size() - 1) % sizeof(PageNo) == 0);
        std::vector<std::byte> longer = v;
        longer.push_back(std::byte(7));
        CHECK(!forEachFreePage(Slice(longer.data(), longer.size()), 300, count, [](PageNo) {}) ||
              longer.size() % sizeof(PageNo) == 0);
    }
    // A run whose length would wrap the page numbers.
    std::byte wrap[16];
    std::byte* at = putVarint(wrap, 1);
    at = putVarint(at, ~std::uint64_t(0) - 3);
    at = putVarint(at, 9);
    std::size_t count = 0;
    CHECK(!forEachFreePage(Slice(wrap, std::size_t(at - wrap)), 300, count, [](PageNo) {}));
}

/// A store at format 8 lists freed pages as runs: a commit that frees a
/// large contiguous tree needs far fewer free-list pages, and the pages come
/// back all the same. (A store at 7 keeps arrays, which older builds read.)
TEST(freeListRunsTakeFewerPagesAndStillReclaim)
{
    auto run = [](bool format8) {
        tst::Scratch s("reclaim-runs");
        Env e = Env::configure().pageSize(4096).open(s.file());
        if (format8)  // raised by its first PrefixKeys tree
            e.write([](Txn& t) { t.db("p", DbFlags::Create | DbFlags::PrefixKeys).put("k", "v"); });
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 100000; ++i)
                d.put(tst::keyOf(i), tst::blob(200, i));
        });
        const std::uint64_t full = e.stats().usedPages;
        std::uint64_t grown = 0;
        {
            Txn pin = e.readTxn();  // keeps the freed pages listed, not reused
            const std::uint64_t before = e.stats().usedPages;
            e.write([](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 100000; ++i)
                    d.erase(tst::keyOf(i));
            });
            grown = e.stats().usedPages - before;
            CHECK(e.stats().freePages > full / 2);
        }
        e.read([](Txn& t) { checkIntegrity(t); });
        // Released: a refill takes the listed pages instead of growing. (The
        // ticks go elsewhere: a refill landing in front of a key splits its
        // pages evenly rather than densely, and takes twice the pages.)
        e.write([](Txn& t) { t.db("tick", DbFlags::Create).put("tick", "1"); });
        e.write([](Txn& t) { t.db("tick").put("tick", "2"); });
        const std::uint64_t used = e.stats().usedPages;
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 100000; ++i)
                d.put(tst::keyOf(i), tst::blob(200, i + 1));
        });
        CHECK(e.stats().usedPages < used + used / 10);
        e.read([](Txn& t) { checkIntegrity(t); });
        return grown;
    };
    const std::uint64_t arrays = run(false), runs = run(true);
    std::printf("    pages the deleting commit added: %llu as arrays, %llu as runs\n",
                (unsigned long long)arrays, (unsigned long long)runs);
    // Some 5,600 freed pages: a dozen pages of arrays, a node of runs; the
    // rest is the commit's own tree and catalog pages.
    if (!NOSQL_PREFIX_KEYS_DEFAULT)  // else both stores are at 8
        CHECK(runs + 8 < arrays);
}

TEST(steadyStateRewritesDoNotGrowTheFile)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(4096).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 5000; ++i)
            d.put(tst::keyOf(i), tst::blob(100, i));
    });
    const std::uint64_t warm = e.stats().usedPages;

    for (int round = 0; round < 60; ++round)
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 200; ++i) {
                const int k = (round * 200 + i) % 5000;
                d.put(tst::keyOf(k), tst::blob(100, k + round));
            }
        });

    const std::uint64_t after = e.stats().usedPages;
    // Without reclamation this would be tens of thousands of pages larger.
    CHECK(after < warm * 2);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(deleteEverythingThenRefillReusesPages)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 8000; ++i)
            d.put(tst::keyOf(i), tst::blob(50, i));
    });
    const std::uint64_t full = e.stats().usedPages;

    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 8000; ++i)
            d.erase(tst::keyOf(i));
        CHECK_EQ(d.count(), std::uint64_t(0));
    });
    // Two commits later the emptied pages must be back on the free list.
    e.write([](Txn& t) { t.mainDb().put("tick", "1"); });
    e.write([](Txn& t) { t.mainDb().put("tick", "2"); });
    CHECK(e.stats().freePages > full / 2);

    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 8000; ++i)
            d.put(tst::keyOf(i), tst::blob(50, i));
    });
    // The refill lands in front of "tick", so its splits are balanced rather
    // than append-dense; compressed leaves make `full` smaller, not that.
    CHECK(e.stats().usedPages < full * (NOSQL_PREFIX_KEYS_DEFAULT ? 3 : 2));
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(aParkedReaderHoldsPagesBackThenLetsThemGo)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 3000; ++i)
            d.put(tst::keyOf(i), tst::blob(80, i));
    });

    std::uint64_t pinnedGrowth = 0, freeGrowth = 0;
    {
        Txn reader = e.readTxn();
        CHECK_EQ(reader.mainDb().count(), std::uint64_t(3000));
        const std::uint64_t start = e.stats().usedPages;
        for (int round = 0; round < 20; ++round)
            e.write([&](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 300; ++i)
                    d.put(tst::keyOf(i), tst::blob(80, i + round * 7));
            });
        pinnedGrowth = e.stats().usedPages - start;
        // The snapshot still reads its own version of the world.
        CHECK_EQ(reader.mainDb().at(tst::keyOf(0)).string(), tst::blob(80, 0));
        CHECK_EQ(e.stats().oldestReader, reader.id());
        reader.abort();
    }
    {
        const std::uint64_t start = e.stats().usedPages;
        for (int round = 0; round < 20; ++round)
            e.write([&](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 300; ++i)
                    d.put(tst::keyOf(i), tst::blob(80, i + round * 11));
            });
        freeGrowth = e.stats().usedPages - start;
    }
    // With nothing pinning the free list, the same work should reuse pages.
    CHECK(freeGrowth < pinnedGrowth);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(freeListSurvivesReopen)
{
    tst::Scratch s("reclaim");
    {
        Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 4000; ++i)
                d.put(tst::keyOf(i), tst::blob(60, i));
        });
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 4000; ++i)
                d.erase(tst::keyOf(i));
        });
    }
    Env e = Env::configure().pageSize(1024).open(s.file());
    const std::uint64_t reopened = e.stats().usedPages;
    CHECK(e.stats().freePages > 100);
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 4000; ++i)
            d.put(tst::keyOf(i), tst::blob(60, i));
        checkIntegrity(t);
    });
    CHECK(e.stats().usedPages < reopened + 200);
}

TEST(dropReturnsEveryPage)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.db("scratch", DbFlags::Create);
        for (int i = 0; i < 5000; ++i)
            d.put(tst::keyOf(i), tst::blob(200, i));
    });
    const std::uint64_t full = e.stats().usedPages;
    e.write([](Txn& t) { t.dropDb("scratch"); });
    e.write([](Txn& t) { t.mainDb().put("tick", "1"); });
    CHECK(e.stats().freePages > full / 2);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

int main()
{
    return tst::runAll("reclaim");
}
