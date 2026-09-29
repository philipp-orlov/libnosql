// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The free list is what keeps a copy-on-write store from growing for ever.
#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

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
    CHECK(e.stats().usedPages < full * 2);
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
