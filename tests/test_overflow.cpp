// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <map>
#include <random>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

TEST(largeValuesRoundtrip)
{
    tst::Scratch s("overflow");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    const std::size_t sizes[] = {1, 500, 1023, 1024, 4096, 65536, 1u << 20};
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (std::size_t i = 0; i < std::size(sizes); ++i)
            d.put(tst::keyOf(i), tst::blob(sizes[i], i));
        checkIntegrity(t);
    });
    e.read([&](Txn& t) {
        auto d = t.mainDb();
        for (std::size_t i = 0; i < std::size(sizes); ++i) {
            const Slice v = d.at(tst::keyOf(i));
            CHECK_EQ(v.size(), sizes[i]);
            CHECK_EQ(v.string(), tst::blob(sizes[i], i));
        }
    });
}

TEST(overflowPagesAreCountedAndReleased)
{
    tst::Scratch s("overflow");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 100; ++i)
            d.put(tst::keyOf(i), tst::blob(10000, i));
        // 10000 bytes + header over 1024-byte pages => 10 pages each.
        CHECK_EQ(d.stats().overflowPages, std::uint64_t(1000));
        checkIntegrity(t);
    });
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 100; ++i)
            d.erase(tst::keyOf(i));
        CHECK_EQ(d.stats().overflowPages, std::uint64_t(0));
        CHECK_EQ(d.count(), std::uint64_t(0));
        checkIntegrity(t);
    });
}

TEST(growingAndShrinkingAValueAcrossTheInlineBoundary)
{
    tst::Scratch s("overflow");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    const std::size_t sizes[] = {10, 5000, 20, 40000, 3, 900, 100000, 1};
    for (std::size_t round = 0; round < std::size(sizes); ++round) {
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            d.put("shape-shifter", tst::blob(sizes[round], round));
            checkIntegrity(t);
        });
        e.read([&](Txn& t) {
            CHECK_EQ(t.mainDb().at("shape-shifter").string(), tst::blob(sizes[round], round));
        });
    }
}

TEST(sameSizeLargeOverwriteReusesTheRun)
{
    tst::Scratch s("overflow");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    e.write([](Txn& t) { t.mainDb().put("k", tst::blob(50000, 0)); });
    const std::uint64_t before = e.stats().usedPages;
    for (int i = 1; i <= 20; ++i)
        e.write([&](Txn& t) {
            t.mainDb().put("k", tst::blob(50000, i));
            checkIntegrity(t);
        });
    e.read([&](Txn& t) { CHECK_EQ(t.mainDb().at("k").string(), tst::blob(50000, 20)); });
    // Each rewrite copies the run once; the free list must hand the pages back
    // rather than letting the file march forwards for ever.
    CHECK(e.stats().usedPages < before + 200);
}

TEST(reserveForALargeValue)
{
    tst::Scratch s("overflow");
    Env e = Env::configure().pageSize(512).maxSize(1ull << 28).open(s.file());
    e.write([](Txn& t) {
        auto w = t.mainDb().reserve("big", 30000);
        CHECK_EQ(w.size(), std::size_t(30000));
        for (std::size_t i = 0; i < w.size(); ++i)
            w.chars()[i] = char('0' + i % 10);
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        const Slice v = t.mainDb().at("big");
        CHECK_EQ(v.size(), std::size_t(30000));
        for (std::size_t i = 0; i < v.size(); ++i)
            CHECK_EQ(v.chars()[i], char('0' + i % 10));
    });
}

TEST(manyLargeValuesMixedWithSmallOnes)
{
    tst::Scratch s("overflow");
    Env e = Env::configure().pageSize(4096).maxSize(1ull << 30).open(s.file());
    std::mt19937_64 rng(4242);
    std::map<std::string, std::size_t> sizes;
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 600; ++i) {
            const std::size_t n = (i % 4 == 0) ? 5000 + rng() % 50000 : rng() % 100;
            d.put(tst::keyOf(i), tst::blob(n, i));
            sizes[tst::keyOf(i)] = n;
        }
        checkIntegrity(t);
    });
    e.read([&](Txn& t) {
        auto d = t.mainDb();
        for (const auto& [k, n] : sizes)
            CHECK_EQ(d.at(k).size(), n);
    });
}

int main()
{
    return tst::runAll("overflow");
}
