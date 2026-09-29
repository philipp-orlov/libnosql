// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Randomised differential testing against std::map, with a full structural
// check after every batch. This is where splits, merges, overflow reuse and
// the free list all get exercised at once.
#include <map>
#include <random>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {

void compareAll(Txn& t, const std::map<std::string, std::string>& oracle)
{
    auto d = t.mainDb();
    CHECK_EQ(d.count(), std::uint64_t(oracle.size()));
    auto it = oracle.begin();
    std::size_t n = 0;
    for (auto [k, v] : d.all()) {
        CHECK(it != oracle.end());
        CHECK_EQ(k.string(), it->first);
        CHECK_EQ(v.string(), it->second);
        ++it;
        ++n;
    }
    CHECK_EQ(n, oracle.size());
    CHECK(it == oracle.end());
}

void mixedWorkload(std::size_t pageSize, unsigned seed, int rounds, int opsPerRound,
                   std::size_t maxValue)
{
    tst::Scratch s("torture");
    Env e = Env::configure().pageSize(pageSize).maxSize(1ull << 30).open(s.file());
    std::map<std::string, std::string> oracle;
    std::mt19937_64 rng(seed);

    for (int round = 0; round < rounds; ++round) {
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < opsPerRound; ++i) {
                const int roll = int(rng() % 100);
                const std::string key = tst::keyOf(rng() % 4000);
                if (roll < 55) {
                    const std::string val = tst::blob(1 + rng() % maxValue, rng());
                    d.put(key, val);
                    oracle[key] = val;
                } else if (roll < 90) {
                    const bool had = oracle.erase(key) != 0;
                    CHECK_EQ(d.erase(key), had);
                } else {
                    auto got = d.get(key);
                    auto want = oracle.find(key);
                    CHECK_EQ(got.has_value(), want != oracle.end());
                    if (got)
                        CHECK_EQ(got->string(), want->second);
                }
            }
            checkIntegrity(t);
            compareAll(t, oracle);
        });

        e.read([&](Txn& t) {
            checkIntegrity(t);
            compareAll(t, oracle);
        });
    }

    // And it all has to still be there after a reopen.
    e.close();
    Env re = Env::configure().pageSize(pageSize).open(s.file());
    re.read([&](Txn& t) {
        checkIntegrity(t);
        compareAll(t, oracle);
    });
}

}  // namespace

TEST(mixedWorkload4kPages)
{
    mixedWorkload(4096, 12345, 25, 400, 64);
}

TEST(mixedWorkload512bPages)
{
    // The smallest legal page size: two entries per page, so nearly every write
    // splits and nearly every erase rebalances.
    mixedWorkload(512, 987654, 25, 250, 40);
}

TEST(mixedWorkloadWithOverflowValues)
{
    mixedWorkload(1024, 555, 15, 200, 6000);
}

TEST(ascendingBulkLoadThenFullDelete)
{
    tst::Scratch s("torture");
    Env e = Env::configure().pageSize(1024).open(s.file());
    constexpr int kN = 20000;

    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < kN; ++i)
            d.put(tst::keyOf(i), tst::blob(30, i), PutMode::Append);
        checkIntegrity(t);
        CHECK_EQ(d.count(), std::uint64_t(kN));
    });

    e.read([&](Txn& t) {
        int i = 0;
        for (auto [k, v] : t.mainDb().all()) {
            CHECK_EQ(k.string(), tst::keyOf(i));
            CHECK_EQ(v.string(), tst::blob(30, i));
            ++i;
        }
        CHECK_EQ(i, kN);
    });

    // Delete from both ends inwards; the tree has to collapse cleanly to empty.
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int lo = 0, hi = kN - 1; lo <= hi; ++lo, --hi) {
            CHECK(d.erase(tst::keyOf(lo)));
            if (lo != hi)
                CHECK(d.erase(tst::keyOf(hi)));
        }
        checkIntegrity(t);
        CHECK_EQ(d.count(), std::uint64_t(0));
        CHECK_EQ(d.stats().depth, 0u);
        CHECK_EQ(d.stats().leafPages, std::uint64_t(0));
    });
}

TEST(descendingInsertOrder)
{
    tst::Scratch s("torture");
    Env e = Env::configure().pageSize(512).open(s.file());
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 4000; i >= 0; --i)
            d.put(tst::keyOf(i), "v");
        checkIntegrity(t);
        CHECK_EQ(d.count(), std::uint64_t(4001));
    });
}

TEST(randomKeyLengths)
{
    tst::Scratch s("torture");
    Env e = Env::configure().pageSize(4096).open(s.file());
    std::map<std::string, std::string> oracle;
    std::mt19937_64 rng(31337);
    const std::size_t kmax = e.maxKeySize();

    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 3000; ++i) {
            const std::string k = tst::blob(1 + rng() % kmax, rng());
            const std::string v = tst::blob(rng() % 300, rng());
            d.put(k, v);
            oracle[k] = v;
        }
        checkIntegrity(t);
        compareAll(t, oracle);
    });

    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (auto it = oracle.begin(); it != oracle.end();) {
            if (rng() % 2) {
                CHECK(d.erase(it->first));
                it = oracle.erase(it);
            } else {
                ++it;
            }
        }
        checkIntegrity(t);
        compareAll(t, oracle);
    });
}

int main()
{
    return tst::runAll("torture");
}
