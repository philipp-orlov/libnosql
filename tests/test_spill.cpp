// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Write transactions larger than dirtyLimit: pages spilled to the file early,
// read back, rewritten, freed, carried through nested transactions and
// thrown away on abort.

#include <map>
#include <random>
#include <vector>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {

using State = std::map<std::string, std::string>;

constexpr std::uint64_t kLimit = 256u << 10;

Env::Options spilling()
{
    return Env::configure().pageSize(4096).sync(Durability::None).dirtyLimit(kLimit);
}

State contentsOf(Txn& txn)
{
    State state;
    for (auto [key, value] : txn.mainDb().all())
        state[key.string()] = value.string();
    return state;
}

/// Puts, overwrites and erases that touch pages spilled earlier in the same
/// transaction, reads some back, and checks the budget as it goes. Values up
/// to three pages, so overflow runs spill as well.
void churn(Env& env, Txn& txn, State& expect, std::uint64_t seed, int ops, std::uint64_t* peak)
{
    std::mt19937_64 rng(seed);
    Db db = txn.mainDb();
    for (int i = 0; i < ops; ++i) {
        const std::string key = tst::keyOf(rng() % 20000);
        switch (rng() % 8) {
            case 0:
                if (expect.erase(key))
                    CHECK(db.erase(key));
                break;
            case 1: {
                const auto it = expect.find(key);
                if (it != expect.end())
                    CHECK(db.at(key).string() == it->second);
                break;
            }
            default: {
                const std::size_t size = rng() % 32 == 0 ? 4096 + rng() % 8192 : 16 + rng() % 300;
                const std::string value = tst::blob(size, rng());
                db.put(key, value);
                expect[key] = value;
            }
        }
        if (peak)
            *peak = std::max(*peak, env.stats().dirtyBytes);
    }
}

}  // namespace

/// A transaction many times the budget commits whole, never holding more
/// than the budget, and lays out exactly the pages it would have without one.
TEST(aTransactionLargerThanTheDirtyBudgetCommits)
{
    State expect;
    std::uint64_t peak = 0;
    std::uint64_t spilledPages = 0;
    {
        tst::Scratch scratch("spill-big");
        Env env = spilling().open(scratch.file());
        env.write([&](Txn& txn) { churn(env, txn, expect, 1, 60000, &peak); });
        env.read([&](Txn& txn) {
            checkIntegrity(txn);
            CHECK(contentsOf(txn) == expect);
        });
        spilledPages = env.stats().usedPages;
        CHECK_EQ(env.stats().dirtyBytes, 0u);
    }
    CHECK(peak <= kLimit);
    CHECK(peak > kLimit / 2);  // it did fill the budget

    // The same work, unbounded.
    tst::Scratch scratch("spill-unbounded");
    State again;
    Env env =
        Env::configure().pageSize(4096).sync(Durability::None).dirtyLimit(0).open(scratch.file());
    env.write([&](Txn& txn) { churn(env, txn, again, 1, 60000, nullptr); });
    CHECK(again == expect);
    CHECK_EQ(env.stats().usedPages, spilledPages);
}

/// Spilled pages reach the file before commit, but nothing names them until
/// it: the store reopens on the last commit, whole.
TEST(spilledPagesSurviveReopenAndAbort)
{
    tst::Scratch scratch("spill-reopen");
    State expect;
    {
        Env env = spilling().sync(Durability::Safe).open(scratch.file());
        env.write([&](Txn& txn) { churn(env, txn, expect, 2, 30000, nullptr); });
        State lost = expect;
        CHECK_THROWS(env.write([&](Txn& txn) {
            churn(env, txn, lost, 3, 30000, nullptr);
            throw Error(ErrorCode::InvalidArgument, "abort");
        }),
                     ErrorCode::InvalidArgument);
        CHECK_EQ(env.stats().dirtyBytes, 0u);
        env.read([&](Txn& txn) {
            checkIntegrity(txn);
            CHECK(contentsOf(txn) == expect);
        });
        // The aborted transaction's pages are free again and get reused.
        env.write([&](Txn& txn) { churn(env, txn, expect, 4, 30000, nullptr); });
    }
    Env env = Env::configure().readOnly().open(scratch.file());
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        CHECK(contentsOf(txn) == expect);
    });
}

/// A child spills its own pages and its parent's; what it commits joins the
/// parent's spilled set, and what an aborted child spilled disappears.
TEST(nestedTransactionsSpill)
{
    tst::Scratch scratch("spill-nested");
    Env env = spilling().open(scratch.file());
    State expect;
    std::uint64_t peak = 0;
    env.write([&](Txn& txn) {
        churn(env, txn, expect, 5, 15000, &peak);
        txn.nested([&](Txn& child) { churn(env, child, expect, 6, 15000, &peak); });
        State lost = expect;
        CHECK_THROWS(txn.nested([&](Txn& child) {
            churn(env, child, lost, 7, 15000, &peak);
            throw Error(ErrorCode::InvalidArgument, "abort");
        }),
                     ErrorCode::InvalidArgument);
        CHECK(contentsOf(txn) == expect);
        churn(env, txn, expect, 8, 15000, &peak);
    });
    CHECK(peak <= kLimit);
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        CHECK(contentsOf(txn) == expect);
    });
}

/// A cursor kept across spills re-anchors rather than reading a buffer whose
/// memory went back, and writes through it land.
TEST(cursorsSurviveSpills)
{
    tst::Scratch scratch("spill-cursor");
    Env env = spilling().open(scratch.file());
    State expect;
    env.write([&](Txn& txn) {
        Db db = txn.mainDb();
        for (int i = 0; i < 20000; ++i) {
            const std::string key = tst::keyOf(std::uint64_t(i) * 2);
            const std::string value = tst::blob(100, std::uint64_t(i));
            db.put(key, value);
            expect[key] = value;
        }
        // Walk every key, writing a new one beside each and rewriting every
        // third through the cursor: the writes keep spilling under it.
        Cursor c = db.cursor();
        Slice k, v;
        unsigned seen = 0, visits = 0;
        for (bool ok = c.first(k, v); ok; ok = c.next(k, v)) {
            const std::string key = k.string();
            CHECK(v.string() == expect.at(key));
            ++visits;
            if (key.back() == '+')
                continue;
            ++seen;
            const std::string beside = key + "+";
            const std::string value = tst::blob(150, seen);
            db.put(beside, value);
            expect[beside] = value;
            if (seen % 3 == 0) {
                const std::string again = tst::blob(120, seen + 1000000);
                c.put(key, again);
                expect[key] = again;
            }
        }
        CHECK_EQ(seen, 20000u);
        CHECK_EQ(visits, 40000u);
    });
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        CHECK(contentsOf(txn) == expect);
    });
}

/// A key or value the caller read from this transaction's own pages may sit
/// in a buffer the write spills: it is copied before the buffer goes.
TEST(argumentsReadFromTheTransactionSurviveTheSpillTheyTrigger)
{
    tst::Scratch scratch("spill-alias");
    Env env = spilling().open(scratch.file());
    State expect;
    env.write([&](Txn& txn) {
        Db db = txn.mainDb();
        std::mt19937_64 rng(11);
        for (int i = 0; i < 6000; ++i) {
            const std::string key = tst::keyOf(std::uint64_t(i));
            const std::string value = tst::blob(200 + i % 50, std::uint64_t(i));
            db.put(key, value);
            expect[key] = value;
            // From any page written so far: dirty, or spilled already. Spills
            // go in page order, so the older pages are the ones that go.
            const std::string from = tst::keyOf(rng() % std::uint64_t(i + 1));
            const Slice stored = db.at(from);
            db.put("copy-" + from, stored);
            expect["copy-" + from] = expect.at(from);
            Cursor c = db.cursor();
            CHECK(c.seekExact(from));
            const Slice storedKey = c.key();
            db.put(storedKey, db.at(from));  // the same pair again, both from pages
        }
    });
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        CHECK(contentsOf(txn) == expect);
    });
}

/// A spill alone, with no structural change after it (an erase that finds
/// nothing), still sends open cursors back to re-anchor.
TEST(aSpillWithoutAWriteStillReanchorsCursors)
{
    tst::Scratch scratch("spill-quiet");
    Env env = spilling().open(scratch.file());
    State expect;
    env.write([&](Txn& txn) {
        // A deep tree: its writes claim more headroom than the main tree's.
        Db deep = txn.db("deep", DbFlags::Create);
        // Long keys with one long prefix: branch separators stay long too.
        for (int i = 0; i < 300; ++i)
            deep.put(std::string(900, 'a') + tst::keyOf(std::uint64_t(i)), "v");
        Db db = txn.mainDb();
        for (int i = 0; i < 2000; ++i) {
            const std::string key = tst::keyOf(std::uint64_t(i) * 10);
            db.put(key, tst::blob(100, std::uint64_t(i)));
            expect[key] = tst::blob(100, std::uint64_t(i));
        }
    });
    env.write([&](Txn& txn) {
        Db db = txn.mainDb();
        Db deep = txn.db("deep");
        CHECK(deep.stats().depth >= 4u);
        CHECK(db.stats().depth <= 2u);
        // Rewrite until the budget is close enough to full that the deep
        // tree's headroom, but not the main tree's, overflows it.
        int i = 0;
        for (; i < 20000; ++i) {
            const std::uint64_t dirty = env.stats().dirtyBytes;
            if (dirty > kLimit - 66 * 1024 && dirty <= kLimit - 50 * 1024)
                break;
            const std::string key = tst::keyOf(std::uint64_t(i % 2000) * 10);
            const std::string value = tst::blob(100, std::uint64_t(i) + 7);
            db.put(key, value);
            expect[key] = value;
        }
        CHECK(i < 20000);
        std::vector<Cursor> cursors;
        for (int k = 0; k < 2000; k += 100) {
            cursors.push_back(db.cursor());
            CHECK(cursors.back().seekExact(tst::keyOf(std::uint64_t(k) * 10)));
        }
        const std::uint64_t before = env.stats().dirtyBytes;
        CHECK(!deep.erase("absent"));
        CHECK(env.stats().dirtyBytes < before);  // it spilled
        int k = 0;
        for (Cursor& c : cursors) {
            Slice key, value;
            CHECK(c.key().string() == tst::keyOf(std::uint64_t(k) * 10));
            CHECK(c.next(key, value));
            const auto it = expect.upper_bound(tst::keyOf(std::uint64_t(k) * 10));
            CHECK(key.string() == it->first);
            CHECK(value.string() == it->second);
            k += 100;
        }
    });
}

/// With spilling off the budget is a hard limit, as before.
TEST(spillingOffFailsAtTheBudget)
{
    tst::Scratch scratch("spill-off");
    Env env = spilling().spillDirtyPages(false).open(scratch.file());
    State expect;
    CHECK_THROWS(env.write([&](Txn& txn) { churn(env, txn, expect, 9, 60000, nullptr); }),
                 ErrorCode::OutOfMemory);
    CHECK_EQ(env.stats().dirtyBytes, 0u);
}

int main()
{
    return tst::runAll("spill");
}
