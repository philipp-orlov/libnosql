// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Optimistic write transactions: buffered writes validated against the keys
// later commits touched, replayed under the writer slot.
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "nosql/optimistic.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

static std::optional<std::string> valueOf(Env& e, const char* key)
{
    return e.read([&](Txn& t) -> std::optional<std::string> {
        const auto v = t.mainDb().get(key);
        if (!v)
            return std::nullopt;
        return v->string();
    });
}

TEST(optimisticReadsItsOwnWrites)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("a", "1"); });

    OptimisticTxn t(e);
    CHECK(t.get("", "a") == std::optional<std::string>("1"));
    t.put("", "a", "2");
    CHECK(t.get("", "a") == std::optional<std::string>("2"));
    t.erase("", "a");
    CHECK(!t.get("", "a").has_value());
    t.put("", "b", "3");
    t.commit();

    CHECK(!valueOf(e, "a").has_value());
    CHECK(valueOf(e, "b") == std::optional<std::string>("3"));
}

TEST(optimisticWritesAreInvisibleUntilCommit)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    OptimisticTxn t(e);
    t.put("", "pending", "1");
    CHECK(!valueOf(e, "pending").has_value());
    t.abort();
    CHECK(!valueOf(e, "pending").has_value());
}

TEST(optimisticCreatesNamedDbsAtCommit)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    OptimisticTxn t(e);
    CHECK(!t.get("tags", "k").has_value());  // absent sub-database reads as absent
    t.put("tags", "k", "v");
    t.commit();
    e.read([](Txn& r) {
        CHECK(r.hasDb("tags"));
        CHECK(r.db("tags").get("k") && r.db("tags").get("k")->string() == "v");
    });
    OptimisticTxn gone(e);
    gone.erase("missing", "k");  // deleting from an absent sub-database is a no-op
    gone.commit();
}

TEST(optimisticSameKeyConflicts)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("a", "0"); });

    OptimisticTxn first(e);
    OptimisticTxn second(e);
    first.get("", "a");
    first.put("", "a", "1");
    second.get("", "a");
    second.put("", "a", "2");

    first.commit();
    CHECK_THROWS([&] { second.commit(); }(), ErrorCode::Conflict);
    CHECK(valueOf(e, "a") == std::optional<std::string>("1"));
}

TEST(optimisticDisjointKeysBothCommit)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    OptimisticTxn first(e);
    OptimisticTxn second(e);
    first.put("", "a", "1");
    second.put("", "b", "2");
    first.commit();
    second.commit();
    CHECK(valueOf(e, "a") == std::optional<std::string>("1"));
    CHECK(valueOf(e, "b") == std::optional<std::string>("2"));
}

TEST(optimisticReadOnlyUseIsValidated)
{
    // A transaction that only read a key still has to serialize after the
    // commit that changed it, or its decisions were made on stale data.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("a", "0"); });

    OptimisticTxn reader(e);
    CHECK(reader.get("", "a") == std::optional<std::string>("0"));
    e.write([](Txn& t) { t.mainDb().put("a", "1"); });
    CHECK_THROWS([&] { reader.commit(); }(), ErrorCode::Conflict);
}

TEST(optimisticConflictsWithPlainWriterOnAnyKey)
{
    // A plain write names no keys, so it conservatively conflicts with every
    // optimistic transaction that began before it.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    OptimisticTxn t(e);
    t.put("", "mine", "1");
    e.write([](Txn& w) { w.mainDb().put("unrelated", "x"); });
    CHECK_THROWS([&] { t.commit(); }(), ErrorCode::Conflict);
    CHECK(!valueOf(e, "mine").has_value());
}

TEST(optimisticFinishedTxnRejectsUse)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    OptimisticTxn t(e);
    t.commit();
    CHECK_THROWS([&] { t.put("", "a", "1"); }(), ErrorCode::BadTransaction);
    CHECK_THROWS([&] { t.commit(); }(), ErrorCode::BadTransaction);
}

TEST(optimisticHelperRetriesUntilCommitted)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("n", "0"); });
    std::atomic<unsigned> bodies{0};
    // A plain write between the body's read and its commit forces one retry.
    optimistic(e, [&](OptimisticTxn& t) {
        const unsigned attempt = ++bodies;
        const std::string cur = *t.get("", "n");
        if (attempt == 1)
            e.write([](Txn& w) { w.mainDb().put("n", "5"); });
        t.put("", "n", std::to_string(std::stoi(cur) + 1));
    });
    CHECK_EQ(bodies.load(), 2u);
    CHECK(valueOf(e, "n") == std::optional<std::string>("6"));
}

TEST(optimisticConcurrentIncrementsLoseNothing)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    e.write([](Txn& t) { t.mainDb().put("n", "0"); });
    constexpr int kThreads = 4;
    constexpr int kPerThread = 300;
    std::vector<std::thread> workers;
    for (int i = 0; i < kThreads; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < kPerThread; ++j) {
                optimistic(e, [](OptimisticTxn& t) {
                    const std::string cur = *t.get("", "n");
                    t.put("", "n", std::to_string(std::stoll(cur) + 1));
                });
            }
        });
    }
    for (auto& w : workers)
        w.join();
    CHECK(valueOf(e, "n") == std::optional<std::string>(std::to_string(kThreads * kPerThread)));
}

TEST(optimisticWriteSkewIsPrevented)
{
    // Two doctors on call; each may go off call only if the other stays on.
    // Serially the invariant "at least one on call" always holds. Without
    // read validation both could leave at once.
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    constexpr int kRounds = 200;
    std::atomic<int> broken{0};
    for (int round = 0; round < kRounds; ++round) {
        e.write([](Txn& t) {
            t.mainDb().put("alice", "1");
            t.mainDb().put("bob", "1");
        });
        auto leave = [&](const char* self, const char* other) {
            optimistic(e, [&](OptimisticTxn& t) {
                const bool otherOn = *t.get("", other) == "1";
                if (otherOn)
                    t.put("", self, "0");
            });
        };
        std::thread a([&] { leave("alice", "bob"); });
        std::thread b([&] { leave("bob", "alice"); });
        a.join();
        b.join();
        const bool alice = valueOf(e, "alice") == std::optional<std::string>("1");
        const bool bob = valueOf(e, "bob") == std::optional<std::string>("1");
        if (!alice && !bob)
            ++broken;
    }
    CHECK_EQ(broken.load(), 0);
}

int main()
{
    return tst::runAll("optimistic");
}
