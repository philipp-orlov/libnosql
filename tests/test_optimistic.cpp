// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Optimistic write transactions: buffered writes validated against the keys
// later commits touched, replayed under the writer slot.
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
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

TEST(optimisticPlainWriterConflictsOnlyOnItsKeys)
{
    // While an optimistic transaction is alive, plain write transactions record
    // the keys they write, so they conflict with it only on those.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) { w.mainDb().put("read", "0"); });
    OptimisticTxn t(e);
    t.get("", "read");
    t.put("", "mine", "1");
    e.write([](Txn& w) { w.mainDb().put("unrelated", "x"); });
    t.commit();
    CHECK(valueOf(e, "mine") == std::optional<std::string>("1"));

    OptimisticTxn u(e);
    u.get("", "read");
    u.put("", "mine", "2");
    e.write([](Txn& w) { w.mainDb().put("read", "changed"); });
    CHECK_THROWS(u.commit(), ErrorCode::Conflict);
    CHECK(valueOf(e, "mine") == std::optional<std::string>("1"));
}

TEST(optimisticPlainWriterFromBeforeAnyOptimisticIsUnknown)
{
    // A write transaction that began while no optimistic one existed did not
    // record its keys, so it conflicts with every optimistic snapshot it follows.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    Txn w = e.writeTxn();
    OptimisticTxn t(e);
    t.put("", "mine", "1");
    w.mainDb().put("unrelated", "x");
    w.commit();
    CHECK_THROWS(t.commit(), ErrorCode::Conflict);
}

TEST(optimisticPlainClearIsUnknown)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) { w.db("other", DbFlags::Create).put("k", "v"); });
    OptimisticTxn t(e);
    t.put("", "mine", "1");
    e.write([](Txn& w) { w.db("other").clear(); });  // names no keys
    CHECK_THROWS(t.commit(), ErrorCode::Conflict);
    OptimisticTxn u(e);
    u.put("", "mine", "1");
    e.write([](Txn& w) { w.dropDb("other"); });
    CHECK_THROWS(u.commit(), ErrorCode::Conflict);
}

TEST(optimisticPlainNestedAbortDropsItsKeys)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) { w.mainDb().put("x", "0"); });
    OptimisticTxn t(e);
    t.get("", "x");
    t.put("", "mine", "1");
    e.write([](Txn& w) {
        Txn n = w.nested();
        n.mainDb().put("x", "discarded");
        n.abort();
        w.mainDb().put("y", "1");
    });
    t.commit();  // the write to x never happened

    OptimisticTxn u(e);
    u.get("", "x");
    u.put("", "mine", "2");
    e.write([](Txn& w) {
        Txn n = w.nested();
        n.mainDb().put("x", "kept");
        n.commit();
    });
    CHECK_THROWS(u.commit(), ErrorCode::Conflict);
}

TEST(optimisticPlainCursorAndReserveWritesAreTracked)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) {
        w.mainDb().put("a", "0");
        w.mainDb().put("b", "0");
    });
    OptimisticTxn t(e);
    t.get("", "a");
    t.put("", "mine", "1");
    e.write([](Txn& w) {
        Cursor c = w.mainDb().cursor();
        CHECK(c.seekExact("a"));
        c.erase();
    });
    CHECK_THROWS(t.commit(), ErrorCode::Conflict);

    OptimisticTxn u(e);
    u.get("", "b");
    u.put("", "mine", "2");
    e.write([](Txn& w) { std::memcpy(w.mainDb().reserve("b", 1).data(), "R", 1); });
    CHECK_THROWS(u.commit(), ErrorCode::Conflict);

    OptimisticTxn v(e);
    v.get("", "c");
    v.put("", "mine", "3");
    e.write([](Txn& w) { w.mainDb().cursor().put("c", "new"); });
    CHECK_THROWS(v.commit(), ErrorCode::Conflict);
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

namespace {

/// Waits until `done` holds, however slowly the machine runs; fails after a
/// minute rather than hanging the suite.
template <class Pred>
void waitUntil(Pred done)
{
    const auto t0 = std::chrono::steady_clock::now();
    while (!done()) {
        if (std::chrono::steady_clock::now() - t0 > std::chrono::minutes(1))
            CHECK(!"timed out waiting for the commit queue");
        std::this_thread::yield();
    }
}

/// Runs `fn` on its own thread once the writer slot is taken, and waits until
/// its commit is queued before the next: the first committer leads a group of
/// its own and blocks on the slot, the ones after it queue behind it in order
/// and go in the next group together. Each `fn` must commit.
struct HeldWriterSlot
{
    explicit HeldWriterSlot(Env& e) : env(e), blocker(e.writeTxn()) {}
    void run(std::function<void()> fn)
    {
        threads.emplace_back(std::move(fn));
        const unsigned n = unsigned(threads.size());
        waitUntil([&] {
            const EnvStats s = env.stats();
            return s.optimisticCommits == n && s.optimisticQueued == n - 1;
        });
    }
    void release()
    {
        blocker.abort();  // publishes nothing, so it conflicts with nothing
        for (auto& t : threads)
            t.join();
    }
    Env& env;
    Txn blocker;
    std::vector<std::thread> threads;
};

}  // namespace

TEST(optimisticGroupCommitAppliesQueuedCommitsTogether)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    const std::uint64_t before = e.commitGeneration();
    constexpr int kThreads = 8;
    {
        HeldWriterSlot slot(e);
        for (int i = 0; i < kThreads; ++i) {
            slot.run(
                [&e, i] {
                    OptimisticTxn t(e);
                    t.put("", "k" + std::to_string(i), "v");
                    t.commit();
                });
        }
        slot.release();
    }
    for (int i = 0; i < kThreads; ++i)
        CHECK(valueOf(e, ("k" + std::to_string(i)).c_str()) == std::optional<std::string>("v"));
    // One commit for the leader's own group, one for everyone queued behind it.
    CHECK_EQ(e.commitGeneration() - before, std::uint64_t(2));
}

TEST(optimisticConflictInsideAGroupFailsOnlyTheLater)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    e.write([](Txn& t) { t.mainDb().put("x", "start"); });
    std::atomic<int> committed{0}, conflicted{0};
    auto contender = [&](const char* name) {
        return [&, name] {
            OptimisticTxn t(e);
            t.get("", "x");
            t.put("", "x", name);
            try {
                t.commit();
                ++committed;
            } catch (const Error& err) {
                if (err.code() == ErrorCode::Conflict)
                    ++conflicted;
            }
        };
    };
    HeldWriterSlot slot(e);
    slot.run([&] {
        OptimisticTxn t(e);
        t.put("", "lead", "1");
        t.commit();
    });
    slot.run(contender("a"));
    slot.run(contender("b"));
    slot.release();
    CHECK_EQ(committed.load(), 1);
    CHECK_EQ(conflicted.load(), 1);
    const auto x = valueOf(e, "x");
    CHECK(x == std::optional<std::string>("a") || x == std::optional<std::string>("b"));
}

TEST(optimisticReadsDoNotConflictWithLaterWriters)
{
    // Only written keys are recorded: a commit that merely read x does not stop
    // a transaction from writing x, since ordering the reader first explains both.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        t.mainDb().put("x", "0");
        t.mainDb().put("y", "0");
    });
    OptimisticTxn writer(e);
    OptimisticTxn other(e);
    CHECK(other.get("", "x") == std::optional<std::string>("0"));
    other.put("", "y", "1");
    other.commit();
    writer.put("", "x", "2");
    writer.commit();
    CHECK(valueOf(e, "x") == std::optional<std::string>("2"));
    CHECK(valueOf(e, "y") == std::optional<std::string>("1"));
}

TEST(optimisticFailedMemberDoesNotSinkItsGroup)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    e.write([](Txn& t) {
        t.db("ints", DbFlags::Create | DbFlags::IntegerKey)
            .put(Slice::ref(std::uint64_t(1)), "one");
    });
    ErrorCode badCode = ErrorCode::Ok;
    bool goodCommitted = false;
    HeldWriterSlot slot(e);
    slot.run([&] {
        OptimisticTxn t(e);
        t.put("", "lead", "1");
        t.commit();
    });
    slot.run(
        [&] {
            OptimisticTxn t(e);
            t.put("ints", "five!", "bad");  // 5 bytes in an 8-byte integer tree
            try {
                t.commit();
            } catch (const Error& err) {
                badCode = err.code();
            }
        });
    slot.run([&] {
        OptimisticTxn t(e);
        t.put("", "good", "1");
        try {
            t.commit();
            goodCommitted = true;
        } catch (const Error&) {}
    });
    slot.release();
    CHECK(badCode == ErrorCode::InvalidArgument);
    CHECK(goodCommitted);
    CHECK(valueOf(e, "good") == std::optional<std::string>("1"));
    e.read([](Txn& t) { CHECK_EQ(t.db("ints").count(), std::uint64_t(1)); });
}

TEST(optimisticLargeTransactionsLookUpTheirOwnWrites)
{
    // Past a few dozen writes, read-your-writes goes through a hash index.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    std::map<std::string, std::optional<std::string>> model;
    OptimisticTxn t(e);
    for (int i = 0; i < 500; ++i) {
        const std::string k = "k" + std::to_string(i);
        t.put("", k, "v" + std::to_string(i));
        model[k] = "v" + std::to_string(i);
    }
    for (int i = 0; i < 500; i += 3) {
        const std::string k = "k" + std::to_string(i);
        t.erase("", k);
        model[k] = std::nullopt;
    }
    for (int i = 0; i < 500; i += 5) {
        const std::string k = "k" + std::to_string(i);
        t.put("", k, "w" + std::to_string(i));
        model[k] = "w" + std::to_string(i);
    }
    for (const auto& [k, v] : model)
        CHECK(t.get("", k) == v);
    t.commit();
    for (const auto& [k, v] : model)
        CHECK(valueOf(e, k.c_str()) == v);
}

TEST(optimisticViewAndPrecheckedLimits)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("a", "snapshot"); });
    OptimisticTxn t(e);
    const auto fromSnapshot = t.view("", "a");
    CHECK(fromSnapshot && fromSnapshot->string() == "snapshot");
    t.put("", "a", "mine");
    const auto mine = t.view("", "a");
    CHECK(mine && mine->string() == "mine");
    CHECK_THROWS(t.put("", "", "v"), ErrorCode::KeyTooLarge);
    CHECK_THROWS(t.put("", std::string(e.maxKeySize() + 1, 'k'), "v"), ErrorCode::KeyTooLarge);
    t.commit();
    CHECK(valueOf(e, "a") == std::optional<std::string>("mine"));
}

TEST(optimisticReusedBuffersStartEmpty)
{
    // Buffers are recycled per thread; nothing of an earlier transaction, even
    // an aborted or a large one, may show through.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    {
        OptimisticTxn t(e);
        for (int i = 0; i < 100; ++i)
            t.put("named", "k" + std::to_string(i), std::string(100000, 'x'));
        t.abort();
    }
    OptimisticTxn t(e);
    CHECK(!t.get("named", "k1").has_value());
    CHECK(!t.get("", "k1").has_value());
    t.commit();
    e.read([](Txn& r) { CHECK(!r.hasDb("named")); });
}

TEST(optimisticExclusiveAttemptHoldsBackOtherCommits)
{
    // From kExclusiveAfter lost attempts on, a transaction has the store's
    // optimistic commits to itself until it finishes, so it cannot lose again.
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    std::atomic<bool> otherCommitted{false};
    {
        OptimisticTxn t(e, OptimisticTxn::kExclusiveAfter);
        t.put("", "x", "exclusive");
        std::thread other([&] {
            OptimisticTxn o(e);  // its body runs; only its commit waits
            o.put("", "y", "other");
            o.commit();
            otherCommitted = true;
        });
        // A chance for the other commit to go ahead, which it must not take:
        // on a slow machine the check is weaker, never wrong.
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        const bool committedEarly = otherCommitted.load();
        t.commit();
        other.join();  // before any CHECK can throw past a joinable thread
        CHECK(!committedEarly);
    }
    CHECK(otherCommitted.load());
    CHECK(valueOf(e, "x") == std::optional<std::string>("exclusive"));
    CHECK(valueOf(e, "y") == std::optional<std::string>("other"));

    // Aborting gives the turn back too.
    {
        OptimisticTxn t(e, OptimisticTxn::kExclusiveAfter);
        t.abort();
    }
    OptimisticTxn after(e);
    after.put("", "z", "1");
    after.commit();
    CHECK(valueOf(e, "z") == std::optional<std::string>("1"));
}

TEST(optimisticWaitingCommitStillSeesConflictsAfterReleasingItsSnapshot)
{
    // A commit gives back its reader slot while it waits for its group, keeping
    // only its snapshot's id pinned in the log. A write that lands in the
    // meantime must still be in the log when the waiting commit validates, or
    // it would be a lost update.
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    e.write([](Txn& t) { t.mainDb().put("x", "0"); });
    Txn blocker = e.writeTxn();
    ErrorCode outcome = ErrorCode::Ok;
    std::thread waiter([&] {
        OptimisticTxn t(e);
        t.get("", "x");
        t.put("", "x", "waiter");
        try {
            t.commit();  // leads its own group, then waits for the writer slot
        } catch (const Error& err) {
            outcome = err.code();
        }
    });
    // Its commit leads its own group and waits for the writer slot.
    waitUntil([&] { return e.stats().optimisticCommits == 1; });
    blocker.mainDb().put("x", "blocker");
    blocker.commit();
    waiter.join();
    CHECK(outcome == ErrorCode::Conflict);
    CHECK(valueOf(e, "x") == std::optional<std::string>("blocker"));
}

namespace {

std::vector<std::pair<std::string, std::string>> scanAll(OptimisticTxn& t, const char* db,
                                                         Slice lo = Slice(), Slice hi = Slice())
{
    std::vector<std::pair<std::string, std::string>> out;
    t.scan(db, lo, hi, [&](Slice k, Slice v) { out.emplace_back(k.string(), v.string()); });
    return out;
}

using Pairs = std::vector<std::pair<std::string, std::string>>;

}  // namespace

TEST(optimisticScanMergesItsOwnWrites)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) {
        Db d = w.db("items", DbFlags::Create);
        for (const char* k : {"a", "b", "c", "d"})
            d.put(k, std::string("old-") + k);
    });
    OptimisticTxn t(e);
    t.put("items", "b", "new-b");    // overwrite
    t.erase("items", "c");           // delete
    t.put("items", "e", "new-e");    // insert past the end
    t.put("items", "aa", "x");       // insert between
    t.put("items", "aa", "new-aa");  // the latest write wins
    t.erase("items", "zz");          // erase of an absent key shows nothing
    t.put("other", "b", "elsewhere");
    CHECK(
        scanAll(t, "items") ==
        Pairs({{"a", "old-a"}, {"aa", "new-aa"}, {"b", "new-b"}, {"d", "old-d"}, {"e", "new-e"}}));
    CHECK(scanAll(t, "items", "aa", "d") == Pairs({{"aa", "new-aa"}, {"b", "new-b"}}));
    CHECK(scanAll(t, "items", "c", "") == Pairs({{"d", "old-d"}, {"e", "new-e"}}));
    // Stopping early.
    int seen = 0;
    t.scan("items", "", "", [&](Slice, Slice) { return ++seen < 2; });
    CHECK_EQ(seen, 2);
    // A sub-database only this transaction has created.
    t.put("fresh", "k", "v");
    CHECK(scanAll(t, "fresh") == Pairs({{"k", "v"}}));
    CHECK(scanAll(t, "absent").empty());
    t.commit();
}

TEST(optimisticScanFollowsTheSubDatabaseOrder)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) {
        Db r = w.db("rev", DbFlags::Create | DbFlags::ReverseKey);
        r.put("xa", "1");
        r.put("ab", "2");
        Db n = w.db("ints", DbFlags::Create | DbFlags::IntegerKey);
        n.put(Slice::ref(std::uint64_t(300)), "300");
        n.put(Slice::ref(std::uint64_t(2)), "2");
    });
    OptimisticTxn t(e);
    t.put("rev", "zb", "3");  // sorts between "xa" and "ab" back to front
    // Reverse order compares from the last byte: "xa" < "ab" < "zb".
    CHECK(scanAll(t, "rev") == Pairs({{"xa", "1"}, {"ab", "2"}, {"zb", "3"}}));
    const std::uint64_t forty = 40;
    t.put("ints", Slice::ref(forty), "40");
    std::vector<std::string> order;
    t.scan("ints", "", "", [&](Slice, Slice v) { order.push_back(v.string()); });
    CHECK(order == std::vector<std::string>({"2", "40", "300"}));
    t.commit();
}

TEST(optimisticScanConflictsWithLaterWritesToItsSubDatabase)
{
    // A phantom: the scan saw nothing in range, then someone inserted there.
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) { w.db("items", DbFlags::Create).put("z", "outside the range"); });
    OptimisticTxn t(e);
    CHECK(scanAll(t, "items", "a", "c").empty());
    t.put("summary", "count", "0");
    e.write([](Txn& w) { w.db("items").put("b", "phantom"); });
    CHECK_THROWS(t.commit(), ErrorCode::Conflict);

    // An optimistic insert counts the same.
    OptimisticTxn u(e);
    scanAll(u, "items");
    u.put("summary", "count", "2");
    OptimisticTxn inserter(e);
    inserter.put("items", "c", "another");
    inserter.commit();
    CHECK_THROWS(u.commit(), ErrorCode::Conflict);

    // Writes to other sub-databases, and point use of the scanned one by
    // others, do not.
    OptimisticTxn v(e);
    scanAll(v, "items");
    v.put("summary", "count", "3");
    e.write([](Txn& w) { w.db("elsewhere", DbFlags::Create).put("k", "v"); });
    v.commit();
}

TEST(optimisticScanWriteSkewIsPrevented)
{
    // The on-call rule again, but each doctor counts the others with a scan.
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    std::atomic<int> broken{0};
    for (int round = 0; round < 150; ++round) {
        e.write([](Txn& w) {
            Db d = w.db("oncall", DbFlags::Create);
            d.put("alice", "1");
            d.put("bob", "1");
        });
        auto leave = [&](const char* self) {
            optimistic(e, [&](OptimisticTxn& t) {
                int on = 0;
                t.scan("oncall", "", "", [&](Slice, Slice v) { on += v.view() == "1"; });
                if (on >= 2)
                    t.put("oncall", self, "0");
            });
        };
        std::thread a([&] { leave("alice"); });
        std::thread b([&] { leave("bob"); });
        a.join();
        b.join();
        int on = 0;
        e.read([&](Txn& r) {
            Cursor c = r.db("oncall").cursor();
            Slice k, v;
            for (bool ok = c.first(k, v); ok; ok = c.next(k, v))
                on += v.view() == "1";
        });
        if (on == 0)
            ++broken;
    }
    CHECK_EQ(broken.load(), 0);
}

TEST(optimisticWriteDuringScanIsRefused)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& w) { w.db("items", DbFlags::Create).put("a", "1"); });
    OptimisticTxn t(e);
    CHECK_THROWS(t.scan("items", "", "", [&](Slice, Slice) { t.put("items", "b", "2"); }),
                 ErrorCode::BadTransaction);
    t.put("items", "b", "2");  // fine once the scan is over
    t.commit();
}

TEST(optimisticScanConflictsInsideAGroup)
{
    tst::Scratch s("optimistic");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    e.write([](Txn& w) { w.db("items", DbFlags::Create).put("a", "1"); });
    ErrorCode scanner = ErrorCode::Ok;
    HeldWriterSlot slot(e);
    slot.run([&] {
        OptimisticTxn t(e);
        t.put("", "lead", "1");
        t.commit();
    });
    slot.run(
        [&] {
            OptimisticTxn t(e);
            t.put("items", "b", "2");
            t.commit();
        });
    slot.run([&] {
        OptimisticTxn t(e);
        scanAll(t, "items");
        t.put("", "summary", "1");
        try {
            t.commit();
        } catch (const Error& err) {
            scanner = err.code();
        }
    });
    slot.release();
    CHECK(scanner == ErrorCode::Conflict);
}

TEST(optimisticCommitSpinsDoNotChangeOutcomes)
{
    // Zero spins sleep at once; a large count rarely sleeps. Either way every
    // increment lands exactly once.
    for (const unsigned spins : {0u, 4096u}) {
        tst::Scratch s("optimistic");
        Env e = Env::configure().sync(Durability::None).commitSpins(spins).open(s.file());
        e.write([](Txn& t) { t.mainDb().put("n", "0"); });
        std::vector<std::thread> workers;
        for (int i = 0; i < 4; ++i) {
            workers.emplace_back([&] {
                for (int j = 0; j < 200; ++j) {
                    optimistic(e, [](OptimisticTxn& t) {
                        const std::string cur = *t.get("", "n");
                        t.put("", "n", std::to_string(std::stoll(cur) + 1));
                    });
                }
            });
        }
        for (auto& w : workers)
            w.join();
        CHECK(valueOf(e, "n") == std::optional<std::string>("800"));
    }
}

int main()
{
    return tst::runAll("optimistic");
}
