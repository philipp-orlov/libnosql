// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Randomised equivalence: whatever a primary does, a replica advanced only
// by shipped deltas must hold exactly the same data.
//
// The workloads here deliberately produce splits, merges, overflow pages,
// sub-database creation and drops, page reclamation and file growth, because
// those are the paths where a capture that reports the wrong page set stops
// being obviously wrong and starts being quietly wrong.
#include <filesystem>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "nosql/replication.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
namespace fs = std::filesystem;

namespace {

using Dump = std::vector<std::pair<std::string, std::string>>;

/// Every tree in the store, named ones included, flattened for comparison.
Dump dumpEnv(Env& e)
{
    Dump out;
    e.read([&](Txn& t) {
        for (auto [k, v] : t.mainDb().all())
            out.emplace_back("main/" + k.string(), v.string());
        for (const std::string& name : t.listDbs()) {
            Db d = t.db(name);
            for (auto [k, v] : d.all())
                out.emplace_back(name + "/" + k.string(), v.string());
        }
    });
    return out;
}

Dump dumpFile(const fs::path& p)
{
    Env e = Env::configure().createIfMissing(false).open(p);
    return dumpEnv(e);
}

/// One transaction's worth of random work.
void randomCommit(Env& e, std::mt19937_64& rng)
{
    const int shape = int(rng() % 100);
    e.write([&](Txn& t) {
        Db main = t.mainDb();
        if (shape < 45) {  // bulk insert
            const std::uint64_t base = rng() % 20000;
            for (int i = 0; i < 200; ++i)
                main.put(tst::keyOf(base + std::uint64_t(i)), tst::blob(8 + rng() % 200, rng()));
        } else if (shape < 60) {  // scattered overwrite
            for (int i = 0; i < 150; ++i)
                main.put(tst::keyOf(rng() % 20000), tst::blob(8 + rng() % 64, rng()));
        } else if (shape < 75) {  // erase a run, which merges pages
            const std::uint64_t base = rng() % 20000;
            for (int i = 0; i < 300; ++i)
                main.erase(tst::keyOf(base + std::uint64_t(i)));
        } else if (shape < 85) {  // values that need overflow pages
            for (int i = 0; i < 6; ++i)
                main.put(tst::keyOf(rng() % 500, "big"), tst::blob(20000 + rng() % 40000, rng()));
        } else if (shape < 95) {  // a named sub-database
            Db d = t.db("sub-" + std::to_string(rng() % 5), DbFlags::Create);
            for (int i = 0; i < 120; ++i)
                d.put(tst::keyOf(rng() % 3000), tst::blob(8 + rng() % 100, rng()));
        } else {  // drop one, so its pages go back to the free list
            const std::vector<std::string> names = t.listDbs();
            if (!names.empty())
                t.db(names[rng() % names.size()]).drop();
        }
    });
}

}  // namespace

TEST(aReplicaFollowsARandomWorkloadExactly)
{
    tst::Scratch dir("repltorture");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db"), bundle = dir.file("d.bundle");

    std::mt19937_64 rng(20240607);
    Env e = Env::configure()
                .sync(Durability::None)
                .initialSize(64 << 10)
                .growthStep(256 << 10)
                .shipTo(ship)
                .shipRetain(1000)
                .open(store);

    for (int i = 0; i < 10; ++i)
        randomCommit(e, rng);
    {
        Txn rt = e.readTxn();
        copySnapshot(rt, replica);
        rt.abort();
    }
    Checkpoint at = checkpointOf(replica);
    ShipLog log(ship);

    // Batches of varying length, so coalescing is exercised over one commit,
    // over a handful, and over a few dozen.
    for (int round = 0; round < 25; ++round) {
        const int commits = 1 + int(rng() % 12);
        for (int i = 0; i < commits; ++i)
            randomCommit(e, rng);

        CHECK(log.extract(at, bundle) == ShipStatus::Ok);
        at = applyBundle(replica, bundle);
        CHECK_EQ(at, checkpointOf(store));
        CHECK(dumpFile(replica) == dumpEnv(e));
    }

    CHECK(at.txnid > 30u);
    Env r = Env::configure().createIfMissing(false).open(replica);
    r.read([](Txn& t) { checkIntegrity(t); });
}

TEST(replayingTheWholeChainOneCommitAtATimeGivesTheSameResult)
{
    tst::Scratch dir("repltorture");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path stepwise = dir.file("stepwise.db"), leap = dir.file("leap.db");
    const fs::path bundle = dir.file("d.bundle");

    std::mt19937_64 rng(987654321);
    Env e = Env::configure()
                .sync(Durability::None)
                .shipTo(ship)
                .shipRetain(1000)
                .open(store);
    for (int i = 0; i < 5; ++i)
        randomCommit(e, rng);
    {
        Txn rt = e.readTxn();
        copySnapshot(rt, stepwise);
        copySnapshot(rt, leap);
        rt.abort();
    }
    const Checkpoint base = checkpointOf(stepwise);

    for (int i = 0; i < 40; ++i)
        randomCommit(e, rng);

    ShipLog log(ship);
    // One bundle per commit ...
    Checkpoint at = base;
    while (at.txnid < log.available().newest) {
        CHECK(log.extract(at, bundle, at.txnid + 1) == ShipStatus::Ok);
        at = applyBundle(stepwise, bundle);
    }
    // ... against one bundle for the lot.
    CHECK(log.extract(base, bundle) == ShipStatus::Ok);
    const Checkpoint jumped = applyBundle(leap, bundle);

    CHECK_EQ(jumped, at);
    CHECK_EQ(jumped, checkpointOf(store));
    CHECK(dumpFile(stepwise) == dumpFile(leap));
    CHECK(dumpFile(leap) == dumpEnv(e));
}

TEST(baseImagesTakenUnderLoadAreAllUsable)
{
    tst::Scratch dir("repltorture");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path bundle = dir.file("d.bundle");

    std::mt19937_64 rng(555);
    Env e = Env::configure()
                .sync(Durability::None)
                .shipTo(ship)
                .shipRetain(1000)
                .open(store);
    for (int i = 0; i < 8; ++i)
        randomCommit(e, rng);

    // A base image every few commits; each must be a store in its own right
    // and each must accept the deltas that follow it.
    std::vector<std::pair<fs::path, Checkpoint>> images;
    for (int round = 0; round < 6; ++round) {
        const fs::path img = dir.file(("base" + std::to_string(round) + ".db").c_str());
        {
            Txn rt = e.readTxn();
            copySnapshot(rt, img);
            rt.abort();
        }
        images.emplace_back(img, checkpointOf(img));
        for (int i = 0; i < 4; ++i)
            randomCommit(e, rng);
    }

    ShipLog log(ship);
    const Dump truth = dumpEnv(e);
    for (auto& [img, taken] : images) {
        CHECK_EQ(checkpointOf(img), taken);
        CHECK(log.extract(taken, bundle) == ShipStatus::Ok);
        CHECK_EQ(applyBundle(img, bundle), checkpointOf(store));
        CHECK(dumpFile(img) == truth);
    }
}

int main()
{
    return tst::runAll("replicationtorture");
}
