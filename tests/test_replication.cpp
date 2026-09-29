// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Replication: checkpoints, base images, bundle format, apply, and the
// divergence checks that make a wrong apply fail rather than corrupt.
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "nosql/replication.hpp"
#include "nosql/internal/atomic_file.hpp"
#include "nosql/internal/io_observer.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
namespace fs = std::filesystem;

namespace {

using Dump = std::vector<std::pair<std::string, std::string>>;

void put(Env& e, std::uint64_t from, std::uint64_t to, const char* value = "v")
{
    e.write([&](Txn& t) {
        Db db = t.mainDb();
        for (std::uint64_t i = from; i < to; ++i)
            db.put(tst::keyOf(i), value);
    });
}

/// Every pair in the main tree, so two stores can be compared without caring
/// how their pages happen to be laid out.
Dump dumpEnv(Env& e)
{
    Dump out;
    e.read([&](Txn& t) {
        for (auto [k, v] : t.mainDb().all())
            out.emplace_back(k.string(), v.string());
    });
    return out;
}

/// The store must not be open: one process, one exclusive lock.
Dump dumpFile(const fs::path& p)
{
    Env e = Env::configure().createIfMissing(false).open(p);
    return dumpEnv(e);
}

void checkFile(const fs::path& p)
{
    Env e = Env::configure().createIfMissing(false).open(p);
    e.read([](Txn& t) { checkIntegrity(t); });
}

void baseImage(Env& e, const fs::path& dst)
{
    Txn rt = e.readTxn();
    copySnapshot(rt, dst);
    rt.abort();
}

/// Flips one byte of a file.
void corrupt(const fs::path& p, std::uint64_t offset)
{
    std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
    f.seekg(std::streamoff(offset));
    char c = 0;
    f.read(&c, 1);
    c = char(c ^ 0xFF);
    f.seekp(std::streamoff(offset));
    f.write(&c, 1);
}

}  // namespace

// ------------------------------------------------------------ checkpoints ---

TEST(checkpointEqualityChecksEveryIdentityField)
{
    const Checkpoint base{1, 2, {3, 4}, {5, 6}};
    CHECK(base == base);
    CHECK(!(base != base));
    for (unsigned field = 0; field < 6; ++field) {
        auto other = base;
        if (field == 0)
            ++other.txnid;
        else if (field == 1)
            ++other.metaChecksum;
        else if (field < 4)
            ++other.storeId[field - 2];
        else
            ++other.commitId[field - 4];
        CHECK(base != other);
        CHECK(other != base);
        CHECK(!(base == other));
    }
}

TEST(checkpointTracksTheLastCommittedTransaction)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file();
    {
        Env e = Env::configure().sync(Durability::None).open(store);
        CHECK_EQ(checkpointOf(store).txnid, 0u);
        for (int i = 0; i < 5; ++i) {
            put(e, std::uint64_t(i) * 10, std::uint64_t(i) * 10 + 10);
            CHECK_EQ(checkpointOf(store).txnid, e.stats().lastTxn);
        }
    }
    const Checkpoint c = checkpointOf(store);
    CHECK_EQ(c.txnid, 5u);
    CHECK(c.valid());
    CHECK(c == checkpointOf(store));
}

TEST(checkpointsDistinguishIndependentStoresAndDivergentClones)
{
    tst::Scratch dir("identity");
    Env primary = Env::configure().shipTo(dir.file("ship")).open(dir.file());
    Env other = Env::configure().open(dir.file("other.db"));
    primary.write([](Txn& txn) { txn.mainDb().put("key", "base"); });
    other.write([](Txn& txn) { txn.mainDb().put("key", "evil"); });
    CHECK(checkpointOf(dir.file()) != checkpointOf(dir.file("other.db")));
    baseImage(primary, dir.file("clone.db"));
    Env clone = Env::configure().open(dir.file("clone.db"));
    primary.write([](Txn& txn) { txn.mainDb().put("key", "next"); });
    clone.write([](Txn& txn) { txn.mainDb().put("key", "evil"); });
    const Checkpoint divergent = checkpointOf(dir.file("clone.db"));
    CHECK(divergent != checkpointOf(dir.file()));
    CHECK_THROWS(ShipLog(dir.file("ship")).extract(divergent, dir.file("delta")),
                 ErrorCode::Incompatible);
}

TEST(checkpointRejectsSomethingThatIsNotAStore)
{
    tst::Scratch dir("repl");
    const fs::path junk = dir.file("junk.bin");
    std::ofstream(junk, std::ios::binary) << std::string(4096, 'x');
    CHECK_THROWS(checkpointOf(junk), ErrorCode::Corrupted);
}

// ------------------------------------------------------------ base images ---

TEST(copySnapshotProducesAStoreAtTheSameCheckpoint)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), copy = dir.file("copy.db");

    Env e = Env::configure().sync(Durability::None).open(store);
    put(e, 0, 500);
    const Checkpoint taken = checkpointOf(store);
    baseImage(e, copy);

    CHECK_EQ(checkpointOf(copy), taken);
    CHECK_EQ(dumpFile(copy).size(), 500u);
    checkFile(copy);
}

TEST(copySnapshotIgnoresWritesThatLandWhileItRuns)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), copy = dir.file("copy.db");

    Env e = Env::configure().sync(Durability::None).open(store);
    put(e, 0, 2000);

    Txn rt = e.readTxn();
    const Checkpoint pinned = checkpointOf(store);
    std::thread writer([&] { put(e, 2000, 6000); });
    copySnapshot(rt, copy);
    writer.join();
    rt.abort();

    CHECK_EQ(checkpointOf(copy), pinned);
    CHECK_EQ(dumpFile(copy).size(), 2000u);
    checkFile(copy);
}

TEST(copySnapshotRefusesAWriteTransactionAndAnExistingPath)
{
    tst::Scratch dir("repl");
    Env e = Env::configure().sync(Durability::None).open(dir.file());
    put(e, 0, 10);

    Txn wt = e.writeTxn();
    CHECK_THROWS(copySnapshot(wt, dir.file("a.db")), ErrorCode::BadTransaction);
    wt.abort();
    CHECK(!fs::exists(dir.file("a.db")));

    Txn rt = e.readTxn();
    copySnapshot(rt, dir.file("b.db"));
    CHECK_THROWS(copySnapshot(rt, dir.file("b.db")), ErrorCode::InvalidArgument);
    rt.abort();
}

// ---------------------------------------------------------------- bundles ---

TEST(aBundleCarriesTheRangeItSays)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship"), bundle = dir.file("d.bundle");

    Env e = Env::configure().sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 100);
    const Checkpoint base = checkpointOf(store);
    put(e, 100, 400);
    put(e, 400, 500);
    const Checkpoint target = checkpointOf(store);

    ShipLog log(ship);
    CHECK_EQ(log.available().oldest, 1u);
    CHECK_EQ(log.available().newest, target.txnid);
    CHECK(log.extract(base, bundle) == ShipStatus::Ok);

    const BundleInfo info = inspectBundle(bundle);
    CHECK_EQ(info.pageSize, std::uint32_t(e.pageSize()));
    CHECK_EQ(info.base, base);
    CHECK_EQ(info.targetTxnid, target.txnid);
    CHECK(info.pageCount > 0);
}

TEST(applyingDeltasReproducesThePrimaryExactly)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db"), bundle = dir.file("d.bundle");

    Env e = Env::configure().sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 300);
    baseImage(e, replica);
    Checkpoint at = checkpointOf(replica);

    ShipLog log(ship);
    for (int round = 0; round < 4; ++round) {
        put(e, 300 + std::uint64_t(round) * 200, 500 + std::uint64_t(round) * 200);
        e.write([&](Txn& t) { t.mainDb().erase(tst::keyOf(std::uint64_t(round))); });

        CHECK(log.extract(at, bundle) == ShipStatus::Ok);
        at = applyBundle(replica, bundle);
        CHECK_EQ(at, checkpointOf(store));
        CHECK(dumpFile(replica) == dumpEnv(e));
    }
    checkFile(replica);
}

TEST(abandonedReplacementLeavesPublishedReplicaUnchanged)
{
    tst::Scratch dir("atomic");
    {
        Env env = Env::configure().open(dir.file());
        env.write([](Txn& txn) { txn.mainDb().put("key", "base"); });
    }
    const Checkpoint before = checkpointOf(dir.file());
    {
        internal::AtomicFile replacement(dir.file());
        const std::string garbage(4096, 'x');
        internal::os::writeAt(replacement.fd(), 0, garbage.data(), garbage.size());
        internal::os::syncFile(replacement.fd(), true);
    }
    CHECK_EQ(checkpointOf(dir.file()), before);
    CHECK_EQ(dumpFile(dir.file()).front().second, std::string("base"));
}

TEST(applyIsIdempotentAndUpToDateIsNotAnError)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db"), bundle = dir.file("d.bundle");

    Env e = Env::configure().sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 50);
    baseImage(e, replica);
    const Checkpoint base = checkpointOf(replica);
    put(e, 50, 150);

    ShipLog log(ship);
    CHECK(log.extract(base, bundle) == ShipStatus::Ok);
    const Checkpoint after = applyBundle(replica, bundle);
    CHECK_EQ(applyBundle(replica, bundle), after);  // replaying it changes nothing
    CHECK(log.extract(after, bundle) == ShipStatus::UpToDate);
}

TEST(aBundleDeliveredOutOfOrderIsRefused)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db");
    const fs::path first = dir.file("1.bundle"), second = dir.file("2.bundle");

    Env e = Env::configure().sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 50);
    baseImage(e, replica);
    const Checkpoint base = checkpointOf(replica);

    put(e, 50, 100);
    const Checkpoint mid = checkpointOf(store);
    put(e, 100, 150);

    ShipLog log(ship);
    CHECK(log.extract(base, first, mid.txnid) == ShipStatus::Ok);
    CHECK(log.extract(mid, second) == ShipStatus::Ok);

    // The replica is still on `base`, so the second bundle must not apply.
    CHECK_THROWS(applyBundle(replica, second), ErrorCode::Incompatible);
    CHECK_EQ(checkpointOf(replica), base);

    applyBundle(replica, first);
    applyBundle(replica, second);
    CHECK(dumpFile(replica) == dumpEnv(e));
}

TEST(aDivergedReplicaIsDetectedWhenTheDeltaIsBuilt)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship"), bundle = dir.file("d.bundle");

    Env e = Env::configure().sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 50);
    const Checkpoint real = checkpointOf(store);
    put(e, 50, 100);

    ShipLog log(ship);
    CHECK_THROWS(log.extract(Checkpoint{real.txnid, real.metaChecksum ^ 1u}, bundle),
                 ErrorCode::Incompatible);
    CHECK(!fs::exists(bundle));
}

TEST(aCorruptOrTruncatedBundleIsRejectedBeforeTheReplicaIsTouched)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db"), bundle = dir.file("d.bundle");

    Env e = Env::configure().sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 50);
    baseImage(e, replica);
    const Checkpoint base = checkpointOf(replica);
    put(e, 50, 400);

    ShipLog log(ship);
    CHECK(log.extract(base, bundle) == ShipStatus::Ok);
    corrupt(bundle, std::uint64_t(fs::file_size(bundle)) / 2);
    CHECK_THROWS(applyBundle(replica, bundle), ErrorCode::Corrupted);
    CHECK_EQ(checkpointOf(replica), base);

    CHECK(log.extract(base, bundle) == ShipStatus::Ok);
    fs::resize_file(bundle, fs::file_size(bundle) - 200);
    CHECK_THROWS(applyBundle(replica, bundle), ErrorCode::Corrupted);
    CHECK_EQ(checkpointOf(replica), base);

    // Still healthy: a good copy of the same bundle applies.
    CHECK(log.extract(base, bundle) == ShipStatus::Ok);
    applyBundle(replica, bundle);
    CHECK(dumpFile(replica) == dumpEnv(e));
}

TEST(aBundleFromADifferentPageSizeIsRefused)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship"), bundle = dir.file("d.bundle");
    const fs::path replica = dir.file("replica.db");

    Env e = Env::configure().pageSize(4096).sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 50);
    const Checkpoint base = checkpointOf(store);
    put(e, 50, 100);
    CHECK(ShipLog(ship).extract(base, bundle) == ShipStatus::Ok);

    Env other = Env::configure().pageSize(8192).sync(Durability::None).open(replica);
    put(other, 0, 50);
    other.close();
    CHECK_THROWS(applyBundle(replica, bundle), ErrorCode::Incompatible);
}

// -------------------------------------------------------------- retention ---

TEST(aReplicaPastTheRetentionWindowIsToldToTakeABase)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db"), bundle = dir.file("d.bundle");

    // Tiny segments and a one-segment window, so the log rolls past a stale
    // replica within a handful of commits.
    Env e = Env::configure()
                .sync(Durability::None)
                .shipTo(ship)
                .shipSegmentSize(64 << 10)
                .shipRetain(1)
                .open(store);
    put(e, 0, 10);
    baseImage(e, replica);
    const Checkpoint stale = checkpointOf(replica);

    for (int i = 0; i < 40; ++i)
        put(e, 100 + std::uint64_t(i) * 100, 200 + std::uint64_t(i) * 100);

    ShipLog log(ship);
    CHECK(log.available().oldest > stale.txnid + 1);
    CHECK(log.extract(stale, bundle) == ShipStatus::NeedBase);
    CHECK(!fs::exists(bundle));

    // Recovery is a fresh base image, after which deltas resume.
    fs::remove(replica);
    baseImage(e, replica);
    put(e, 100000, 100050);
    CHECK(log.extract(checkpointOf(replica), bundle) == ShipStatus::Ok);
    applyBundle(replica, bundle);
    CHECK(dumpFile(replica) == dumpEnv(e));
}

TEST(corruptSegmentAddressesAreNotReframedAsValidBundles)
{
    tst::Scratch dir("frame");
    Env env = Env::configure().shipTo(dir.file("ship")).open(dir.file());
    put(env, 0, 20);
    const Checkpoint base = checkpointOf(dir.file());
    put(env, 20, 40);
    CHECK_EQ(env.stats().captureFailures, 0u);
    CHECK_EQ(env.stats().capturedTxnid, env.commitGeneration());
    for (const auto& entry : fs::directory_iterator(dir.file("ship")))
        if (entry.path().extension() == ".seg")
            corrupt(entry.path(), 32 + 24);
    CHECK_THROWS(ShipLog(dir.file("ship")).extract(base, dir.file("bundle")), ErrorCode::Corrupted);
    CHECK(!fs::exists(dir.file("bundle")));
}

TEST(pruneDropsOnlySegmentsNobodyNeeds)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");

    Env e = Env::configure()
                .sync(Durability::None)
                .shipTo(ship)
                .shipSegmentSize(32 << 10)
                .shipRetain(1000)
                .open(store);
    for (int i = 0; i < 30; ++i)
        put(e, std::uint64_t(i) * 100, std::uint64_t(i) * 100 + 100);

    ShipLog log(ship);
    const auto before = log.available();
    CHECK(log.prune(before.newest) > 0);
    const auto after = log.available();
    CHECK(after.oldest > before.oldest);
    CHECK_EQ(after.newest, before.newest);
    CHECK_EQ(log.prune(0), 0u);
}

// ------------------------------------------------------- capture fidelity ---

TEST(everyCommitShipsEverythingItChanged)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db"), bundle = dir.file("d.bundle");

    // Small pages, so splits, merges and overflow pages all happen within a
    // few hundred keys.
    Env e = Env::configure().pageSize(512).sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 200);
    baseImage(e, replica);
    Checkpoint at = checkpointOf(replica);

    // One bundle per commit, so coalescing cannot hide a missed page: if a
    // commit changed a page it did not report, the replica diverges here.
    ShipLog log(ship);
    for (int i = 0; i < 25; ++i) {
        e.write([&](Txn& t) {
            Db db = t.mainDb();
            for (std::uint64_t k = 0; k < 40; ++k)
                db.put(tst::keyOf(k * 7 + std::uint64_t(i)), tst::blob(300, std::uint64_t(i)));
            db.erase(tst::keyOf(std::uint64_t(i) * 3));
        });
        CHECK(log.extract(at, bundle, at.txnid + 1) == ShipStatus::Ok);
        at = applyBundle(replica, bundle);
        CHECK_EQ(at, checkpointOf(store));
        CHECK(dumpFile(replica) == dumpEnv(e));
    }
    checkFile(replica);
}

TEST(coalescingShipsAPageOnceWithItsFinalContents)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db");
    const fs::path one = dir.file("one.bundle"), all = dir.file("all.bundle");

    Env e = Env::configure().sync(Durability::None).shipTo(ship).open(store);
    put(e, 0, 100);
    baseImage(e, replica);
    const Checkpoint base = checkpointOf(replica);

    // Twenty commits that all rewrite the same handful of keys, so the root
    // and its leaves are touched over and over.
    for (int i = 0; i < 20; ++i)
        e.write([&](Txn& t) {
            Db db = t.mainDb();
            for (std::uint64_t k = 0; k < 5; ++k)
                db.put(tst::keyOf(k), tst::blob(64, std::uint64_t(i)));
        });

    ShipLog log(ship);
    CHECK(log.extract(base, one, base.txnid + 1) == ShipStatus::Ok);
    CHECK(log.extract(base, all, 0, 64u << 10) == ShipStatus::Ok);

    const BundleInfo single = inspectBundle(one);
    const BundleInfo whole = inspectBundle(all);
    CHECK_EQ(whole.targetTxnid, base.txnid + 20);
    CHECK(whole.pageCount < single.pageCount * 20);

    applyBundle(replica, all);
    CHECK(dumpFile(replica) == dumpEnv(e));
    checkFile(replica);
}

TEST(shippingSurvivesTheStoreGrowingMidRange)
{
    tst::Scratch dir("repl");
    const fs::path store = dir.file(), ship = dir.file("ship");
    const fs::path replica = dir.file("replica.db"), bundle = dir.file("d.bundle");

    // Start small enough that the writes below force the file to grow.
    Env e = Env::configure()
                .sync(Durability::None)
                .initialSize(64 << 10)
                .growthStep(64 << 10)
                .shipTo(ship)
                .open(store);
    put(e, 0, 10);
    baseImage(e, replica);
    const Checkpoint base = checkpointOf(replica);
    const auto before = fs::file_size(store);

    for (int i = 0; i < 20; ++i)
        put(e, 100 + std::uint64_t(i) * 300, 400 + std::uint64_t(i) * 300, "a longer value here");
    CHECK(fs::file_size(store) > before);

    CHECK(ShipLog(ship).extract(base, bundle) == ShipStatus::Ok);
    applyBundle(replica, bundle);
    CHECK(dumpFile(replica) == dumpEnv(e));
    checkFile(replica);
}

TEST(missingOrCorruptSegmentIndexFallsBackToFrames)
{
    tst::Scratch scratch("index-fallback");
    Env env = Env::configure().shipTo(scratch.file("ship")).open(scratch.file());
    put(env, 0, 100);
    const auto base = checkpointOf(scratch.file());
    put(env, 100, 200);
    for (const auto& file : fs::directory_iterator(scratch.file("ship")))
        if (file.path().extension() == ".idx")
            corrupt(file.path(), 0);
    ShipLog log(scratch.file("ship"));
    CHECK_EQ(log.available().newest, env.commitGeneration());
    CHECK(log.extract(base, scratch.file("bundle")) == ShipStatus::Ok);
}

TEST(segmentRangeQueriesUseIndexInsteadOfPayloadScan)
{
    tst::Scratch scratch("indexed-reads");
    Env env = Env::configure().shipTo(scratch.file("ship")).open(scratch.file());
    put(env, 0, 3000);
    put(env, 3000, 6000);
    std::uint64_t segmentBytes = 0, readBytes = 0;
    for (const auto& entry : fs::directory_iterator(scratch.file("ship")))
        if (entry.path().extension() == ".seg")
            segmentBytes += fs::file_size(entry.path());
    {
        internal::os::ScopedIoObserver observer([&](internal::os::IoEvent event, std::size_t bytes) {
            if (event == internal::os::IoEvent::Read)
                readBytes += bytes;
        });
        CHECK_EQ(ShipLog(scratch.file("ship")).available().newest, env.commitGeneration());
    }
    CHECK(readBytes < segmentBytes / 4);
}

int main()
{
    return tst::runAll("replication");
}
