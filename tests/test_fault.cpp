// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <map>

#include "nosql/replication.hpp"
#include "nosql/internal/io_observer.hpp"
#include "nosql/internal/atomic_file.hpp"
#include "nosql/internal/format.hpp"
#include "nosql/internal/os.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
using internal::os::IoEvent;
using internal::os::ScopedIoObserver;

TEST(failedDataBarrierPreservesPreviousCheckpoint)
{
    tst::Scratch scratch("fault-commit");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "old"); });
    const auto before = checkpointOf(scratch.file());
    {
        ScopedIoObserver fault([](IoEvent event, std::size_t) {
            if (event == IoEvent::DataBarrier)
                throw Error(ErrorCode::IoError, "injected data barrier failure");
        });
        CHECK_THROWS(env.write([](Txn& txn) { txn.mainDb().put("key", "new"); }), ErrorCode::IoError);
    }
    env.close();
    CHECK_EQ(checkpointOf(scratch.file()), before);
    env = Env::configure().open(scratch.file());
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("old")); });
}

TEST(replacementFailuresLeaveEitherCompleteCheckpoint)
{
    for (const auto failure : {IoEvent::Write, IoEvent::WriteProgress, IoEvent::Resize,
                               IoEvent::Sync, IoEvent::Replace, IoEvent::Replaced}) {
        tst::Scratch scratch("fault-apply");
        Env primary = Env::configure().shipTo(scratch.file("ship")).open(scratch.file());
        primary.write([](Txn& txn) { txn.mainDb().put("key", "old"); });
        {
            auto reader = primary.readTxn();
            copySnapshot(reader, scratch.file("replica"));
        }
        const auto base = checkpointOf(scratch.file("replica"));
        for (unsigned index = 0; index < 12; ++index)
            primary.write([](Txn& txn) { txn.mainDb().put("key", "new"); });
        CHECK(ShipLog(scratch.file("ship")).extract(base, scratch.file("bundle")) == ShipStatus::Ok);
        {
            ScopedIoObserver fault([failure](IoEvent event, std::size_t) {
                if (event == failure)
                    throw Error(ErrorCode::IoError, "injected apply failure");
            });
            CHECK_THROWS(applyBundle(scratch.file("replica"), scratch.file("bundle")), ErrorCode::IoError);
        }
        Env replica = Env::configure().readOnly().open(scratch.file("replica"));
        replica.read([&](Txn& txn) {
            checkIntegrity(txn);
            CHECK_EQ(txn.mainDb().at("key").string(), std::string(failure == IoEvent::Replaced ? "new" : "old"));
        });
        replica.close();
        CHECK_EQ(applyBundle(scratch.file("replica"), scratch.file("bundle")), checkpointOf(scratch.file()));
    }
}

TEST(copyFallbackAndCloneKeepSourceIndependent)
{
    for (bool clone : {false, true}) {
        tst::Scratch scratch("clone");
        {
            auto env = Env::configure().open(scratch.file());
            env.write([](Txn& txn) { txn.mainDb().put("key", "old"); });
        }
        const auto source = internal::os::openFile(scratch.file(), true, false, false);
        try {
            internal::AtomicFile replacement(scratch.file());
            replacement.copyFrom(source, clone);
            const std::uint64_t junk = 0;
            internal::os::writeAt(replacement.fd(), 0, &junk, sizeof junk);
        } catch (...) {
            internal::os::closeFile(source);
            throw;
        }
        internal::os::closeFile(source);
        auto env = Env::configure().open(scratch.file());
        env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("old")); });
    }
}

TEST(interruptedBulkBuildNeverPublishesDestination)
{
    tst::Scratch scratch("bulk-fault");
    {
        Env env = Env::configure().open(scratch.file());
        env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    }
    {
        ScopedIoObserver observer([](IoEvent event, std::size_t) {
            if (event == IoEvent::WriteProgress)
                throw Error(ErrorCode::IoError, "injected partial bulk write");
        });
        CHECK_THROWS(compact(scratch.file(), scratch.file("copy")), ErrorCode::IoError);
    }
    CHECK(!std::filesystem::exists(scratch.file("copy")));
    compact(scratch.file(), scratch.file("copy"));
}

TEST(writeFailureCanLeaveAnActualPrefix)
{
    tst::Scratch scratch("short-write");
    const auto file = internal::os::openFile(scratch.file(), false, true, true);
    try {
        std::string bytes(128u << 10, 'x');
        {
            ScopedIoObserver observer([](IoEvent event, std::size_t) {
                if (event == IoEvent::WriteProgress)
                    throw Error(ErrorCode::IoError, "injected short write");
            });
            CHECK_THROWS(internal::os::writeAt(file, 0, bytes.data(), bytes.size()), ErrorCode::IoError);
        }
        CHECK(internal::os::fileSize(file) > 0);
        CHECK(internal::os::fileSize(file) < bytes.size());
    } catch (...) {
        internal::os::closeFile(file);
        throw;
    }
    internal::os::closeFile(file);
}

// ------------------------------------------------------------ power cuts ---
//
// A power cut is modelled by copying the file at a chosen moment: the copy
// holds every write issued so far (the page cache is what a disk would hold
// if all of them had landed) and nothing after. Variants then undo the one
// write the protocol does not order: the newest meta page.

namespace {

using State = std::map<std::string, std::string>;

/// The store at `file` as it would open: its newest snapshot, checked.
State contentsOf(const std::filesystem::path& file, std::uint64_t* txnid = nullptr)
{
    Env env = Env::configure().readOnly().open(file);
    State state;
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        if (txnid)
            *txnid = txn.id();
        for (auto [key, value] : txn.mainDb().all())
            state[key.string()] = value.string();
    });
    return state;
}

/// Tears the meta page that holds `txnid`, as a partly landed write would.
void tearMetaOf(const std::filesystem::path& file, std::uint64_t txnid, std::size_t pageSize)
{
    const auto fd = internal::os::openFile(file, false, false, false, false);
    bool torn = false;
    for (unsigned slot = 0; slot < 2 && !torn; ++slot) {
        internal::Meta meta{};
        internal::os::readAt(fd, slot * pageSize + internal::kPageHdr, &meta, sizeof meta);
        if (meta.txnid != txnid)
            continue;
        const std::uint64_t garbage = 0xdeadbeefcafef00dull;
        internal::os::writeAt(fd, slot * pageSize + internal::kPageHdr + 64, &garbage,
                              sizeof garbage);
        torn = true;
    }
    internal::os::closeFile(fd);
    CHECK(torn);
}

/// Rewrites every key with values derived from `round`, so each commit
/// retires the pages of the one before and the next reuses them.
void rewriteAll(Env& env, int round, State& expect)
{
    env.write([&](Txn& txn) {
        Db db = txn.mainDb();
        for (int i = 0; i < 400; ++i) {
            const std::string key = tst::keyOf(i);
            const std::string value = tst::blob(100, std::uint64_t(i + round * 1000));
            db.put(key, value);
            expect[key] = value;
        }
    });
}

}  // namespace

/// NoMetaSync flushes a commit's meta page with the next commit's data, in no
/// particular order. A power cut during that flush may keep the data and lose
/// the meta, so the next commit must not have reused the pages the commit
/// before retired: the snapshot two back is what the store then opens on.
TEST(noMetaSyncPowerCutKeepsTheSnapshotBeforeAnUnflushedMeta)
{
    tst::Scratch scratch("power-nometa");
    const auto file = scratch.file();
    const auto cut = scratch.file("cut.db");
    Env env = Env::configure().pageSize(4096).sync(Durability::NoMetaSync).open(file);
    State expect;
    for (int round = 0; round < 5; ++round)
        rewriteAll(env, round, expect);
    const State older = expect;
    const std::uint64_t olderTxn = env.commitGeneration();
    rewriteAll(env, 5, expect);  // its meta stays in the cache
    {
        ScopedIoObserver observer([&](IoEvent event, std::size_t) {
            if (event == IoEvent::DataBarrier)
                std::filesystem::copy_file(file, cut,
                                           std::filesystem::copy_options::overwrite_existing);
        });
        rewriteAll(env, 6, expect);
    }
    tearMetaOf(cut, olderTxn + 1, 4096);
    std::uint64_t opened = 0;
    CHECK(contentsOf(cut, &opened) == older);
    CHECK_EQ(opened, olderTxn);
}

/// Under Lazy and None the meta page reaches the file only in a sync, after
/// the data it names has been flushed. At any moment in between, the file
/// opens on the last sync with that snapshot whole, however much the commits
/// since have rewritten; each step of the sync leaves either that snapshot or
/// the new one.
TEST(deferredMetaPowerCutsOpenOnTheLastSync)
{
    for (const Durability mode : {Durability::Lazy, Durability::None}) {
        tst::Scratch scratch("power-deferred");
        const auto file = scratch.file();
        const auto cut = scratch.file("cut.db");
        State expect;
        {
            Env env = Env::configure()
                          .pageSize(4096)
                          .sync(mode)
                          .syncInterval(std::chrono::hours(1))
                          .open(file);
            rewriteAll(env, 0, expect);
            env.sync();
            // Mid-commit and between commits: the copy is the last sync. A
            // commit may sync on its own once enough retired pages wait for
            // reuse; it does so after writing its pages, so a copy taken
            // while they are written still shows the sync before.
            State synced = expect;
            std::uint64_t syncedTxn = env.commitGeneration();
            CHECK_EQ(env.stats().durableTxn, syncedTxn);
            unsigned cuts = 0;
            for (int round = 1; round < 8; ++round) {
                bool taken = false;
                {
                    ScopedIoObserver observer([&](IoEvent event, std::size_t) {
                        if (event == IoEvent::WriteProgress && !taken) {
                            std::filesystem::copy_file(
                                file, cut, std::filesystem::copy_options::overwrite_existing);
                            taken = true;
                        }
                    });
                    rewriteAll(env, round, expect);
                }
                if (taken) {
                    std::uint64_t opened = 0;
                    CHECK(contentsOf(cut, &opened) == synced);
                    CHECK_EQ(opened, syncedTxn);
                    ++cuts;
                }
                if (env.stats().durableTxn != syncedTxn) {
                    CHECK_EQ(env.stats().durableTxn, env.commitGeneration());
                    synced = expect;
                    syncedTxn = env.commitGeneration();
                }
                std::filesystem::copy_file(file, cut,
                                           std::filesystem::copy_options::overwrite_existing);
                CHECK(contentsOf(cut) == synced);
            }
            CHECK(cuts > 0);
            CHECK(syncedTxn < env.commitGeneration());

            // Each step of the sync.
            const std::uint64_t newest = env.commitGeneration();
            const auto beforeData = scratch.file("before-data.db");
            const auto beforeMetaFlush = scratch.file("before-meta-flush.db");
            {
                ScopedIoObserver observer([&](IoEvent event, std::size_t) {
                    if (event == IoEvent::DataBarrier)
                        std::filesystem::copy_file(file, beforeData);
                    if (event == IoEvent::MetaBarrier)
                        std::filesystem::copy_file(file, beforeMetaFlush);
                });
                env.sync();
            }
            CHECK_EQ(env.stats().durableTxn, newest);
            std::uint64_t opened = 0;
            CHECK(contentsOf(beforeData, &opened) == synced);
            CHECK_EQ(opened, syncedTxn);
            CHECK(contentsOf(beforeMetaFlush, &opened) == expect);
            CHECK_EQ(opened, newest);
            tearMetaOf(beforeMetaFlush, newest, 4096);
            CHECK(contentsOf(beforeMetaFlush, &opened) == synced);
            CHECK_EQ(opened, syncedTxn);
            std::filesystem::copy_file(file, cut,
                                       std::filesystem::copy_options::overwrite_existing);
            CHECK(contentsOf(cut) == expect);

            // One more commit, then closing syncs it.
            rewriteAll(env, 8, expect);
        }
        CHECK(contentsOf(file) == expect);
    }
}

/// A sync that fails part-way poisons the store; it reopens on one of the two
/// snapshots the sync moved between, never on anything else.
TEST(failedDeferredSyncReopensOnAWholeSnapshot)
{
    for (const IoEvent failure : {IoEvent::DataBarrier, IoEvent::Sync, IoEvent::MetaBarrier}) {
        tst::Scratch scratch("deferred-sync-fault");
        State synced, expect;
        {
            Env env = Env::configure()
                          .sync(Durability::Lazy)
                          .syncInterval(std::chrono::hours(1))
                          .open(scratch.file());
            rewriteAll(env, 0, expect);
            env.sync();
            synced = expect;
            for (int round = 1; round < 4; ++round)
                rewriteAll(env, round, expect);
            {
                ScopedIoObserver fault([failure](IoEvent event, std::size_t) {
                    if (event == failure)
                        throw Error(ErrorCode::IoError, "injected sync failure");
                });
                CHECK_THROWS(env.sync(), ErrorCode::IoError);
            }
            CHECK_THROWS(env.writeTxn(), ErrorCode::BadTransaction);
            CHECK_THROWS(env.sync(), ErrorCode::BadTransaction);
        }
        const State reopened = contentsOf(scratch.file());
        if (failure == IoEvent::MetaBarrier)
            CHECK(reopened == expect || reopened == synced);
        else
            CHECK(reopened == synced);
    }
}

int main() { return tst::runAll("fault"); }