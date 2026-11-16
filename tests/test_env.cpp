// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Store-level behaviour: geometry, Durability modes, locking, compaction.
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/format.hpp"
#include "nosql/internal/io_observer.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

TEST(pageSizeIsAdoptedFromAnExistingStore)
{
    tst::Scratch s("env");
    {
        Env e = Env::configure().pageSize(1024).open(s.file());
    }
    {
        Env e = Env::configure().open(s.file());  // no pageSize given
        CHECK_EQ(e.pageSize(), std::size_t(1024));
    }
    CHECK_THROWS(Env::configure().pageSize(8192).open(s.file()), ErrorCode::Incompatible);
}

TEST(pageSizeMustBeAPowerOfTwoInRange)
{
    tst::Scratch s("env");
    CHECK_THROWS(Env::configure().pageSize(3000).open(s.file()), ErrorCode::InvalidArgument);
    CHECK_THROWS(Env::configure().pageSize(256).open(s.file()), ErrorCode::InvalidArgument);
    CHECK_THROWS(Env::configure().pageSize(1 << 20).open(s.file()), ErrorCode::InvalidArgument);
    for (std::size_t ps : {1024u, 2048u, 4096u, 16384u, 65536u}) {
        tst::Scratch one("env");
        Env e = Env::configure().pageSize(ps).open(one.file());
        CHECK_EQ(e.pageSize(), ps);
        e.write([&](Txn& t) {
            t.mainDb().put("k", tst::blob(ps * 3, ps));
            checkIntegrity(t);
        });
        e.read([&](Txn& t) { CHECK_EQ(t.mainDb().at("k").size(), ps * 3); });
    }
}

TEST(createIfMissingAndErrorIfExists)
{
    tst::Scratch s("env");
    CHECK_THROWS(Env::configure().createIfMissing(false).open(s.file()), ErrorCode::NotFound);
    {
        Env e = Env::configure().errorIfExists(true).open(s.file());
    }
    CHECK_THROWS(Env::configure().errorIfExists(true).open(s.file()), ErrorCode::InvalidArgument);
    Env e = Env::configure().createIfMissing(false).open(s.file());
    CHECK(e.valid());
}

TEST(readOnlyStoresRejectWriters)
{
    tst::Scratch s("env");
    {
        Env e = Env::configure().open(s.file());
        e.write([](Txn& t) { t.mainDb().put("k", "v"); });
    }
    Env e = Env::configure().readOnly().open(s.file());
    CHECK_THROWS(e.writeTxn(), ErrorCode::ReadOnly);
    e.read([](Txn& t) {
        CHECK_EQ(t.mainDb().at("k").string(), std::string("v"));
        CHECK(t.isReadOnly());
    });
}

TEST(aTimedWriterGivesUpWhileTheSlotIsTaken)
{
    tst::Scratch s("env");
    Env e = Env::configure().open(s.file());
    Txn held = e.writeTxn();
    const auto t0 = std::chrono::steady_clock::now();
    std::thread other([&] { CHECK_THROWS(e.writeTxn(std::chrono::milliseconds(50)), ErrorCode::Busy); });
    other.join();
    CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(45));
    held.commit();
    // Free again: a timed writer gets the slot at once.
    Txn w = e.writeTxn(std::chrono::milliseconds(50));
    w.mainDb().put("k", "v");
    w.commit();
    // A waiter is woken when the holder finishes inside its timeout.
    Txn again = e.writeTxn();
    std::thread waiter([&] {
        Txn mine = e.writeTxn(std::chrono::seconds(10));
        CHECK_EQ(mine.mainDb().at("k").string(), std::string("v"));
        mine.abort();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    again.abort();
    waiter.join();
}

TEST(aSecondProcessStyleOpenIsRefused)
{
    tst::Scratch s("env");
    Env first = Env::configure().open(s.file());
    // The whole-file lock is per open-file-description, so a second handle in
    // this same process exercises exactly what a second process would hit.
    CHECK_THROWS(Env::configure().open(s.file()), ErrorCode::Busy);
}

TEST(mapFullIsReportedNotCrashed)
{
    tst::Scratch s("env");
    Env e = Env::configure().pageSize(1024).maxSize(128 * 1024).open(s.file());
    bool hit = false;
    try {
        for (int round = 0; round < 200 && !hit; ++round)
            e.write([&](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 100; ++i)
                    d.put(tst::keyOf(round * 100 + i), tst::blob(200, i));
            });
    } catch (const Error& ex) {
        CHECK_EQ(int(ex.code()), int(ErrorCode::MapFull));
        hit = true;
    }
    CHECK(hit);
    // The store must still be usable and consistent on its last good commit.
    e.read([](Txn& t) { checkIntegrity(t); });
}

TEST(durabilityModesAllRoundTrip)
{
    for (Durability d :
         {Durability::Safe, Durability::NoMetaSync, Durability::None, Durability::Lazy}) {
        tst::Scratch s("env");
        {
            Env e = Env::configure().sync(d).open(s.file());
            e.write([](Txn& t) {
                for (int i = 0; i < 500; ++i)
                    t.mainDb().put(tst::keyOf(i), tst::blob(80, i));
            });
            e.sync(true);
        }
        Env e = Env::configure().open(s.file());
        e.read([](Txn& t) {
            CHECK_EQ(t.mainDb().count(), std::uint64_t(500));
            checkIntegrity(t);
        });
    }
}

namespace {

/// Device flushes (data barriers, meta barriers) that `fn` causes.
template <class F>
std::pair<unsigned, unsigned> barriersOf(F&& fn)
{
    unsigned data = 0, meta = 0;
    internal::os::ScopedIoObserver observer([&](internal::os::IoEvent event, std::size_t) {
        data += event == internal::os::IoEvent::DataBarrier;
        meta += event == internal::os::IoEvent::MetaBarrier;
    });
    fn();
    return {data, meta};
}

}  // namespace

TEST(lazyDurabilitySyncsOncePerInterval)
{
    tst::Scratch s("env");
    using std::chrono::milliseconds;
    const auto put = [](Env& e, int i) {
        e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(i), "v"); });
    };
    {
        // A zero interval syncs every commit, like Safe.
        Env e =
            Env::configure().sync(Durability::Lazy).syncInterval(milliseconds(0)).open(s.file());
        CHECK(barriersOf([&] { put(e, 0); }) == std::make_pair(1u, 1u));
        CHECK_EQ(e.stats().durableTxn, e.stats().lastTxn);
    }
    {
        // Five commits must fit inside the interval, so it is generous: a
        // slow machine, a sanitizer, must not see one sync early.
        Env e =
            Env::configure().sync(Durability::Lazy).syncInterval(milliseconds(1000)).open(s.file());
        const std::uint64_t durable = e.stats().durableTxn;
        CHECK(barriersOf([&] {
                  for (int i = 1; i < 6; ++i)
                      put(e, i);
              }) == std::make_pair(0u, 0u));
        CHECK_EQ(e.stats().durableTxn, durable);
        CHECK_EQ(e.stats().lastTxn, durable + 5);
        // Readers see every commit, durable or not.
        e.read([](Txn& t) { CHECK_EQ(t.mainDb().count(), std::uint64_t(6)); });
        std::this_thread::sleep_for(milliseconds(1050));
        CHECK(barriersOf([&] { put(e, 6); }) == std::make_pair(1u, 1u));
        CHECK_EQ(e.stats().durableTxn, e.stats().lastTxn);
        put(e, 7);
        CHECK(barriersOf([&] { e.sync(false); }) == std::make_pair(1u, 1u));
        CHECK_EQ(e.stats().durableTxn, e.stats().lastTxn);
        // Nothing new: nothing to flush.
        CHECK(barriersOf([&] { e.sync(false); }) == std::make_pair(0u, 0u));
        put(e, 8);
    }
    // Closing synced the last commit.
    Env e = Env::configure().readOnly().open(s.file());
    e.read([](Txn& t) { CHECK_EQ(t.mainDb().count(), std::uint64_t(9)); });
}

TEST(lazyBackgroundSyncBoundsAnIdleStoresLoss)
{
    tst::Scratch s("env");
    using std::chrono::milliseconds;
    const auto put = [](Env& e, int i) {
        e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(i), "v"); });
    };
    {
        // Without it, a store that stops committing keeps its newest commit in
        // memory however long it waits.
        Env e = Env::configure()
                    .sync(Durability::Lazy)
                    .syncInterval(milliseconds(30))
                    .open(s.file("plain.db"));
        // A commit made more than an interval after the last sync syncs
        // itself, as a slow open can make the first one: commit until one is
        // left in memory, then wait.
        int i = 0;
        do
            put(e, i++);
        while (e.stats().durableTxn == e.stats().lastTxn && i < 100);
        CHECK(e.stats().durableTxn < e.stats().lastTxn);
        std::this_thread::sleep_for(milliseconds(150));
        CHECK(e.stats().durableTxn < e.stats().lastTxn);
    }
    {
        Env e = Env::configure()
                    .sync(Durability::Lazy)
                    .syncInterval(milliseconds(30))
                    .backgroundSync()
                    .open(s.file("background.db"));
        put(e, 1);
        const std::uint64_t last = e.stats().lastTxn;
        const auto t0 = std::chrono::steady_clock::now();
        while (e.stats().durableTxn < last &&
               std::chrono::steady_clock::now() - t0 < std::chrono::seconds(5))
            std::this_thread::sleep_for(milliseconds(5));
        CHECK_EQ(e.stats().durableTxn, last);
        // The file holds it now, as a power cut would find it.
        std::filesystem::copy_file(s.file("background.db"), s.file("cut.db"));
        Env cut = Env::configure().readOnly().open(s.file("cut.db"));
        cut.read([](Txn& t) { CHECK(t.mainDb().contains(tst::keyOf(1))); });
    }
    {
        // Closing does not wait out the interval the thread is sleeping on.
        const auto t0 = std::chrono::steady_clock::now();
        {
            Env e = Env::configure()
                        .sync(Durability::Lazy)
                        .syncInterval(std::chrono::seconds(10))
                        .backgroundSync()
                        .open(s.file("long.db"));
            put(e, 2);
        }
        CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(2));
        Env e = Env::configure().readOnly().open(s.file("long.db"));
        e.read([](Txn& t) { CHECK(t.mainDb().contains(tst::keyOf(2))); });  // the closing sync
    }
}

TEST(durableTxnFollowsTheMode)
{
    for (Durability d : {Durability::Safe, Durability::NoMetaSync, Durability::None}) {
        tst::Scratch s("env");
        Env e = Env::configure().sync(d).open(s.file());
        for (int i = 0; i < 3; ++i)
            e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(i), "v"); });
        const EnvStats st = e.stats();
        if (d == Durability::Safe)
            CHECK_EQ(st.durableTxn, st.lastTxn);
        else if (d == Durability::NoMetaSync)
            CHECK_EQ(st.durableTxn, st.lastTxn - 1);  // flushed with the next commit
        else
            CHECK_EQ(st.durableTxn, std::uint64_t(0));
        e.sync(false);
        CHECK_EQ(e.stats().durableTxn, st.lastTxn);
    }
}

TEST(syncDoesNotBlockReadersDuringTheFlush)
{
    tst::Scratch s("env");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    e.write([](Txn& t) { t.mainDb().put("k", "v"); });
    std::mutex m;
    std::condition_variable cv;
    bool flushing = false, released = false;
    std::thread syncer([&] {
        internal::os::ScopedIoObserver observer([&](internal::os::IoEvent event, std::size_t) {
            if (event != internal::os::IoEvent::Sync)
                return;
            std::unique_lock<std::mutex> lk(m);
            flushing = true;
            cv.notify_all();
            cv.wait_for(lk, std::chrono::seconds(5), [&] { return released; });
        });
        e.sync();
    });
    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return flushing; });
    }
    // The flush is in progress; a reader and a writer still get through.
    const auto t0 = std::chrono::steady_clock::now();
    e.read([](Txn& t) { CHECK_EQ(t.mainDb().at("k").string(), std::string("v")); });
    e.write([](Txn& t) { t.mainDb().put("k2", "v2"); });
    const auto waited = std::chrono::steady_clock::now() - t0;
    {
        std::lock_guard<std::mutex> lk(m);
        released = true;
    }
    cv.notify_all();
    syncer.join();
    CHECK(waited < std::chrono::seconds(2));
}

TEST(compactRebuildsADenseCopy)
{
    tst::Scratch s("env");
    const auto src = s.file("src.db");
    const auto dst = s.file("dst.db");
    {
        Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(src);
        e.write([](Txn& t) {
            auto m = t.mainDb();
            auto n = t.db("named", DbFlags::Create);
            auto i = t.db("ints", DbFlags::Create | DbFlags::IntegerKey);
            for (int k = 0; k < 4000; ++k) {
                m.put(tst::keyOf(k), tst::blob(120, k));
                n.put(tst::keyOf(k), tst::blob(90, k));
                i.put(Slice::ref(std::uint64_t(k)), tst::blob(40, k));
            }
        });
        // Churn to leave holes behind.
        for (int round = 0; round < 8; ++round)
            e.write([&](Txn& t) {
                auto m = t.mainDb();
                for (int k = 0; k < 4000; k += 2)
                    m.put(tst::keyOf(k), tst::blob(400, k + round));
            });
        e.write([](Txn& t) {
            auto m = t.mainDb();
            for (int k = 0; k < 4000; k += 3)
                m.erase(tst::keyOf(k));
        });
    }

    const std::uint64_t srcSize = std::filesystem::file_size(src);
    compact(src, dst);
    const std::uint64_t dstSize = std::filesystem::file_size(dst);
    CHECK(dstSize < srcSize);

    Env a = Env::configure().open(src);
    Env b = Env::configure().open(dst);
    a.read([&](Txn& ta) {
        b.read([&](Txn& tb) {
            checkIntegrity(tb);
            CHECK_EQ(tb.listDbs().size(), ta.listDbs().size());
            CHECK_EQ(tb.db("ints").flags(), DbFlags::IntegerKey);
            for (const char* name : {"", "named", "ints"}) {
                auto da = ta.db(name);
                auto db_ = tb.db(name);
                CHECK_EQ(db_.count(), da.count());
                auto ca = da.cursor();
                auto cb = db_.cursor();
                bool oka = ca.first(), okb = cb.first();
                while (oka && okb) {
                    CHECK_EQ(ca.key().string(), cb.key().string());
                    CHECK_EQ(ca.value().string(), cb.value().string());
                    oka = ca.next();
                    okb = cb.next();
                }
                CHECK_EQ(oka, okb);
            }
        });
    });
}

TEST(statsTrackTheStore)
{
    tst::Scratch s("env");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    const EnvStats empty = e.stats();
    CHECK_EQ(empty.pageSize, 1024u);
    CHECK_EQ(empty.usedPages, std::uint64_t(2));  // just the two meta pages
    CHECK_EQ(empty.readers, 0u);

    e.write([](Txn& t) {
        for (int i = 0; i < 1000; ++i)
            t.mainDb().put(tst::keyOf(i), tst::blob(100, i));
    });
    const EnvStats full = e.stats();
    CHECK(full.usedPages > 100);
    CHECK_EQ(full.lastTxn, std::uint64_t(1));

    Txn r1 = e.readTxn();
    Txn r2 = e.readTxn();
    CHECK_EQ(e.stats().readers, 2u);
    r1.abort();
    r2.abort();
    CHECK_EQ(e.stats().readers, 0u);

    e.read([](Txn& t) {
        const TreeStats ts = t.mainDb().stats();
        CHECK_EQ(ts.entries, std::uint64_t(1000));
        CHECK(ts.depth >= 2);
        CHECK(ts.leafPages > 0);
    });
}

TEST(usingAFinishedTransactionIsAnError)
{
    tst::Scratch s("env");
    Env e = Env::configure().open(s.file());
    Txn t = e.writeTxn();
    auto d = t.mainDb();
    t.commit();
    CHECK_THROWS(d.put("k", "v"), ErrorCode::BadTransaction);
    CHECK_THROWS(d.get("k"), ErrorCode::BadTransaction);
    CHECK_THROWS(d.count(), ErrorCode::BadTransaction);
    CHECK_THROWS(d.cursor(), ErrorCode::BadTransaction);
}

TEST(handlesMayOutliveTheirTransaction)
{
    tst::Scratch s("env");
    Env e = Env::configure().open(s.file());
    Db stale;
    Cursor staleCursor;
    {
        Txn t = e.writeTxn();
        stale = t.mainDb();
        stale.put("k", "v");
        staleCursor = stale.cursor();
        CHECK(staleCursor.first());
        t.commit();
    }
    // The handles keep the transaction's bookkeeping alive, so this is a clean
    // error rather than a read of released memory.
    CHECK(!staleCursor.valid());
    CHECK_THROWS(staleCursor.first(), ErrorCode::BadTransaction);
    CHECK_THROWS(staleCursor.key(), ErrorCode::BadTransaction);
    CHECK_THROWS(stale.put("k2", "v"), ErrorCode::BadTransaction);
}

TEST(aTransactionMayOutliveTheEnvHandle)
{
    tst::Scratch s("env");
    Txn reader;
    {
        Env e = Env::configure().open(s.file());
        e.write([](Txn& t) { t.mainDb().put("k", "v"); });
        reader = e.readTxn();
        e.close();  // the store stays open underneath the live snapshot
    }
    CHECK_EQ(reader.mainDb().at("k").string(), std::string("v"));
    checkIntegrity(reader);
    reader.abort();
}

namespace {

/// Scribble over one meta page, the way a torn write at the wrong moment
/// would. Slot n holds transaction n, so slot = txnid & 1.
void wreckMetaSlot(const std::filesystem::path& p, unsigned slot, std::size_t pageSize)
{
    std::FILE* f = std::fopen(p.string().c_str(), "r+b");
    CHECK(f != nullptr);
    // Flip the transaction id without touching the checksum -- indistinguishable
    // from a write that landed only partly.
    // Page header (16) + magic (16) + version/pageSize (8) lands on txnid
    CHECK_EQ(std::fseek(f, long(slot * pageSize) + 16 + 24, SEEK_SET), 0);
    const std::uint64_t garbage = 0xdeadbeefcafef00dull;
    CHECK_EQ(std::fwrite(&garbage, sizeof garbage, 1, f), std::size_t(1));
    std::fclose(f);
}

}  // namespace

TEST(aTornMetaWriteFallsBackToThePreviousSnapshot)
{
    tst::Scratch s("env");
    std::uint64_t last = 0;
    {
        Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 3000; ++i)
                d.put(tst::keyOf(i), tst::blob(90, i));
        });
        // Churn hard enough that the newest transaction is reusing pages the free
        // list handed back -- exactly the case where an unsafe reclamation rule
        // would have poisoned the older snapshot.
        for (int round = 0; round < 12; ++round)
            e.write([&](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 3000; ++i)
                    d.put(tst::keyOf(i), tst::blob(90, i + round * 1000));
            });
        e.write([](Txn& t) { t.mainDb().put("marker", "newest"); });
        last = e.stats().lastTxn;
    }

    wreckMetaSlot(s.file(), unsigned(last & 1), 1024);

    Env e = Env::configure().open(s.file());
    CHECK_EQ(e.stats().lastTxn, last - 1);
    e.read([&](Txn& t) {
        // The older snapshot must be whole: every page it names still holds what
        // it held, and nothing the newer transaction recycled has damaged it.
        checkIntegrity(t);
        auto d = t.mainDb();
        CHECK(!d.contains("marker"));
        CHECK_EQ(d.count(), std::uint64_t(3000));
        for (int i = 0; i < 3000; ++i)
            CHECK_EQ(d.at(tst::keyOf(i)).string(), tst::blob(90, i + 11 * 1000));
    });
    // ...and the store keeps working from there.
    e.write([](Txn& t) {
        t.mainDb().put("recovered", "yes");
        checkIntegrity(t);
    });
    e.read([&](Txn& t) { CHECK_EQ(t.mainDb().at("recovered").string(), std::string("yes")); });
}

TEST(maximumPageSizeRecoversUsingSecondMeta)
{
    tst::Scratch scratch("max-page");
    {
        Env env = Env::configure().pageSize(65536).open(scratch.file());
        env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    }
    wreckMetaSlot(scratch.file(), 0, 65536);
    Env env = Env::configure().open(scratch.file());
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("value")); });
}

TEST(bothMetasWreckedIsACleanError)
{
    tst::Scratch s("env");
    {
        Env e = Env::configure().pageSize(1024).open(s.file());
        e.write([](Txn& t) { t.mainDb().put("k", "v"); });
    }
    wreckMetaSlot(s.file(), 0, 1024);
    wreckMetaSlot(s.file(), 1, 1024);
    CHECK_THROWS(Env::configure().open(s.file()), ErrorCode::Corrupted);
}

TEST(aForeignFileIsRejectedNotMisread)
{
    tst::Scratch s("env");
    {
        std::FILE* f = std::fopen(s.file().string().c_str(), "wb");
        const std::vector<char> junk(8192, 'Z');
        std::fwrite(junk.data(), 1, junk.size(), f);
        std::fclose(f);
    }
    CHECK_THROWS(Env::configure().open(s.file()), ErrorCode::Corrupted);
}

TEST(commitGenerationAdvancesAndSurvivesReopen)
{
    tst::Scratch s("env");
    {
        Env e = Env::configure().open(s.file());
        CHECK_EQ(e.commitGeneration(), std::uint64_t(0));
        e.write([](Txn& t) { t.mainDb().put("a", "1"); });
        const std::uint64_t g1 = e.commitGeneration();
        CHECK(g1 > 0);
        e.write([](Txn& t) { t.mainDb().put("b", "2"); });
        CHECK_EQ(e.commitGeneration(), g1 + 1);
        e.read([&](Txn& t) { CHECK_EQ(t.id(), e.commitGeneration()); });
    }
    Env e = Env::configure().open(s.file());
    CHECK(e.commitGeneration() >= 2);
}

TEST(waitForCommitReturnsOnCommitAndOnTimeout)
{
    tst::Scratch s("env");
    Env e = Env::configure().open(s.file());

    const std::uint64_t start = e.commitGeneration();
    const auto t0 = std::chrono::steady_clock::now();
    CHECK_EQ(e.waitForCommit(start, std::chrono::milliseconds(60)), start);
    CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(50));

    std::thread writer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        e.write([](Txn& t) { t.mainDb().put("k", "v"); });
    });
    const std::uint64_t got = e.waitForCommit(start, std::chrono::seconds(5));
    writer.join();
    CHECK(got > start);
}

TEST(waitForCommitDoesNotSleepThroughARaceWithTheCommit)
{
    tst::Scratch s("env");
    Env e = Env::configure().open(s.file());

    // `seen` is sampled before the commit, so the wait must return at once even
    // though it starts after the commit has already landed. This is the lost
    // wakeup the sample-then-check-then-wait ordering exists to prevent.
    for (int round = 0; round < 200; ++round) {
        const std::uint64_t seen = e.commitGeneration();
        e.write([&](Txn& t) { t.mainDb().put("k", tst::keyOf(std::uint64_t(round))); });
        CHECK(e.waitForCommit(seen, std::chrono::milliseconds(0)) > seen);
    }
}

namespace {

/// The newest valid meta record in `file`, read from the file.
internal::Meta newestMetaOf(const std::filesystem::path& file, std::size_t ps)
{
    std::ifstream in(file, std::ios::binary);
    internal::Meta best{};
    for (unsigned slot = 0; slot < 2; ++slot) {
        internal::Meta m{};
        in.seekg(std::streamoff(slot * ps + internal::kPageHdr));
        in.read(reinterpret_cast<char*>(&m), sizeof m);
        if (internal::metaValid(m, ps) && m.txnid >= best.txnid)
            best = m;
    }
    return best;
}

struct Barriers
{
    unsigned data = 0, meta = 0;
};

/// Device flushes a commit of `puts` small values makes.
Barriers barriersOf(Env& env, int puts, int seed)
{
    Barriers b;
    internal::os::ScopedIoObserver watch([&](internal::os::IoEvent event, std::size_t) {
        b.data += event == internal::os::IoEvent::DataBarrier;
        b.meta += event == internal::os::IoEvent::MetaBarrier;
    });
    env.write([&](Txn& t) {
        for (int i = 0; i < puts; ++i)
            t.mainDb().put(tst::keyOf(std::uint64_t(seed) * 100000 + i), tst::blob(300, i));
    });
    return b;
}

}  // namespace

/// Options::sealedCommits(): one flush per commit where the commit's pages fit the meta
/// page's manifest, two where they do not or where a meta page has no room for one; the
/// store is at format 9 from its first sealed commit, and opens with or without the option.
TEST(sealedCommitsFlushOnceWhereTheManifestFits)
{
    tst::Scratch s("sealed");
    {
        Env e = Env::configure().sealedCommits(false).open(s.file());
        const Barriers plain = barriersOf(e, 5, 0);
        CHECK_EQ(plain.data, 1u);
        CHECK_EQ(plain.meta, 1u);
    }
    CHECK_EQ(internal::metaVersion(newestMetaOf(s.file(), 4096)), internal::kOldestFormatVersion);
    {
        Env e = Env::configure().sealedCommits().open(s.file());
        for (int i = 1; i < 6; ++i) {
            const Barriers sealed = barriersOf(e, 5, i);
            CHECK_EQ(sealed.data, 1u);
            CHECK_EQ(sealed.meta, 0u);
            CHECK(internal::metaSealed(newestMetaOf(s.file(), 4096)));
            CHECK_EQ(e.stats().durableTxn, e.stats().lastTxn);
        }
        // More pages than a meta page lists: flushed as before, and not marked sealed.
        const Barriers wide = barriersOf(e, 6000, 7);
        CHECK_EQ(wide.data, 1u);
        CHECK_EQ(wide.meta, 1u);
        const internal::Meta m = newestMetaOf(s.file(), 4096);
        CHECK(!internal::metaSealed(m));
        CHECK_EQ(internal::metaVersion(m), internal::kFormatSealed);
        CHECK_EQ(barriersOf(e, 5, 8).meta, 0u);
    }
    {
        // Without the option: the sealed meta is read, the next commit flushes twice.
        Env e = Env::configure().sealedCommits(false).open(s.file());
        CHECK_EQ(e.stats().rolledBackTxn, 0u);
        e.read([](Txn& t) {
            checkIntegrity(t);
            CHECK_EQ(t.mainDb().count(), std::uint64_t(5 * 6 + 6000 + 5));
        });
        CHECK_EQ(barriersOf(e, 5, 9).meta, 1u);
        CHECK_EQ(internal::metaVersion(newestMetaOf(s.file(), 4096)), internal::kFormatSealed);
    }
    {
        // Durability modes that keep the meta back, and pipelined commits, do not seal.
        tst::Scratch other("sealed-modes");
        Env lazy = Env::configure().sealedCommits().sync(Durability::NoMetaSync).open(other.file());
        barriersOf(lazy, 5, 0);
        lazy.close();
        CHECK_EQ(internal::metaVersion(newestMetaOf(other.file(), 4096)),
                 internal::kOldestFormatVersion);
        Env piped = Env::configure().sealedCommits().pipelineCommits().open(other.file());
        barriersOf(piped, 5, 1);
        piped.close();
        CHECK(!internal::metaSealed(newestMetaOf(other.file(), 4096)));
    }
    {
        // A 1 KiB meta page has no room for a manifest: the option changes nothing.
        tst::Scratch small("sealed-small");
        Env e = Env::configure().pageSize(1024).sealedCommits().open(small.file());
        const Barriers b = barriersOf(e, 5, 0);
        CHECK_EQ(b.meta, 1u);
        e.close();
        CHECK_EQ(internal::metaVersion(newestMetaOf(small.file(), 1024)),
                 internal::kOldestFormatVersion);
    }
}

/// A sealed commit counts only with every page it wrote. One with a damaged page is passed
/// over when the store is opened, which is how a commit cut short by a power failure is
/// told; the store opens on the commit before it and reports the one it passed over.
TEST(sealedCommitWithADamagedPageIsPassedOver)
{
    tst::Scratch s("sealed-damage");
    std::uint64_t lost = 0;
    {
        Env e = Env::configure().sealedCommits().open(s.file());
        e.write([](Txn& t) { t.mainDb().put("key", "old"); });
        e.write([](Txn& t) { t.mainDb().put("key", "new"); });
        lost = e.stats().lastTxn;
    }
    const internal::Meta m = newestMetaOf(s.file(), 4096);
    CHECK(internal::metaSealed(m));
    CHECK_EQ(std::uint64_t(m.txnid), lost);
    {
        std::fstream file(s.file(), std::ios::in | std::ios::out | std::ios::binary);
        file.seekg(std::streamoff(std::uint64_t(m.mainTree.root) * 4096 + 200));
        const int c = file.get();
        file.seekp(std::streamoff(std::uint64_t(m.mainTree.root) * 4096 + 200));
        file.put(char(c ^ 0x5a));
    }
    {
        Env e = Env::configure().sealedCommits().open(s.file());
        CHECK_EQ(e.stats().rolledBackTxn, lost);
        CHECK_EQ(e.stats().lastTxn, lost - 1);
        e.read([](Txn& t) {
            checkIntegrity(t);
            CHECK_EQ(t.mainDb().at("key").string(), std::string("old"));
        });
        // The store goes on from there: the passed-over meta's slot is the next one written.
        e.write([](Txn& t) { t.mainDb().put("key", "newer"); });
        CHECK_EQ(e.stats().lastTxn, lost);
    }
    Env e = Env::configure().open(s.file());
    CHECK_EQ(e.stats().rolledBackTxn, 0u);
    e.read([](Txn& t) {
        checkIntegrity(t);
        CHECK_EQ(t.mainDb().at("key").string(), std::string("newer"));
    });
}

/// A flush that fails leaves no trace of a sealed commit: its meta page, written before
/// the flush, is taken back, and the store reopens on the commit before.
TEST(sealedCommitWhoseFlushFailsIsTakenBack)
{
    tst::Scratch s("sealed-fault");
    Env e = Env::configure().sealedCommits().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("key", "old"); });
    const std::uint64_t before = e.stats().lastTxn;
    {
        internal::os::ScopedIoObserver fault([](internal::os::IoEvent event, std::size_t) {
            if (event == internal::os::IoEvent::DataBarrier)
                throw Error(ErrorCode::IoError, "injected flush failure");
        });
        CHECK_THROWS(e.write([](Txn& t) { t.mainDb().put("key", "new"); }), ErrorCode::IoError);
    }
    CHECK_THROWS(e.writeTxn(), ErrorCode::BadTransaction);  // poisoned, as without the option
    e.close();
    e = Env::configure().sealedCommits().open(s.file());
    CHECK_EQ(e.stats().lastTxn, before);
    CHECK_EQ(e.stats().rolledBackTxn, 0u);
    e.read([](Txn& t) { CHECK_EQ(t.mainDb().at("key").string(), std::string("old")); });
}

int main()
{
    return tst::runAll("env");
}
