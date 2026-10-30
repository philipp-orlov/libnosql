// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Pipelined commits (Env::Options::pipelineCommits under Durability::Safe):
// a commit's meta reaches the device with the next commit's data, yet a commit
// still returns only once durable and readers still see only durable commits.
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "nosql/internal/core.hpp"
#include "nosql/internal/io_observer.hpp"
#include "nosql/nosql.hpp"
#include "nosql/optimistic.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
using internal::os::IoEvent;
using internal::os::ScopedIoObserver;

namespace {

constexpr int kKeys = 200;

std::string valueFor(int key, int round)
{
    return tst::blob(64, std::uint64_t(key) * 1000003u + std::uint64_t(round));
}

/// One commit that rewrites every key for the next round, chosen under the
/// writer slot so that rounds and commits come in the same order.
int writeRound(Env& e, std::atomic<int>& counter)
{
    int round = -1;
    e.write([&](Txn& t) {
        round = counter.fetch_add(1);
        Db d = t.mainDb();
        for (int k = 0; k < kKeys; ++k)
            d.put(tst::keyOf(std::uint64_t(k)), valueFor(k, round));
        d.put("round", std::to_string(round));
    });
    return round;
}

void raiseTo(std::atomic<int>& at, int value)
{
    int prev = at.load();
    while (prev < value && !at.compare_exchange_weak(prev, value)) {}
}

/// The meta in the newest slot of the file, and which slot holds it.
internal::Meta newestMeta(const std::filesystem::path& file, std::size_t pageSize, unsigned* slot)
{
    const auto fd = internal::os::openFile(file, true, false, false, false);
    internal::Meta best{};
    for (unsigned s = 0; s < 2; ++s) {
        internal::Meta m{};
        internal::os::readAt(fd, s * pageSize + internal::kPageHdr, &m, sizeof m);
        if (m.txnid >= best.txnid) {
            best = m;
            *slot = s;
        }
    }
    internal::os::closeFile(fd);
    return best;
}

/// Tears the meta in `slot`, as a write that only partly landed would.
void tearSlot(const std::filesystem::path& file, std::size_t pageSize, unsigned slot)
{
    const auto fd = internal::os::openFile(file, false, false, false, false);
    const std::uint64_t garbage = 0xdeadbeefcafef00dull;
    internal::os::writeAt(fd, slot * pageSize + internal::kPageHdr + 64, &garbage, sizeof garbage);
    internal::os::closeFile(fd);
}

/// The round a store opens on, after checking it is one whole round.
int openedRound(const std::filesystem::path& file)
{
    Env env = Env::configure().readOnly().open(file);
    int round = -1;
    env.read([&](Txn& t) {
        checkIntegrity(t);
        Db d = t.mainDb();
        const auto r = d.get("round");
        if (!r) {
            CHECK_EQ(d.count(), std::uint64_t(0));  // the store before any round
            return;
        }
        round = std::stoi(r->string());
        for (int k = 0; k < kKeys; ++k) {
            const auto v = d.get(tst::keyOf(std::uint64_t(k)));
            CHECK(v && v->string() == valueFor(k, round));
        }
    });
    return round;
}

}  // namespace

TEST(pipelinedCommitIsDurableAndVisibleWhenItReturns)
{
    // A lone writer has no successor to carry its meta, so each commit flushes
    // its own: one data barrier and one meta barrier, as without pipelining.
    tst::Scratch s("pipeline");
    Env e = Env::configure().pipelineCommits().open(s.file());
    std::atomic<int> counter{0};
    for (int i = 0; i < 20; ++i) {
        int data = 0, meta = 0;
        int round;
        {
            ScopedIoObserver count([&](IoEvent ev, std::size_t) {
                data += ev == IoEvent::DataBarrier;
                meta += ev == IoEvent::MetaBarrier;
            });
            round = writeRound(e, counter);
        }
        CHECK_EQ(data, 1);
        CHECK_EQ(meta, 1);
        const EnvStats st = e.stats();
        CHECK_EQ(st.durableTxn, st.lastTxn);
        e.read([&](Txn& t) { CHECK(t.mainDb().get("round")->string() == std::to_string(round)); });
    }
}

TEST(pipelinedPowerCutsKeepEveryReturnedAndEverySeenCommit)
{
    // Two writers commit in a stream, so metas ride along with the next
    // commit's data. At a data barrier, a copy of the file is what a power cut
    // would leave; the newest meta in it, if not yet durable, may also be torn.
    // The copy must open on one whole round, and on none older than the last
    // whose commit() had returned, or than what a reader could see.
    tst::Scratch s("pipeline");
    const auto file = s.file();
    constexpr std::size_t kPage = 4096;
    Env e = Env::configure().pageSize(kPage).pipelineCommits().open(file);
    std::atomic<int> counter{0}, returned{-1};
    struct Cut
    {
        std::filesystem::path copy;
        int returnedAt;
        int seenAt;
        std::uint64_t durableAt;
    };
    std::vector<Cut> cuts;
    auto writer = [&](bool cutter) {
        std::optional<ScopedIoObserver> observer;
        if (cutter) {
            observer.emplace([&](IoEvent ev, std::size_t) {
                if (ev != IoEvent::DataBarrier || cuts.size() >= 16)
                    return;
                Cut c;
                c.returnedAt = returned.load();
                c.seenAt = e.read([&](Txn& t) {
                    const auto r = t.mainDb().get("round");
                    return r ? std::stoi(r->string()) : -1;
                });
                c.durableAt = e.stats().durableTxn;
                c.copy = s.file(("cut" + std::to_string(cuts.size()) + ".db").c_str());
                std::filesystem::copy_file(file, c.copy,
                                           std::filesystem::copy_options::overwrite_existing);
                cuts.push_back(c);
            });
        }
        for (int i = 0; i < 40; ++i)
            raiseTo(returned, writeRound(e, counter));
    };
    std::thread a(writer, true);
    std::thread b(writer, false);
    a.join();
    b.join();
    CHECK(cuts.size() >= 8);
    int torn = 0;
    for (const Cut& c : cuts) {
        unsigned slot = 0;
        const internal::Meta newest = newestMeta(c.copy, kPage, &slot);
        if (newest.txnid > c.durableAt) {
            tearSlot(c.copy, kPage, slot);  // not durable yet: a cut may lose it
            ++torn;
        }
        const int round = openedRound(c.copy);
        CHECK(round >= c.returnedAt);
        CHECK(round >= c.seenAt);
    }
    std::printf("    %d of %zu cuts caught a meta waiting for the next commit's flush\n", torn,
                cuts.size());
    // And the store itself ends on the last round, durable.
    e.close();
    CHECK_EQ(openedRound(file), counter.load() - 1);
}

TEST(pipelinedOptimisticGroupsLoseNothing)
{
    // Group commit under pipelining: the leader hands the lead on before its
    // group's meta is flushed, and the next group's data flush carries it.
    tst::Scratch s("pipeline");
    Env e = Env::configure().pipelineCommits().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("n", "0"); });
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < 60; ++j) {
                optimistic(e, [](OptimisticTxn& t) {
                    const std::string cur = *t.get("", "n");
                    t.put("", "n", std::to_string(std::stoll(cur) + 1));
                });
            }
        });
    }
    for (auto& w : workers)
        w.join();
    e.read([](Txn& t) { CHECK(t.mainDb().get("n")->string() == "240"); });
    const EnvStats st = e.stats();
    CHECK_EQ(st.durableTxn, st.lastTxn);
}

TEST(pipelinedCommitInFlightKeepsReadersChecksWhole)
{
    // Writers build on a commit readers cannot see yet: the free-page count
    // EnvStats keeps follows the former, and a reader's integrity check of
    // the newest snapshot it sees must not hold it to that count.
    tst::Scratch s("pipeline");
    Env e = Env::configure().pipelineCommits().open(s.file());
    e.write([](Txn& t) {
        for (int k = 0; k < 2000; ++k)
            t.mainDb().put(tst::keyOf(std::uint64_t(k)), tst::blob(300, std::uint64_t(k)));
    });
    std::mutex mtx;
    std::condition_variable cv;
    bool paused = false, resume = false;
    std::thread writer([&] {
        ScopedIoObserver hold([&](IoEvent ev, std::size_t) {
            if (ev != IoEvent::MetaBarrier)
                return;
            std::unique_lock<std::mutex> lk(mtx);
            paused = true;
            cv.notify_all();
            cv.wait(lk, [&] { return resume; });
        });
        // Frees far more pages than the meta page lists: the free list grows.
        e.write([](Txn& t) { t.mainDb().clear(); });
    });
    {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&] { return paused; });
    }
    std::string failed;
    std::uint64_t seen = 0;
    const EnvStats st = e.stats();
    try {
        e.read([&](Txn& t) {
            seen = t.id();
            checkIntegrity(t);
        });
    } catch (const std::exception& ex) {
        failed = ex.what();
    }
    {
        std::lock_guard<std::mutex> lk(mtx);
        resume = true;
    }
    cv.notify_all();
    writer.join();
    CHECK_EQ(seen, st.lastTxn);  // the commit in flight is not seen
    CHECK_EQ(failed, std::string());
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(pipelinedFailedOwnFlushFailsTheCommit)
{
    // A commit that flushes its own meta and fails must not report success:
    // the kernel may have dropped the page, so the store is poisoned too.
    tst::Scratch s("pipeline");
    Env e = Env::configure().pipelineCommits().open(s.file());
    std::atomic<int> counter{0};
    writeRound(e, counter);
    {
        ScopedIoObserver fail([](IoEvent ev, std::size_t) {
            if (ev == IoEvent::MetaBarrier)
                throw Error(ErrorCode::IoError, "injected meta flush failure");
        });
        CHECK_THROWS(writeRound(e, counter), ErrorCode::IoError);
    }
    CHECK_THROWS(e.writeTxn(), ErrorCode::BadTransaction);
}

int main()
{
    return tst::runAll("pipeline");
}
