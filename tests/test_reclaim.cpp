// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The free list is what keeps a copy-on-write store from growing for ever.
#include <atomic>
#include <fcntl.h>
#include <random>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "nosql/inspect.hpp"
#include "nosql/nosql.hpp"
#include "nosql/internal/free_list.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

/// A free-list chunk as runs: what it lists comes back, it is never longer
/// than the array, and a value that is neither form, or lists more than the
/// limit, is refused before any page is handed out.
TEST(freeListRunsRoundTripAndRefuseNonsense)
{
    using namespace nosql::internal;
    std::mt19937_64 rng(8);
    for (int round = 0; round < 2000; ++round) {
        std::vector<PageNo> pages;
        PageNo p = 2 + rng() % 1000;
        const std::size_t n = rng() % 300;
        for (std::size_t i = 0; i < n; ++i) {
            pages.push_back(p);
            p += rng() % 4 ? 1 : 1 + rng() % (round % 3 ? 50 : 1ull << 40);
        }
        const std::size_t bytes = freeRunsBytes(pages.data(), pages.size());
        if (!bytes)
            continue;  // the array is no longer
        CHECK(bytes < pages.size() * sizeof(PageNo));
        CHECK(bytes % sizeof(PageNo) != 0);
        std::vector<std::byte> v(bytes);
        writeFreeRuns(v.data(), bytes, pages.data(), pages.size());
        std::vector<PageNo> back;
        std::size_t count = 0;
        CHECK(forEachFreePage(Slice(v.data(), v.size()), 300, count,
                              [&](PageNo q) { back.push_back(q); }));
        CHECK_EQ(count, pages.size());
        CHECK(back == pages);
        // Below the count it lists: refused, and nothing handed out.
        if (!pages.empty()) {
            std::size_t calls = 0;
            CHECK(!forEachFreePage(Slice(v.data(), v.size()), pages.size() - 1, count,
                                   [&](PageNo) { ++calls; }));
            CHECK_EQ(calls, 0u);
        }
        // Cut short, or with a byte too many: neither form.
        CHECK(!forEachFreePage(Slice(v.data(), v.size() - 1), 300, count, [](PageNo) {}) ||
              (v.size() - 1) % sizeof(PageNo) == 0);
        std::vector<std::byte> longer = v;
        longer.push_back(std::byte(7));
        CHECK(!forEachFreePage(Slice(longer.data(), longer.size()), 300, count, [](PageNo) {}) ||
              longer.size() % sizeof(PageNo) == 0);
    }
    // A run whose length would wrap the page numbers.
    std::byte wrap[16];
    std::byte* at = putVarint(wrap, 1);
    at = putVarint(at, ~std::uint64_t(0) - 3);
    at = putVarint(at, 9);
    std::size_t count = 0;
    CHECK(!forEachFreePage(Slice(wrap, std::size_t(at - wrap)), 300, count, [](PageNo) {}));
}

/// A store at format 8 lists freed pages as runs: a commit that frees a
/// large contiguous tree needs far fewer free-list pages, and the pages come
/// back all the same. (A store at 7 keeps arrays, which older builds read.)
TEST(freeListRunsTakeFewerPagesAndStillReclaim)
{
    auto run = [](bool format8) {
        tst::Scratch s("reclaim-runs");
        Env e = Env::configure().pageSize(4096).open(s.file());
        if (format8)  // raised by its first PrefixKeys tree
            e.write([](Txn& t) { t.db("p", DbFlags::Create | DbFlags::PrefixKeys).put("k", "v"); });
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 100000; ++i)
                d.put(tst::keyOf(i), tst::blob(200, i));
        });
        const std::uint64_t full = e.stats().usedPages;
        std::uint64_t grown = 0;
        {
            Txn pin = e.readTxn();  // keeps the freed pages listed, not reused
            const std::uint64_t before = e.stats().usedPages;
            e.write([](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 100000; ++i)
                    d.erase(tst::keyOf(i));
            });
            grown = e.stats().usedPages - before;
            CHECK(e.stats().freePages > full / 2);
        }
        e.read([](Txn& t) { checkIntegrity(t); });
        // Released: a refill takes the listed pages instead of growing. (The
        // ticks go elsewhere: a refill landing in front of a key splits its
        // pages evenly rather than densely, and takes twice the pages.)
        e.write([](Txn& t) { t.db("tick", DbFlags::Create).put("tick", "1"); });
        e.write([](Txn& t) { t.db("tick").put("tick", "2"); });
        const std::uint64_t used = e.stats().usedPages;
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 100000; ++i)
                d.put(tst::keyOf(i), tst::blob(200, i + 1));
        });
        CHECK(e.stats().usedPages < used + used / 10);
        e.read([](Txn& t) { checkIntegrity(t); });
        return grown;
    };
    const std::uint64_t arrays = run(false), runs = run(true);
    std::printf("    pages the deleting commit added: %llu as arrays, %llu as runs\n",
                (unsigned long long)arrays, (unsigned long long)runs);
    // Some 5,600 freed pages: a dozen pages of arrays, a node of runs; the
    // rest is the commit's own tree and catalog pages.
    if (!NOSQL_PREFIX_KEYS_DEFAULT)  // else both stores are at 8
        CHECK(runs + 8 < arrays);
}

namespace {

using Contents = std::vector<std::pair<std::string, std::string>>;

Contents contentsOf(Db db)
{
    Contents out;
    for (auto [k, v] : db.all())
        out.emplace_back(k.string(), v.string());
    return out;
}

/// A store whose live pages are scattered through the file: two trees and
/// overflow values filled, then four in five entries erased at random.
void scatter(Env& e)
{
    e.write([](Txn& t) {
        Db main = t.mainDb();
        Db named = t.db("named", DbFlags::Create);
        for (int i = 0; i < 30000; ++i) {
            main.put(tst::keyOf(i), tst::blob(i % 97 == 0 ? 9000 : 120, i));
            named.put(tst::keyOf(i, "n"), tst::blob(60, i));
        }
    });
    std::mt19937_64 rng(12);
    e.write([&](Txn& t) {
        Db main = t.mainDb();
        Db named = t.db("named");
        for (int i = 0; i < 30000; ++i) {
            if (rng() % 5 == 0)
                continue;
            main.erase(tst::keyOf(i));
            named.erase(tst::keyOf(i, "n"));
        }
    });
    e.write([](Txn& t) { t.db("tick", DbFlags::Create).put("t", "1"); });
}

}  // namespace

/// shrink() moves the live pages down and gives the free tail back: the
/// file ends up near its live size, holds the same data, passes the
/// integrity check, reopens, and grows again as it is written.
TEST(shrinkGivesTheFreeTailBack)
{
    for (const Durability mode : {Durability::Safe, Durability::Lazy, Durability::None}) {
        tst::Scratch s("shrink");
        Contents main, named;
        std::uint64_t released = 0, live = 0;
        {
            Env e = Env::configure().pageSize(4096).sync(mode).open(s.file());
            scatter(e);
            e.sync();
            const EnvStats st = e.stats();
            live = st.usedPages - st.freePages;
            e.read([&](Txn& t) {
                main = contentsOf(t.mainDb());
                named = contentsOf(t.db("named"));
            });
            const std::uint64_t size = e.stats().fileSize;
            released = e.shrink();
            CHECK_EQ(e.stats().fileSize, size - released);
            std::printf("    mode %d: %llu of %llu pages given back, %llu live\n", int(mode),
                        (unsigned long long)(released / 4096), (unsigned long long)(size / 4096),
                        (unsigned long long)live);
#ifndef _WIN32
            // Down to about the live pages and the free list's slack.
            CHECK(e.stats().fileSize / 4096 < live + live / 4 + 256);
#endif
            e.read([&](Txn& t) {
                checkIntegrity(t);
                CHECK(contentsOf(t.mainDb()) == main);
                CHECK(contentsOf(t.db("named")) == named);
            });
            // It grows again as it is written.
            e.write([](Txn& t) {
                for (int i = 0; i < 20000; ++i)
                    t.mainDb().put(tst::keyOf(i, "more"), tst::blob(200, i));
            });
            e.read([](Txn& t) { checkIntegrity(t); });
        }
        Env e = Env::configure().pageSize(4096).sync(mode).open(s.file());
        e.read([&](Txn& t) {
            checkIntegrity(t);
            CHECK(contentsOf(t.db("named")) == named);
            CHECK_EQ(t.mainDb().count(), std::uint64_t(main.size() + 20000));
        });
    }
}

/// A read transaction open across shrink() keeps its snapshot whole: what it
/// can see is neither moved from nor cut. Once it ends, another shrink()
/// gets the rest.
TEST(shrinkLeavesAnOpenSnapshotWhole)
{
    tst::Scratch s("shrink-reader");
    Env e = Env::configure().pageSize(4096).open(s.file());
    scatter(e);
    Contents seen;
    std::uint64_t first = 0;
    {
        Txn reader = e.readTxn();
        seen = contentsOf(reader.mainDb());
        // Pages it sees are pinned; what was free before it began is not.
        e.write([](Txn& t) {
            for (int i = 0; i < 30000; i += 3)
                t.mainDb().erase(tst::keyOf(i));
        });
        first = e.shrink();
        CHECK(contentsOf(reader.mainDb()) == seen);
        checkIntegrity(reader);
    }
    const std::uint64_t second = e.shrink();
    std::printf("    with a reader open: %llu bytes, after it: %llu more\n",
                (unsigned long long)first, (unsigned long long)second);
#ifndef _WIN32
    CHECK(second > 0u);
#endif
    e.read([](Txn& t) { checkIntegrity(t); });
}

/// Nothing to give back: a store with no free tail is left as it is.
TEST(shrinkOfADenseStoreChangesNothing)
{
    tst::Scratch s("shrink-dense");
    Env e = Env::configure().pageSize(4096).open(s.file());
    e.write([](Txn& t) {
        for (int i = 0; i < 5000; ++i)
            t.mainDb().put(tst::keyOf(i), tst::blob(100, i));
    });
    Contents before;
    e.read([&](Txn& t) { before = contentsOf(t.mainDb()); });
    (void)e.shrink();
    const std::uint64_t again = e.shrink();
    CHECK_EQ(again, 0u);
    e.read([&](Txn& t) {
        checkIntegrity(t);
        CHECK(contentsOf(t.mainDb()) == before);
    });
}

TEST(steadyStateRewritesDoNotGrowTheFile)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(4096).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 5000; ++i)
            d.put(tst::keyOf(i), tst::blob(100, i));
    });
    const std::uint64_t warm = e.stats().usedPages;

    for (int round = 0; round < 60; ++round)
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 200; ++i) {
                const int k = (round * 200 + i) % 5000;
                d.put(tst::keyOf(k), tst::blob(100, k + round));
            }
        });

    const std::uint64_t after = e.stats().usedPages;
    // Without reclamation this would be tens of thousands of pages larger.
    CHECK(after < warm * 2);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(deleteEverythingThenRefillReusesPages)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 8000; ++i)
            d.put(tst::keyOf(i), tst::blob(50, i));
    });
    const std::uint64_t full = e.stats().usedPages;

    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 8000; ++i)
            d.erase(tst::keyOf(i));
        CHECK_EQ(d.count(), std::uint64_t(0));
    });
    // Two commits later the emptied pages must be back on the free list.
    e.write([](Txn& t) { t.mainDb().put("tick", "1"); });
    e.write([](Txn& t) { t.mainDb().put("tick", "2"); });
    CHECK(e.stats().freePages > full / 2);

    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 8000; ++i)
            d.put(tst::keyOf(i), tst::blob(50, i));
    });
    // The refill lands in front of "tick", so its splits are balanced rather
    // than append-dense; compressed leaves make `full` smaller, not that.
    CHECK(e.stats().usedPages < full * (NOSQL_PREFIX_KEYS_DEFAULT ? 3 : 2));
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(aParkedReaderHoldsPagesBackThenLetsThemGo)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 3000; ++i)
            d.put(tst::keyOf(i), tst::blob(80, i));
    });

    std::uint64_t pinnedGrowth = 0, freeGrowth = 0;
    {
        Txn reader = e.readTxn();
        CHECK_EQ(reader.mainDb().count(), std::uint64_t(3000));
        const std::uint64_t start = e.stats().usedPages;
        for (int round = 0; round < 20; ++round)
            e.write([&](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 300; ++i)
                    d.put(tst::keyOf(i), tst::blob(80, i + round * 7));
            });
        pinnedGrowth = e.stats().usedPages - start;
        // The snapshot still reads its own version of the world.
        CHECK_EQ(reader.mainDb().at(tst::keyOf(0)).string(), tst::blob(80, 0));
        CHECK_EQ(e.stats().oldestReader, reader.id());
        reader.abort();
    }
    {
        const std::uint64_t start = e.stats().usedPages;
        for (int round = 0; round < 20; ++round)
            e.write([&](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 300; ++i)
                    d.put(tst::keyOf(i), tst::blob(80, i + round * 11));
            });
        freeGrowth = e.stats().usedPages - start;
    }
    // With nothing pinning the free list, the same work should reuse pages.
    CHECK(freeGrowth < pinnedGrowth);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(freeListSurvivesReopen)
{
    tst::Scratch s("reclaim");
    {
        Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 4000; ++i)
                d.put(tst::keyOf(i), tst::blob(60, i));
        });
        e.write([](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 4000; ++i)
                d.erase(tst::keyOf(i));
        });
    }
    Env e = Env::configure().pageSize(1024).open(s.file());
    const std::uint64_t reopened = e.stats().usedPages;
    CHECK(e.stats().freePages > 100);
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 4000; ++i)
            d.put(tst::keyOf(i), tst::blob(60, i));
        checkIntegrity(t);
    });
    CHECK(e.stats().usedPages < reopened + 200);
}

TEST(dropReturnsEveryPage)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).open(s.file());
    e.write([](Txn& t) {
        auto d = t.db("scratch", DbFlags::Create);
        for (int i = 0; i < 5000; ++i)
            d.put(tst::keyOf(i), tst::blob(200, i));
    });
    const std::uint64_t full = e.stats().usedPages;
    e.write([](Txn& t) { t.dropDb("scratch"); });
    e.write([](Txn& t) { t.mainDb().put("tick", "1"); });
    CHECK(e.stats().freePages > full / 2);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

/// Under None and Lazy a commit's scan of the free tree starts where the last one stopped
/// finding anything (EnvImpl::reclaimSkip). What it skipped must be found again once a sync or
/// a departing reader makes it reusable, and an aborted transaction must not move the mark.
TEST(skippedFreeEntriesAreReusedOnceASyncReleasesThem)
{
    tst::Scratch s("reclaim");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).sync(Durability::None).open(s.file());
    const int keys = 3000;
    std::vector<int> version(keys, 0);
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < keys; ++i)
            d.put(tst::keyOf(i), tst::blob(60, i));
    });
    e.sync();
    std::mt19937_64 rng(11);
    const auto churn = [&](int commits) {
        for (int n = 0; n < commits; ++n) {
            const int k = int(rng() % keys);
            if (n % 7 == 3) {
                // Takes pages from the free list, then gives everything back.
                Txn t = e.writeTxn();
                for (int j = 0; j < 20; ++j)
                    t.mainDb().put(tst::keyOf((k + j) % keys), tst::blob(60, 999));
                t.abort();
                continue;
            }
            Txn held = n % 500 == 100 ? e.readTxn() : Txn();
            for (int extra = 0; extra < (n % 500 == 100 ? 20 : 1); ++extra) {
                const int kk = (k + extra) % keys;
                e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(kk), tst::blob(60, ++version[kk] * keys + kk)); });
            }
        }
    };
    churn(4000);
    const std::uint64_t settled = e.stats().usedPages;
    // Each round the sync releases what the scans had been passing over; if a scan kept
    // skipping it the file would have to grow by about that much every round.
    for (int round = 0; round < 6; ++round) {
        e.sync();
        churn(4000);
    }
    CHECK(e.stats().usedPages < settled + settled / 4);
    e.read([&](Txn& t) {
        checkIntegrity(t);
        auto d = t.mainDb();
        for (int i = 0; i < keys; ++i)
            CHECK(d.get(tst::keyOf(i)).value() == Slice(tst::blob(60, version[i] ? version[i] * keys + i : i)));
    });
}

/// Under None and Lazy the pages a small commit retires are ones no durable snapshot names,
/// most often: they go into the meta page's inline entry and the next commit takes them from
/// there (EnvImpl::inlineExemptTxn), so such commits in a row write no free-tree page. A
/// reader holds the entry back, a sync makes it an ordinary one, and neither loses a page or
/// lists one twice.
TEST(smallCommitsWithoutSyncLeaveTheFreeTreeAlone)
{
    tst::Scratch s("reclaim");
    const int keys = 3000;
    std::vector<int> version(keys, 0);
    std::mt19937_64 rng(23);
    {
        Env e = Env::configure().pageSize(1024).maxSize(1ull << 30).sync(Durability::None).open(s.file());
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < keys; ++i)
                d.put(tst::keyOf(i), tst::blob(60, i));
        });
        const auto rewrite = [&](int commits) {
            for (int n = 0; n < commits; ++n) {
                const int k = int(rng() % keys);
                e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(k), tst::blob(60, ++version[k] * keys + k)); });
            }
        };
        const auto freeRoot = [&] {
            std::uint64_t root = 0;
            e.read([&](Txn& t) { root = inspect::trees(t).at(0).root; });
            return root;
        };
        rewrite(50);
        const std::uint64_t root = freeRoot();
        const std::uint64_t used = e.stats().usedPages;
        rewrite(3000);
        CHECK_EQ(freeRoot(), root);
        CHECK_EQ(e.stats().usedPages, used);
        e.read([&](Txn& t) { checkIntegrity(t); });

        // A reader holds every entry back; what piled up is found again after it.
        {
            Txn reader = e.readTxn();
            rewrite(300);
            CHECK(e.stats().usedPages > used);
            e.read([&](Txn& t) { checkIntegrity(t); });
            reader.abort();
        }
        rewrite(600);
        const std::uint64_t settled = e.stats().usedPages;
        rewrite(3000);
        CHECK(e.stats().usedPages <= settled + 8);
        e.read([&](Txn& t) { checkIntegrity(t); });

        // A sync: the durable snapshot names every page, and the entry of the commit before
        // it is no longer one to take early.
        for (int round = 0; round < 4; ++round) {
            e.sync();
            rewrite(1500);
            e.read([&](Txn& t) { checkIntegrity(t); });
        }
        CHECK(e.stats().usedPages <= settled + settled / 4);
        // Aborts in between give back what they took.
        for (int n = 0; n < 200; ++n) {
            Txn t = e.writeTxn();
            for (int j = 0; j < 5; ++j)
                t.mainDb().put(tst::keyOf(int(rng() % keys)), tst::blob(60, 7));
            t.abort();
            rewrite(3);
        }
        e.read([&](Txn& t) { checkIntegrity(t); });
        e.sync();
        rewrite(40);
        e.sync();
    }
    Env e = Env::configure().pageSize(1024).open(s.file());
    e.read([&](Txn& t) {
        checkIntegrity(t);
        auto d = t.mainDb();
        for (int i = 0; i < keys; ++i)
            CHECK(d.get(tst::keyOf(i)).value() == Slice(tst::blob(60, version[i] ? version[i] * keys + i : i)));
    });
    e.write([&](Txn& t) { t.mainDb().put(tst::keyOf(1), tst::blob(60, 1)); });
    e.read([&](Txn& t) { checkIntegrity(t); });
}

int main()
{
    return tst::runAll("reclaim");
}

#ifndef _WIN32
#include "nosql/internal/format.hpp"

/// Two shrink() calls at once must never cut the file below what the newest durable meta names.
/// A monitor reads both meta pages from the file and checks the newest valid one against the size.
TEST(concurrentShrinksNeverCutBelowTheDurableFrontier)
{
    using namespace nosql::internal;
    tst::Scratch s("shrink2");
    const std::size_t ps = 4096;
    Env e = Env::configure().pageSize(ps).sync(Durability::None).open(s.file());
    const int fd = ::open(s.file().c_str(), O_RDONLY);
    CHECK(fd >= 0);
    std::atomic<bool> stop{false};
    std::atomic<unsigned> violations{0};
    std::thread monitor([&] {
        std::vector<char> buf(ps);
        while (!stop.load()) {
            std::uint64_t bestTxn = 0, bestLast = 0;
            bool any = false;
            for (unsigned slot = 0; slot < 2; ++slot) {
                if (::pread(fd, buf.data(), ps, off_t(slot * ps)) != ssize_t(ps))
                    continue;
                const auto* p = reinterpret_cast<const Page*>(buf.data());
                if (p->flags != P_META)
                    continue;
                const Meta* m = metaOf(p);
                if (!metaValid(*m, ps))
                    continue;
                if (!any || m->txnid > bestTxn) {
                    any = true;
                    bestTxn = m->txnid;
                    bestLast = m->lastPgno;
                }
            }
            struct stat st;
            if (any && ::fstat(fd, &st) == 0 && (bestLast + 1) * ps > std::uint64_t(st.st_size))
                violations.fetch_add(1);
        }
    });
    std::thread writer([&] {
        for (int round = 0; !stop.load(); ++round) {
            e.write([&](Txn& t) {
                for (int i = 0; i < 400; ++i)
                    t.mainDb().put(tst::keyOf(i, "r"), tst::blob(300, i + round));
            });
            e.write([&](Txn& t) {
                for (int i = 0; i < 400; ++i)
                    t.mainDb().erase(tst::keyOf(i, "r"));
            });
        }
    });
    std::vector<std::thread> shrinkers;
    for (int k = 0; k < 2; ++k)
        shrinkers.emplace_back([&] {
            while (!stop.load())
                e.shrink();
        });
    std::this_thread::sleep_for(std::chrono::seconds(3));
    stop.store(true);
    for (auto& t : shrinkers)
        t.join();
    writer.join();
    monitor.join();
    ::close(fd);
    CHECK_EQ(violations.load(), 0u);
}
#endif

TEST(shrinkWithATimeoutGivesUpWhileAWriterHoldsTheSlot)
{
    tst::Scratch s("shrink-timeout");
    Env e = Env::configure().pageSize(4096).open(s.file());
    e.write([](Txn& t) { t.mainDb().put("k", "v"); });
    Txn held = e.writeTxn();
    const auto started = std::chrono::steady_clock::now();
    CHECK_THROWS(e.shrink(std::chrono::milliseconds(100)), ErrorCode::Busy);
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(5));
    held.abort();
    e.shrink(std::chrono::milliseconds(1000));  // free now
}
