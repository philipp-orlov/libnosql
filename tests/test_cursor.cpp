// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <map>
#include <random>
#include <set>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {
void seed(Env& e, int n)
{
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < n; ++i)
            d.put(tst::keyOf(i), "v" + std::to_string(i));
    });
}
}  // namespace

TEST(iteratesInKeyOrder)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    seed(e, 500);
    e.read([](Txn& t) {
        int i = 0;
        for (auto [k, v] : t.mainDb().all()) {
            CHECK_EQ(k.string(), tst::keyOf(i));
            CHECK_EQ(v.string(), "v" + std::to_string(i));
            ++i;
        }
        CHECK_EQ(i, 500);
    });
}

TEST(movingAndReadingInOneCall)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        Db d = t.mainDb();
        for (const char* k : {"a", "c", "e"})
            d.put(k, std::string(k) + "!");
    });
    e.read([](Txn& t) {
        Cursor c = t.mainDb().cursor();
        Slice k, v;
        std::string seen;
        for (bool ok = c.first(k, v); ok; ok = c.next(k, v))
            seen += k.string() + "=" + v.string() + ";";
        CHECK_EQ(seen, std::string("a=a!;c=c!;e=e!;"));
        CHECK(c.seek(Slice("b"), k, v));
        CHECK_EQ(k.string() + v.string(), std::string("cc!"));
        CHECK(!c.seek(Slice("f"), k, v));
    });
}

TEST(reverseIteration)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    seed(e, 300);
    e.read([](Txn& t) {
        auto c = t.mainDb().cursor();
        int i = 299;
        for (bool ok = c.last(); ok; ok = c.prev(), --i)
            CHECK_EQ(c.key().string(), tst::keyOf(i));
        CHECK_EQ(i, -1);
    });
}

TEST(seekLandsOnLowerBound)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (const char* k : {"aa", "cc", "ee", "gg"})
            d.put(k, k);
    });
    e.read([](Txn& t) {
        auto c = t.mainDb().cursor();
        CHECK(c.seek("bb"));
        CHECK_EQ(c.key().string(), std::string("cc"));
        CHECK(c.seek("ee"));
        CHECK_EQ(c.key().string(), std::string("ee"));
        CHECK(!c.seek("zz"));
        CHECK(!c.valid());
        CHECK(c.seekExact("aa"));
        CHECK(!c.seekExact("bb"));
    });
}

TEST(ranges)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    seed(e, 100);
    e.read([](Txn& t) {
        auto d = t.mainDb();
        int n = 0;
        for (auto [k, v] : d.between(tst::keyOf(10), tst::keyOf(20))) {
            (void)v;
            CHECK_EQ(k.string(), tst::keyOf(10 + n));
            ++n;
        }
        CHECK_EQ(n, 10);

        n = 0;
        for (auto e2 : d.from(tst::keyOf(95))) {
            (void)e2;
            ++n;
        }
        CHECK_EQ(n, 5);

        n = 0;
        for (auto e2 : d.upto(tst::keyOf(7))) {
            (void)e2;
            ++n;
        }
        CHECK_EQ(n, 7);
    });
}

TEST(prefixRange)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 40; ++i)
            d.put(tst::keyOf(i, "user"), "u");
        for (int i = 0; i < 25; ++i)
            d.put(tst::keyOf(i, "post"), "p");
    });
    e.read([](Txn& t) {
        int users = 0, posts = 0;
        for (auto x : t.mainDb().prefix("user:")) {
            (void)x;
            ++users;
        }
        for (auto x : t.mainDb().prefix("post:")) {
            (void)x;
            ++posts;
        }
        CHECK_EQ(users, 40);
        CHECK_EQ(posts, 25);
    });
}

/// Under ReverseKey a prefix in the tree's order is a common ending: those
/// keys sit together, where the ones starting alike do not.
TEST(prefixRangeUnderReverseKeyMatchesEndings)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().maxDbs(2).open(s.file());
    e.write([](Txn& t) {
        auto d = t.db("hosts", DbFlags::ReverseKey | DbFlags::Create);
        for (int i = 0; i < 300; ++i) {
            d.put("h" + std::to_string(i) + ".example.com", "e");
            d.put("h" + std::to_string(i) + ".example.org", "o");
            d.put("example.com.h" + std::to_string(i), "x");
        }
    });
    e.read([](Txn& t) {
        int n = 0;
        for (auto [key, value] : t.db("hosts").prefix(".example.com")) {
            CHECK(key.endsWith(".example.com"));
            CHECK(value.view() == "e");
            ++n;
        }
        CHECK_EQ(n, 300);
    });
}

TEST(eraseWhileIterating)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    seed(e, 400);
    e.write([](Txn& t) {
        auto d = t.mainDb();
        auto c = d.cursor();
        int removed = 0;
        bool ok = c.first();
        while (ok) {
            const int idx = std::stoi(c.key().string().substr(4));
            if (idx % 3 == 0) {
                ok = c.erase();
                ++removed;
            } else {
                ok = c.next();
            }
        }
        CHECK_EQ(removed, 134);
        CHECK_EQ(d.count(), std::uint64_t(400 - 134));
        for (int i = 0; i < 400; ++i)
            CHECK_EQ(d.contains(tst::keyOf(i)), i % 3 != 0);
    });
}

TEST(cursorSurvivesWritesThroughAnotherCursor)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    seed(e, 200);
    e.write([](Txn& t) {
        auto d = t.mainDb();
        auto reader = d.cursor();
        CHECK(reader.seek(tst::keyOf(100)));
        // Force page splits underneath the parked cursor.
        for (int i = 1000; i < 2000; ++i)
            d.put(tst::keyOf(i), tst::blob(200, i));
        CHECK_EQ(reader.key().string(), tst::keyOf(100));
        CHECK(reader.next());
        CHECK_EQ(reader.key().string(), tst::keyOf(101));
        // ...and merges.
        for (int i = 0; i < 200; ++i)
            if (i != 100 && i != 101)
                d.erase(tst::keyOf(i));
        CHECK_EQ(reader.key().string(), tst::keyOf(101));
    });
}

TEST(cursorPutPositionsItself)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto c = t.mainDb().cursor();
        CHECK(c.put("mid", "1"));
        CHECK_EQ(c.key().string(), std::string("mid"));
        CHECK_EQ(c.value().string(), std::string("1"));
        CHECK(c.put("aaa", "0"));
        CHECK_EQ(c.key().string(), std::string("aaa"));
        CHECK(c.next());
        CHECK_EQ(c.key().string(), std::string("mid"));
    });
}

TEST(emptyDatabaseIteratesZeroTimes)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    e.read([](Txn& t) {
        auto c = t.mainDb().cursor();
        CHECK(!c.first());
        CHECK(!c.last());
        CHECK(!c.seek("x"));
        int n = 0;
        for (auto x : t.mainDb().all()) {
            (void)x;
            ++n;
        }
        CHECK_EQ(n, 0);
    });
}

namespace {

std::string randomKey(std::mt19937_64& rng)
{
    // Mostly short, sometimes long enough to make branch pages split early.
    const std::size_t n = rng() % 8 == 0 ? 40 + rng() % 120 : 1 + rng() % 12;
    std::string k(n, 'a');
    for (char& ch : k)
        ch = char('a' + rng() % 26);
    return k;
}

std::string randomValue(std::mt19937_64& rng)
{
    const std::size_t n = rng() % 50 == 0 ? 3000 + rng() % 6000 : rng() % 200;
    return std::string(n, char('0' + rng() % 10));
}

/// Every count the tree can answer, against the model.
void checkCounts(Txn& t, const std::map<std::string, std::string>& model, std::mt19937_64& rng)
{
    checkIntegrity(t);
    Db d = t.mainDb();
    CHECK_EQ(d.count(), std::uint64_t(model.size()));
    CHECK_EQ(d.countRange(Slice(), Slice()), std::uint64_t(model.size()));
    for (int q = 0; q < 40; ++q) {
        std::string lo = randomKey(rng), hi = randomKey(rng);
        if (hi < lo)
            std::swap(lo, hi);
        const auto want = std::uint64_t(std::distance(model.lower_bound(lo), model.lower_bound(hi)));
        CHECK_EQ(d.countRange(lo, hi), want);
        CHECK_EQ(d.countRange(Slice(), hi), std::uint64_t(std::distance(model.begin(), model.lower_bound(hi))));
        CHECK_EQ(d.countRange(lo, Slice()), std::uint64_t(std::distance(model.lower_bound(lo), model.end())));
    }
    if (model.empty())
        return;
    for (int q = 0; q < 20; ++q) {
        Cursor c = d.cursor();
        const std::string from = randomKey(rng);
        auto it = model.lower_bound(from);
        if (!c.seek(from)) {
            CHECK(it == model.end());
            continue;
        }
        const std::uint64_t n = rng() % 3 == 0 ? rng() % 5 : rng() % (model.size() + 10);
        const std::uint64_t left = std::uint64_t(std::distance(it, model.end())) - 1;
        if (n > left) {
            CHECK(!c.skip(n));
            continue;
        }
        CHECK(c.skip(n));
        std::advance(it, std::ptrdiff_t(n));
        CHECK_EQ(c.key().string(), it->first);
        CHECK_EQ(c.value().string(), it->second);
    }
}

}  // namespace

TEST(branchCountsSurviveEverySplitAndMerge)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    std::mt19937_64 rng(42);
    std::map<std::string, std::string> model;
    for (int round = 0; round < 40; ++round) {
        Txn t = e.writeTxn();
        Db d = t.mainDb();
        // Grow early rounds, shrink late ones, so pages split, merge, shift
        // and the root grows and collapses.
        const int ops = 600;
        const bool shrinking = round >= 25;
        for (int i = 0; i < ops; ++i) {
            const unsigned what = unsigned(rng() % 10);
            if ((shrinking && what < 6) || (!shrinking && what < 2)) {
                if (model.empty())
                    continue;
                auto it = model.lower_bound(randomKey(rng));
                if (it == model.end())
                    it = model.begin();
                CHECK(d.erase(it->first));
                model.erase(it);
            } else {
                const std::string k = what == 9 && !model.empty() ? model.begin()->first : randomKey(rng);
                const std::string v = randomValue(rng);
                d.put(k, v);
                model[k] = v;
            }
        }
        // A nested transaction whose changes are thrown away must leave the
        // counts as they were.
        {
            Txn n = t.nested();
            Db nd = n.mainDb();
            for (int i = 0; i < 300; ++i)
                nd.put(randomKey(rng), randomValue(rng));
            n.abort();
        }
        checkCounts(t, model, rng);
        t.commit();
        Txn r = e.readTxn();
        checkCounts(r, model, rng);
    }
    // Down to nothing and back.
    e.write([&](Txn& t) {
        Db d = t.mainDb();
        for (const auto& [k, v] : model)
            CHECK(d.erase(k));
        model.clear();
        checkCounts(t, model, rng);
        for (int i = 0; i < 3000; ++i) {
            const std::string k = tst::keyOf(i);
            d.put(k, "v");
            model[k] = "v";
        }
        checkCounts(t, model, rng);
    });
}

TEST(seeksNearTheLastOneStayCorrect)
{
    tst::Scratch s("cursor");
    Env e = Env::configure().open(s.file());
    std::map<std::string, std::string> model;
    e.write([&](Txn& t) {
        Db d = t.mainDb();
        for (int i = 0; i < 20000; i += 2) {
            d.put(tst::keyOf(i), "v" + std::to_string(i));
            model[tst::keyOf(i)] = "v" + std::to_string(i);
        }
    });
    std::mt19937_64 rng(7);
    auto probe = [&](Cursor& c, int target) {
        const std::string k = tst::keyOf(target);
        const auto it = model.lower_bound(k);
        const bool ok = c.seek(k);
        CHECK_EQ(ok, it != model.end());
        if (ok)
            CHECK_EQ(c.key().string(), it->first);
        Slice v;
        const bool exact = c.seekExact(k, v);
        CHECK_EQ(exact, model.count(k) == 1);
        if (exact)
            CHECK_EQ(v.string(), model[k]);
    };
    e.read([&](Txn& t) {
        Cursor c = t.mainDb().cursor();
        CHECK(c.boundTo(t.mainDb()));
        // Ascending neighbours (the leaf hint hits), then random jumps (it
        // misses and backs off), then neighbours again.
        for (int i = 0; i < 3000; ++i)
            probe(c, 5000 + i);
        for (int i = 0; i < 3000; ++i)
            probe(c, int(rng() % 21000));
        for (int i = 0; i < 3000; ++i)
            probe(c, 15000 - i);
    });
    // In a write transaction a write in between must stop the hint from
    // trusting a page that has changed.
    e.write([&](Txn& t) {
        Db d = t.mainDb();
        Cursor c = d.cursor();
        for (int i = 0; i < 4000; ++i) {
            probe(c, 8000 + i);
            if (i % 7 == 0) {
                const std::string k = tst::keyOf(8000 + i + 1);
                d.put(k, "w");
                model[k] = "w";
            }
            if (i % 11 == 0) {
                const std::string k = tst::keyOf(8000 + i + 2);
                d.erase(k);
                model.erase(k);
            }
        }
        checkIntegrity(t);
    });
    Txn r = e.readTxn();
    Cursor stale = r.mainDb().cursor();
    r.abort();
    Txn r2 = e.readTxn();
    CHECK(!stale.boundTo(r2.mainDb()));
}

int main()
{
    return tst::runAll("cursor");
}
