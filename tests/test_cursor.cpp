// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

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

int main()
{
    return tst::runAll("cursor");
}
