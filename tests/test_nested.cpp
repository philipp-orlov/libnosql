// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

TEST(nestedCommitFoldsIntoParent)
{
    tst::Scratch s("nested");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        t.mainDb().put("outer", "1");
        t.nested([](Txn& n) { n.mainDb().put("inner", "2"); });
        CHECK(t.mainDb().contains("inner"));
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        CHECK(t.mainDb().contains("outer"));
        CHECK(t.mainDb().contains("inner"));
    });
}

TEST(nestedAbortDiscardsOnlyTheChild)
{
    tst::Scratch s("nested");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        d.put("keep", "1");
        {
            Txn n = t.nested();
            n.mainDb().put("gone", "2");
            n.mainDb().erase("keep");
            n.abort();
        }
        CHECK(d.contains("keep"));
        CHECK(!d.contains("gone"));
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        CHECK(t.mainDb().contains("keep"));
        CHECK(!t.mainDb().contains("gone"));
    });
}

TEST(nestedExceptionRollsTheChildBack)
{
    tst::Scratch s("nested");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        t.mainDb().put("a", "1");
        try {
            t.nested([](Txn& n) {
                n.mainDb().put("b", "2");
                throw std::runtime_error("nope");
            });
        } catch (const std::runtime_error&) {}
        CHECK(t.mainDb().contains("a"));
        CHECK(!t.mainDb().contains("b"));
        checkIntegrity(t);
    });
}

TEST(deeplyNestedTransactions)
{
    tst::Scratch s("nested");
    Env e = Env::configure().pageSize(512).open(s.file());
    e.write([](Txn& t) {
        t.mainDb().put("d0", "v");
        t.nested([](Txn& n1) {
            n1.mainDb().put("d1", "v");
            n1.nested([](Txn& n2) {
                n2.mainDb().put("d2", "v");
                n2.nested([](Txn& n3) {
                    n3.mainDb().put("d3", "v");
                    n3.nested([](Txn& n4) {
                        for (int i = 0; i < 500; ++i)
                            n4.mainDb().put(tst::keyOf(i), tst::blob(60, i));
                        n4.mainDb().put("d4", "v");
                    });
                });
                // Abort at this level: d3, d4 and the bulk rows all vanish.
                Txn doomed = n2.nested();
                doomed.mainDb().put("doomed", "v");
                doomed.abort();
            });
        });
        auto d = t.mainDb();
        for (const char* k : {"d0", "d1", "d2", "d3", "d4"})
            CHECK(d.contains(k));
        CHECK(!d.contains("doomed"));
        CHECK_EQ(d.count(), std::uint64_t(505));
        checkIntegrity(t);
    });
}

TEST(nestedCanCreateAndDropSubDatabases)
{
    tst::Scratch s("nested");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        t.nested([](Txn& n) { n.db("made-inside", DbFlags::Create).put("k", "v"); });
        CHECK(t.hasDb("made-inside"));
        CHECK_EQ(t.db("made-inside").at("k").string(), std::string("v"));
        checkIntegrity(t);
    });
    e.write([](Txn& t) {
        Txn n = t.nested();
        n.db("made-inside").put("k2", "v2");
        n.abort();
        CHECK(!t.db("made-inside").contains("k2"));
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        CHECK(t.hasDb("made-inside"));
        CHECK(!t.db("made-inside").contains("k2"));
    });
}

TEST(parentIsFrozenWhileAChildIsOpen)
{
    tst::Scratch s("nested");
    Env e = Env::configure().open(s.file());
    Txn t = e.writeTxn();
    Txn n = t.nested();
    CHECK_THROWS(t.mainDb(), ErrorCode::BadTransaction);
    CHECK_THROWS(t.commit(), ErrorCode::BadTransaction);
    n.abort();
    t.mainDb().put("ok", "1");
    t.commit();
}

TEST(abortedChildPagesAreReusable)
{
    tst::Scratch s("nested");
    Env e = Env::configure().pageSize(512).open(s.file());
    e.write([](Txn& t) {
        for (int i = 0; i < 200; ++i)
            t.mainDb().put(tst::keyOf(i), tst::blob(80, i));
    });
    const std::uint64_t before = e.stats().usedPages;
    for (int round = 0; round < 30; ++round) {
        e.write([&](Txn& t) {
            Txn n = t.nested();
            for (int i = 0; i < 200; ++i)
                n.mainDb().put(tst::keyOf(i), tst::blob(80, i + round));
            n.abort();
            t.mainDb().put("tick", std::to_string(round));
        });
    }
    const std::uint64_t after = e.stats().usedPages;
    // Repeatedly throwing away the same work must not grow the file without
    // bound; a little slack covers the free-list bookkeeping itself.
    CHECK(after < before + 220);
}

int main()
{
    return tst::runAll("nested");
}
