// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <cstring>
#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

TEST(openCreatesAStore)
{
    tst::Scratch s("basic");
    {
        Env e = Env::configure().open(s.file());
        CHECK(e.valid());
        CHECK_EQ(e.pageSize(), std::size_t(4096));
    }
    CHECK(std::filesystem::exists(s.file()));
    CHECK(std::filesystem::file_size(s.file()) >= 2 * 4096);
}

TEST(putGetRoundtrip)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        CHECK(d.put("alpha", "one"));
        CHECK(d.put("beta", "two"));
        CHECK_EQ(d.at("alpha").string(), std::string("one"));
        CHECK_EQ(d.count(), std::uint64_t(2));
    });
    e.read([](Txn& t) {
        auto d = t.mainDb();
        CHECK_EQ(d.at("beta").string(), std::string("two"));
        CHECK(!d.get("gamma").has_value());
        CHECK(d.contains("alpha"));
    });
}

TEST(commitIsDurableAcrossReopen)
{
    tst::Scratch s("basic");
    {
        Env e = Env::configure().open(s.file());
        e.write([](Txn& t) { t.mainDb().put("persisted", "yes"); });
    }
    {
        Env e = Env::configure().open(s.file());
        e.read([](Txn& t) { CHECK_EQ(t.mainDb().at("persisted").string(), std::string("yes")); });
    }
}

TEST(abortDiscardsEverything)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.mainDb().put("keep", "1"); });
    {
        Txn t = e.writeTxn();
        t.mainDb().put("discard", "1");
        t.mainDb().erase("keep");
        t.abort();
    }
    e.read([](Txn& t) {
        CHECK(t.mainDb().contains("keep"));
        CHECK(!t.mainDb().contains("discard"));
    });
}

TEST(exceptionInWriteScopeRollsBack)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    bool threw = false;
    try {
        e.write([](Txn& t) {
            t.mainDb().put("half", "written");
            throw std::runtime_error("boom");
        });
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
    e.read([](Txn& t) { CHECK(!t.mainDb().contains("half")); });
}

TEST(putModes)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        CHECK(d.put("k", "v1", PutMode::InsertUnique));
        CHECK(!d.put("k", "v2", PutMode::InsertUnique));
        CHECK_EQ(d.at("k").string(), std::string("v1"));
        CHECK(d.put("k", "v3", PutMode::UpdateOnly));
        CHECK_EQ(d.at("k").string(), std::string("v3"));
        CHECK(!d.put("absent", "x", PutMode::UpdateOnly));
        CHECK(d.put("k", "v4"));
        CHECK_EQ(d.count(), std::uint64_t(1));
    });
}

TEST(eraseAndReinsert)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        d.put("x", "1");
        CHECK(d.erase("x"));
        CHECK(!d.erase("x"));
        CHECK_EQ(d.count(), std::uint64_t(0));
        d.put("x", "2");
        CHECK_EQ(d.at("x").string(), std::string("2"));
    });
}

TEST(reserveWritesInPlace)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        auto w = d.reserve("blob", 5);
        CHECK_EQ(w.size(), std::size_t(5));
        std::memcpy(w.data(), "hello", 5);
    });
    e.read([](Txn& t) { CHECK_EQ(t.mainDb().at("blob").string(), std::string("hello")); });
}

TEST(rejectsOversizedAndEmptyKeys)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    const std::string huge(e.maxKeySize() + 1, 'k');
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        CHECK_THROWS(d.put(huge, "v"), ErrorCode::KeyTooLarge);
        CHECK_THROWS(d.put("", "v"), ErrorCode::KeyTooLarge);
        const std::string ok(e.maxKeySize(), 'k');
        CHECK(d.put(ok, "v"));
        CHECK_EQ(d.at(ok).string(), std::string("v"));
    });
}

TEST(valuesSurviveManyUpdates)
{
    tst::Scratch s("basic");
    Env e = Env::configure().open(s.file());
    for (int round = 0; round < 20; ++round) {
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 50; ++i)
                d.put(tst::keyOf(i), tst::blob(3 + (i * round) % 200, i * 31 + round));
        });
    }
    e.read([&](Txn& t) {
        auto d = t.mainDb();
        CHECK_EQ(d.count(), std::uint64_t(50));
        for (int i = 0; i < 50; ++i)
            CHECK_EQ(d.at(tst::keyOf(i)).string(), tst::blob(3 + (i * 19) % 200, i * 31 + 19));
    });
}

TEST(appendModeDemandsAscendingKeys)
{
    tst::Scratch s("basic");
    Env e = Env::configure().pageSize(512).open(s.file());
    e.write([](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 2000; ++i)
            d.put(tst::keyOf(i), "v", PutMode::Append);
        CHECK_THROWS(d.put(tst::keyOf(500), "v", PutMode::Append), ErrorCode::InvalidArgument);
        CHECK_THROWS(d.put(tst::keyOf(1999), "v", PutMode::Append), ErrorCode::InvalidArgument);
        CHECK(d.put(tst::keyOf(2000), "v", PutMode::Append));
        checkIntegrity(t);
        CHECK_EQ(d.count(), std::uint64_t(2001));
        // Ascending bulk loads should pack pages nearly full.
        CHECK(d.stats().leafPages < 2001 / 12);
    });
}

int main()
{
    return tst::runAll("basic");
}
