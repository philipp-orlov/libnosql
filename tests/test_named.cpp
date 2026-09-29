// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <fstream>
#include "nosql/internal/format.hpp"

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

TEST(createAndReopenNamedDbs)
{
    tst::Scratch s("named");
    {
        Env e = Env::configure().open(s.file());
        e.write([](Txn& t) {
            t.db("users", DbFlags::Create).put("u1", "alice");
            t.db("posts", DbFlags::Create).put("p1", "hello");
            checkIntegrity(t);
        });
    }
    Env e = Env::configure().open(s.file());
    e.read([](Txn& t) {
        auto names = t.listDbs();
        std::sort(names.begin(), names.end());
        CHECK_EQ(names.size(), std::size_t(2));
        CHECK_EQ(names[0], std::string("posts"));
        CHECK_EQ(names[1], std::string("users"));
        CHECK_EQ(t.db("users").at("u1").string(), std::string("alice"));
        CHECK_EQ(t.db("posts").at("p1").string(), std::string("hello"));
    });
}

TEST(namedDbsAreIndependentKeyspaces)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto a = t.db("a", DbFlags::Create);
        auto b = t.db("b", DbFlags::Create);
        for (int i = 0; i < 500; ++i) {
            a.put(tst::keyOf(i), "A");
            b.put(tst::keyOf(i), "B");
        }
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        CHECK_EQ(t.db("a").at(tst::keyOf(42)).string(), std::string("A"));
        CHECK_EQ(t.db("b").at(tst::keyOf(42)).string(), std::string("B"));
        CHECK_EQ(t.db("a").count(), std::uint64_t(500));
    });
}

TEST(missingDbWithoutCreateIsNotFound)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.read([](Txn& t) {
        CHECK(!t.hasDb("nope"));
        CHECK_THROWS(t.db("nope"), ErrorCode::NotFound);
    });
}

TEST(dropDbRemovesDataAndDirectoryEntry)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.db("temp", DbFlags::Create);
        for (int i = 0; i < 2000; ++i)
            d.put(tst::keyOf(i), tst::blob(100, i));
    });
    e.write([](Txn& t) {
        t.dropDb("temp");
        CHECK(!t.hasDb("temp"));
        CHECK_EQ(t.listDbs().size(), std::size_t(0));
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        CHECK(!t.hasDb("temp"));
        CHECK_THROWS(t.db("temp"), ErrorCode::NotFound);
    });
}

TEST(clearKeepsTheDbButDropsTheRows)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.db("c", DbFlags::Create);
        for (int i = 0; i < 300; ++i)
            d.put(tst::keyOf(i), "x");
        d.clear();
        CHECK_EQ(d.count(), std::uint64_t(0));
        d.put("after", "y");
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        CHECK(t.hasDb("c"));
        CHECK_EQ(t.db("c").count(), std::uint64_t(1));
    });
}

TEST(mainTreeAndNamedDbsCoexist)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        t.mainDb().put("plain", "value");
        t.db("sub", DbFlags::Create).put("k", "v");
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        auto m = t.mainDb();
        // The directory entry for "sub" must not show up as a row.
        CHECK_EQ(m.count(), std::uint64_t(1));
        int rows = 0;
        for (auto [k, v] : m.all()) {
            (void)v;
            CHECK_EQ(k.string(), std::string("plain"));
            ++rows;
        }
        CHECK_EQ(rows, 1);
        CHECK(!m.get("sub").has_value());
    });
    e.write([](Txn& t) {
        t.mainDb().put("sub", "ordinary value");
        CHECK_EQ(t.db("sub").at("k").string(), std::string("v"));
        t.mainDb().clear();
        CHECK(t.hasDb("sub"));
        CHECK_EQ(t.mainDb().count(), 0u);
    });
}

TEST(integerKeyOrdering)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.db("ints", DbFlags::Create | DbFlags::IntegerKey);
        for (std::uint64_t i = 0; i < 1000; ++i) {
            const std::uint64_t k = (i * 2654435761u) % 100000;
            d.put(Slice::ref(k), Slice::ref(i));
        }
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        auto d = t.db("ints");
        std::uint64_t prev = 0;
        bool first = true;
        for (auto [k, v] : d.all()) {
            (void)v;
            const auto cur = k.as<std::uint64_t>();
            if (!first)
                CHECK(prev < cur);
            prev = cur;
            first = false;
        }
        CHECK_EQ(d.flags(), DbFlags::IntegerKey);
    });
}

TEST(reverseKeyOrdering)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) {
        auto d = t.db("rev", DbFlags::Create | DbFlags::ReverseKey);
        d.put("aaa-z", "1");
        d.put("bbb-a", "2");
        d.put("ccc-m", "3");
        checkIntegrity(t);
    });
    e.read([](Txn& t) {
        auto c = t.db("rev").cursor();
        CHECK(c.first());
        CHECK_EQ(c.key().string(), std::string("bbb-a"));  // sorted by last char
        CHECK(c.next());
        CHECK_EQ(c.key().string(), std::string("ccc-m"));
        CHECK(c.next());
        CHECK_EQ(c.key().string(), std::string("aaa-z"));
    });
}

TEST(reopeningWithConflictingFlagsIsRejected)
{
    tst::Scratch s("named");
    Env e = Env::configure().open(s.file());
    e.write([](Txn& t) { t.db("x", DbFlags::Create | DbFlags::IntegerKey); });
    e.write([](Txn& t) {
        CHECK_THROWS(t.db("x", DbFlags::Create | DbFlags::ReverseKey), ErrorCode::Incompatible);
    });
}

TEST(maxDbsIsEnforced)
{
    tst::Scratch s("named");
    Env e = Env::configure().maxDbs(4).open(s.file());
    e.write([](Txn& t) {
        for (int i = 0; i < 4; ++i)
            t.db("db" + std::to_string(i), DbFlags::Create);
        CHECK_THROWS(t.db("one-too-many", DbFlags::Create), ErrorCode::TooManyDbs);
    });
}

TEST(oldReaderDoesNotObserveNewlyRegisteredDatabase)
{
    tst::Scratch scratch("old-reader");
    Env env = Env::configure().open(scratch.file());
    Txn reader = env.readTxn();
    env.write([](Txn& txn) { txn.db("new", DbFlags::Create).put("key", "value"); });
    CHECK_THROWS(reader.db("new"), ErrorCode::NotFound);
}

TEST(distinctDroppedNamesDoNotExhaustHandles)
{
    tst::Scratch scratch("drop-names");
    Env env = Env::configure().maxDbs(2).sync(Durability::None).open(scratch.file());
    for (int index = 0; index < 100; ++index) {
        env.write([&](Txn& txn) {
            const auto name = "temporary-" + std::to_string(index);
            txn.db(name, DbFlags::Create).put("key", "value");
            txn.dropDb(name);
        });
    }
    env.read([](Txn& txn) { CHECK(txn.listDbs().empty()); });
}

TEST(integerTreesRejectMixedWidths)
{
    tst::Scratch scratch("integer-width");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) {
        Db db = txn.db("ints", DbFlags::Create | DbFlags::IntegerKey);
        const std::uint32_t first = 1;
        const std::uint64_t second = 256;
        db.put(Slice::ref(first), "value");
        CHECK_THROWS(db.put(Slice::ref(second), "wrong"), ErrorCode::InvalidArgument);
        CHECK_THROWS(db.get("x"), ErrorCode::InvalidArgument);
    });
}

TEST(integrityDiscoversUnopenedNamedTrees)
{
    tst::Scratch scratch("unopened-tree");
    {
        Env env = Env::configure().open(scratch.file());
        env.write([](Txn& txn) { txn.db("hidden", DbFlags::Create).put("key", "value"); });
    }
    std::fstream file(scratch.file(), std::ios::in | std::ios::out | std::ios::binary);
    internal::Meta meta{};
    file.seekg(4096 + internal::kPageHdr);
    file.read(reinterpret_cast<char*>(&meta), sizeof meta);
    std::vector<std::uint64_t> buffer(4096 / 8);
    file.seekg(std::streamoff(meta.catalogTree.root * 4096));
    file.read(reinterpret_cast<char*>(buffer.data()), 4096);
    const auto* page = reinterpret_cast<const internal::Page*>(buffer.data());
    const auto* node = internal::lnode(page, 0);
    internal::Tree tree{};
    std::memcpy(&tree, reinterpret_cast<const char*>(node) + sizeof(*node) + node->ksize, sizeof tree);
    const std::uint64_t invalid = UINT64_MAX;
    file.seekp(std::streamoff(tree.root * 4096));
    file.write(reinterpret_cast<const char*>(&invalid), sizeof invalid);
    file.close();
    Env env = Env::configure().open(scratch.file());
    env.read([](Txn& txn) { CHECK_THROWS(checkIntegrity(txn), ErrorCode::Corrupted); });
}

int main()
{
    return tst::runAll("named");
}
