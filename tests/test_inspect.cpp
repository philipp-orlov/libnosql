// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <cstdio>
#include <map>
#include <set>

#include "nosql/inspect.hpp"
#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
namespace ins = nosql::inspect;

namespace {

/// A store with a main tree, two sub-databases (one with large values), deletes (so the free list is not empty).
void fill(Env& e)
{
    e.write([](Txn& t) {
        auto main = t.mainDb();
        for (int i = 0; i < 400; ++i)
            main.put(tst::keyOf(i), tst::blob(60, i));
        auto big = t.db("big", DbFlags::Create);
        for (int i = 0; i < 12; ++i)
            big.put(tst::keyOf(i), tst::blob(5000 + i * 700, i));
        auto prefixed = t.db("p", DbFlags::Create | DbFlags::PrefixKeys);
        for (int i = 0; i < 300; ++i)
            prefixed.put(std::string("customer/region-west/") + tst::keyOf(i), "v" + std::to_string(i));
    });
    e.write([](Txn& t) {
        auto main = t.mainDb();
        for (int i = 0; i < 400; i += 2)
            main.erase(tst::keyOf(i));
    });
    e.write([](Txn& t) { t.mainDb().put("tail", "x"); });
}

}  // namespace

TEST(treesAndMetaAgreeWithTheStats)
{
    tst::Scratch s("inspect");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    fill(e);
    Txn t = e.readTxn();
    const auto trees = ins::trees(t);
    CHECK(trees.size() >= 5);
    CHECK(trees[0].role == ins::TreeRole::Free);
    CHECK(trees[1].role == ins::TreeRole::Main);
    CHECK(trees[2].role == ins::TreeRole::Catalog);
    std::set<std::string> names;
    for (const auto& tr : trees)
        if (tr.role == ins::TreeRole::Named)
            names.insert(tr.name);
    CHECK(names == (std::set<std::string>{"big", "p"}));
    CHECK_EQ(trees[1].entries, std::uint64_t(201));
    CHECK(trees[1].depth >= 2);

    const auto metas = ins::metas(t);
    CHECK_EQ(metas.size(), std::size_t(2));
    CHECK(metas[0].valid && metas[1].valid);
    CHECK(metas[0].current != metas[1].current);
    const auto& now = metas[0].current ? metas[0] : metas[1];
    CHECK_EQ(now.txnid, t.id());
    CHECK_EQ(now.pageSize, std::uint32_t(1024));
    CHECK_EQ(now.checksum, now.computedChecksum);
    CHECK_EQ(now.storeId.size(), std::size_t(32));
    CHECK(now.storeId != now.commitId);
}

TEST(everyPageIsAccountedForInTheMap)
{
    tst::Scratch s("inspect");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    fill(e);
    Txn t = e.readTxn();
    const auto trees = ins::trees(t);
    const auto runs = ins::map(t, trees);
    std::uint64_t next = 0, total[7] = {};
    std::map<int, std::uint64_t> branch, leaf, overflow;
    for (const auto& r : runs) {
        CHECK_EQ(r.first, next);  // consecutive, nothing missing
        next += r.count;
        total[int(r.kind)] += r.count;
        if (r.kind == ins::PageKind::Branch)
            branch[r.tree] += r.count;
        if (r.kind == ins::PageKind::Leaf)
            leaf[r.tree] += r.count;
        if (r.kind == ins::PageKind::Overflow || r.kind == ins::PageKind::Continued)
            overflow[r.tree] += r.count;
    }
    const auto stats = e.stats();
    CHECK_EQ(next, stats.usedPages);
    CHECK_EQ(total[int(ins::PageKind::Meta)], std::uint64_t(2));
    CHECK_EQ(total[int(ins::PageKind::Unknown)], std::uint64_t(0));
    CHECK_EQ(total[int(ins::PageKind::Free)], stats.freePages);
    for (std::size_t i = 0; i < trees.size(); ++i) {
        CHECK_EQ(branch[int(i)], trees[i].branchPages);
        CHECK_EQ(leaf[int(i)], trees[i].leafPages);
        CHECK_EQ(overflow[int(i)], trees[i].overflowPages);
    }
}

TEST(aPageHasItsHeaderHashAndNodes)
{
    tst::Scratch s("inspect");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    fill(e);
    Txn t = e.readTxn();
    const auto trees = ins::trees(t);
    const ins::TreeInfo& main = trees[1];

    const ins::PageInfo root = ins::page(t, main.root);
    CHECK(root.kind == ins::PageKind::Branch);
    CHECK(root.hashOk && root.headerOk);
    CHECK_EQ(root.entries, main.entries);
    CHECK_EQ(root.storedHash, root.computedHash);
    CHECK_EQ(root.lower, std::uint16_t(16 + 2 * root.nkeys));
    CHECK_EQ(root.freeBytes, std::uint32_t(root.upper - root.lower));

    const auto kids = ins::nodes(t, main.root);
    CHECK_EQ(kids.size(), std::size_t(root.nkeys));
    CHECK(kids[0].key.empty());  // the -infinity child
    std::uint64_t counted = 0;
    for (const auto& n : kids) {
        CHECK(n.child >= 2);
        counted += n.count;
        const ins::PageInfo child = ins::page(t, n.child);
        CHECK(child.hashOk);
        CHECK_EQ(child.entries, n.count);
    }
    CHECK_EQ(counted, main.entries);

    // descend to a leaf and read its items
    std::uint64_t pg = main.root;
    while (ins::page(t, pg).kind == ins::PageKind::Branch)
        pg = ins::nodes(t, pg).front().child;
    const auto items = ins::nodes(t, pg);
    CHECK(!items.empty());
    CHECK_EQ(items[0].key, tst::keyOf(1));  // key 0 was erased
    CHECK_EQ(items[0].valueBytes, std::uint32_t(60));
    CHECK_EQ(items[0].value, tst::blob(60, 1));
    CHECK(!items[0].big);
    CHECK(items[0].offset >= ins::page(t, pg).upper);

    // raw bytes carry the page number and end with the hash
    const std::string raw = ins::pageBytes(t, pg);
    CHECK_EQ(raw.size(), std::size_t(1024));
    std::uint64_t stored = 0;
    std::memcpy(&stored, raw.data() + 1016, 8);
    CHECK_EQ(stored, ins::page(t, pg).storedHash);
}

TEST(largeValuesAndSharedPrefixesAreVisible)
{
    tst::Scratch s("inspect");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    fill(e);
    Txn t = e.readTxn();
    const auto trees = ins::trees(t);
    const auto find = [&](const char* name) -> const ins::TreeInfo& {
        for (const auto& tr : trees)
            if (tr.name == name)
                return tr;
        throw std::runtime_error("no tree");
    };
    // big: every value is on an overflow run
    std::uint64_t pg = find("big").root;
    while (ins::page(t, pg).kind == ins::PageKind::Branch)
        pg = ins::nodes(t, pg).front().child;
    const auto items = ins::nodes(t, pg, 16);
    CHECK(items[0].big);
    CHECK_EQ(items[0].valueBytes, std::uint32_t(5000));
    CHECK_EQ(items[0].value, tst::blob(5000, 0).substr(0, 16));  // read through the run
    const ins::PageInfo run = ins::page(t, items[0].overflow);
    CHECK(run.kind == ins::PageKind::Overflow);
    CHECK(run.runPages >= 5);
    CHECK(run.hashOk && run.headerOk);

    // p: pages store the prefix once; the whole key comes back
    pg = find("p").root;
    while (ins::page(t, pg).kind == ins::PageKind::Branch)
        pg = ins::nodes(t, pg).front().child;
    const ins::PageInfo leaf = ins::page(t, pg);
    CHECK(leaf.prefixKeys);
    CHECK(leaf.sharedBytes >= 20);
    const auto kv = ins::nodes(t, pg);
    CHECK(kv[0].key.rfind("customer/region-west/", 0) == 0);
}

TEST(theFreeListIsListed)
{
    tst::Scratch s("inspect");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    fill(e);
    Txn t = e.readTxn();
    const auto free = ins::freeList(t, 100000);
    std::uint64_t pages = 0;
    for (const auto& f : free)
        pages += f.pages;
    CHECK_EQ(pages, e.stats().freePages);
    for (std::size_t i = 1; i < free.size(); ++i)
        CHECK(free[i - 1].txn >= free[i].txn);  // newest first
}

TEST(checkFindsADamagedPage)
{
    tst::Scratch s("inspect");
    std::uint64_t victim = 0;
    {
        Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
        fill(e);
        Txn t = e.readTxn();
        CHECK(ins::check(t).ok());
        CHECK(ins::check(t).pagesChecked > 20);
        std::uint64_t pg = ins::trees(t)[1].root;
        while (ins::page(t, pg).kind == ins::PageKind::Branch)
            pg = ins::nodes(t, pg).back().child;
        victim = pg;
    }
    {
        std::FILE* f = std::fopen(s.file().c_str(), "r+b");
        CHECK(f != nullptr);
        std::fseek(f, long(victim * 1024 + 700), SEEK_SET);  // inside the page, below its hash
        const unsigned char junk[4] = {0xde, 0xad, 0xbe, 0xef};
        std::fwrite(junk, 1, 4, f);
        std::fclose(f);
    }
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    Txn t = e.readTxn();
    const ins::PageInfo info = ins::page(t, victim);
    CHECK(!info.hashOk);
    CHECK(info.storedHash != info.computedHash);
    const ins::CheckResult r = ins::check(t);
    CHECK(!r.ok());
    CHECK(r.badCount >= 1);
    CHECK(std::find(r.badPages.begin(), r.badPages.end(), victim) != r.badPages.end());
    CHECK(!r.integrity.empty());
}

TEST(badRequestsAreErrors)
{
    tst::Scratch s("inspect");
    Env e = Env::configure().pageSize(1024).maxSize(1ull << 28).open(s.file());
    fill(e);
    Txn t = e.readTxn();
    CHECK_THROWS(ins::page(t, 1u << 30), ErrorCode::InvalidArgument);
    CHECK_THROWS(ins::nodes(t, 0), ErrorCode::InvalidArgument);
    CHECK_THROWS(ins::pageBytes(t, 2, 0), ErrorCode::InvalidArgument);
}

int main()
{
    return tst::runAll("inspect");
}
