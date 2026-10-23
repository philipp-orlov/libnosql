// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// DbFlags::PrefixKeys: leaves that store their keys' shared prefix once.
// Everything a byte-ordered tree does, checked against a std::map, plus what
// compression is for: fewer pages for keys with long common prefixes.

#include <algorithm>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/format.hpp"
#include "nosql/internal/os.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {

using State = std::map<std::string, std::string>;

/// Keys in a few families with long shared prefixes, of mixed lengths, so
/// pages hold one family or straddle two.
std::string keyFor(std::uint64_t n)
{
    static const char* const families[] = {
        "tenant/0042/orders/2026-10-",
        "tenant/0042/orders/2026-11-",
        "tenant/0042/users/",
        "tenant/0917/sessions/very-long-session-namespace/",
        "z",
    };
    const std::string family = families[n % 5];
    return family + tst::keyOf(n / 5, "") + (n % 3 == 0 ? "/detail" : "");
}

State contentsOf(Db db)
{
    State state;
    for (auto [key, value] : db.all())
        state[key.string()] = value.string();
    return state;
}

/// Random puts, overwrites and erases with values up to a few pages, checked
/// against `expect` after every commit, inside and out.
void churn(Env& env, const std::string& name, DbFlags flags, State& expect, std::uint64_t seed,
           int commits)
{
    std::mt19937_64 rng(seed);
    for (int commit = 0; commit < commits; ++commit) {
        env.write([&](Txn& txn) {
            Db db = txn.db(name, flags | DbFlags::Create);
            const int ops = 50 + int(rng() % 400);
            for (int i = 0; i < ops; ++i) {
                const std::string key = keyFor(rng() % 6000);
                const unsigned what = unsigned(rng() % 10);
                if (what < 3) {
                    CHECK_EQ(db.erase(key), expect.erase(key) == 1);
                } else {
                    const std::size_t size = rng() % 40 == 0 ? 5000 + rng() % 9000 : rng() % 120;
                    const std::string value = tst::blob(size, rng());
                    db.put(key, value);
                    expect[key] = value;
                }
            }
            if (commit % 7 == 0)
                CHECK(contentsOf(db) == expect);
        });
        if (commit % 5 == 0)
            env.read([&](Txn& txn) {
                checkIntegrity(txn);
                CHECK(contentsOf(txn.db(name)) == expect);
            });
    }
}

std::uint32_t versionOf(const std::filesystem::path& file, std::size_t pageSize)
{
    const auto fd = internal::os::openFile(file, true, false, false, false);
    std::uint32_t version = 0;
    std::uint64_t newest = 0;
    for (unsigned slot = 0; slot < 2; ++slot) {
        internal::Meta meta{};
        internal::os::readAt(fd, slot * pageSize + internal::kPageHdr, &meta, sizeof meta);
        if (meta.txnid >= newest) {
            newest = meta.txnid;
            version = meta.version;
        }
    }
    internal::os::closeFile(fd);
    return version;
}

}  // namespace

/// The same work on a compressed and a plain sub-database: identical contents
/// at every step, and fewer leaves for the compressed one.
TEST(prefixKeysMatchAPlainTreeInFewerPages)
{
    tst::Scratch scratch("prefix-same");
    Env env = Env::configure().pageSize(4096).sync(Durability::None).open(scratch.file());
    State a, b;
    churn(env, "plain", DbFlags::None, a, 1, 60);
    churn(env, "prefixed", DbFlags::PrefixKeys, b, 1, 60);
    CHECK(a == b);
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        const TreeStats plain = txn.db("plain").stats();
        const TreeStats prefixed = txn.db("prefixed").stats();
        CHECK_EQ(plain.entries, prefixed.entries);
        if (!NOSQL_PREFIX_KEYS_DEFAULT)                            // else both compress
            CHECK(prefixed.leafPages * 10 < plain.leafPages * 9);  // at least a tenth fewer
        CHECK(txn.db("prefixed").flags() & DbFlags::PrefixKeys);
    });
}

/// Separators share prefixes too: keys behind a long common path fill a
/// branch page with what follows it, so the tree needs fewer branch pages and
/// fewer levels. Growing it, then erasing most of it at random, splits and
/// merges branch pages whose prefixes change, all checked against a map.
TEST(branchPagesStoreTheirSeparatorsPrefixOnce)
{
    tst::Scratch scratch("prefix-branch");
    Env env = Env::configure().pageSize(4096).sync(Durability::None).open(scratch.file());
    const std::string path =
        "region/eu-central/cluster/0007/namespace/payments/ledger/" + std::string(120, 'p') + "/";
    auto key = [&](std::uint64_t i) { return path + tst::keyOf(i % 7) + "/" + tst::keyOf(i); };
    State plain, prefixed;
    std::mt19937_64 rng(4);
    std::vector<std::uint64_t> ids(30000);
    for (std::uint64_t i = 0; i < ids.size(); ++i)
        ids[i] = i;
    std::shuffle(ids.begin(), ids.end(), rng);
    for (std::size_t at = 0; at < ids.size(); at += 3000)
        env.write([&](Txn& txn) {
            Db a = txn.db("plain", DbFlags::Create);
            Db b = txn.db("prefixed", DbFlags::Create | DbFlags::PrefixKeys);
            for (std::size_t i = at; i < at + 3000; ++i) {
                a.put(key(ids[i]), "v");
                b.put(key(ids[i]), "v");
                plain[key(ids[i])] = prefixed[key(ids[i])] = "v";
            }
        });
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        const TreeStats a = txn.db("plain").stats();
        const TreeStats b = txn.db("prefixed").stats();
        if (!NOSQL_PREFIX_KEYS_DEFAULT) {               // else both compress
            CHECK(b.branchPages * 20 < a.branchPages);  // 4 against 228: 23 with leaves alone
            CHECK(b.depth < a.depth);
        }
        CHECK(contentsOf(txn.db("prefixed")) == prefixed);
    });
    std::shuffle(ids.begin(), ids.end(), rng);
    for (std::size_t at = 0; at < 27000; at += 1500) {
        env.write([&](Txn& txn) {
            Db b = txn.db("prefixed");
            for (std::size_t i = at; i < at + 1500; ++i) {
                CHECK(b.erase(key(ids[i])));
                prefixed.erase(key(ids[i]));
                // Now and then a key outside the path: a separator without
                // its page's prefix.
                if (i % 97 == 0) {
                    const std::string other = "other/" + tst::keyOf(i);
                    b.put(other, "w");
                    prefixed[other] = "w";
                }
            }
        });
        env.read([&](Txn& txn) {
            checkIntegrity(txn);
            CHECK(contentsOf(txn.db("prefixed")) == prefixed);
            CHECK_EQ(txn.db("prefixed").count(), std::uint64_t(prefixed.size()));
        });
    }
}

/// Seeks, ranges, prefix scans, counts, ranks and backward walks see the keys
/// whole, wherever a page boundary and its prefix fall.
TEST(prefixKeysReadLikeAnyOther)
{
    tst::Scratch scratch("prefix-read");
    Env env = Env::configure().pageSize(4096).sync(Durability::None).open(scratch.file());
    State expect;
    churn(env, "p", DbFlags::PrefixKeys, expect, 2, 20);
    env.read([&](Txn& txn) {
        Db db = txn.db("p");
        std::mt19937_64 rng(3);
        for (int i = 0; i < 2000; ++i) {
            const std::string probe = keyFor(rng() % 7000) + (i % 2 ? "" : "~");
            // seek: the first key at or after the probe
            Cursor c = db.cursor();
            const auto it = expect.lower_bound(probe);
            Slice k, v;
            const bool found = c.seek(Slice(probe), k, v);
            CHECK_EQ(found, it != expect.end());
            if (found) {
                CHECK(k.string() == it->first);
                CHECK(v.string() == it->second);
                // and one step back
                const bool back = c.prev();
                CHECK_EQ(back, it != expect.begin());
                if (back)
                    CHECK(c.key().string() == std::prev(it)->first);
            }
            const auto exact = expect.find(probe);
            const auto got = db.get(probe);
            CHECK_EQ(got.has_value(), exact != expect.end());
            if (got)
                CHECK(got->string() == exact->second);
        }
        // A prefix scan over one family, and a count of it.
        const std::string family = "tenant/0042/orders/";
        std::vector<std::string> want;
        for (auto it = expect.lower_bound(family);
             it != expect.end() && it->first.rfind(family, 0) == 0; ++it)
            want.push_back(it->first);
        std::vector<std::string> got;
        for (auto [key, value] : db.prefix(family))
            got.push_back(key.string());
        CHECK(got == want);
        // Backward over everything.
        std::vector<std::string> backward;
        Cursor c = db.cursor();
        for (bool ok = c.last(); ok; ok = c.prev())
            backward.push_back(c.key().string());
        CHECK_EQ(backward.size(), expect.size());
        auto it = expect.rbegin();
        for (const std::string& key : backward)
            CHECK(key == (it++)->first);
    });
}

/// Nested transactions, aborts, cursors written through and a reopen.
TEST(prefixKeysUnderNestingAndReopen)
{
    tst::Scratch scratch("prefix-nested");
    State expect;
    {
        Env env = Env::configure().pageSize(4096).open(scratch.file());
        churn(env, "p", DbFlags::PrefixKeys, expect, 4, 10);
        env.write([&](Txn& txn) {
            State inner = expect;
            txn.nested([&](Txn& child) {
                Db db = child.db("p");
                for (int i = 0; i < 3000; ++i) {
                    const std::string key = keyFor(std::uint64_t(i) * 7);
                    db.put(key, "child");
                    inner[key] = "child";
                }
            });
            expect = inner;
            CHECK_THROWS(txn.nested([&](Txn& child) {
                Db db = child.db("p");
                for (int i = 0; i < 3000; ++i)
                    db.erase(keyFor(std::uint64_t(i) * 3));
                throw Error(ErrorCode::InvalidArgument, "abort");
            }),
                         ErrorCode::InvalidArgument);
            // Rewrite every value through a cursor walking the tree.
            Db db = txn.db("p");
            Cursor c = db.cursor();
            for (bool ok = c.first(); ok; ok = c.next()) {
                const std::string key = c.key().string();
                const std::string value = "walked:" + key;
                c.put(key, value);
                expect[key] = value;
            }
            CHECK(contentsOf(db) == expect);
            checkIntegrity(txn);
        });
    }
    Env env = Env::configure().readOnly().open(scratch.file());
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        CHECK(contentsOf(txn.db("p")) == expect);
    });
}

/// Compaction keeps the leaves compressed, packed by their compressed size.
TEST(compactionKeepsPrefixKeys)
{
    tst::Scratch scratch("prefix-compact");
    State expect;
    std::uint64_t leaves = 0;
    {
        Env env = Env::configure().pageSize(4096).sync(Durability::None).open(scratch.file());
        churn(env, "p", DbFlags::PrefixKeys, expect, 5, 30);
        env.read([&](Txn& txn) { leaves = txn.db("p").stats().leafPages; });
    }
    const auto copy = scratch.file("compact.db");
    compact(scratch.file(), copy);
    Env env = Env::configure().readOnly().open(copy);
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        Db db = txn.db("p");
        CHECK(db.flags() & DbFlags::PrefixKeys);
        CHECK(contentsOf(db) == expect);
        CHECK(db.stats().leafPages <= leaves);
    });
    CHECK_EQ(versionOf(copy, 4096), internal::kFormatVersion);
}

/// A store stays at the format older builds read until it holds a PrefixKeys
/// sub-database; then it is raised, for good.
TEST(theFormatVersionRisesOnlyWithPrefixKeys)
{
    if (NOSQL_PREFIX_KEYS_DEFAULT)  // every store starts compressed
        return;
    tst::Scratch scratch("prefix-version");
    const auto file = scratch.file();
    {
        Env env = Env::configure().pageSize(4096).open(file);
        env.write([](Txn& txn) { txn.db("plain", DbFlags::Create).put("k", "v"); });
    }
    CHECK_EQ(versionOf(file, 4096), internal::kOldestFormatVersion);
    {
        Env env = Env::configure().pageSize(4096).open(file);
        // Created and dropped by a nested transaction that aborts: no raise.
        env.write([](Txn& txn) {
            CHECK_THROWS(txn.nested([](Txn& child) {
                child.db("p", DbFlags::Create | DbFlags::PrefixKeys).put("k", "v");
                throw Error(ErrorCode::InvalidArgument, "abort");
            }),
                         ErrorCode::InvalidArgument);
        });
    }
    CHECK_EQ(versionOf(file, 4096), internal::kOldestFormatVersion);
    {
        Env env = Env::configure().pageSize(4096).open(file);
        env.write(
            [](Txn& txn) { txn.db("p", DbFlags::Create | DbFlags::PrefixKeys).put("k", "v"); });
        env.write([](Txn& txn) { txn.dropDb("p"); });
    }
    CHECK_EQ(versionOf(file, 4096), internal::kFormatVersion);
    Env env = Env::configure().open(file);
    env.read([](Txn& txn) { checkIntegrity(txn); });
}

/// Only byte order compresses, and the flag belongs to the sub-database.
TEST(prefixKeysNeedsByteOrderAndAMatchingTree)
{
    tst::Scratch scratch("prefix-flags");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) {
        CHECK_THROWS(txn.db("r", DbFlags::Create | DbFlags::PrefixKeys | DbFlags::ReverseKey),
                     ErrorCode::InvalidArgument);
        CHECK_THROWS(txn.db("i", DbFlags::Create | DbFlags::PrefixKeys | DbFlags::IntegerKey),
                     ErrorCode::InvalidArgument);
        txn.db("plain", DbFlags::Create).put("k", "v");
        if (!NOSQL_PREFIX_KEYS_DEFAULT)  // else it compresses already
            CHECK_THROWS(txn.db("plain", DbFlags::PrefixKeys), ErrorCode::Incompatible);
        txn.db("p", DbFlags::Create | DbFlags::PrefixKeys).put("k", "v");
        CHECK(txn.db("p").get("k").has_value());
    });
}

int main()
{
    return tst::runAll("prefix");
}
