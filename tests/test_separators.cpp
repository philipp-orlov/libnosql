// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Branch separators written between two adjacent leaves are the shortest key
// that still sorts after the left leaf's last key: a prefix of the right
// leaf's first key in byte order, a suffix of it in reverse order. These tests
// drive keys that stress that choice (long shared prefixes or suffixes, keys
// that are prefixes or suffixes of one another, bytes at the edges of the
// range) through splits, rebalancing and compaction, and check every lookup,
// seek and scan against std::map, plus checkIntegrity.
#include <cstdint>
#include <iterator>
#include <map>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {

/// Back-to-front byte order, the order of a DbFlags::ReverseKey tree.
struct ReverseOrder
{
    bool operator()(const std::string& a, const std::string& b) const
    {
        for (std::size_t i = 1; i <= a.size() && i <= b.size(); ++i) {
            const auto x = static_cast<unsigned char>(a[a.size() - i]);
            const auto y = static_cast<unsigned char>(b[b.size() - i]);
            if (x != y)
                return x < y;
        }
        return a.size() < b.size();
    }
};

using Model = std::map<std::string, std::string>;
using ReverseModel = std::map<std::string, std::string, ReverseOrder>;

/// The order a model stands for, and the tree that holds it.
template <class M>
constexpr DbFlags orderOf()
{
    return std::is_same_v<M, ReverseModel> ? DbFlags::ReverseKey : DbFlags::None;
}

template <class M>
Db treeOf(Txn& t)
{
    return t.db("tree", DbFlags::Create | orderOf<M>());
}

/// Checks the tree against the model: every key by lookup, every key by seek
/// (including a seek to a key between two entries), and a full forward scan.
template <class M>
void checkAgainstModel(Db& d, const M& model, std::mt19937_64& rng)
{
    CHECK_EQ(d.count(), std::uint64_t(model.size()));
    for (const auto& [k, v] : model) {
        const auto got = d.get(k);
        CHECK(got.has_value());
        if (got)
            CHECK(got->string() == v);
    }
    Cursor c = d.cursor();
    Slice k, v;
    auto it = model.begin();
    bool ok = c.first(k, v);
    for (; it != model.end(); ++it) {
        CHECK(ok);
        if (!ok)
            return;
        CHECK(k.string() == it->first);
        ok = c.next(k, v);
    }
    CHECK(!ok);

    for (int i = 0; i < 200 && !model.empty(); ++i) {
        // Probe with a key drawn from the model, then one changed at the end
        // that sorts first, which may fall between two entries.
        std::string probe = std::next(model.begin(), std::ptrdiff_t(rng() % model.size()))->first;
        if (i % 2) {
            if (orderOf<M>() == DbFlags::ReverseKey)
                probe.insert(probe.begin(), char('a' + rng() % 3));
            else
                probe.push_back(char('a' + rng() % 3));
        }
        const auto lb = model.lower_bound(probe);
        const bool found = c.seek(probe, k, v);
        CHECK_EQ(found, lb != model.end());
        if (found && lb != model.end())
            CHECK(k.string() == lb->first);
    }
}

std::string randomKey(std::mt19937_64& rng, unsigned maxLen, const std::string& alphabet,
                      const std::string& prefix, const std::string& suffix = std::string())
{
    std::string k = prefix;
    const unsigned len = 1 + unsigned(rng() % maxLen);
    for (unsigned i = 0; i < len; ++i)
        k.push_back(alphabet[rng() % alphabet.size()]);
    return k + suffix;
}

/// Insert, delete half, insert again, checking the tree after each stage.
template <class M>
void exerciseTree(const std::string& prefix, const std::string& suffix, const std::string& alphabet,
                  unsigned maxLen, std::uint64_t seed, unsigned count)
{
    tst::Scratch s("separators");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    std::mt19937_64 rng(seed);
    M model;
    for (unsigned i = 0; i < count; ++i)
        model[randomKey(rng, maxLen, alphabet, prefix, suffix)] = std::to_string(i);
    const auto check = [&] {
        e.read([&](Txn& t) {
            Db d = treeOf<M>(t);
            checkAgainstModel(d, model, rng);
            checkIntegrity(t);
        });
    };

    e.write([&](Txn& t) {
        Db d = treeOf<M>(t);
        for (const auto& [k, v] : model)
            d.put(k, v);
    });
    check();

    std::vector<std::string> doomed;
    for (const auto& [k, v] : model)
        if (rng() % 2)
            doomed.push_back(k);
    e.write([&](Txn& t) {
        Db d = treeOf<M>(t);
        for (const auto& k : doomed) {
            d.erase(k);
            model.erase(k);
        }
    });
    check();

    e.write([&](Txn& t) {
        Db d = treeOf<M>(t);
        for (unsigned i = 0; i < count; ++i) {
            const std::string k = randomKey(rng, maxLen, alphabet, prefix, suffix);
            d.put(k, "again");
            model[k] = "again";
        }
    });
    check();
}

/// Deletes leave pages underfull, and rebalancing shifts single entries between
/// siblings, writing a new separator each time. Waves of random and contiguous
/// deletes drive many shifts in both directions.
template <class M>
void deleteInWaves(const std::string& prefix, const std::string& suffix, std::uint64_t seed)
{
    tst::Scratch s("separators");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    std::mt19937_64 rng(seed);
    M model;
    for (unsigned i = 0; i < 30000; ++i)
        model[randomKey(rng, 10, "0123456789", prefix, suffix)] = std::to_string(i);
    e.write([&](Txn& t) {
        Db d = treeOf<M>(t);
        for (const auto& [k, v] : model)
            d.put(k, v);
    });
    for (int wave = 0; wave < 6; ++wave) {
        std::vector<std::string> doomed;
        if (wave % 2 == 0) {
            for (const auto& [k, v] : model)
                if (rng() % 3 == 0)
                    doomed.push_back(k);
        } else {
            // A contiguous run from a random start, so neighbours empty together.
            auto it = std::next(model.begin(), std::ptrdiff_t(rng() % model.size()));
            for (std::size_t n = model.size() / 4; n && it != model.end(); --n, ++it)
                doomed.push_back(it->first);
        }
        e.write([&](Txn& t) {
            Db d = treeOf<M>(t);
            for (const auto& k : doomed) {
                d.erase(k);
                model.erase(k);
            }
        });
        e.read([&](Txn& t) {
            Db d = treeOf<M>(t);
            checkAgainstModel(d, model, rng);
            checkIntegrity(t);
        });
    }
}

std::string edgeBytes()
{
    std::string alphabet;
    alphabet.push_back('\0');
    alphabet.push_back('\x01');
    alphabet.push_back(char(0xfe));
    alphabet.push_back(char(0xff));
    return alphabet;
}

}  // namespace

TEST(separatorsWithLongSharedPrefixes)
{
    // Keys differ only in their last few bytes: the separator is the whole
    // distinguishing tail, not the shared prefix.
    exerciseTree<Model>(std::string(120, 'p'), "", "0123456789abcdef", 12, 11, 20000);
}

TEST(separatorsWhenKeysArePrefixesOfEachOther)
{
    // Over a two-letter alphabet most keys are prefixes of many others, which
    // makes the left key a prefix of the right one at the split.
    exerciseTree<Model>("", "", "ab", 14, 22, 20000);
}

TEST(separatorsWithBytesAtTheEdgesOfTheRange)
{
    // 0x00 and 0xff bytes, and keys that are all 0xff, sit at both ends of the
    // byte order, where a short separator is easiest to get wrong.
    exerciseTree<Model>(std::string(3, char(0xff)), "", edgeBytes(), 10, 33, 15000);
}

TEST(reverseSeparatorsWithLongSharedSuffixes)
{
    // Host names under one domain: in reverse order the shared part comes
    // first, and the separator is a suffix reaching just into what differs.
    exerciseTree<ReverseModel>("", ".svc.cluster.internal.example.org", "0123456789abcdef", 12, 77,
                               20000);
}

TEST(reverseSeparatorsWhenKeysAreSuffixesOfEachOther)
{
    exerciseTree<ReverseModel>("", "", "ab", 14, 88, 20000);
}

TEST(reverseSeparatorsWithBytesAtTheEdgesOfTheRange)
{
    exerciseTree<ReverseModel>("", std::string(3, char(0xff)), edgeBytes(), 10, 99, 15000);
}

TEST(separatorsForIntegerKeysStayWhole)
{
    // Integer order never shortens a separator: a shorter key has a different
    // width. It must still be correct under the same stress.
    tst::Scratch s("separators");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    e.write([&](Txn& t) {
        Db ints = t.db("ints", DbFlags::Create | DbFlags::IntegerKey);
        for (std::uint64_t i = 0; i < 15000; ++i)
            ints.put(Slice::ref(i * 7919u), Slice::ref(i));
    });
    e.read([&](Txn& t) {
        checkIntegrity(t);
        Db ints = t.db("ints");
        CHECK_EQ(ints.count(), std::uint64_t(15000));
        for (std::uint64_t i = 0; i < 15000; ++i) {
            const auto got = ints.get(Slice::ref(i * 7919u));
            CHECK(got.has_value());
        }
    });
}

TEST(separatorsSurviveDeleteHeavyRebalancing)
{
    deleteInWaves<Model>(std::string(60, 'r'), "", 55);
}

TEST(reverseSeparatorsSurviveDeleteHeavyRebalancing)
{
    deleteInWaves<ReverseModel>("", std::string(60, 'r'), 56);
}

TEST(separatorsInCompactedStores)
{
    // compact() rebuilds every tree through the bulk builder, which shortens
    // separators itself: by prefix in byte order, by suffix in reverse order.
    tst::Scratch s("separators");
    std::mt19937_64 rng(66);
    Model main, named;
    ReverseModel reversed, domains;
    {
        Env e = Env::configure().sync(Durability::None).open(s.file());
        e.write([&](Txn& t) {
            Db d = t.mainDb();
            Db n = t.db("names", DbFlags::Create);
            Db r = t.db("rev", DbFlags::Create | DbFlags::ReverseKey);
            Db h = t.db("hosts", DbFlags::Create | DbFlags::ReverseKey);
            for (unsigned i = 0; i < 20000; ++i) {
                const std::string a = randomKey(rng, 12, "0123456789abcdef", std::string(80, 'm'));
                const std::string b = randomKey(rng, 14, "ab", "");
                const std::string c = randomKey(rng, 12, "ab", std::string(30, 'z'));
                const std::string d2 = randomKey(rng, 12, "0123456789abcdef", "", ".example.org");
                d.put(a, "m");
                main[a] = "m";
                n.put(b, "n");
                named[b] = "n";
                r.put(c, "r");
                reversed[c] = "r";
                h.put(d2, "h");
                domains[d2] = "h";
            }
        });
        e.close();
    }
    compact(s.file(), s.file("compacted.db"));
    Env e = Env::configure().open(s.file("compacted.db"));
    e.read([&](Txn& t) {
        checkIntegrity(t);
        Db d = t.mainDb();
        checkAgainstModel(d, main, rng);
        Db n = t.db("names");
        checkAgainstModel(n, named, rng);
        Db r = t.db("rev");
        checkAgainstModel(r, reversed, rng);
        Db h = t.db("hosts");
        checkAgainstModel(h, domains, rng);
    });
}

int main()
{
    return tst::runAll("separators");
}
