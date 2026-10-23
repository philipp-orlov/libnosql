// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Branch separators written at leaf splits are the shortest prefix of the right
// leaf's first key that still sorts after the left leaf's last key. These tests
// drive keys that stress that choice (long shared prefixes, keys that are
// prefixes of one another, bytes at the edges of the range) and check every
// lookup, seek and scan against std::map, plus checkIntegrity.
#include <cstdint>
#include <iterator>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {

using Model = std::map<std::string, std::string>;

/// Checks the tree against the model: every key by lookup, every key by seek
/// (including a seek to a key between two entries), and a full forward scan.
void checkAgainstModel(Db& d, const Model& model, std::mt19937_64& rng)
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
        // Probe with a key drawn from the model, then one just past it, which may
        // fall between two entries.
        std::string probe = std::next(model.begin(), std::ptrdiff_t(rng() % model.size()))->first;
        if (i % 2)
            probe.push_back(char('a' + rng() % 3));
        const auto lb = model.lower_bound(probe);
        const bool found = c.seek(probe, k, v);
        CHECK_EQ(found, lb != model.end());
        if (found && lb != model.end())
            CHECK(k.string() == lb->first);
    }
}

std::string randomKey(std::mt19937_64& rng, unsigned maxLen, const std::string& alphabet,
                      const std::string& prefix)
{
    std::string k = prefix;
    const unsigned len = 1 + unsigned(rng() % maxLen);
    for (unsigned i = 0; i < len; ++i)
        k.push_back(alphabet[rng() % alphabet.size()]);
    return k;
}

/// Insert, delete half, insert again, checking the tree after each stage.
void exerciseTree(const std::string& prefix, const std::string& alphabet, unsigned maxLen,
                  std::uint64_t seed, unsigned count)
{
    tst::Scratch s("separators");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    std::mt19937_64 rng(seed);
    Model model;
    for (unsigned i = 0; i < count; ++i)
        model[randomKey(rng, maxLen, alphabet, prefix)] = std::to_string(i);

    e.write([&](Txn& t) {
        Db d = t.mainDb();
        for (const auto& [k, v] : model)
            d.put(k, v);
    });
    e.read([&](Txn& t) {
        Db d = t.mainDb();
        checkAgainstModel(d, model, rng);
        checkIntegrity(t);
    });

    std::vector<std::string> doomed;
    for (const auto& [k, v] : model)
        if (rng() % 2)
            doomed.push_back(k);
    e.write([&](Txn& t) {
        Db d = t.mainDb();
        for (const auto& k : doomed) {
            d.erase(k);
            model.erase(k);
        }
    });
    e.read([&](Txn& t) {
        Db d = t.mainDb();
        checkAgainstModel(d, model, rng);
        checkIntegrity(t);
    });

    e.write([&](Txn& t) {
        Db d = t.mainDb();
        for (unsigned i = 0; i < count; ++i) {
            const std::string k = randomKey(rng, maxLen, alphabet, prefix);
            d.put(k, "again");
            model[k] = "again";
        }
    });
    e.read([&](Txn& t) {
        Db d = t.mainDb();
        checkAgainstModel(d, model, rng);
        checkIntegrity(t);
    });
}

}  // namespace

TEST(separatorsWithLongSharedPrefixes)
{
    // Keys differ only in their last few bytes: the separator is the whole
    // distinguishing tail, not the shared prefix.
    exerciseTree(std::string(120, 'p'), "0123456789abcdef", 12, 11, 20000);
}

TEST(separatorsWhenKeysArePrefixesOfEachOther)
{
    // Over a two-letter alphabet most keys are prefixes of many others, which
    // makes the left key a prefix of the right one at the split.
    exerciseTree("", "ab", 14, 22, 20000);
}

TEST(separatorsWithBytesAtTheEdgesOfTheRange)
{
    // 0x00 and 0xff bytes, and keys that are all 0xff, sit at both ends of the
    // byte order, where a short separator is easiest to get wrong.
    std::string alphabet;
    alphabet.push_back('\0');
    alphabet.push_back('\x01');
    alphabet.push_back(char(0xfe));
    alphabet.push_back(char(0xff));
    exerciseTree(std::string(3, char(0xff)), alphabet, 10, 33, 15000);
}

TEST(separatorsForReverseAndIntegerKeysStayCorrect)
{
    // Only byte-wise order may shorten a separator; the others keep whole keys.
    // Both must still be correct under the same stress.
    tst::Scratch s("separators");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    std::mt19937_64 rng(44);
    Model model;
    e.write([&](Txn& t) {
        Db rev = t.db("rev", DbFlags::Create | DbFlags::ReverseKey);
        for (unsigned i = 0; i < 15000; ++i) {
            const std::string k = randomKey(rng, 12, "ab", std::string(40, 'x'));
            rev.put(k, std::to_string(i));
            model[k] = std::to_string(i);
        }
        Db ints = t.db("ints", DbFlags::Create | DbFlags::IntegerKey);
        for (std::uint64_t i = 0; i < 15000; ++i)
            ints.put(Slice::ref(i * 7919u), Slice::ref(i));
    });
    e.read([&](Txn& t) {
        Db rev = t.db("rev");
        CHECK_EQ(rev.count(), std::uint64_t(model.size()));
        for (const auto& [k, v] : model)
            CHECK(rev.get(k).has_value());
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
    // Deletes leave pages underfull, and rebalancing shifts single entries
    // between siblings, writing a new separator each time. Waves of random and
    // contiguous deletes drive many shifts in both directions.
    tst::Scratch s("separators");
    Env e = Env::configure().sync(Durability::None).open(s.file());
    std::mt19937_64 rng(55);
    Model model;
    const std::string prefix(60, 'r');
    for (unsigned i = 0; i < 30000; ++i)
        model[randomKey(rng, 10, "0123456789", prefix)] = std::to_string(i);
    e.write([&](Txn& t) {
        Db d = t.mainDb();
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
            Db d = t.mainDb();
            for (const auto& k : doomed) {
                d.erase(k);
                model.erase(k);
            }
        });
        e.read([&](Txn& t) {
            Db d = t.mainDb();
            checkAgainstModel(d, model, rng);
            checkIntegrity(t);
        });
    }
}

TEST(separatorsInCompactedStores)
{
    // compact() rebuilds every tree through the bulk builder, which shortens
    // separators itself: byte-ordered trees only, the others keep whole keys.
    tst::Scratch s("separators");
    std::mt19937_64 rng(66);
    Model main, named, reversed;
    {
        Env e = Env::configure().sync(Durability::None).open(s.file());
        e.write([&](Txn& t) {
            Db d = t.mainDb();
            Db n = t.db("names", DbFlags::Create);
            Db r = t.db("rev", DbFlags::Create | DbFlags::ReverseKey);
            for (unsigned i = 0; i < 20000; ++i) {
                const std::string a = randomKey(rng, 12, "0123456789abcdef", std::string(80, 'm'));
                const std::string b = randomKey(rng, 14, "ab", "");
                const std::string c = randomKey(rng, 12, "ab", std::string(30, 'z'));
                d.put(a, "m");
                main[a] = "m";
                n.put(b, "n");
                named[b] = "n";
                r.put(c, "r");
                reversed[c] = "r";
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
        CHECK_EQ(r.count(), std::uint64_t(reversed.size()));
        for (const auto& [k, v] : reversed)
            CHECK(r.get(k).has_value());
    });
}

int main()
{
    return tst::runAll("separators");
}
