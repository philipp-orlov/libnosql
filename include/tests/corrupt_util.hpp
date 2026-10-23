// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Shared by tests/test_corrupt.cpp and fuzz/fuzz_pages.cpp: a store with every
// kind of page, ways to lie inside one page and re-seal it so its checksum
// still holds, and a workout that reads and writes such a store every way.

#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/format.hpp"
#include "tests/test_util.hpp"

namespace tst::corrupt {

inline constexpr std::size_t kPage = 4096;

/// Branch and leaf pages of several trees, byte-ordered, reversed, integer
/// and prefix-compressed, plus overflow runs.
/// Returns how many pages it uses; the file has room past them.
inline std::size_t buildStore(const std::filesystem::path& file)
{
    using namespace nosql;
    Env env = Env::configure().pageSize(kPage).sync(Durability::None).open(file);
    env.write([](Txn& txn) {
        Db main = txn.mainDb();
        for (std::uint64_t i = 0; i < 1500; ++i)
            main.put(keyOf(i), blob(40 + i % 60, i));
        for (std::uint64_t i = 0; i < 6; ++i)
            main.put(keyOf(i, "big"), blob(9000 + i * 100, i));
        Db reversed = txn.db("reversed", DbFlags::Create | DbFlags::ReverseKey);
        for (std::uint64_t i = 0; i < 400; ++i)
            reversed.put(keyOf(i, "r"), blob(30, i));
        Db integers = txn.db("integers", DbFlags::Create | DbFlags::IntegerKey);
        for (std::uint64_t i = 0; i < 400; ++i)
            integers.put(Slice::ref(i), blob(20, i));
        Db prefixed = txn.db("prefixed", DbFlags::Create | DbFlags::PrefixKeys);
        for (std::uint64_t i = 0; i < 1200; ++i)
            prefixed.put("tenant/0042/orders/" + keyOf(i, ""), blob(24, i));
    });
    return std::size_t(env.stats().usedPages);
}

inline std::string readAll(const std::filesystem::path& file)
{
    std::ifstream in(file, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

inline void writeAll(const std::filesystem::path& file, const std::string& bytes)
{
    std::ofstream out(file, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), std::streamsize(bytes.size()));
}

/// Re-seals page `page` of `bytes`: its checksum, over the run it now claims
/// when it says it is an overflow page and that run fits the file.
inline void reseal(std::string& bytes, std::size_t page)
{
    using namespace nosql::internal;
    auto* p = reinterpret_cast<Page*>(bytes.data() + page * kPage);
    std::size_t run = 1;
    if (isOverflow(p)) {
        const std::size_t claimed = ovPages(p);
        if (claimed && page + claimed <= bytes.size() / kPage)
            run = claimed;
    }
    sealPage(p, run * kPage);
}

/// One lie in page `page`, chosen by `rng`, then re-sealed.
template <class Rng>
void mutatePage(std::string& bytes, std::size_t page, Rng& rng)
{
    using namespace nosql::internal;
    std::byte* base = reinterpret_cast<std::byte*>(bytes.data() + page * kPage);
    auto* p = reinterpret_cast<Page*>(base);
    const auto any16 = [&]() -> std::uint16_t {
        switch (rng() % 4) {
            case 0: return std::uint16_t(rng());
            case 1: return std::uint16_t(kPage - rng() % 64);
            case 2: return std::uint16_t(rng() % 64);
            default: return std::uint16_t(0xffff - rng() % 4);
        }
    };
    const unsigned n = p->nkeys;
    const auto node = [&]() -> std::byte* {
        const std::uint16_t off = n ? std::uint16_t(slots(p)[rng() % n]) : 0;
        return off + 16 <= kPage ? base + off : nullptr;
    };
    for (unsigned lies = 1 + unsigned(rng() % 3); lies--;) {
        switch (rng() % 9) {
            case 0:  // any byte but the checksum
                base[rng() % (kPage - kPageChecksumBytes)] = std::byte(rng());
                break;
            case 1: p->nkeys = any16(); break;
            case 2: (rng() & 1 ? p->lower : p->upper) = any16(); break;
            case 3: {
                static const std::uint16_t kinds[] = {
                    P_LEAF, P_BRANCH, P_OVERFLOW, P_META, P_LEAF | P_PREFIX, P_BRANCH | P_PREFIX,
                    0xffff};
                p->flags = kinds[rng() % 7];
                break;
            }
            case 4:
                if (n)
                    slots(p)[rng() % n] = any16();
                break;
            case 5:  // a node's key or value length, either header layout
                if (std::byte* x = node()) {
                    const std::size_t at = rng() % 3 == 0 ? 6 : (rng() & 1 ? 2 : 4);
                    writeLittle(x + at, any16());
                }
                break;
            case 6:  // a branch child or overflow page number
                if (std::byte* x = node())
                    writeLittle(
                        x, std::uint32_t(rng() % 3 ? rng() % (bytes.size() / kPage + 4) : rng()));
                break;
            case 7:  // the prefix length of a P_PREFIX leaf
                writeLittle(base + kPage - kPageChecksumBytes - 2, any16());
                break;
            default:  // an overflow run's length
                writeLittle(base + 12, std::uint32_t(rng() % 3 ? rng() % 8 : rng()));
                break;
        }
    }
    reseal(bytes, page);
}

/// Applies a fuzzer input to `bytes`, a store using `pages` pages: its first
/// two bytes pick a page, then each (offset, byte) triple patches it. Re-seals.
inline void applyFuzzInput(std::string& bytes, std::size_t pages, const std::uint8_t* data,
                           std::size_t size)
{
    if (size < 2 || pages <= 2)
        return;
    const std::size_t page = 2 + (std::size_t(data[0]) | std::size_t(data[1]) << 8) % (pages - 2);
    char* at = bytes.data() + page * kPage;
    for (std::size_t i = 2; i + 3 <= size; i += 3) {
        const std::size_t offset =
            (std::size_t(data[i]) | std::size_t(data[i + 1]) << 8) % (kPage - 8);
        at[offset] = char(data[i + 2]);
    }
    reseal(bytes, page);
}

/// Reads `file` every way and writes to it. True if that all worked, false
/// on an Error; anything worse is what the caller is looking for.
inline bool exercise(const std::filesystem::path& file)
{
    using namespace nosql;
    constexpr int kSteps = 20000;  // a cycle of pages must not spin a walk forever
    try {
        {
            Env env = Env::configure().readOnly().open(file);
            env.read([&](Txn& txn) {
                std::vector<Db> dbs{txn.mainDb()};
                for (const std::string& name : txn.listDbs())
                    dbs.push_back(txn.db(name));
                for (Db& db : dbs) {
                    (void)db.stats();
                    (void)db.count();
                    int steps = 0;
                    for (auto [key, value] : db.all()) {
                        (void)key.string();
                        (void)value.string();
                        if (++steps > kSteps)
                            break;
                    }
                    Cursor c = db.cursor();
                    steps = 0;
                    for (bool ok = c.last(); ok && ++steps < kSteps; ok = c.prev())
                        (void)c.value().size();
                    const bool integer = db.flags() & DbFlags::IntegerKey;
                    for (std::uint64_t i = 0; i < 64; ++i) {
                        const std::uint64_t k = i * 37;
                        const std::string key = keyOf(k);
                        const Slice probe = integer ? Slice::ref(k) : Slice(key);
                        (void)db.get(probe);
                        Cursor s = db.cursor();
                        if (s.seek(probe))
                            (void)s.key().string();
                    }
                }
                checkIntegrity(txn);
            });
        }
        Env env = Env::configure().open(file);
        env.write([](Txn& txn) {
            std::vector<Db> dbs{txn.mainDb()};
            for (const std::string& name : txn.listDbs())
                dbs.push_back(txn.db(name));
            for (Db& db : dbs) {
                const bool integer = db.flags() & DbFlags::IntegerKey;
                for (std::uint64_t i = 0; i < 40; ++i) {
                    const std::uint64_t k = i * 31;
                    const std::string key = keyOf(k);
                    const Slice slice = integer ? Slice::ref(k) : Slice(key);
                    if (i % 3)
                        db.put(slice, blob(i % 5 ? 30 : 6000, i));
                    else
                        db.erase(slice);
                }
            }
        });
        env.read([](Txn& txn) { checkIntegrity(txn); });
        return true;
    } catch (const Error& e) {
        if (std::getenv("CORRUPT_DEBUG"))
            std::fprintf(stderr, "exercise: %s\n", e.what());
        return false;
    }
}

}  // namespace tst::corrupt
