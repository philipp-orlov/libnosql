// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

//
// Steady-state behaviour. This library is expected to run unattended for a
// very long time, so "does not grow" is a correctness property, not a nicety:
// a store that leaks a few pages per commit is fine for an afternoon and
// useless after a month.
#include <cstring>
#include <random>
#include <thread>

#include "nosql/nosql.hpp"
#include "nosql/internal/buffer_pool.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {

/// Keys and values that allocate nothing, so the measurements below are about
/// the library rather than about the test.
struct FixedKey
{
    char b[20];
    explicit FixedKey(std::uint64_t i)
    {
        std::snprintf(b, sizeof b, "key:%012llu", (unsigned long long)i);
    }
    operator Slice() const { return Slice(b, 16); }
};

}  // namespace

TEST(rewritingTheSameKeysForeverDoesNotGrowTheStore)
{
    tst::Scratch s("steady");
    Env e =
        Env::configure().pageSize(4096).maxSize(1ull << 30).sync(Durability::None).open(s.file());
    constexpr int kKeys = 20000;
    char vbuf[100];
    std::memset(vbuf, 'x', sizeof vbuf);
    const Slice v(vbuf, sizeof vbuf);

    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < kKeys; ++i)
            d.put(FixedKey(i), v, PutMode::Append);
    });

    auto round = [&](int r) {
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 10; ++i)
                d.put(FixedKey((r * 10 + i) % kKeys), v);
        });
        e.read([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 10; ++i)
                (void)d.get(FixedKey(i));
        });
    };

    for (int r = 0; r < 500; ++r)
        round(r);
    const EnvStats warm = e.stats();
    for (int r = 500; r < 6000; ++r)
        round(r);
    const EnvStats late = e.stats();

    // Eleven thousand more commits must not cost more than a handful of pages.
    CHECK(late.usedPages <= warm.usedPages + 8);
    CHECK(late.fileSize == warm.fileSize);
    CHECK(late.freePages < 200);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(mixedInsertDeleteChurnReachesAPlateau)
{
    tst::Scratch s("steady");
    Env e =
        Env::configure().pageSize(1024).maxSize(1ull << 30).sync(Durability::None).open(s.file());
    std::mt19937_64 rng(20260831);

    auto churn = [&](int rounds) {
        for (int r = 0; r < rounds; ++r)
            e.write([&](Txn& t) {
                auto d = t.mainDb();
                for (int i = 0; i < 40; ++i) {
                    const std::uint64_t k = rng() % 8000;
                    if (rng() % 2)
                        d.put(FixedKey(k), tst::blob(30 + rng() % 200, k));
                    else
                        d.erase(FixedKey(k));
                }
            });
    };

    churn(400);
    const std::uint64_t warm = e.stats().usedPages;
    churn(1600);
    const std::uint64_t late = e.stats().usedPages;
    // The working set is bounded, so the file must be too.
    CHECK(late < warm * 2);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(largeValueRewritesReachAPlateau)
{
    tst::Scratch s("steady");
    Env e =
        Env::configure().pageSize(4096).maxSize(1ull << 30).sync(Durability::None).open(s.file());

    auto round = [&](int r) {
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 4; ++i)
                d.put(FixedKey(i), tst::blob(60000, r * 4 + i));
        });
    };
    for (int r = 0; r < 100; ++r)
        round(r);
    const std::uint64_t warm = e.stats().usedPages;
    for (int r = 100; r < 600; ++r)
        round(r);
    CHECK(e.stats().usedPages < warm * 2);
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(createAndDropSubDatabasesForever)
{
    tst::Scratch s("steady");
    Env e = Env::configure()
                .pageSize(1024)
                .maxSize(1ull << 30)
                .sync(Durability::None)
                .maxDbs(8)
                .open(s.file());

    auto cycle = [&](int r) {
        e.write([&](Txn& t) {
            auto d = t.db("scratch", DbFlags::Create);
            for (int i = 0; i < 500; ++i)
                d.put(FixedKey(i), tst::blob(80, r + i));
        });
        e.write([&](Txn& t) { t.dropDb("scratch"); });
    };
    for (int r = 0; r < 30; ++r)
        cycle(r);
    const std::uint64_t warm = e.stats().usedPages;
    for (int r = 30; r < 200; ++r)
        cycle(r);
    CHECK(e.stats().usedPages < warm * 2);
    e.read([&](Txn& t) {
        checkIntegrity(t);
        CHECK_EQ(t.listDbs().size(), std::size_t(0));
    });
}

TEST(recycledBuffersStayWithinTheirBudget)
{
    tst::Scratch s("steady");
    Env e = Env::configure()
                .pageSize(4096)
                .maxSize(1ull << 30)
                .bufferCache(1u << 20)  // deliberately small
                .sync(Durability::None)
                .open(s.file());
    e.write([&](Txn& t) {
        auto d = t.mainDb();
        for (int i = 0; i < 40000; ++i)
            d.put(FixedKey(i), tst::blob(60, i), PutMode::Append);
    });
    // The transaction dirtied far more than a megabyte of pages; the pool must
    // hand the excess back rather than grow past what it was told to keep.
    CHECK(e.stats().bufferBytes <= (1u << 20));
    for (int r = 0; r < 50; ++r)
        e.write([&](Txn& t) {
            auto d = t.mainDb();
            for (int i = 0; i < 10; ++i)
                d.put(FixedKey(r * 10 + i), tst::blob(60, r));
        });
    CHECK(e.stats().bufferBytes <= (1u << 20));
    e.read([&](Txn& t) { checkIntegrity(t); });
}

TEST(activeDirtyBudgetIsEnforcedAndReleased)
{
    tst::Scratch scratch("dirty-budget");
    Env env = Env::configure().dirtyLimit(32u << 10).open(scratch.file());
    CHECK_THROWS(env.write([](Txn& txn) { txn.mainDb().put("large", std::string(64u << 10, 'x')); }),
                 ErrorCode::OutOfMemory);
    CHECK_EQ(env.stats().dirtyBytes, 0u);
    env.write([](Txn& txn) { txn.mainDb().put("small", "value"); });
    CHECK_EQ(env.stats().dirtyBytes, 0u);
}

TEST(pageBufferRecyclingReusesEverySizeClass)
{
    internal::BufferPool buffers;
    constexpr std::size_t pageSize = 4096;
    buffers.configure(pageSize, 4u << 20);
    for (unsigned pages : {1u, 2u, 3u, 7u, 15u, 17u, 63u, 129u}) {
        const auto bytes = buffers.allocationSize(pages);
        auto* first = buffers.acquire(pages);
        auto* second = buffers.acquire(pages);
        for (unsigned round = 0; round < 1000; ++round) {
            std::memset(first, 0xa5, bytes);
            std::memset(second, 0x5a, bytes);
            buffers.release(first, pages);
            buffers.release(second, pages);
            CHECK_EQ(buffers.retainedBytes(), 2 * bytes);
            CHECK(buffers.acquire(pages) == second);
            CHECK(buffers.acquire(pages) == first);
            CHECK_EQ(buffers.retainedBytes(), 0u);
        }
        buffers.release(first, pages);
        buffers.release(second, pages);
        buffers.purge();
        CHECK_EQ(buffers.retainedBytes(), 0u);
    }
}

TEST(pageBufferRecyclingHonorsExactCacheBoundaries)
{
    internal::BufferPool buffers;
    constexpr std::size_t pageSize = 4096;
    buffers.configure(pageSize, 32 * pageSize);
    auto* cached = buffers.acquire(17);
    auto* excess = buffers.acquire(17);
    CHECK_EQ(buffers.allocationSize(17), 32 * pageSize);
    buffers.release(cached, 17);
    buffers.release(excess, 17);
    CHECK_EQ(buffers.retainedBytes(), 32 * pageSize);
    CHECK(buffers.acquire(31) == cached);
    buffers.release(cached, 31);
    buffers.purge();
    CHECK_EQ(buffers.allocationSize(33), 33 * pageSize);
    buffers.release(buffers.acquire(33), 33);
    CHECK_EQ(buffers.retainedBytes(), 0u);
}

TEST(pageBufferRecyclingSupportsConcurrentTeardown)
{
    internal::BufferPool buffers;
    constexpr std::size_t budget = 4u << 20;
    buffers.configure(4096, budget);
    std::atomic<bool> intact{true};
    std::vector<std::thread> workers;
    for (unsigned worker = 0; worker < 8; ++worker)
        workers.emplace_back([&, worker] {
            for (unsigned round = 0; round < 1000; ++round)
                for (unsigned pages : {1u, 3u, 17u, 65u}) {
                    auto* buffer = buffers.acquire(pages);
                    const auto bytes = buffers.allocationSize(pages);
                    auto* data = reinterpret_cast<unsigned char*>(buffer);
                    std::memset(data, worker, bytes);
                    std::this_thread::yield();
                    for (std::size_t offset = 0; offset < bytes; offset += 64)
                        if (data[offset] != worker)
                            intact.store(false);
                    buffers.release(buffer, pages);
                }
        });
    for (auto& worker : workers)
        worker.join();
    CHECK(intact.load());
    CHECK(buffers.retainedBytes() <= budget);
    buffers.purge();
    CHECK_EQ(buffers.retainedBytes(), 0u);
}

int main()
{
    return tst::runAll("steady");
}
