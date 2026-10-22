// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The allocation policy, enforced: after warm-up a transaction allocates
// nothing from the heap, a churning workload keeps the heap flat, and a
// growing store keeps every slice it handed out valid.

#include <malloc.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

namespace {

std::atomic<std::size_t> heapCalls{0};

/// The process heap's free and used bytes.
struct Heap
{
    std::size_t used = 0;
    std::size_t free = 0;
};

Heap heapNow()
{
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
    const struct mallinfo2 m = ::mallinfo2();
    return {m.uordblks, m.fordblks};
#else
    const struct mallinfo m = ::mallinfo();
    return {std::size_t(m.uordblks), std::size_t(m.fordblks)};
#endif
}

}  // namespace

void* operator new(std::size_t size)
{
    heapCalls.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(size ? size : 1))
        return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size)
{
    return ::operator new(size);
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept
{
    heapCalls.fetch_add(1, std::memory_order_relaxed);
    return std::malloc(size ? size : 1);
}
void operator delete(void* p) noexcept
{
    std::free(p);
}
void operator delete[](void* p) noexcept
{
    std::free(p);
}
void operator delete(void* p, std::size_t) noexcept
{
    std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept
{
    std::free(p);
}

#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
// The library's only direct use of the C allocator (arena chunks).
extern "C" void* __real_aligned_alloc(std::size_t, std::size_t);
extern "C" void* __wrap_aligned_alloc(std::size_t alignment, std::size_t size)
{
    heapCalls.fetch_add(1, std::memory_order_relaxed);
    return __real_aligned_alloc(alignment, size);
}
#endif

using namespace nosql;

namespace {

void bigEndian(std::byte* out, std::uint64_t v)
{
    for (int i = 7; i >= 0; --i, v >>= 8)
        out[i] = std::byte(v & 0xff);
}

struct Key
{
    std::byte b[16];
    Key(std::uint64_t id, std::uint64_t seq)
    {
        bigEndian(b, id);
        bigEndian(b + 8, seq);
    }
    operator Slice() const { return Slice(b, 16); }
};

// Longer than the small-string buffer, as a real sub-database name may be.
const char* const kBlobs = "metrics_blob_rows";
const char* const kIndex = "metrics_by_key";

/// The probe's workload: per write, ten 512-byte rows into one sub-database
/// and ten index entries into another, over a fixed set of keys; per read, a
/// get and a 100-step scan.
struct Workload
{
    Env& env;
    std::vector<std::byte> value = std::vector<std::byte>(512, std::byte{0x5a});
    std::uint64_t seq = 0;

    void write()
    {
        Txn t = env.writeTxn();
        Db blobs = t.db(kBlobs);
        Db index = t.db(kIndex);
        // A fixed key set, so the store stops growing once it is warm.
        for (int i = 0; i < 10; ++i, ++seq) {
            const std::uint64_t id = seq % 4000;
            blobs.put(Key(id % 97, id), Slice(value.data(), value.size()));
            index.put(Key(id % 97, ~id), Slice::ref(seq));
        }
        t.commit();
    }

    void read()
    {
        Txn t = env.readTxn();
        Db blobs = t.db(kBlobs);
        CHECK(blobs.get(Key(5, 5)).has_value());
        Cursor c = blobs.cursor();
        Slice k, v;
        int n = 0;
        for (bool ok = c.seek(Key(7, 0), k, v); ok && n < 100; ok = c.next(k, v))
            ++n;
        CHECK_EQ(n, 100);
        t.abort();
    }
};

void checkWarmTransactions(Durability mode, bool cachedChecksums)
{
    tst::Scratch scratch("alloc-budget");
    Env env = Env::configure()
                  .pageSize(4096)
                  .sync(mode)
                  .cacheReadChecksums(cachedChecksums)
                  .open(scratch.file());
    env.write([](Txn& t) {
        t.db(kBlobs, DbFlags::Create);
        t.db(kIndex, DbFlags::Create);
    });
    Workload work{env};
    // Safe needs longer to warm: the free list's commit scratch reaches its
    // high-water mark late in this workload, and it does so later once separator
    // keys are shorter. The high-water mark is reached once, after which the
    // measured window allocates nothing.
    const int warm = mode == Durability::Safe ? 2000 : 1000;
    const int measured = mode == Durability::Safe ? 50 : 1000;
    for (int i = 0; i < warm; ++i)
        work.write();
    for (int i = 0; i < 100; ++i)
        work.read();
    const std::size_t before = heapCalls.load();
    for (int i = 0; i < measured; ++i)
        work.write();
    const std::size_t afterWrites = heapCalls.load();
    for (int i = 0; i < measured; ++i)
        work.read();
    const std::size_t afterReads = heapCalls.load();
    std::printf("    mode %d, cached %d: %zu allocations in %d writes, %zu in %d reads\n",
                int(mode), int(cachedChecksums), afterWrites - before, measured,
                afterReads - afterWrites, measured);
    CHECK_EQ(afterWrites - before, 0u);
    CHECK_EQ(afterReads - afterWrites, 0u);
    env.read([](Txn& t) { checkIntegrity(t); });
}

}  // namespace

TEST(warmTransactionsDoNotAllocate)
{
    checkWarmTransactions(Durability::None, false);
    checkWarmTransactions(Durability::None, true);
    checkWarmTransactions(Durability::Lazy, true);
    checkWarmTransactions(Durability::Safe, false);
}

/// The cluster-store pattern: a large store, and every round rewrites a fifth
/// of it in one transaction. After warm-up a round leaves the heap no bigger
/// and no more fragmented than it found it; no page buffer touches it.
TEST(churnRoundsKeepTheHeapFlat)
{
    tst::Scratch scratch("alloc-churn");
    Env env =
        Env::configure().pageSize(4096).sync(Durability::None).dirtyLimit(0).open(scratch.file());
    const std::uint64_t groups = 1000, groupSize = 50;
    std::vector<std::byte> member(200, std::byte{1});
    env.write([&](Txn& t) {
        Db d = t.db(kBlobs, DbFlags::Create);
        for (std::uint64_t g = 0; g < groups; ++g)
            for (std::uint64_t m = 0; m < groupSize; ++m)
                d.put(Key(g, m), Slice(member.data(), member.size()));
    });
    const auto round = [&](int r) {
        env.write([&](Txn& t) {
            Db d = t.db(kBlobs);
            for (std::uint64_t g = std::uint64_t(r) % 5; g < groups; g += 5) {
                for (std::uint64_t m = 0; m < groupSize; ++m)
                    d.erase(Key(g, m));
                member[0] = std::byte(r);
                for (std::uint64_t m = 0; m < groupSize; ++m)
                    d.put(Key(g, m), Slice(member.data(), member.size()));
            }
        });
    };
    for (int r = 0; r < 20; ++r)
        round(r);
    const Heap warm = heapNow();
    const std::size_t before = heapCalls.load();
    for (int r = 20; r < 40; ++r)
        round(r);
    const std::size_t calls = heapCalls.load() - before;
    const Heap late = heapNow();
    std::printf("    20 churn rounds: %zu allocations; heap used %zu -> %zu, free %zu -> %zu\n",
                calls, warm.used, late.used, warm.free, late.free);
    // A round dirties more than a transaction keeps of its scratch, so it may
    // take some back from the heap; what it must not do is leave the heap any
    // bigger, or with more free space stranded in it, than it found it.
    CHECK(late.used + late.free <= warm.used + warm.free);
    CHECK(late.free <= warm.free + (64u << 10));
    env.read([](Txn& t) { checkIntegrity(t); });
}

/// A writer that grows the store keeps the slices it holds, while a reader
/// on another thread begins and ends; the mapping does not move.
TEST(heldSliceSurvivesGrowthWhileAReaderComesAndGoes)
{
    tst::Scratch scratch("alloc-growth");
    Env env = Env::configure()
                  .pageSize(4096)
                  .initialSize(16 * 4096)
                  .growthStep(4096)
                  .sync(Durability::Lazy)
                  .open(scratch.file());
    const std::string value(1000, 'h');
    env.write([&](Txn& t) { t.mainDb().put("anchor", value); });
    const std::uint64_t mapBefore = env.stats().mapSize;
    Txn w = env.writeTxn();
    Db d = w.mainDb();
    const Slice held = d.at("anchor");
    for (int i = 0; i < 400; ++i)
        d.put(tst::keyOf(i), value);
    std::thread([&] {
        Env other = env.share();
        other.read([&](Txn& r) { CHECK_EQ(r.mainDb().at("anchor").string(), value); });
    }).join();
    CHECK(env.stats().mapSize > mapBefore);
    CHECK_EQ(held.string(), value);
    w.commit();
    env.read([](Txn& t) { checkIntegrity(t); });
}

int main()
{
    return tst::runAll("alloc_budget");
}
