// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <array>
#include <atomic>
#include <thread>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <utility>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/arena.hpp"
#include "nosql/internal/buffer_pool.hpp"
#include "nosql/internal/core.hpp"
#include "tests/test_util.hpp"

#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
#include <sys/mman.h>
#endif

namespace {

struct AllocationCounts
{
    std::size_t heap = 0;
    std::size_t backing = 0;
    std::size_t mapped = 0;
    std::size_t releases = 0;
    std::size_t trackedReleases = 0;
};

thread_local AllocationCounts counts;
thread_local bool rejectHeap = false;
thread_local bool rejectBacking = false;
thread_local std::size_t rejectHeapAt = 0;
#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
thread_local void* lastBacking = nullptr;
#endif

class AllocationProbe
{
public:
    explicit AllocationProbe(bool failHeap = false, bool failBacking = false,
                             std::size_t failHeapAt = 0)
    {
        counts = {};
        rejectHeap = failHeap;
        rejectBacking = failBacking;
        rejectHeapAt = failHeapAt;
    }
    ~AllocationProbe()
    {
        rejectHeap = false;
        rejectBacking = false;
        rejectHeapAt = 0;
    }
};

}  // namespace

void* operator new(std::size_t size)
{
    ++counts.heap;
    if (rejectHeap || counts.heap == rejectHeapAt)
        throw std::bad_alloc();
    if (void* allocation = std::malloc(size ? size : 1))
        return allocation;
    throw std::bad_alloc();
}

void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* allocation) noexcept { std::free(allocation); }
void operator delete[](void* allocation) noexcept { std::free(allocation); }
void operator delete(void* allocation, std::size_t) noexcept { std::free(allocation); }
void operator delete[](void* allocation, std::size_t) noexcept { std::free(allocation); }

#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
extern "C" void* __real_aligned_alloc(std::size_t, std::size_t);
extern "C" void __real_free(void*);
extern "C" void* __real_mmap(void*, std::size_t, int, int, int, off_t);
extern "C" int __real_munmap(void*, std::size_t);

extern "C" void* __wrap_aligned_alloc(std::size_t alignment, std::size_t size)
{
    ++counts.backing;
    if (rejectBacking)
        return nullptr;
    lastBacking = __real_aligned_alloc(alignment, size);
    return lastBacking;
}

extern "C" void __wrap_free(void* allocation)
{
    if (allocation)
        ++counts.releases;
    if (allocation && allocation == lastBacking)
        ++counts.trackedReleases;
    __real_free(allocation);
}

extern "C" void* __wrap_mmap(void* address, std::size_t size, int protection, int flags,
                            int descriptor, off_t offset)
{
    ++counts.backing;
    ++counts.mapped;
    if (rejectBacking)
        return MAP_FAILED;
    lastBacking = __real_mmap(address, size, protection, flags, descriptor, offset);
    return lastBacking;
}

extern "C" int __wrap_munmap(void* address, std::size_t size)
{
    ++counts.releases;
    if (address == lastBacking)
        ++counts.trackedReleases;
    return __real_munmap(address, size);
}
#endif

using namespace nosql;

TEST(pageAllocationsAreAlignedAndReleaseTheirBacking)
{
    for (const std::size_t bytes : {512u, 4096u, 65535u, 65536u, 65537u, 1048577u}) {
        AllocationCounts measured;
        bool aligned;
        {
            AllocationProbe probe;
            void* buffer = internal::os::allocPage(bytes);
            aligned = reinterpret_cast<std::uintptr_t>(buffer) % 64 == 0;
            std::memset(buffer, 0xa5, bytes);
            internal::os::freePage(buffer);
            measured = counts;
        }
        CHECK(aligned);
#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
        CHECK_EQ(measured.backing, 1u);
        CHECK_EQ(measured.mapped, bytes >= 65536 ? 1u : 0u);
        CHECK_EQ(measured.trackedReleases, 1u);
#endif
    }
    CHECK_THROWS(internal::os::allocPage(std::numeric_limits<std::size_t>::max()),
                 ErrorCode::OutOfMemory);
    internal::os::freePage(nullptr);
}

TEST(firstReleaseAndWarmReuseNeverAllocate)
{
    internal::BufferPool buffers;
    buffers.configure(4096, 8u << 20);
    constexpr std::array<unsigned, 8> pages{1, 2, 3, 8, 15, 17, 63, 129};
    std::array<internal::Page*, pages.size()> allocations{};
    for (std::size_t index = 0; index < pages.size(); ++index)
        allocations[index] = buffers.acquire(pages[index]);
    AllocationCounts measured;
    {
        AllocationProbe probe(true, true);
        for (std::size_t index = 0; index < pages.size(); ++index)
            buffers.release(allocations[index], pages[index]);
        for (unsigned round = 0; round < 10000; ++round)
            for (const auto count : pages)
                buffers.release(buffers.acquire(count), count);
        measured = counts;
    }
    CHECK_EQ(measured.heap, 0u);
    CHECK_EQ(measured.backing, 0u);
    CHECK_EQ(measured.releases, 0u);
    CHECK(buffers.retainedBytes() > 0);
}

TEST(disabledBufferCacheReturnsBackingStorageImmediately)
{
    internal::BufferPool buffers;
    buffers.configure(4096, 0);
    AllocationCounts measured;
    {
        AllocationProbe probe;
        for (unsigned pages : {1u, 17u})
            buffers.release(buffers.acquire(pages), pages);
        measured = counts;
    }
    CHECK_EQ(buffers.retainedBytes(), 0u);
    CHECK_EQ(measured.heap, 0u);
#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
    CHECK_EQ(measured.backing, 2u);
    CHECK_EQ(measured.releases, 2u);
#endif
}

TEST(concurrentReleaseNeverHandsOutABufferBeingDiscarded)
{
    // BUG-M4: settling runs madvise/munmap without the mutex. Buffers being discarded are claimed first, so another
    // thread can never receive one mid-discard (its contents would vanish under it). Each thread writes a tag over
    // its buffer and checks it is intact before releasing; a small budget sends most releases to settle().
    internal::BufferPool buffers;
    buffers.configure(4096, 16 * 4096);
    std::atomic<int> bad{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t)
        threads.emplace_back([&, t] {
            std::uint32_t seed = 1234u + std::uint32_t(t);
            internal::Page* held[8] = {};
            unsigned heldPages[8] = {};
            for (int i = 0; i < 20000; ++i) {
                seed = seed * 1664525u + 1013904223u;
                const unsigned k = (seed >> 8) & 7u;
                if (held[k]) {
                    const auto* bytes = reinterpret_cast<const unsigned char*>(held[k]);
                    const std::size_t n = std::size_t(heldPages[k]) * 4096;
                    for (std::size_t off = 0; off < n; off += 512)
                        if (bytes[off] != (unsigned char)(t + 1))
                            ++bad;
                    buffers.release(held[k], heldPages[k]);
                    held[k] = nullptr;
                } else {
                    heldPages[k] = 1u + ((seed >> 16) % 6u);
                    held[k] = buffers.acquire(heldPages[k]);
                    auto* bytes = reinterpret_cast<unsigned char*>(held[k]);
                    for (std::size_t off = 0; off < std::size_t(heldPages[k]) * 4096; off += 512)
                        bytes[off] = (unsigned char)(t + 1);
                }
            }
            for (unsigned k = 0; k < 8; ++k)
                if (held[k])
                    buffers.release(held[k], heldPages[k]);
        });
    for (auto& th : threads)
        th.join();
    CHECK_EQ(bad.load(), 0);
    buffers.purge();
    CHECK_EQ(buffers.retainedBytes(), 0u);
}

TEST(pageBuffersComeFromSlabsAndEmptySlabsAreUnmapped)
{
    // 600 single pages need three 1 MiB slabs at 4 KiB pages. With a budget
    // of 64 pages, the rest goes back to the OS and slabs left with nothing
    // kept are unmapped.
    internal::BufferPool buffers;
    buffers.configure(4096, 64 * 4096);
    std::vector<internal::Page*> pages(600);
    AllocationCounts measured;
    {
        AllocationProbe probe;
        for (auto& page : pages) {
            page = buffers.acquire(1);
            CHECK(reinterpret_cast<std::uintptr_t>(page) % 4096 == 0);
            std::memset(page, 0x5a, 4096);
        }
        std::vector<std::pair<internal::Page*, unsigned>> owned;
        owned.reserve(pages.size());
        for (auto* page : pages)
            owned.emplace_back(page, 1u);
        const auto heapBefore = counts.heap;
        buffers.release(owned.data(), owned.size());
        CHECK_EQ(counts.heap, heapBefore);
        measured = counts;
    }
    CHECK_EQ(buffers.retainedBytes(), 64u * 4096);
#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
    CHECK_EQ(measured.mapped, 3u);
    // The kept pages are the first ones handed back, so all of them sit in
    // the first slab and the other two are unmapped.
    CHECK_EQ(measured.releases, 2u);
#endif
    {
        AllocationProbe probe;
        buffers.purge();
        measured = counts;
    }
    CHECK_EQ(buffers.retainedBytes(), 0u);
#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
    CHECK_EQ(measured.releases, 1u);
#endif
}

TEST(arenaScopesReuseTheirBackingStorage)
{
    internal::Arena arena;
    const auto round = [&] {
        internal::Arena::Scope outer(arena);
        arena.alloc(60000);
        internal::Arena::Scope inner(arena);
        arena.alloc(20000);
    };
    round();
    AllocationCounts measured;
    {
        AllocationProbe probe(true, true);
        for (unsigned iteration = 0; iteration < 10000; ++iteration)
            round();
        measured = counts;
    }
    CHECK_EQ(measured.heap, 0u);
    CHECK_EQ(measured.backing, 0u);
    CHECK_EQ(measured.releases, 0u);
}

TEST(arenaReservesBookkeepingBeforeAllocatingChunks)
{
    internal::Arena arena;
    bool failed = false;
    AllocationCounts measured;
    {
        AllocationProbe probe(true);
        try {
            arena.alloc(100);
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        measured = counts;
    }
    CHECK(failed);
    CHECK_EQ(measured.backing, 0u);
    CHECK_EQ(arena.retainedBytes(), 0u);
    CHECK(arena.alloc(100) != nullptr);
}

#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
TEST(arenaRetainsOldChunkWhenReplacementFails)
{
    internal::Arena arena;
    {
        internal::Arena::Scope outer(arena);
        arena.alloc(60000);
        arena.alloc(20000);
    }
    const auto retained = arena.retainedBytes();
    internal::Arena::Scope outer(arena);
    arena.alloc(60000);
    AllocationCounts measured;
    bool failed = false;
    {
        AllocationProbe probe(false, true);
        try {
            arena.alloc(128u << 10);
        } catch (const Error& error) {
            failed = error.code() == ErrorCode::OutOfMemory;
            measured = counts;
        }
    }
    CHECK(failed);
    CHECK_EQ(measured.trackedReleases, 0u);
    CHECK_EQ(arena.retainedBytes(), retained);
    CHECK(arena.alloc(20000) != nullptr);
}
#endif

TEST(warmOverflowTransactionsReusePageAllocations)
{
    tst::Scratch scratch("memory");
    auto env = Env::configure().sync(Durability::None).open(scratch.file());
    const std::string value(256u << 10, 'x');
    const auto round = [&] {
        env.write([&](Txn& txn) { txn.mainDb().put("key", value); });
        env.read([&](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").size(), value.size()); });
    };
    for (unsigned iteration = 0; iteration < 100; ++iteration)
        round();
    AllocationCounts measured;
    {
        AllocationProbe probe;
        for (unsigned iteration = 0; iteration < 1000; ++iteration)
            round();
        measured = counts;
    }
    std::printf("    1000 warm transactions: new=%zu backing=%zu frees=%zu\n",
                measured.heap, measured.backing, measured.releases);
    CHECK_EQ(measured.heap, 0u);
    CHECK_EQ(measured.backing, 0u);
    CHECK_EQ(measured.releases, 0u);
    env.read([](Txn& txn) { checkIntegrity(txn); });
}

TEST(warmBatchTransactionsReuseBoundedScratch)
{
    tst::Scratch scratch("batch-memory");
    auto env = Env::configure().sync(Durability::None).open(scratch.file());
    std::array<std::array<char, 16>, 8192> keys{};
    for (std::size_t index = 0; index < keys.size(); ++index)
        std::snprintf(keys[index].data(), keys[index].size(), "key:%08u", unsigned(index));
    const std::string value(1500, 'x');
    const auto round = [&] {
        env.write([&](Txn& txn) {
            auto database = txn.mainDb();
            for (const auto& key : keys)
                database.put(key.data(), value);
        });
    };
    for (unsigned iteration = 0; iteration < 12; ++iteration)
        round();
    AllocationCounts measured;
    {
        AllocationProbe probe;
        for (unsigned iteration = 0; iteration < 12; ++iteration)
            round();
        measured = counts;
    }
    std::printf("    12 warm 8192-record batches: new=%zu backing=%zu frees=%zu\n",
                measured.heap, measured.backing, measured.releases);
    CHECK_EQ(measured.heap, 0u);
    CHECK_EQ(measured.backing, 0u);
    CHECK_EQ(measured.releases, 0u);
    env.read([](Txn& txn) { checkIntegrity(txn); });
}

TEST(pageTableVisitsOnlyLiveEntriesWhateverItsCapacity)
{
    internal::PageTable table;
    std::vector<internal::Page> pages(5000);
    // Grow it the way a bulk load does, then work at the size a small commit does.
    for (std::size_t i = 0; i < pages.size(); ++i)
        table.set(internal::PageNo(i * 7 + 3), &pages[i]);
    CHECK_EQ(table.size(), pages.size());
    for (std::size_t i = 0; i < pages.size(); i += 2)
        CHECK(table.erase(internal::PageNo(i * 7 + 3)));
    CHECK(!table.erase(internal::PageNo(3)));
    CHECK_EQ(table.size(), pages.size() / 2);
    std::size_t visited = 0;
    table.forEach([&](internal::PageNo key, internal::Page* value) {
        ++visited;
        CHECK_EQ(std::size_t(key), std::size_t((value - pages.data()) * 7 + 3));
        CHECK((key - 3) / 7 % 2 == 1);
    });
    CHECK_EQ(visited, pages.size() / 2);
    for (std::size_t i = 1; i < pages.size(); i += 2)
        CHECK(table.find(internal::PageNo(i * 7 + 3)) == &pages[i]);

    const std::size_t capacity = table.capacity();
    table.clear();
    CHECK(table.empty());
    CHECK_EQ(table.capacity(), capacity);
    for (std::size_t i = 0; i < pages.size(); ++i)
        CHECK(table.find(internal::PageNo(i * 7 + 3)) == nullptr);

    // A handful of entries in the big table: visiting must find exactly them.
    table.set(11, &pages[0]);
    table.set(12, &pages[1]);
    table.set(4096, &pages[2]);
    CHECK(table.erase(12));
    visited = 0;
    table.forEach([&](internal::PageNo, internal::Page*) { ++visited; });
    CHECK_EQ(visited, 2u);
    CHECK(table.find(11) == &pages[0]);
    CHECK(table.find(4096) == &pages[2]);
    CHECK(table.find(12) == nullptr);
    table.release();
    CHECK_EQ(table.retainedBytes(), 0u);
}

TEST(transactionScratchRetentionIsByteBounded)
{
    internal::TxnImpl txn;
    txn.dirty.reserve(1u << 18);
    txn.owned.reserve(65536);
    txn.pending.reserve(131072);
    AllocationCounts measured;
    {
        AllocationProbe probe(true, true);
        txn.releaseResources();
        measured = counts;
    }
    CHECK_EQ(measured.heap, 0u);
    CHECK_EQ(measured.backing, 0u);
    CHECK_EQ(txn.dirty.retainedBytes(), 0u);
    const auto retained = txn.owned.capacity() * sizeof(decltype(txn.owned)::value_type) +
                          txn.pending.capacity() * sizeof(decltype(txn.pending)::value_type);
    CHECK(retained <= internal::TxnImpl::kRetainedScratchBytes);
}

TEST(pageTableGrowthFailurePreservesEntries)
{
    for (std::size_t failAt : {1u, 2u}) {
        internal::PageTable table;
        internal::Page page{};
        for (unsigned key = 0; key < 48; ++key)
            table.set(key, &page);
        const auto capacity = table.capacity();
        bool failed = false;
        {
            AllocationProbe probe(false, false, failAt);
            try {
                table.set(48, &page);
            } catch (const std::bad_alloc&) {
                failed = true;
            }
        }
        CHECK(failed);
        CHECK_EQ(table.capacity(), capacity);
        CHECK_EQ(table.size(), 48u);
        for (unsigned key = 0; key < 48; ++key)
            CHECK(table.find(key) == &page);
        CHECK(table.find(48) == nullptr);
        table.set(48, &page);
        CHECK_EQ(table.size(), 49u);
        table.clear();
        CHECK(table.empty());
    }
}

TEST(pageTableUpdatesAtLoadLimitDoNotAllocate)
{
    internal::PageTable table;
    internal::Page first{}, second{};
    for (unsigned key = 0; key < 48; ++key)
        table.set(key, &first);
    const auto retained = table.retainedBytes();
    AllocationCounts measured;
    {
        AllocationProbe probe(true, true);
        for (unsigned round = 0; round < 10000; ++round)
            for (unsigned key = 0; key < 48; ++key)
                table.set(key, &second);
        measured = counts;
    }
    CHECK_EQ(measured.heap, 0u);
    CHECK_EQ(table.retainedBytes(), retained);
    CHECK_EQ(table.size(), 48u);
    for (unsigned key = 0; key < 48; ++key)
        CHECK(table.find(key) == &second);
}

TEST(pageTableReserveAllocatesOnlyItsFinalBuffers)
{
    internal::PageTable table;
    internal::Page page{};
    table.set(7, &page);
    AllocationCounts measured;
    {
        AllocationProbe probe;
        table.reserve(65536);
        measured = counts;
    }
    CHECK_EQ(measured.heap, 2u);
    CHECK(table.find(7) == &page);
    {
        AllocationProbe probe(true, true);
        for (unsigned key = 0; key < 65536; ++key)
            table.set(key, &page);
    }
    CHECK_EQ(table.size(), 65536u);
    bool failed = false;
    try {
        table.reserve(std::numeric_limits<std::size_t>::max());
    } catch (const std::length_error&) {
        failed = true;
    }
    CHECK(failed);
    CHECK_EQ(table.size(), 65536u);
    CHECK(table.find(7) == &page);
}

int main()
{
    return tst::runAll("memory");
}