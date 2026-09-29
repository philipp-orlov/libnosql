// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <array>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>

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
#ifdef NOSQL_TEST_WRAP_ALLOCATIONS
thread_local void* lastBacking = nullptr;
#endif

class AllocationProbe
{
public:
    explicit AllocationProbe(bool failHeap = false, bool failBacking = false)
    {
        counts = {};
        rejectHeap = failHeap;
        rejectBacking = failBacking;
    }
    ~AllocationProbe()
    {
        rejectHeap = false;
        rejectBacking = false;
    }
};

}  // namespace

void* operator new(std::size_t size)
{
    ++counts.heap;
    if (rejectHeap)
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

int main()
{
    return tst::runAll("memory");
}