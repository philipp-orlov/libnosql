// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <utility>
#include <vector>

#include "nosql/internal/format.hpp"
#include "nosql/internal/os.hpp"

namespace nosql::internal {

/// Recycles page buffers across transactions.
///
/// Copy-on-write means a busy writer allocates and frees one page-sized block
/// per page it touches. Handing that stream to the system allocator is the
/// textbook way to fragment a heap that has to stay healthy for months, so
/// page buffers never come from the heap at all: they are carved from slabs
/// the pool maps from the OS itself.
///
/// Buckets are powers of two in pages; a request is served from the bucket
/// that fits, so every buffer in a bucket is exactly the same size and slots
/// straight back in on release. A slab (1 MiB, or 64 pages if that is more)
/// holds buffers of one bucket only; a bucket larger than that gets a slab
/// of its own. Up to the configured budget, released buffers stay resident
/// on a free list that holds its links in the idle buffers themselves. Past
/// it, a buffer's memory goes back to the OS (its address stays with the
/// slab), and a slab with nothing left in use or kept is unmapped. Every
/// slab's bookkeeping lives in its own first pages, so recycling never
/// allocates from the heap either.
class BufferPool
{
public:
    static constexpr unsigned kClasses = std::numeric_limits<unsigned>::digits;

    BufferPool() = default;
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;
    ~BufferPool();

    void configure(std::size_t pageSize, std::size_t capBytes);

    Page* acquire(unsigned npages);
    void release(Page* buf, unsigned npages) noexcept;
    /// Hands back a transaction's whole buffer list under one lock.
    void release(const std::pair<Page*, unsigned>* items, std::size_t count) noexcept;
    /// Gives every kept buffer back to the OS.
    void purge() noexcept;

    std::size_t retainedBytes() const noexcept
    {
        std::lock_guard<std::mutex> lk(mtx_);
        return retained_;
    }

    std::size_t allocationSize(unsigned npages) const noexcept
    {
        const int bucket = classOf(npages);
        return (bucket < 0 ? npages : classPages(bucket)) * page_size_;
    }

private:
    struct Slab;

    static int classOf(unsigned npages) noexcept
    {
#if defined(__GNUC__) || defined(__clang__)
        const unsigned bucket = npages <= 1 ? 0 : kClasses - __builtin_clz(npages - 1);
#else
        unsigned bucket = 0;
        for (unsigned remaining = npages > 1 ? npages - 1 : 0; remaining; remaining >>= 1)
            ++bucket;
#endif
        return bucket < kClasses ? static_cast<int>(bucket) : -1;
    }
    static std::size_t classPages(int c) noexcept { return std::size_t(1) << c; }

    // Callers hold mtx_.
    Page* carve(int c);
    Slab* newSlab(int c);
    Slab* slabOf(const Page* buf) const noexcept;
    void giveBack(Page* buf, int c) noexcept;
    void toSlab(Page* buf, int c) noexcept;
    void settle() noexcept;
    void unlinkOpen(Slab* s) noexcept;

    mutable std::mutex mtx_;
    Page* free_[kClasses]{};    ///< kept buffers, linked through their first bytes
    Slab* open_[kClasses]{};    ///< slabs with a buffer to hand out, per bucket
    std::vector<Slab*> slabs_;  ///< every slab, by address
    Slab* touched_ = nullptr;   ///< slabs that took buffers back since the last settle()
    std::size_t page_size_ = kDefaultPageSize;
    std::size_t slab_bytes_ = 1u << 20;
    std::size_t system_page_ = 4096;
    std::size_t cap_ = 0;
    std::size_t retained_ = 0;
};

}  // namespace nosql::internal
