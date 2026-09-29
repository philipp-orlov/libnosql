// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstring>
#include <limits>
#include <mutex>

#include "nosql/internal/format.hpp"
#include "nosql/internal/os.hpp"

namespace nosql::internal {

/// Recycles page buffers across transactions.
///
/// Copy-on-write means a busy writer allocates and frees one page-sized block
/// per page it touches. Handing that stream to the system allocator is the
/// textbook way to fragment a heap that has to stay healthy for months, so
/// the buffers are bucketed by run length and reused instead.
///
/// Buckets are powers of two in pages; a request is served
/// from the bucket that fits, so every buffer in a bucket is exactly the same
/// size and slots straight back in on release. Idle buffers hold their own
/// free-list links, so recycling never allocates bookkeeping. Large runs are
/// rounded only when their size class fits the configured cache budget.
class BufferPool
{
public:
    static constexpr unsigned kClasses = std::numeric_limits<unsigned>::digits;

    BufferPool() = default;
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;
    ~BufferPool() { purge(); }

    void configure(std::size_t pageSize, std::size_t capBytes) noexcept
    {
        page_size_ = pageSize;
        cap_ = capBytes;
    }

    Page* acquire(unsigned npages)
    {
        const int c = classOf(npages);
        if (c >= 0) {
            std::lock_guard<std::mutex> lk(mtx_);
            auto& bucket = free_[c];
            if (bucket) {
                Page* p = bucket;
                Page* next;
                std::memcpy(&next, p, sizeof next);
                bucket = next;
                retained_ -= classPages(c) * page_size_;
                return p;
            }
        }
        const std::size_t pages = c >= 0 ? classPages(c) : npages;
        return static_cast<Page*>(os::allocPage(pages * page_size_));
    }

    void release(Page* buf, unsigned npages) noexcept
    {
        const int c = classOf(npages);
        if (c >= 0) {
            const std::size_t bytes = classPages(c) * page_size_;
            std::lock_guard<std::mutex> lk(mtx_);
            if (bytes <= cap_ - retained_) {
                std::memcpy(buf, &free_[c], sizeof free_[c]);
                free_[c] = buf;
                retained_ += bytes;
                return;
            }
        }
        os::freePage(buf);
    }

    void purge() noexcept
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto& bucket : free_) {
            while (bucket) {
                Page* p = bucket;
                Page* next;
                std::memcpy(&next, p, sizeof next);
                bucket = next;
                os::freePage(p);
            }
        }
        retained_ = 0;
    }

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
    int classOf(unsigned npages) const noexcept
    {
    #if defined(__GNUC__) || defined(__clang__)
        const unsigned bucket = npages <= 1 ? 0 : kClasses - __builtin_clz(npages - 1);
    #else
        unsigned bucket = 0;
        for (unsigned remaining = npages > 1 ? npages - 1 : 0; remaining; remaining >>= 1)
            ++bucket;
    #endif
        if (bucket >= kClasses ||
            (npages > 16 && classPages(bucket) > cap_ / page_size_))
            return -1;
        return static_cast<int>(bucket);
    }
    static std::size_t classPages(int c) noexcept { return std::size_t(1) << c; }

    mutable std::mutex mtx_;
    Page* free_[kClasses]{};
    std::size_t page_size_ = kDefaultPageSize;
    std::size_t cap_ = 0;
    std::size_t retained_ = 0;
};

}  // namespace nosql::internal
