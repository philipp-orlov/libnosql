// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "nosql/internal/format.hpp"

namespace nosql::internal {

/// Remembers which committed pages have had their checksum verified.
///
/// Copy-on-write means a committed page is immutable until its page number is
/// recycled by a later commit, so a successful check stays true until the
/// writer overwrites that page -- and the writer knows exactly which pages it
/// writes. One bit per page number, set by readers after a check and cleared
/// by the writer before it publishes the meta page that makes the new bytes
/// visible, therefore caches verification across transactions and threads
/// without ever trusting a page this process has not checked since it was
/// last written.
///
/// What the cache cannot see is the page changing underneath the process:
/// bit rot on the device, or a fault in memory the OS has the file cached
/// in. Every bit is dropped periodically so such damage is caught on the next
/// access after the interval instead of never. The reset interval is the
/// knob between "verify every access" (zero) and "verify once" (no reset).
///
/// Readers and the writer touch the bitmap concurrently with relaxed atomics.
/// That is sufficient because a bit only ever claims something about bytes no
/// live snapshot could still be reading when the writer replaces them, and the
/// writer's clears are ordered before the meta publication by the environment
/// mutex every transaction takes to read the meta.
class ValidationCache
{
public:
    static constexpr std::size_t kMaxBytes = 4u << 20;  ///< 32 M page numbers at most

    ValidationCache() = default;
    ValidationCache(const ValidationCache&) = delete;
    ValidationCache& operator=(const ValidationCache&) = delete;

    /// Sizes the bitmap for the pages `maxFileBytes` can hold, capped at
    /// kMaxBytes; page numbers beyond the cap are simply never cached.
    void configure(std::size_t pageSize, std::uint64_t maxFileBytes, std::chrono::nanoseconds resetEvery)
    {
        const std::uint64_t pages = maxFileBytes / pageSize + 1;
        std::uint64_t words = (pages + 63) / 64;
        words = std::min<std::uint64_t>(words, kMaxBytes / sizeof(std::uint64_t));
        words_.reset(new std::atomic<std::uint64_t>[std::size_t(words)]());
        wordCount_ = std::size_t(words);
        resetEveryNs_ = resetEvery.count();
        lastResetNs_.store(nowNs(), std::memory_order_relaxed);
    }

    bool enabled() const noexcept { return wordCount_ != 0; }
    std::size_t bytes() const noexcept { return wordCount_ * sizeof(std::uint64_t); }

    bool isValidated(PageNo p) const noexcept
    {
        const std::size_t w = std::size_t(p >> 6);
        if (w >= wordCount_)
            return false;
        return (words_[w].load(std::memory_order_relaxed) >> (p & 63)) & 1u;
    }

    void markValidated(PageNo p) noexcept
    {
        const std::size_t w = std::size_t(p >> 6);
        if (w < wordCount_)
            words_[w].fetch_or(std::uint64_t(1) << (p & 63), std::memory_order_relaxed);
    }

    /// The writer is about to make `count` pages from `first` hold new bytes.
    void invalidate(PageNo first, std::uint64_t count) noexcept
    {
        if (!count)
            return;
        const PageNo last = first + count - 1;
        std::size_t w = std::size_t(first >> 6);
        const std::size_t wLast = std::size_t(last >> 6);
        if (w >= wordCount_)
            return;
        for (; w <= wLast && w < wordCount_; ++w) {
            const PageNo lo = PageNo(w) << 6;
            const PageNo hi = lo + 63;
            if (first <= lo && last >= hi) {
                words_[w].store(0, std::memory_order_relaxed);
                continue;
            }
            std::uint64_t mask = ~std::uint64_t(0);
            if (first > lo)
                mask &= ~std::uint64_t(0) << (first - lo);
            if (last < hi)
                mask &= ~std::uint64_t(0) >> (hi - last);
            words_[w].fetch_and(~mask, std::memory_order_relaxed);
        }
    }

    /// Drops every bit once the reset interval has elapsed. Cheap enough to
    /// call on every transaction start: one clock read in the common case.
    void maybeReset() noexcept
    {
        if (!wordCount_ || resetEveryNs_ <= 0)
            return;
        const std::int64_t now = nowNs();
        std::int64_t last = lastResetNs_.load(std::memory_order_relaxed);
        if (now - last < resetEveryNs_)
            return;
        // One thread wins the reset; the others carry on with the bits they
        // see, which is no worse than the moment before the interval elapsed.
        if (!lastResetNs_.compare_exchange_strong(last, now, std::memory_order_relaxed))
            return;
        for (std::size_t w = 0; w < wordCount_; ++w)
            words_[w].store(0, std::memory_order_relaxed);
    }

    /// Everything forgotten now, for tests and for an explicit re-check.
    void reset() noexcept
    {
        for (std::size_t w = 0; w < wordCount_; ++w)
            words_[w].store(0, std::memory_order_relaxed);
        lastResetNs_.store(nowNs(), std::memory_order_relaxed);
    }

private:
    static std::int64_t nowNs() noexcept
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    std::unique_ptr<std::atomic<std::uint64_t>[]> words_;
    std::size_t wordCount_ = 0;
    std::int64_t resetEveryNs_ = 0;
    std::atomic<std::int64_t> lastResetNs_{0};
};

}  // namespace nosql::internal
