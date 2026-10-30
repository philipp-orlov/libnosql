// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The value of a free-list chunk (the GC tree's entries): the pages it lists.
//
// Format 7 knows one form, an array of little-endian u64 page numbers. A
// store at format 8 may also hold a chunk as runs of consecutive pages, which
// is what a commit's freed pages mostly are: a varint count of runs, then for
// each the gap from the end of the one before (from page 0 for the first) and
// its length less one, both varints. An array's length is a multiple of eight
// and the runs' never is: when they would be, one zero byte follows. A chunk
// takes runs only where they are shorter, so a chunk is never longer for it.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "nosql/internal/format.hpp"

namespace nosql::internal {

/// The most pages one free-list chunk can list: a commit's chunk, or an
/// inline entry moved into the tree (Meta::deferredPages).
inline constexpr std::size_t freeChunkLimit(std::size_t pageSize) noexcept
{
    constexpr std::size_t inlined = std::tuple_size<decltype(Meta::deferredPages)>::value;
    return freeChunkPages(pageSize) > inlined ? freeChunkPages(pageSize) : inlined;
}

inline std::size_t varintBytes(std::uint64_t v) noexcept
{
    std::size_t n = 1;
    for (; v >= 0x80; v >>= 7)
        ++n;
    return n;
}

inline std::byte* putVarint(std::byte* out, std::uint64_t v) noexcept
{
    for (; v >= 0x80; v >>= 7)
        *out++ = std::byte(0x80 | (v & 0x7f));
    *out++ = std::byte(v);
    return out;
}

/// Reads a varint of at most ten bytes from [in, end); false if it runs past
/// `end` or does not fit 64 bits.
inline bool getVarint(const std::byte*& in, const std::byte* end, std::uint64_t& v) noexcept
{
    v = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        if (in == end)
            return false;
        const auto b = std::to_integer<std::uint64_t>(*in++);
        if (shift == 63 && b > 1)
            return false;
        v |= (b & 0x7f) << shift;
        if (!(b & 0x80))
            return true;
    }
    return false;
}

/// Calls fn(start, length) for each run of consecutive pages in sorted,
/// distinct `pages`.
template <class Fn>
void forEachRun(const PageNo* pages, std::size_t n, Fn fn)
{
    for (std::size_t i = 0; i < n;) {
        std::size_t j = i + 1;
        while (j < n && pages[j] == pages[j - 1] + 1)
            ++j;
        fn(pages[i], std::uint64_t(j - i));
        i = j;
    }
}

/// Bytes sorted, distinct `pages` take as runs, the padding byte included;
/// 0 when that is no shorter than the array.
inline std::size_t freeRunsBytes(const PageNo* pages, std::size_t n) noexcept
{
    std::size_t runs = 0, bytes = 0;
    PageNo end = 0;
    forEachRun(pages, n, [&](PageNo start, std::uint64_t length) {
        ++runs;
        bytes += varintBytes(start - end) + varintBytes(length - 1);
        end = start + length;
    });
    bytes += varintBytes(runs);
    if (bytes % sizeof(PageNo) == 0)
        ++bytes;
    return bytes < n * sizeof(PageNo) ? bytes : 0;
}

/// Writes sorted, distinct `pages` as runs into the `size` bytes at `out`,
/// size being freeRunsBytes() of them.
inline void writeFreeRuns(std::byte* out, std::size_t size, const PageNo* pages,
                          std::size_t n) noexcept
{
    std::size_t runs = 0;
    forEachRun(pages, n, [&](PageNo, std::uint64_t) { ++runs; });
    std::byte* at = putVarint(out, runs);
    PageNo end = 0;
    forEachRun(pages, n, [&](PageNo start, std::uint64_t length) {
        at = putVarint(at, start - end);
        at = putVarint(at, length - 1);
        end = start + length;
    });
    if (at < out + size)
        *at = std::byte(0);  // the padding byte
}

/// Calls fn(page) for each page chunk value `v` lists, in either form, and
/// returns how many; false if `v` is neither form or lists more than `limit`
/// pages, before calling fn for any of them.
template <class Fn>
bool forEachFreePage(Slice v, std::size_t limit, std::size_t& count, Fn fn)
{
    const std::byte* const begin = v.data();
    const std::byte* const end = begin + v.size();
    if (v.size() % sizeof(PageNo) == 0) {
        count = v.size() / sizeof(PageNo);
        if (count > limit)
            return false;
        for (std::size_t i = 0; i < count; ++i)
            fn(readLittle<PageNo>(begin + i * sizeof(PageNo)));
        return true;
    }
    // Runs: checked whole first, so a bad chunk hands out no page.
    for (const bool apply : {false, true}) {
        const std::byte* at = begin;
        std::uint64_t runs = 0, total = 0;
        PageNo page = 0;
        if (!getVarint(at, end, runs))
            return false;
        for (std::uint64_t r = 0; r < runs; ++r) {
            std::uint64_t gap = 0, length = 0;
            if (!getVarint(at, end, gap) || !getVarint(at, end, length))
                return false;
            if (r && gap == 0)
                return false;  // runs are maximal
            if (gap > ~page || length >= limit || total + length + 1 > limit ||
                length > ~(page + gap))
                return false;
            page += gap;
            if (apply)
                for (std::uint64_t k = 0; k <= length; ++k)
                    fn(page + k);
            page += length + 1;
            total += length + 1;
        }
        // Nothing after the runs but the padding byte, and that only where
        // the runs alone would look like an array.
        const std::size_t rest = std::size_t(end - at);
        const std::size_t used = std::size_t(at - begin);
        if (!(rest == 0 || (rest == 1 && *at == std::byte(0) && used % sizeof(PageNo) == 0)))
            return false;
        count = std::size_t(total);
    }
    return true;
}

}  // namespace nosql::internal
