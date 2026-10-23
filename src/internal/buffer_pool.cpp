// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/internal/buffer_pool.hpp"

#include <algorithm>
#include <cstring>
#include <new>

#include "nosql/error.hpp"

namespace nosql::internal {

namespace {

unsigned lowestBit(std::uint64_t word) noexcept
{
#if defined(__GNUC__) || defined(__clang__)
    return unsigned(__builtin_ctzll(word));
#else
    unsigned bit = 0;
    while (!(word & 1u)) {
        word >>= 1;
        ++bit;
    }
    return bit;
#endif
}

}  // namespace

/// The header at the start of every slab mapping, followed by two bitmaps of
/// `count` bits: buffers free to hand out, and free buffers whose memory has
/// not yet been given back to the OS. The buffers start at the next system
/// page boundary.
struct BufferPool::Slab
{
    std::byte* buffers = nullptr;
    std::size_t mapBytes = 0;
    std::size_t bufferBytes = 0;
    std::uint32_t cls = 0;
    std::uint32_t count = 0;
    std::uint32_t live = 0;  ///< handed out, or kept on a free list
    std::uint32_t freeCount = 0;
    Slab* prevOpen = nullptr;
    Slab* nextOpen = nullptr;
    Slab* nextTouched = nullptr;
    bool isOpen = false;
    bool isTouched = false;

    std::size_t words() const noexcept { return (std::size_t(count) + 63) / 64; }
    std::uint64_t* freeBits() noexcept { return reinterpret_cast<std::uint64_t*>(this + 1); }
    std::uint64_t* dirtyBits() noexcept { return freeBits() + words(); }
};

BufferPool::~BufferPool()
{
    purge();
    // Nothing should still be handed out; if something is, its memory goes.
    for (Slab* s : slabs_)
        os::unmapMemory(s, s->mapBytes);
}

void BufferPool::configure(std::size_t pageSize, std::size_t capBytes)
{
    page_size_ = pageSize;
    cap_ = capBytes;
    slab_bytes_ = std::max<std::size_t>(1u << 20, 64 * pageSize);
    system_page_ = os::systemPageSize();
    // Room for the slabs a typical working set needs, so the table does not
    // reallocate on the commit path.
    slabs_.reserve(64);
}

Page* BufferPool::acquire(unsigned npages)
{
    const int c = classOf(npages);
    if (c < 0)
        throw Error(ErrorCode::OutOfMemory, "page buffer too large");
    std::lock_guard<std::mutex> lk(mtx_);
    if (Page* p = free_[c]) {
        Page* next;
        std::memcpy(&next, static_cast<void*>(p), sizeof next);
        free_[c] = next;
        retained_ -= classPages(c) * page_size_;
        return p;
    }
    return carve(c);
}

void BufferPool::release(Page* buf, unsigned npages) noexcept
{
    std::lock_guard<std::mutex> lk(mtx_);
    giveBack(buf, classOf(npages));
    settle();
}

void BufferPool::release(const std::pair<Page*, unsigned>* items, std::size_t count) noexcept
{
    std::lock_guard<std::mutex> lk(mtx_);
    for (std::size_t i = 0; i < count; ++i)
        giveBack(items[i].first, classOf(items[i].second));
    settle();
}

void BufferPool::purge() noexcept
{
    std::lock_guard<std::mutex> lk(mtx_);
    for (unsigned c = 0; c < kClasses; ++c) {
        while (Page* p = free_[c]) {
            Page* next;
            std::memcpy(&next, static_cast<void*>(p), sizeof next);
            free_[c] = next;
            toSlab(p, int(c));
        }
    }
    retained_ = 0;
    settle();
}

Page* BufferPool::carve(int c)
{
    Slab* s = open_[c] ? open_[c] : newSlab(c);
    std::uint64_t* bits = s->freeBits();
    std::size_t w = 0;
    while (!bits[w])
        ++w;
    const unsigned bit = lowestBit(bits[w]);
    const std::size_t index = w * 64 + bit;
    bits[w] &= ~(std::uint64_t(1) << bit);
    // Handed out again before its memory went back: nothing to give back now.
    s->dirtyBits()[w] &= ~(std::uint64_t(1) << bit);
    --s->freeCount;
    ++s->live;
    if (!s->freeCount)
        unlinkOpen(s);
    return reinterpret_cast<Page*>(s->buffers + index * s->bufferBytes);
}

BufferPool::Slab* BufferPool::newSlab(int c)
{
    const std::size_t bufferBytes = classPages(c) * page_size_;
    const std::size_t count = std::max<std::size_t>(1, slab_bytes_ / bufferBytes);
    const std::size_t words = (count + 63) / 64;
    const std::size_t header =
        (sizeof(Slab) + 2 * words * sizeof(std::uint64_t) + system_page_ - 1) & ~(system_page_ - 1);
    if (count > std::numeric_limits<std::uint32_t>::max() ||
        bufferBytes > (std::numeric_limits<std::size_t>::max() - header) / count)
        throw Error(ErrorCode::OutOfMemory, "page buffer slab size overflow");
    slabs_.reserve(slabs_.size() + 1);  // the only step that can throw after mapping
    const std::size_t mapBytes = header + count * bufferBytes;
    auto* base = static_cast<std::byte*>(os::mapMemory(mapBytes));

    Slab* s = ::new (base) Slab;
    s->buffers = base + header;
    s->mapBytes = mapBytes;
    s->bufferBytes = bufferBytes;
    s->cls = std::uint32_t(c);
    s->count = std::uint32_t(count);
    s->freeCount = std::uint32_t(count);
    std::uint64_t* bits = s->freeBits();
    for (std::size_t w = 0; w < words; ++w) {
        const std::size_t inWord = std::min<std::size_t>(64, count - w * 64);
        bits[w] = inWord == 64 ? ~std::uint64_t(0) : (std::uint64_t(1) << inWord) - 1;
        s->dirtyBits()[w] = 0;
    }
    slabs_.insert(std::upper_bound(slabs_.begin(), slabs_.end(), s), s);
    s->isOpen = true;
    s->nextOpen = open_[c];
    if (open_[c])
        open_[c]->prevOpen = s;
    open_[c] = s;
    return s;
}

BufferPool::Slab* BufferPool::slabOf(const Page* buf) const noexcept
{
    const auto* address = reinterpret_cast<const std::byte*>(buf);
    auto it = std::upper_bound(slabs_.begin(), slabs_.end(), address,
                               [](const std::byte* a, const Slab* s) {
                                   return a < reinterpret_cast<const std::byte*>(s);
                               });
    return it == slabs_.begin() ? nullptr : *(it - 1);
}

void BufferPool::giveBack(Page* buf, int c) noexcept
{
    if (!buf || c < 0)
        return;
    const std::size_t bytes = classPages(c) * page_size_;
    if (bytes <= cap_ - retained_) {
        std::memcpy(static_cast<void*>(buf), &free_[c], sizeof free_[c]);
        free_[c] = buf;
        retained_ += bytes;
        return;
    }
    toSlab(buf, c);
}

void BufferPool::toSlab(Page* buf, int c) noexcept
{
    Slab* s = slabOf(buf);
    if (!s || s->cls != std::uint32_t(c))
        return;  // not ours: never happens, and leaking beats corrupting
    const std::size_t index =
        std::size_t(reinterpret_cast<std::byte*>(buf) - s->buffers) / s->bufferBytes;
    const std::uint64_t mask = std::uint64_t(1) << (index % 64);
    s->freeBits()[index / 64] |= mask;
    s->dirtyBits()[index / 64] |= mask;
    --s->live;
    ++s->freeCount;
    if (!s->isOpen) {
        s->isOpen = true;
        s->prevOpen = nullptr;
        s->nextOpen = open_[c];
        if (open_[c])
            open_[c]->prevOpen = s;
        open_[c] = s;
    }
    if (!s->isTouched) {
        s->isTouched = true;
        s->nextTouched = touched_;
        touched_ = s;
    }
}

/// Gives the memory of buffers that went back to their slab to the OS:
/// a slab nothing uses any more is unmapped whole, and in the others each
/// stretch of adjacent buffers is discarded with one call.
void BufferPool::settle() noexcept
{
    while (Slab* s = touched_) {
        touched_ = s->nextTouched;
        s->isTouched = false;
        if (!s->live) {
            unlinkOpen(s);
            const auto it = std::lower_bound(slabs_.begin(), slabs_.end(), s);
            if (it != slabs_.end() && *it == s)
                slabs_.erase(it);
            os::unmapMemory(s, s->mapBytes);
            continue;
        }
        std::uint64_t* dirty = s->dirtyBits();
        const std::size_t words = s->words();
        std::size_t runStart = 0, runLength = 0;
        const auto flush = [&] {
            if (runLength)
                os::discardMemory(s->buffers + runStart * s->bufferBytes,
                                  runLength * s->bufferBytes);
            runLength = 0;
        };
        for (std::size_t w = 0; w < words; ++w) {
            if (!dirty[w]) {
                flush();
                continue;
            }
            for (unsigned bit = 0; bit < 64; ++bit) {
                if (!((dirty[w] >> bit) & 1u)) {
                    flush();
                    continue;
                }
                if (!runLength)
                    runStart = w * 64 + bit;
                ++runLength;
            }
        }
        flush();
        for (std::size_t w = 0; w < words; ++w)
            dirty[w] = 0;
    }
}

void BufferPool::unlinkOpen(Slab* s) noexcept
{
    if (!s->isOpen)
        return;
    if (s->prevOpen)
        s->prevOpen->nextOpen = s->nextOpen;
    else
        open_[s->cls] = s->nextOpen;
    if (s->nextOpen)
        s->nextOpen->prevOpen = s->prevOpen;
    s->prevOpen = s->nextOpen = nullptr;
    s->isOpen = false;
}

}  // namespace nosql::internal
