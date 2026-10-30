// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <new>
#include <utility>
#include <vector>

#include "nosql/internal/os.hpp"

namespace nosql::internal {

/// A bump allocator for the short-lived scratch that page splits and
/// rebalances need (a Page copy, an entry list, a couple of key buffers).
///
/// Those temporaries used to be `std::vector`s -- five or six allocations on
/// every split, which is precisely the churn a long-lived process must not
/// generate. The arena serves them from chunks it keeps for the life of the
/// transaction and rewinds with a mark, so steady-state cost is a pointer
/// bump.
///
/// Chunks are never moved or coalesced, which matters: splits recurse, and an
/// outer frame holds pointers into the arena while an inner frame allocates.
class Arena
{
public:
    struct Mark
    {
        std::size_t chunk = 0;
        std::size_t off = 0;
    };

    Arena() = default;
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    ~Arena() { purge(); }

    void setChunkSize(std::size_t bytes) noexcept
    {
        chunkSize_ = bytes < kMinChunk ? kMinChunk : bytes;
    }

    Mark save() const noexcept { return {cur_, off_}; }
    void restore(Mark m) noexcept
    {
        cur_ = m.chunk;
        off_ = m.off;
    }

    std::byte* alloc(std::size_t n, std::size_t align = 16)
    {
        if (!chunks_.empty()) {
            const std::size_t a = (off_ + align - 1) & ~(align - 1);
            if (a + n <= chunks_[cur_].cap) {
                off_ = a + n;
                return chunks_[cur_].base + a;
            }
        }
        return allocSlow(n, align);
    }

    template <class T>
    T* allocArray(std::size_t count)
    {
        static_assert(std::is_trivially_destructible_v<T>, "arena never runs destructors");
        return reinterpret_cast<T*>(alloc(count * sizeof(T), alignof(T)));
    }

    /// Release every chunk. Called when the transaction is done with it.
    void purge() noexcept
    {
        for (const Chunk& c : chunks_)
            os::freePage(c.base);
        chunks_.clear();
        cur_ = 0;
        off_ = 0;
    }

    std::size_t retainedBytes() const noexcept
    {
        std::size_t n = 0;
        for (const Chunk& c : chunks_)
            n += c.cap;
        return n;
    }

    /// RAII rewind: everything allocated inside the scope is reclaimed.
    class Scope
    {
    public:
        explicit Scope(Arena& a) noexcept : a_(&a), m_(a.save()) {}
        ~Scope() { a_->restore(m_); }
        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

    private:
        Arena* a_;
        Mark m_;
    };

private:
    static constexpr std::size_t kMinChunk = 64u << 10;

    struct Chunk
    {
        std::byte* base;
        std::size_t cap;
    };

    std::byte* allocSlow(std::size_t n, std::size_t align)
    {
        // Chunk bases come from the aligned allocator, so a fresh chunk always
        // satisfies the alignments this library asks for.
        const std::size_t next = chunks_.empty() ? 0 : cur_ + 1;
        const std::size_t want = n + align > chunkSize_ ? n + align : chunkSize_;
        if (next < chunks_.size()) {
            if (chunks_[next].cap < n + align) {
                // Only chunks at or after `next` can be replaced: nothing still in
                // scope points into them.
                auto* replacement = static_cast<std::byte*>(os::allocPage(want));
                os::freePage(chunks_[next].base);
                chunks_[next] = Chunk{replacement, want};
            }
        } else {
            if (chunks_.size() == chunks_.capacity())
                chunks_.reserve(chunks_.empty() ? 4 : chunks_.size() * 2);
            chunks_.push_back(Chunk{static_cast<std::byte*>(os::allocPage(want)), want});
        }
        cur_ = next;
        off_ = n;
        return chunks_[cur_].base;
    }

    std::vector<Chunk> chunks_;
    std::size_t chunkSize_ = kMinChunk;
    std::size_t cur_ = 0;
    std::size_t off_ = 0;
};

}  // namespace nosql::internal
