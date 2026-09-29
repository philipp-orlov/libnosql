// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "nosql/internal/format.hpp"

namespace nosql::internal {

/// Open-addressed page-number -> dirty-buffer map.
///
/// A write transaction inserts one entry per page it copies and looks one up
/// on every single Page access, so this is both the hottest map in the
/// library and the one that would otherwise hand the allocator a node per
/// dirty page -- exactly the small, short-lived, interleaved allocation
/// pattern that fragments a long-running process's heap.
///
/// Linear probing with backward-shift deletion (no tombstones) keeps
/// everything in one buffer whose capacity is reused for the life of the
/// transaction, and keeps probe runs short even under the heavy
/// insert/erase churn that copy-on-write produces.
///
/// An occupancy bitmap rides alongside the slots so that visiting or clearing
/// the table costs the entries it holds, not the capacity it once grew to: a
/// pooled transaction object keeps the capacity a bulk load needed, and the
/// small commits that follow must not pay for it.
class PageTable
{
public:
    PageTable() = default;
    PageTable(const PageTable&) = delete;
    PageTable& operator=(const PageTable&) = delete;

    bool empty() const noexcept { return size_ == 0; }
    std::size_t size() const noexcept { return size_; }
    std::size_t capacity() const noexcept { return slots_.size(); }
    std::size_t retainedBytes() const noexcept
    {
        return slots_.capacity() * sizeof(Slot) + used_.capacity() * sizeof(std::uint64_t);
    }

    Page* find(PageNo key) const noexcept
    {
        if (size_ == 0)
            return nullptr;
        std::size_t i = home(key);
        for (;;) {
            const Slot& s = slots_[i];
            if (s.key == key)
                return s.val;
            if (s.key == kEmpty)
                return nullptr;
            i = (i + 1) & mask_;
        }
    }

    void set(PageNo key, Page* val)
    {
        // Grow at 3/4 load; linear probing degrades quickly past that.
        if ((size_ + 1) * 4 > slots_.size() * 3)
            grow();
        std::size_t i = home(key);
        for (;;) {
            Slot& s = slots_[i];
            if (s.key == key) {
                s.val = val;
                return;
            }
            if (s.key == kEmpty) {
                s.key = key;
                s.val = val;
                used_[i >> 6] |= std::uint64_t(1) << (i & 63);
                ++size_;
                return;
            }
            i = (i + 1) & mask_;
        }
    }

    bool erase(PageNo key) noexcept
    {
        if (size_ == 0)
            return false;
        std::size_t i = home(key);
        for (;;) {
            if (slots_[i].key == key)
                break;
            if (slots_[i].key == kEmpty)
                return false;
            i = (i + 1) & mask_;
        }
        // Backward shift: pull up any later entry whose ideal slot is at or
        // before the hole, so no probe run is ever broken by the removal. Every
        // slot an entry is pulled into stays occupied; only the final hole
        // empties, so only its bit changes.
        std::size_t j = i;
        for (;;) {
            j = (j + 1) & mask_;
            if (slots_[j].key == kEmpty)
                break;
            const std::size_t k = home(slots_[j].key);
            const bool stays = (i <= j) ? (i < k && k <= j) : (i < k || k <= j);
            if (stays)
                continue;
            slots_[i] = slots_[j];
            i = j;
        }
        slots_[i] = Slot{};
        used_[i >> 6] &= ~(std::uint64_t(1) << (i & 63));
        --size_;
        return true;
    }

    /// Visit every live entry as f(pgno, Page*). Order is unspecified.
    template <class F>
    void forEach(F&& f) const
    {
        if (size_ == 0)
            return;
        for (std::size_t w = 0; w < used_.size(); ++w) {
            std::uint64_t bits = used_[w];
            while (bits) {
                const unsigned b = lowestBit(bits);
                bits &= bits - 1;
                const Slot& s = slots_[(w << 6) | b];
                f(s.key, s.val);
            }
        }
    }

    /// Size the table up front for `n` entries, so a transaction that knows
    /// roughly how many pages it will dirty never rehashes mid-flight.
    void reserve(std::size_t n)
    {
        while (n * 4 > slots_.size() * 3)
            grow();
    }

    /// Empty the table but keep the buffer, so the next transaction reusing
    /// this object allocates nothing.
    void clear() noexcept
    {
        if (size_ == 0)
            return;
        for (std::size_t w = 0; w < used_.size(); ++w) {
            std::uint64_t bits = used_[w];
            while (bits) {
                const unsigned b = lowestBit(bits);
                bits &= bits - 1;
                slots_[(w << 6) | b] = Slot{};
            }
            used_[w] = 0;
        }
        size_ = 0;
    }

    /// Give the buffer back for real.
    void release()
    {
        slots_ = std::vector<Slot>();
        used_ = std::vector<std::uint64_t>();
        mask_ = 0;
        shift_ = 64;
        size_ = 0;
    }

private:
    static constexpr PageNo kEmpty = kInvalidPage;
    static constexpr std::size_t kInitial = 64;

    struct Slot
    {
        PageNo key = kEmpty;
        Page* val = nullptr;
    };

    static unsigned lowestBit(std::uint64_t bits) noexcept
    {
#if defined(__GNUC__) || defined(__clang__)
        return unsigned(__builtin_ctzll(bits));
#else
        unsigned n = 0;
        while (!(bits & 1)) {
            bits >>= 1;
            ++n;
        }
        return n;
#endif
    }

    /// Fibonacci hashing, taking the *high* bits of the product: page numbers
    /// arrive in dense runs, and the low bits of a multiply would preserve
    /// exactly the clustering we are trying to break up.
    std::size_t home(PageNo key) const noexcept
    {
        return std::size_t((key * 0x9E3779B97F4A7C15ull) >> shift_);
    }

    void grow()
    {
        const std::size_t want = slots_.empty() ? kInitial : slots_.size() * 2;
        std::vector<Slot> old(want);
        old.swap(slots_);
        std::vector<std::uint64_t> oldUsed(want / 64);
        oldUsed.swap(used_);
        mask_ = want - 1;
        unsigned bits = 0;
        while ((std::size_t(1) << bits) < want)
            ++bits;
        shift_ = 64 - bits;
        size_ = 0;
        for (std::size_t w = 0; w < oldUsed.size(); ++w) {
            std::uint64_t live = oldUsed[w];
            while (live) {
                const unsigned b = lowestBit(live);
                live &= live - 1;
                const Slot& s = old[(w << 6) | b];
                set(s.key, s.val);
            }
        }
    }

    std::vector<Slot> slots_;
    std::vector<std::uint64_t> used_;  ///< one bit per slot: occupied
    std::size_t mask_ = 0;
    unsigned shift_ = 64;
    std::size_t size_ = 0;
};

}  // namespace nosql::internal
