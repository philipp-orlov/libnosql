// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string_view>
#include <vector>

#include "nosql/internal/format.hpp"

namespace nosql::internal {

/// Hash of one (sub-database, key) pair: the unit optimistic transactions
/// validate on. Two different pairs can hash alike; that makes a conflict
/// spurious, never missed.
inline std::uint64_t occKey(std::string_view db, std::string_view key) noexcept
{
    const std::uint64_t h = std::hash<std::string_view>{}(db);
    return h ^ (std::hash<std::string_view>{}(key) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
}

/// The keys each recent top-level commit wrote, for backward validation of
/// optimistic transactions. Only touched by the writer, which holds the writer
/// slot both when it appends (publish) and when it validates.
///
/// A ring buffer rather than a deque: each slot keeps its key vector's capacity,
/// so a steady stream of commits allocates nothing once the log has warmed up.
class OccLog
{
public:
    /// `keys` must be sorted and unique. Null records a commit whose keys are
    /// unknown (a plain write transaction): it conflicts with every optimistic
    /// transaction that began before it.
    void record(TxnId txnid, const std::vector<std::uint64_t>* keys)
    {
        if (count_ == ring_.size())
            grow();
        Entry& e = ring_[(head_ + count_) % ring_.size()];
        e.txnid = txnid;
        e.unknown = keys == nullptr;
        e.keys.clear();
        if (keys)
            e.keys.insert(e.keys.end(), keys->begin(), keys->end());
        ++count_;
    }

    /// True if a commit made after `snapshot` wrote any key in `keys` (sorted,
    /// unique), or wrote keys nobody can name.
    bool conflicts(TxnId snapshot, const std::vector<std::uint64_t>& keys) const noexcept
    {
        for (std::size_t i = count_; i-- > 0;) {
            const Entry& e = ring_[(head_ + i) % ring_.size()];
            if (e.txnid <= snapshot)
                break;  // entries are in ascending txnid order
            if (e.unknown || intersects(e.keys, keys))
                return true;
        }
        return false;
    }

    /// Drops entries at or before `oldest`, the oldest snapshot a reader still
    /// pins: no transaction can validate against them any more.
    void trim(TxnId oldest) noexcept
    {
        while (count_ > 0 && ring_[head_].txnid <= oldest) {
            head_ = (head_ + 1) % ring_.size();
            --count_;
        }
    }

    std::size_t size() const noexcept { return count_; }

private:
    struct Entry
    {
        TxnId txnid = 0;
        bool unknown = false;
        std::vector<std::uint64_t> keys;
    };

    void grow()
    {
        std::vector<Entry> next(std::max<std::size_t>(8, ring_.size() * 2));
        for (std::size_t i = 0; i < count_; ++i)
            next[i] = std::move(ring_[(head_ + i) % ring_.size()]);
        ring_ = std::move(next);
        head_ = 0;
    }

    static bool intersects(const std::vector<std::uint64_t>& a,
                           const std::vector<std::uint64_t>& b) noexcept
    {
        auto i = a.begin();
        auto j = b.begin();
        while (i != a.end() && j != b.end()) {
            if (*i == *j)
                return true;
            if (*i < *j)
                ++i;
            else
                ++j;
        }
        return false;
    }

    std::vector<Entry> ring_;
    std::size_t head_ = 0;   // index of the oldest entry
    std::size_t count_ = 0;  // live entries
};

}  // namespace nosql::internal
