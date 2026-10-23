// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Leaf key prefix compression (DbFlags::PrefixKeys).
//
// A P_PREFIX leaf stores the bytes every one of its keys starts with once, in
// a block at the top of the node area just under the checksum: the prefix,
// then its length as a little-endian u16, the block padded at its low end to
// a multiple of four. Its nodes hold only what follows the prefix. The block
// sits above every node, so deleting a node, which compacts the node area
// upwards from `upper`, never moves it. Only byte-ordered trees compress:
// there the shared prefix of a sorted page is the shared prefix of its first
// and last key, and a key compares with the page by its prefix first.

#pragma once

#include <algorithm>
#include <cstring>

#include "nosql/internal/format.hpp"

namespace nosql::internal {

/// Room the prefix block of a `plen`-byte prefix takes.
inline constexpr std::size_t prefixBlock(std::size_t plen) noexcept
{
    return align4(plen + 2);
}

/// The prefix the keys of leaf `p` share; empty unless it is P_PREFIX.
inline Slice leafPrefix(const Page* p, std::size_t ps) noexcept
{
    if (!(p->flags & P_PREFIX))
        return Slice();
    const auto* end = reinterpret_cast<const std::byte*>(p) + ps - kPageChecksumBytes - 2;
    const std::size_t n = readLittle<std::uint16_t>(end);
    return Slice(end - n, n);
}

/// Where `key` falls against a prefix all of a page's keys share: below all
/// of them (-1), above all of them (1), or starting with it (0; `rest` is then
/// what follows it).
inline int againstPrefix(Slice prefix, Slice key, Slice* rest) noexcept
{
    const std::size_t n = std::min(prefix.size(), key.size());
    const int c = n ? std::memcmp(prefix.data(), key.data(), n) : 0;
    if (c)
        return c > 0 ? -1 : 1;
    if (key.size() < prefix.size())
        return -1;
    *rest = Slice(key.data() + prefix.size(), key.size() - prefix.size());
    return 0;
}

/// Appends leaf `i`'s whole key, prefix included, to `out`.
template <class Bytes>
void appendLeafKey(const Page* p, unsigned i, std::size_t ps, Bytes& out)
{
    const Slice prefix = leafPrefix(p, ps);
    const Slice suffix = nodeKey(p, i);
    out.insert(out.end(), prefix.data(), prefix.data() + prefix.size());
    out.insert(out.end(), suffix.data(), suffix.data() + suffix.size());
}

inline std::size_t commonPrefix(Slice a, Slice b) noexcept
{
    const std::size_t n = std::min(a.size(), b.size());
    std::size_t i = 0;
    while (i < n && a.data()[i] == b.data()[i])
        ++i;
    return i;
}

// The layout helpers take any entry type with the leaf fields of b_tree.cpp's
// Entry: the whole `key`, the logical `vsize`, node `flags`, and the `paylen`
// payload bytes at `pay` (null for zeros).

/// Bytes `n` entries take on one leaf whose keys share `plen` bytes, slots and
/// prefix block included.
template <class E>
std::size_t leafLayoutBytes(const E* e, unsigned n, std::size_t plen) noexcept
{
    std::size_t total = plen ? prefixBlock(plen) : 0;
    for (unsigned i = 0; i < n; ++i)
        total +=
            2 + leafNodeSize(e[i].key.size() - plen, e[i].vsize, (e[i].flags & N_BIGDATA) != 0);
    return total;
}

/// The prefix `n` sorted entries are best laid out with: what their first and
/// last key share, when that saves room; 0 otherwise.
template <class E>
std::size_t leafPrefixFor(const E* e, unsigned n) noexcept
{
    if (n < 2)
        return 0;
    const std::size_t plen = std::min<std::size_t>(commonPrefix(e[0].key, e[n - 1].key), 0xffff);
    if (!plen || leafLayoutBytes(e, n, plen) >= leafLayoutBytes(e, n, 0))
        return 0;
    return plen;
}

/// Rewrites `p` as leaf `pgno` holding `n` sorted entries whose keys share
/// `plen` bytes. The entries must not point into `p`.
template <class E>
void layoutLeaf(Page* p, PageNo pgno, const E* e, unsigned n, std::size_t plen,
                std::size_t ps) noexcept
{
    p->pgno = pgno;
    p->flags = std::uint16_t(P_LEAF | (plen ? P_PREFIX : 0));
    p->nkeys = 0;
    p->lower = std::uint16_t(kPageHdr);
    auto* base = reinterpret_cast<std::byte*>(p);
    std::size_t upper = ps - kPageChecksumBytes;
    if (plen) {
        upper -= prefixBlock(plen);
        auto* end = base + ps - kPageChecksumBytes - 2;
        std::memset(base + upper, 0, std::size_t(end - plen - (base + upper)));
        std::memcpy(end - plen, e[0].key.data(), plen);
        writeLittle(end, std::uint16_t(plen));
    }
    for (unsigned i = 0; i < n; ++i) {
        const bool big = (e[i].flags & N_BIGDATA) != 0;
        const std::size_t ksize = e[i].key.size() - plen;
        const std::size_t size = leafNodeSize(ksize, e[i].vsize, big);
        upper -= size;
        std::memset(base + upper, 0, size);  // alignment padding included
        auto* node = reinterpret_cast<LeafNode*>(base + upper);
        node->flags = e[i].flags;
        node->ksize = std::uint16_t(ksize);
        node->vsize = e[i].vsize;
        auto* body = reinterpret_cast<std::byte*>(node) + sizeof(LeafNode);
        if (ksize)
            std::memcpy(body, e[i].key.data() + plen, ksize);
        if (e[i].pay)
            std::memcpy(body + ksize, e[i].pay, e[i].paylen);
        else
            std::memset(body + ksize, 0, e[i].paylen);
        slots(p)[i] = std::uint16_t(upper);
    }
    p->nkeys = std::uint16_t(n);
    p->lower = std::uint16_t(kPageHdr + 2 * std::size_t(n));
    p->upper = std::uint16_t(upper);
}

}  // namespace nosql::internal
