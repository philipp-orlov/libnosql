// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Key prefix compression (DbFlags::PrefixKeys).
//
// A P_PREFIX leaf stores the bytes every one of its keys starts with once, in
// a block at the top of the node area just under the checksum: the prefix,
// then its length as a little-endian u16, the block padded at its low end to
// a multiple of four. Its nodes hold only what follows the prefix. The block
// sits above every node, so deleting a node, which compacts the node area
// upwards from `upper`, never moves it. Only byte-ordered trees compress:
// there the shared prefix of a sorted page is the shared prefix of its first
// and last key, and a key compares with the page by its prefix first.
//
// A P_PREFIX branch page does the same for its separators, slots 1 and up:
// slot 0 is the -infinity child, whose key is empty and never compared, so
// its node stores nothing and the prefix is what slots 1 and n-1 share.

#pragma once

#include <algorithm>
#include <cstring>

#include "nosql/error.hpp"
#include "nosql/internal/format.hpp"

/// For the few checks on every cursor step and search probe, which the
/// compiler otherwise leaves out of line once the throw paths make them look big.
#if defined(_MSC_VER)
#define NOSQL_ALWAYS_INLINE __forceinline
#else
#define NOSQL_ALWAYS_INLINE inline __attribute__((always_inline))
#endif

namespace nosql::internal {

/// Room the prefix block of a `plen`-byte prefix takes.
inline constexpr std::size_t prefixBlock(std::size_t plen) noexcept
{
    return align4(plen + 2);
}

/// The prefix the keys of leaf or branch `p` share; empty unless it is
/// P_PREFIX.
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

/// Writes the prefix block of `prefix` at the top of empty page `p`;
/// returns where the nodes under it end.
inline std::size_t writePrefixBlock(Page* p, Slice prefix, std::size_t ps) noexcept
{
    auto* base = reinterpret_cast<std::byte*>(p);
    std::size_t upper = ps - kPageChecksumBytes;
    if (const std::size_t plen = prefix.size()) {
        upper -= prefixBlock(plen);
        auto* end = base + ps - kPageChecksumBytes - 2;
        std::memset(base + upper, 0, std::size_t(end - plen - (base + upper)));
        std::memcpy(end - plen, prefix.data(), plen);
        writeLittle(end, std::uint16_t(plen));
    }
    return upper;
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
    std::size_t upper = writePrefixBlock(p, Slice(n ? e[0].key.data() : nullptr, plen), ps);
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

/// Bytes `n` branch entries take on one page whose separators share `plen`
/// bytes; entry 0's key is not stored.
template <class E>
std::size_t branchLayoutBytes(const E* e, unsigned n, std::size_t plen) noexcept
{
    std::size_t total = plen ? prefixBlock(plen) : 0;
    for (unsigned i = 0; i < n; ++i)
        total += 2 + branchNodeSize(i ? e[i].key.size() - plen : 0);
    return total;
}

/// The prefix `n` sorted branch entries are best laid out with: what the
/// separators of entries 1 and n-1 share, when that saves room; 0 otherwise.
template <class E>
std::size_t branchPrefixFor(const E* e, unsigned n) noexcept
{
    if (n < 2)
        return 0;
    const std::size_t plen = std::min<std::size_t>(commonPrefix(e[1].key, e[n - 1].key), 0xffff);
    if (!plen || branchLayoutBytes(e, n, plen) >= branchLayoutBytes(e, n, 0))
        return 0;
    return plen;
}

// A checksum proves a page holds the bytes that were written, not that their
// writer told the truth. Pages read from the file are checked in three steps,
// each where it is cheapest: their header on every checked read
// (validateHeader), each node as a reader reaches it (checkNode), and every
// node at once where a writer copies the page to change it or the read cache
// checks it once for many reads (validateNodes).

/// End of the node area of slotted page `p`: under its checksum, and under
/// the prefix block of a P_PREFIX leaf. Only for a page whose header passed.
inline std::size_t nodeTop(const Page* p, std::size_t ps) noexcept
{
    std::size_t top = ps - kPageChecksumBytes;
    if (p->flags & P_PREFIX)
        top -=
            prefixBlock(readLittle<std::uint16_t>(reinterpret_cast<const std::byte*>(p) + top - 2));
    return top;
}

/// A branch or leaf page's type, slot array and prefix block lie inside it.
inline void validateHeader(const Page* p, std::size_t ps)
{
    const auto bad = [](const char* what) { throw Error(ErrorCode::Corrupted, what); };
    const std::uint16_t flags = p->flags;
    std::size_t top = ps - kPageChecksumBytes;
    if (flags == (P_LEAF | P_PREFIX) || flags == (P_BRANCH | P_PREFIX)) {
        const std::size_t plen =
            readLittle<std::uint16_t>(reinterpret_cast<const std::byte*>(p) + top - 2);
        if (!plen || prefixBlock(plen) > top - kPageHdr)
            bad("leaf key prefix out of range");
        top -= prefixBlock(plen);
    } else if (flags != P_LEAF && flags != P_BRANCH) {
        bad("page is neither a branch nor a leaf");
    }
    // A writer adds nodes downwards from `upper`, so it stays 4-aligned too.
    if (p->lower != kPageHdr + 2 * std::size_t(p->nkeys) || p->lower > p->upper || p->upper > top ||
        (p->upper & 3))
        bad("page slot and node bounds out of range");
}

/// Throws the Corrupted error for a node outside its page. Out of line and
/// cold, so the checks below stay small enough to inline into every search.
[[noreturn]] void throwNodeOutOfRange();

/// Whether node `i` of a page whose header passed lies inside the node area
/// ending at `top`. Reads the node's header only once its offset is known to
/// leave room for one.
inline bool nodeFits(const Page* p, unsigned i, std::size_t top) noexcept
{
    const auto* base = reinterpret_cast<const std::byte*>(p);
    const std::size_t off = slots(p)[i];
    const bool leaf = isLeaf(p);
    const std::size_t header = leaf ? sizeof(LeafNode) : sizeof(BranchNode);
    if (off < p->upper || (off & 3) || off + header > top)
        return false;
    if (!leaf)
        return header + std::size_t(readLittle<std::uint16_t>(base + off + 6)) <= top - off;
    const std::size_t ksize = readLittle<std::uint16_t>(base + off + 2);
    const std::size_t payload = (readLittle<std::uint16_t>(base + off) & N_BIGDATA)
                                    ? sizeof(PageNo)
                                    : std::size_t(readLittle<std::uint32_t>(base + off + 4));
    return header + ksize + payload <= top - off;
}

/// Node `i` of a page read from the file lies inside it; `i` < nkeys.
inline void checkNode(const Page* p, unsigned i, std::size_t ps)
{
    if (!nodeFits(p, i, nodeTop(p, ps)))
        throwNodeOutOfRange();
}

/// Node `i`'s key, checked to lie inside the node area ending at `top`.
inline Slice checkedKey(const Page* p, unsigned i, std::size_t top)
{
    if (!nodeFits(p, i, top))
        throwNodeOutOfRange();
    return nodeKey(p, i);
}

/// The key of branch node `i` or leaf node `i`, its offset and key checked to
/// lie inside the node area ending at `top`: all a search reads. A leaf's
/// value is checked where it is read (leafValue).
NOSQL_ALWAYS_INLINE Slice checkedBranchKey(const Page* p, unsigned i, std::size_t top)
{
    const auto* base = reinterpret_cast<const std::byte*>(p);
    const std::size_t off = slots(p)[i], upper = p->upper;
    if (off < upper || off + sizeof(BranchNode) > top || (off & 3))
        throwNodeOutOfRange();
    const std::size_t ksize = readLittle<std::uint16_t>(base + off + 6);
    if (off + sizeof(BranchNode) + ksize > top)
        throwNodeOutOfRange();
    return Slice(base + off + sizeof(BranchNode), ksize);
}
NOSQL_ALWAYS_INLINE Slice checkedLeafKey(const Page* p, unsigned i, std::size_t top)
{
    const auto* base = reinterpret_cast<const std::byte*>(p);
    const std::size_t off = slots(p)[i], upper = p->upper;
    if (off < upper || off + sizeof(LeafNode) > top || (off & 3))
        throwNodeOutOfRange();
    const std::size_t ksize = readLittle<std::uint16_t>(base + off + 2);
    if (off + sizeof(LeafNode) + ksize > top)
        throwNodeOutOfRange();
    return Slice(base + off + sizeof(LeafNode), ksize);
}

/// Leaf node `i` of a page whose header passed, key and value both checked to
/// lie inside the node area ending at `top`.
NOSQL_ALWAYS_INLINE const LeafNode* checkedLeafNode(const Page* p, unsigned i, std::size_t top)
{
    const auto* base = reinterpret_cast<const std::byte*>(p);
    const std::size_t off = slots(p)[i];
    if (off < p->upper || off + sizeof(LeafNode) > top || (off & 3))
        throwNodeOutOfRange();
    const auto* n = reinterpret_cast<const LeafNode*>(base + off);
    const std::size_t payload = (n->flags & N_BIGDATA) ? sizeof(PageNo) : std::size_t(n->vsize);
    if (off + sizeof(LeafNode) + n->ksize + payload > top)
        throwNodeOutOfRange();
    return n;
}

/// Branch `p`'s child `i` and the entries under it, the node checked first.
inline PageNo checkedChild(const Page* p, unsigned i, std::size_t ps)
{
    checkNode(p, i, ps);
    return branchChild(p, i);
}
inline std::uint64_t checkedCount(const Page* p, unsigned i, std::size_t ps)
{
    checkNode(p, i, ps);
    return branchCount(p, i);
}

/// Every node of a page whose header passed lies inside it. One loop per page
/// type, so the type is not tested per node.
inline void validateNodes(const Page* p, std::size_t ps)
{
    const std::size_t top = nodeTop(p, ps);
    const unsigned n = p->nkeys;
    if (isLeaf(p)) {
        for (unsigned i = 0; i < n; ++i)
            (void)checkedLeafNode(p, i, top);
    } else {
        for (unsigned i = 0; i < n; ++i)
            (void)checkedBranchKey(p, i, top);
    }
}

}  // namespace nosql::internal
