// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// On-disk layout.
//
//   page 0, Page 1      alternating meta pages
//   page 2 ...          B+tree branch / leaf / overflow pages
//
// Format integers are little-endian; identities and application values are opaque bytes.
#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

#include "nosql/slice.hpp"
#include "nosql/internal/endian.hpp"
#include "nosql/internal/checksum.hpp"

namespace nosql::internal {

using PageNo = std::uint64_t;
using TxnId = std::uint64_t;

inline constexpr PageNo kInvalidPage = ~PageNo(0);

/// Stored verbatim so the first bytes of the file read as the name in any hex
/// dump. Padded to 16 so every 64-bit field after it stays 8-byte aligned.
inline constexpr char kMagic[16] = "NOSQL-ORLOV";
/// The newest format this build writes and reads. A store keeps the oldest version
/// that can read it, so older builds open it for as long as they can: 7 until it first
/// holds a P_PREFIX leaf (DbFlags::PrefixKeys), then 8; 9 from its first sealed commit
/// (Env::Options::sealedCommits()), whose meta page is only valid together with the
/// pages its manifest lists.
inline constexpr std::uint32_t kFormatVersion = 9;
inline constexpr std::uint32_t kFormatPrefix = 8;
inline constexpr std::uint32_t kFormatSealed = 9;
inline constexpr std::uint32_t kOldestFormatVersion = 7;
/// Meta::version holds the format number in its low half. This bit above it says the
/// meta was written by a sealed commit: a manifest follows the record in the page, and
/// the meta counts as written only if the manifest and every page it lists are there
/// (sealedMetaLanded). Under the meta's checksum, so no torn write can drop it.
inline constexpr std::uint32_t kMetaSealed = 1u << 16;
inline constexpr std::size_t kPageChecksumBytes = 8;

using Identity = std::array<std::uint64_t, 2>;

/// A fresh 128-bit identity: never zero, drawn from a generator seeded once
/// per thread from std::random_device rather than from the device itself,
/// which on some platforms is a system call or a file read per word.
Identity newIdentity() noexcept;

inline constexpr std::size_t kMinPageSize = 1024;
inline constexpr std::size_t kMaxPageSize = 65536;
inline constexpr std::size_t kDefaultPageSize = 4096;

/// A power of two within [kMinPageSize, kMaxPageSize].
inline constexpr bool legalPageSize(std::uint64_t pageSize) noexcept
{
    return pageSize >= kMinPageSize && pageSize <= kMaxPageSize && (pageSize & (pageSize - 1)) == 0;
}

/// Page numbers are 48-bit on disk: 2^48 pages x 512 B is 128 PiB of headroom.
inline constexpr PageNo kMaxPgno = (PageNo(1) << 48) - 1;

/// Deepest B+tree we will descend. Even with worst-case 2-key pages this
/// covers far more entries than the 48-bit page space allows.
inline constexpr int kMaxDepth = 48;

// ------------------------------------------------------------- page hdr ---

enum PageFlags : std::uint16_t
{
    P_BRANCH = 1u << 0,
    P_LEAF = 1u << 1,
    P_OVERFLOW = 1u << 2,
    P_META = 1u << 3,
    P_PREFIX = 1u << 4,  ///< page storing its keys' shared prefix once (key_prefix.hpp)
    P_SUFFIX = 1u << 5,  ///< page storing its keys' shared suffix once, in reverse order
};

/// Either: the page has a block of bytes its keys share.
inline constexpr std::uint16_t P_SHARED = P_PREFIX | P_SUFFIX;

/// 16-byte slotted-page header. `lower` is the end of the uint16 slot array,
/// `upper` the start of the packed node data; free space is what lies between.
struct Page
{
    Little<std::uint64_t> pgno;
    Little<std::uint16_t> flags;
    Little<std::uint16_t> nkeys;
    Little<std::uint16_t> lower;
    Little<std::uint16_t> upper;
};
static_assert(sizeof(Page) == 16, "page header must be 16 bytes");

inline constexpr std::size_t kPageHdr = sizeof(Page);

inline bool isLeaf(const Page* p) noexcept
{
    return (p->flags & P_LEAF) != 0;
}
inline bool isBranch(const Page* p) noexcept
{
    return (p->flags & P_BRANCH) != 0;
}
inline bool isOverflow(const Page* p) noexcept
{
    return (p->flags & P_OVERFLOW) != 0;
}

/// Overflow pages reuse the nkeys/lower/upper words to record run length.
inline std::uint32_t ovPages(const Page* p) noexcept
{
    return readLittle<std::uint32_t>(reinterpret_cast<const std::byte*>(p) + 12);
}
inline void setOvPages(Page* p, std::uint32_t v) noexcept
{
    writeLittle(reinterpret_cast<std::byte*>(p) + 12, v);
}

inline Little<std::uint16_t>* slots(Page* p) noexcept
{
    return reinterpret_cast<Little<std::uint16_t>*>(reinterpret_cast<std::byte*>(p) + kPageHdr);
}
inline const Little<std::uint16_t>* slots(const Page* p) noexcept
{
    return reinterpret_cast<const Little<std::uint16_t>*>(reinterpret_cast<const std::byte*>(p) + kPageHdr);
}
inline std::byte* nodePtr(Page* p, unsigned i) noexcept
{
    return reinterpret_cast<std::byte*>(p) + slots(p)[i];
}
inline const std::byte* nodePtr(const Page* p, unsigned i) noexcept
{
    return reinterpret_cast<const std::byte*>(p) + slots(p)[i];
}
/// Bytes still available for one more node (excluding its 2-byte slot).
inline std::size_t pageRoom(const Page* p) noexcept
{
    return std::size_t(p->upper) - p->lower;
}
inline std::size_t pageUpper(const Page* p) noexcept
{
    return p->upper;
}
inline std::size_t pageUsed(const Page* p, std::size_t pageSize) noexcept
{
    return (pageSize - pageUpper(p)) + (std::size_t(p->lower) - kPageHdr);
}

// ----------------------------------------------------------------- node ---

enum NodeFlags : std::uint16_t
{
    N_BIGDATA = 1u << 0,  ///< value lives on an overflow run; payload is a pgno
    N_SUBDB = 1u << 1,    ///< serialized Tree in the catalog
};

/// Leaf node header, followed by `ksize` key bytes then the payload.
/// For N_BIGDATA the payload is an 8-byte page number and `vsize` is the true
/// value length.
struct LeafNode
{
    Little<std::uint16_t> flags;
    Little<std::uint16_t> ksize;
    Little<std::uint32_t> vsize;
};
static_assert(sizeof(LeafNode) == 8);

/// Branch node header, followed by `ksize` key bytes. The child page number
/// is split into 32+16 bits and the entry count into 32+32 so the struct
/// stays 4-aligned. `count` is the number of leaf entries in the child's
/// subtree: ranks, range counts and skips descend by it in O(log n).
struct BranchNode
{
    Little<std::uint32_t> childLo;
    Little<std::uint16_t> childHi;
    Little<std::uint16_t> ksize;
    Little<std::uint32_t> countLo;
    Little<std::uint32_t> countHi;
};
static_assert(sizeof(BranchNode) == 16);

/// Node payloads are kept 4-byte aligned so the headers above can be read
/// through typed pointers on every supported platform.
inline constexpr std::size_t align4(std::size_t n) noexcept
{
    return (n + 3) & ~std::size_t(3);
}

inline LeafNode* lnode(Page* p, unsigned i) noexcept
{
    return reinterpret_cast<LeafNode*>(nodePtr(p, i));
}
inline const LeafNode* lnode(const Page* p, unsigned i) noexcept
{
    return reinterpret_cast<const LeafNode*>(nodePtr(p, i));
}
inline BranchNode* bnode(Page* p, unsigned i) noexcept
{
    return reinterpret_cast<BranchNode*>(nodePtr(p, i));
}
inline const BranchNode* bnode(const Page* p, unsigned i) noexcept
{
    return reinterpret_cast<const BranchNode*>(nodePtr(p, i));
}

inline PageNo branchChild(const Page* p, unsigned i) noexcept
{
    const BranchNode* n = bnode(p, i);
    return PageNo(n->childLo) | (PageNo(n->childHi) << 32);
}
inline void setBranchChild(Page* p, unsigned i, PageNo child) noexcept
{
    BranchNode* n = bnode(p, i);
    n->childLo = std::uint32_t(child);
    n->childHi = std::uint16_t(child >> 32);
}

/// Leaf entries under branch slot `i`.
inline std::uint64_t branchCount(const Page* p, unsigned i) noexcept
{
    const BranchNode* n = bnode(p, i);
    return std::uint64_t(n->countLo) | (std::uint64_t(n->countHi) << 32);
}
inline void setBranchCount(Page* p, unsigned i, std::uint64_t count) noexcept
{
    BranchNode* n = bnode(p, i);
    n->countLo = std::uint32_t(count);
    n->countHi = std::uint32_t(count >> 32);
}
/// Leaf entries in the subtree rooted at `p`.
inline std::uint64_t pageEntries(const Page* p) noexcept
{
    if (isLeaf(p))
        return p->nkeys;
    std::uint64_t total = 0;
    for (unsigned i = 0; i < p->nkeys; ++i)
        total += branchCount(p, i);
    return total;
}

inline Slice nodeKey(const Page* p, unsigned i) noexcept
{
    if (isLeaf(p)) {
        const LeafNode* n = lnode(p, i);
        return Slice(reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode), n->ksize);
    }
    const BranchNode* n = bnode(p, i);
    return Slice(reinterpret_cast<const std::byte*>(n) + sizeof(BranchNode), n->ksize);
}

/// Bytes occupied by node `i`, slot excluded.
inline std::size_t nodeSize(const Page* p, unsigned i) noexcept
{
    if (isLeaf(p)) {
        const LeafNode* n = lnode(p, i);
        const std::size_t payload = (n->flags & N_BIGDATA) ? sizeof(PageNo) : std::size_t(n->vsize);
        return align4(sizeof(LeafNode) + n->ksize + payload);
    }
    return align4(sizeof(BranchNode) + bnode(p, i)->ksize);
}

inline std::size_t leafNodeSize(std::size_t ksize, std::size_t vsize, bool big) noexcept
{
    return align4(sizeof(LeafNode) + ksize + (big ? sizeof(PageNo) : vsize));
}
inline std::size_t branchNodeSize(std::size_t ksize) noexcept
{
    return align4(sizeof(BranchNode) + ksize);
}

/// Overflow page number stored in a N_BIGDATA leaf payload.
inline PageNo bigPgno(const LeafNode* n) noexcept
{
    return readLittle<PageNo>(reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode) + n->ksize);
}
inline void setBigPgno(LeafNode* n, PageNo v) noexcept
{
    writeLittle(reinterpret_cast<std::byte*>(n) + sizeof(LeafNode) + n->ksize, v);
}

// --------------------------------------------------------------- limits ---

/// Largest node (slot included) we allow on a page: guarantees >= 2 per page,
/// which is what the split algorithm needs.
inline constexpr std::size_t maxNodeSize(std::size_t pageSize) noexcept
{
    return ((pageSize - kPageHdr - kPageChecksumBytes) / 2) & ~std::size_t(3);
}
/// Keys are capped at a quarter page so a branch page always holds >= 4.
inline constexpr std::size_t maxKeySize(std::size_t pageSize) noexcept
{
    return (((pageSize - kPageHdr) / 4) & ~std::size_t(3)) - sizeof(BranchNode) - 8;
}

// ------------------------------------------------------------ free list ---

/// Free-list keys name the transaction that retired the pages and a chunk
/// number within it, big-endian in both halves so that byte order is numeric
/// order under the default comparison.
inline constexpr std::size_t kFreeKeyBytes = 16;

struct FreeKey
{
    std::array<std::byte, kFreeKeyBytes> bytes{};
    Slice slice() const noexcept { return Slice(bytes.data(), bytes.size()); }
};

inline void writeBig(std::byte* out, std::uint64_t v) noexcept
{
    for (int i = 7; i >= 0; --i, v >>= 8)
        out[i] = std::byte(v & 0xff);
}
inline std::uint64_t readBig(const std::byte* in) noexcept
{
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v = (v << 8) | std::to_integer<std::uint64_t>(in[i]);
    return v;
}

/// The chunk number an inline free-list entry (Meta::inlineTxn) takes when
/// it moves into the tree: never one a commit writes for its own chunks.
inline constexpr std::uint64_t kInlineChunk = ~std::uint64_t(0);

/// Chunk numbers with the top bit set hold pages a commit absorbed and did
/// not spend, filed back under an older id (Durability::Lazy and None):
/// the writing commit's id above, the chunk's index below, so two commits
/// never write the same key. Ids from kRemainderTxnLimit on do not fit and
/// file such pages under their own id instead.
inline constexpr std::uint64_t kRemainderChunks = std::uint64_t(1) << 15;
inline constexpr std::uint64_t kRemainderTxnLimit = std::uint64_t(1) << 47;
inline constexpr std::uint64_t remainderChunk(TxnId writer, std::uint64_t index) noexcept
{
    return (std::uint64_t(1) << 63) | (writer << 15) | index;
}

/// Chunk numbers with bit 62 set (and 63 clear) hold pages a commit retired
/// that were allocated while `epoch` was the newest durable commit, so that
/// snapshot never named them (Durability::Lazy and None). While it is still
/// the newest durable one, only readers hold such pages back.
inline constexpr std::uint64_t exemptChunk(TxnId epoch, std::uint64_t index) noexcept
{
    return (std::uint64_t(1) << 62) | (epoch << 15) | index;
}
inline constexpr bool isExemptChunk(std::uint64_t chunk) noexcept
{
    return (chunk >> 62) == 1;
}
inline constexpr TxnId exemptEpochOf(std::uint64_t chunk) noexcept
{
    return (chunk & ~(std::uint64_t(3) << 62)) >> 15;
}

inline FreeKey freeKey(TxnId txnid, std::uint64_t chunk) noexcept
{
    FreeKey k;
    writeBig(k.bytes.data(), txnid);
    writeBig(k.bytes.data() + 8, chunk);
    return k;
}
inline TxnId freeKeyTxn(Slice key) noexcept
{
    return readBig(key.data());
}
inline std::uint64_t freeKeyChunk(Slice key) noexcept
{
    return readBig(key.data() + 8);
}

/// Pages per free-list chunk: as many as fit one leaf node held inline, so a
/// chunk is never an overflow run and rewriting one costs a node, not pages.
inline constexpr std::size_t freeChunkPages(std::size_t pageSize) noexcept
{
    return (maxNodeSize(pageSize) - sizeof(LeafNode) - kFreeKeyBytes - 8) / sizeof(PageNo);
}

// ----------------------------------------------------------------- meta ---

/// Root records live in meta or in catalog N_SUBDB values.
struct Tree
{
    Little<std::uint64_t> root;
    Little<std::uint64_t> branchPages;
    Little<std::uint64_t> leafPages;
    Little<std::uint64_t> overflowPages;
    Little<std::uint64_t> entries;
    Little<std::uint32_t> depth;
    Little<std::uint32_t> flags;
};
static_assert(sizeof(Tree) == 48);

inline Tree emptyTree(std::uint32_t flags = 0) noexcept
{
    Tree t{};
    t.root = kInvalidPage;
    t.flags = flags;
    return t;
}

/// A named tree's root record kept in the meta page instead of the catalog
/// (Meta::roots). The catalog keeps the name -- it is what says the tree
/// exists -- but while a slot holds the name, the slot's record is the one
/// that counts and the catalog's is stale. A commit then rewrites only the
/// meta page for the record, not the catalog leaf as well.
struct RootSlot
{
    char name[23];
    std::uint8_t length;  ///< 0: empty slot
    Tree tree;
    Little<std::uint64_t> written;  ///< txnid that last wrote it (eviction order)
};
static_assert(sizeof(RootSlot) == 80);
inline constexpr std::size_t kRootNameMax = sizeof(RootSlot::name);

struct Meta
{
    char magic[16];
    Little<std::uint32_t> version;
    Little<std::uint32_t> pageSize;
    Little<std::uint64_t> txnid;
    Little<std::uint64_t> lastPgno;
    Little<std::uint64_t> fileSize;
    Little<std::uint64_t> namedDbs;
    Tree freeTree;
    Tree mainTree;
    Little<std::uint64_t> checksum;
    Identity storeId{};
    Identity commitId{};
    Identity parentId{};
    Little<std::uint64_t> parentChecksum = 0;
    /// `deferredPages` holds two lists back to back: first `deferredCount`
    /// pages freed too late to be recorded by the commit that freed them,
    /// then `inlineCount` pages retired by transaction `inlineTxn` -- a
    /// free-list entry small enough to live here rather than in the free
    /// tree, which then needs no page of its own rewritten per commit.
    Little<std::uint32_t> deferredCount = 0;
    Little<std::uint32_t> inlineCount = 0;
    Little<std::uint64_t> inlineTxn = 0;
    std::array<Little<PageNo>, 26> deferredPages{};
    Tree catalogTree = emptyTree();
    std::array<RootSlot, 6> roots{};
};
static_assert(offsetof(Meta, storeId) == 160);
static_assert(sizeof(Meta) == 968);
static_assert(kPageHdr + sizeof(Meta) <= kMinPageSize);

inline Meta* metaOf(Page* p) noexcept
{
    return reinterpret_cast<Meta*>(reinterpret_cast<std::byte*>(p) + kPageHdr);
}
inline const Meta* metaOf(const Page* p) noexcept
{
    return reinterpret_cast<const Meta*>(reinterpret_cast<const std::byte*>(p) + kPageHdr);
}

inline std::uint64_t metaChecksum(const Meta& m) noexcept
{
    Meta encoded = m;
    encoded.checksum = 0;
    const auto checksum = checksum64(&encoded, sizeof encoded);
    return checksum ? checksum : 1;
}

inline bool magicOk(const Meta& m) noexcept
{
    return std::memcmp(m.magic, kMagic, sizeof kMagic) == 0;
}

/// The format number of the store as of `m`.
inline std::uint32_t metaVersion(const Meta& m) noexcept
{
    return std::uint32_t(m.version) & 0xffffu;
}
inline bool metaSealed(const Meta& m) noexcept
{
    return (std::uint32_t(m.version) & kMetaSealed) != 0;
}

/// The record itself is whole and one this build reads. A sealed one must also pass
/// sealedMetaLanded() before it is taken for a snapshot.
inline bool metaValid(const Meta& m, std::size_t expectPageSize) noexcept
{
    const std::uint32_t version = metaVersion(m);
    return magicOk(m) && version >= kOldestFormatVersion && version <= kFormatVersion &&
           (std::uint32_t(m.version) & ~(0xffffu | kMetaSealed)) == 0 &&
           (!metaSealed(m) || version >= kFormatSealed) &&
           (expectPageSize == 0 || m.pageSize == expectPageSize) && m.checksum == metaChecksum(m);
}

// ------------------------------------------------------------- manifest ---
//
// A sealed commit writes its pages and its meta page and flushes once. The device may
// then hold the meta without some of the pages, so the meta page lists, after the
// record, every page or overflow run the commit wrote with the checksum in its trailer.
// Whoever picks the newest meta checks the list; a meta that fails is one that was
// never written, and the other slot's is taken, exactly as for a torn meta page.

struct ManifestHeader
{
    Little<std::uint64_t> txnid;         ///< the meta's
    Little<std::uint64_t> metaChecksum;  ///< the meta's: a manifest belongs to one record
    Little<std::uint32_t> count;         ///< entries that follow
    Little<std::uint32_t> reserved;
    Little<std::uint64_t> checksum;      ///< of this header with `checksum` zero, then the entries
};
static_assert(sizeof(ManifestHeader) == 32);

/// One page, or one overflow run, a sealed commit wrote.
struct ManifestEntry
{
    Little<std::uint64_t> run;       ///< first page number, and the page count above bit 48
    Little<std::uint64_t> checksum;  ///< the trailer of the page or run
};
static_assert(sizeof(ManifestEntry) == 16);
inline constexpr std::uint64_t kManifestRunMax = 0xffff;  ///< pages in one entry

inline constexpr std::size_t kManifestOffset = kPageHdr + sizeof(Meta);
/// Entries a meta page of `pageSize` has room for; none at the smallest page size.
inline constexpr std::size_t manifestCapacity(std::size_t pageSize) noexcept
{
    return pageSize < kManifestOffset + sizeof(ManifestHeader)
               ? 0
               : (pageSize - kManifestOffset - sizeof(ManifestHeader)) / sizeof(ManifestEntry);
}

/// Fills in `h` for meta `m` and `count` entries at `entries`.
inline void sealManifest(ManifestHeader& h, const Meta& m, const ManifestEntry* entries,
                         std::size_t count) noexcept
{
    h.txnid = m.txnid;
    h.metaChecksum = m.checksum;
    h.count = std::uint32_t(count);
    h.reserved = 0;
    h.checksum = 0;
    Checksum64 sum;
    sum.update(&h, sizeof h);
    if (count)
        sum.update(entries, count * sizeof(ManifestEntry));
    h.checksum = sum.digest();
}

/// Whether sealed meta `m`, read from a store of `fileBytes` bytes, was written whole:
/// its manifest is there and is its own, and every page it lists is in the file with the
/// checksum listed and bytes that match it. `readAt(offset, out, n)` reads the store;
/// `scratch` is a buffer of at least one page, grown here for longer runs.
template <class ReadAt, class Buffer>
bool sealedMetaLanded(const Meta& m, unsigned slot, std::uint64_t fileBytes, ReadAt&& readAt,
                      Buffer& scratch)
{
    const std::size_t ps = m.pageSize;
    const std::size_t capacity = manifestCapacity(ps);
    if ((std::uint64_t(m.lastPgno) + 1) * ps > fileBytes)
        return false;  // the file never grew to hold it
    ManifestHeader h;
    const std::uint64_t at = std::uint64_t(slot) * ps + kManifestOffset;
    if (capacity == 0)
        return false;
    readAt(at, &h, sizeof h);
    if (h.txnid != m.txnid || h.metaChecksum != m.checksum || h.count > capacity || h.reserved != 0)
        return false;
    const std::size_t count = h.count;
    if (scratch.size() < ps)
        scratch.resize(ps);
    // The entries fit one page; keep them apart from the pages read below.
    std::vector<ManifestEntry> entries(count);
    if (count)
        readAt(at + sizeof h, entries.data(), count * sizeof(ManifestEntry));
    const std::uint64_t stored = h.checksum;
    ManifestHeader check = h;
    sealManifest(check, m, entries.data(), count);
    if (check.checksum != stored)
        return false;
    for (const ManifestEntry& e : entries) {
        const std::uint64_t first = std::uint64_t(e.run) & kMaxPgno;
        const std::uint64_t pages = std::uint64_t(e.run) >> 48;
        if (first < 2 || pages == 0 || first + pages - 1 > m.lastPgno)
            return false;
        const std::size_t bytes = std::size_t(pages * ps);
        if (scratch.size() < bytes)
            scratch.resize(bytes);
        readAt(first * ps, scratch.data(), bytes);
        const auto* data = reinterpret_cast<const std::byte*>(scratch.data());
        const Page* page = reinterpret_cast<const Page*>(data);
        if (page->pgno != first || (isOverflow(page) ? ovPages(page) : 1u) != pages)
            return false;
        if (readLittle<std::uint64_t>(data + bytes - kPageChecksumBytes) != e.checksum ||
            checksum64(data, bytes - kPageChecksumBytes) != e.checksum)
            return false;
    }
    return true;
}

// The two meta slots alternate by transaction id parity, so the previous
// snapshot always survives a torn write of the newest one.
inline unsigned metaSlotFor(TxnId id) noexcept
{
    return unsigned(id & 1);
}

inline void sealPage(Page* page, std::size_t bytes)
{
    writeLittle(reinterpret_cast<std::byte*>(page) + bytes - kPageChecksumBytes,
                checksum64(page, bytes - kPageChecksumBytes));
}

inline void validatePage(const Page* page, std::size_t pageSize, std::uint64_t availablePages)
{
    const std::uint64_t count = isOverflow(page) ? ovPages(page) : 1;
    if (!count || count > availablePages)
        throw Error(ErrorCode::Corrupted, "page checksum span exceeds allocation frontier");
    const std::size_t bytes = std::size_t(count * pageSize);
    const auto* data = reinterpret_cast<const std::byte*>(page);
    if (readLittle<std::uint64_t>(data + bytes - kPageChecksumBytes) != checksum64(data, bytes - kPageChecksumBytes))
        throw Error(ErrorCode::Corrupted, "page checksum mismatch");
}

}  // namespace nosql::internal
