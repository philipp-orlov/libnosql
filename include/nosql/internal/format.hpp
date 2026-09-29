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
inline constexpr std::uint32_t kFormatVersion = 6;
inline constexpr std::size_t kPageChecksumBytes = 8;

using Identity = std::array<std::uint64_t, 2>;

/// A fresh 128-bit identity: never zero, drawn from a generator seeded once
/// per thread from std::random_device rather than from the device itself,
/// which on some platforms is a system call or a file read per word.
Identity newIdentity() noexcept;

inline constexpr std::size_t kMinPageSize = 512;
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
};

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
/// is split into 32+16 bits so the struct stays 4-aligned and 8 bytes wide.
struct BranchNode
{
    Little<std::uint32_t> childLo;
    Little<std::uint16_t> childHi;
    Little<std::uint16_t> ksize;
};
static_assert(sizeof(BranchNode) == 8);

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
    Little<std::uint64_t> deferredCount = 0;
    std::array<Little<PageNo>, 26> deferredPages{};
    Tree catalogTree = emptyTree();
};
static_assert(offsetof(Meta, storeId) == 160);
static_assert(sizeof(Meta) == 480);
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

inline bool metaValid(const Meta& m, std::size_t expectPageSize) noexcept
{
    return magicOk(m) && m.version == kFormatVersion &&
           (expectPageSize == 0 || m.pageSize == expectPageSize) && m.checksum == metaChecksum(m);
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
