// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Looking inside a store: its meta pages, trees, pages, nodes and the free list, as plain structs. For tools that
// draw or debug the file (the sql tool's Visualize tab); nothing here is on a hot path and nothing writes.
//
// Everything reads one snapshot, a read transaction's (`Env::readTxn()`), so it shows what is committed, not what
// a writer holds pending. Pages are read straight from the mapping without the usual checksum test, so a damaged
// page can be looked at: its `hashOk` says what the test would have said.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"

namespace nosql::inspect {

/// What a page is, for the file map.
enum class PageKind : std::uint8_t
{
    Meta,      ///< pages 0 and 1
    Branch,    ///< an inner B+tree page
    Leaf,      ///< a B+tree page holding entries
    Overflow,  ///< the first page of a run that holds one large value
    Continued, ///< a later page of an overflow run
    Free,      ///< on the free list (or deferred): its bytes are stale
    Unknown,   ///< neither reachable from a tree nor free (damage, or pages a writer holds)
};

const char* toString(PageKind) noexcept;

/// How a tree is used by the store itself.
enum class TreeRole : std::uint8_t
{
    Free,     ///< the free list
    Main,     ///< the unnamed main tree
    Catalog,  ///< names and roots of the sub-databases
    Named,    ///< a sub-database
};

const char* toString(TreeRole) noexcept;

struct TreeInfo
{
    std::string name;  ///< empty for Free, Main and Catalog
    TreeRole role = TreeRole::Named;
    std::uint64_t root = 0;  ///< ~0 when the tree is empty
    std::uint32_t depth = 0;
    std::uint64_t branchPages = 0;
    std::uint64_t leafPages = 0;
    std::uint64_t overflowPages = 0;
    std::uint64_t entries = 0;
    std::uint32_t flags = 0;  ///< DbFlags in the low 16 bits; the integer key width in the next 16
};

struct MetaInfo
{
    unsigned slot = 0;  ///< 0 or 1 (the page number)
    bool valid = false;  ///< magic, version and checksum
    bool current = false;  ///< the newer valid one: the one a reader opens
    std::uint32_t version = 0;
    std::uint32_t pageSize = 0;
    std::uint64_t txnid = 0;
    std::uint64_t lastPgno = 0;
    std::uint64_t fileSize = 0;
    std::uint64_t namedDbs = 0;
    std::uint64_t checksum = 0;  ///< as stored
    std::uint64_t computedChecksum = 0;
    std::string storeId, commitId, parentId;  ///< 32 hex digits
    std::uint64_t parentChecksum = 0;
    std::uint32_t deferredCount = 0;  ///< pages freed too late for the commit that freed them
    std::uint32_t inlineCount = 0;
    std::uint64_t inlineTxn = 0;
    std::vector<TreeInfo> trees;  ///< free list, main, catalog, and the sub-databases in the meta's root slots
};

struct PageInfo
{
    std::uint64_t pgno = 0;
    PageKind kind = PageKind::Unknown;
    std::uint16_t flags = 0;  ///< the raw P_* bits
    bool prefixKeys = false;  ///< the page stores a shared key prefix once
    bool suffixKeys = false;  ///< ... or a shared suffix
    std::uint32_t pageSize = 0;
    std::uint16_t nkeys = 0;
    std::uint16_t lower = 0;  ///< end of the slot array
    std::uint16_t upper = 0;  ///< start of the node area
    std::uint32_t sharedBytes = 0;  ///< bytes of the shared prefix/suffix
    std::uint32_t sharedBlock = 0;  ///< room that block takes
    std::uint32_t freeBytes = 0;    ///< the gap between slots and nodes
    std::uint32_t usedBytes = 0;    ///< header, slots and nodes
    std::uint64_t entries = 0;      ///< leaf entries below (a leaf: its own)
    std::uint32_t runPages = 1;     ///< overflow runs: pages in the run
    std::uint32_t runBytes = 0;     ///< ... and the bytes of the value it holds, headers excluded
    std::uint64_t storedHash = 0;
    std::uint64_t computedHash = 0;
    bool hashOk = false;
    bool headerOk = false;  ///< the page number in the header matches, flags are legal, offsets are in range
};

struct NodeInfo
{
    std::uint32_t index = 0;
    std::uint32_t offset = 0;  ///< of the node in the page
    std::uint32_t size = 0;    ///< bytes the node takes
    std::string key;           ///< whole (shared bytes included); empty for branch slot 0
    std::uint32_t keyBytes = 0;  ///< length of `key` before any cut
    std::uint32_t valueBytes = 0;  ///< leaf: the value's true length
    std::string value;             ///< leaf: its first bytes (up to the preview limit)
    bool big = false;      ///< the value lives on an overflow run
    bool subdb = false;    ///< the value is a sub-database descriptor (catalog)
    std::uint64_t child = 0;     ///< branch: the child page
    std::uint64_t count = 0;     ///< branch: leaf entries below it
    std::uint64_t overflow = 0;  ///< big values: the first page of the run
};

/// A stretch of consecutive pages of one kind and owner.
struct MapRun
{
    std::uint64_t first = 0;
    std::uint64_t count = 0;
    PageKind kind = PageKind::Unknown;
    std::int32_t tree = -1;  ///< index into trees(); -1 for meta, free and unknown pages
};

/// One free-list entry: pages retired by a transaction.
struct FreeEntry
{
    std::uint64_t txn = 0;    ///< the transaction that retired them
    std::uint64_t chunk = 0;  ///< the entry's number within it
    std::uint64_t pages = 0;
    std::vector<std::uint64_t> first;  ///< some of the pages
    bool deferred = false;  ///< from the meta page, not the free tree
};

struct CheckResult
{
    std::uint64_t pagesChecked = 0;
    std::vector<std::uint64_t> badPages;  ///< pages whose hash does not match their bytes (up to a limit)
    std::uint64_t badCount = 0;
    std::string integrity;  ///< empty when checkIntegrity() found nothing, else its message
    bool ok() const noexcept { return badCount == 0 && integrity.empty(); }
};

/// The free list, the main tree, the catalog and every sub-database, in that order.
std::vector<TreeInfo> trees(Txn& t);
/// Both meta pages, as the file holds them.
std::vector<MetaInfo> metas(Txn& t);
PageInfo page(Txn& t, std::uint64_t pgno);
/// Every node of a branch or leaf page. Keys and values are cut at `preview` bytes.
std::vector<NodeInfo> nodes(Txn& t, std::uint64_t pgno, std::size_t preview = 64);
/// The raw bytes of `pages` pages from `pgno` (an overflow run is one value).
std::string pageBytes(Txn& t, std::uint64_t pgno, unsigned pages = 1);
/// What every page of the file is, in runs. Throws ErrorCode::Unsupported-like Error above kMapPageLimit pages.
std::vector<MapRun> map(Txn& t, const std::vector<TreeInfo>& treesOfMap);
inline constexpr std::uint64_t kMapPageLimit = std::uint64_t(1) << 24;
/// Free-list entries, newest first; at most `maxPages` page numbers are listed in all.
std::vector<FreeEntry> freeList(Txn& t, std::size_t maxPages = 256);
/// Every page's hash, then the structural check. At most `maxBad` bad pages are listed.
CheckResult check(Txn& t, std::size_t maxBad = 256);

}  // namespace nosql::inspect
