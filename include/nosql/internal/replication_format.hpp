// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Bundles and segments use little-endian integers and in-tree XXH3-64 checksums. Their version
// (kReplicationVersion) is independent of the store's kFormatVersion.
#pragma once

#include <cstdint>
#include <cstring>

#include "nosql/internal/format.hpp"

namespace nosql::internal {

/// Both are `char[16]` to match the store's own `Meta::magic`; each string is
/// 15 bytes plus its terminator and does not fit the 8 the sketch assumed.
inline constexpr char kBundleMagic[16] = "REP-NOSQL-ORLOV";   ///< bundles
inline constexpr char kSegmentMagic[16] = "SEG-NOSQL-ORLOV";  ///< shipping segments
inline constexpr std::uint32_t kReplicationVersion = 5;

/// 72 bytes, no padding, every 64-bit field naturally aligned. The design
/// sketch said 64, which only held with the 8-byte magic it assumed.
struct BundleHeader
{
    char magic[16];                  ///< kBundleMagic
    Little<std::uint32_t> version;
    Little<std::uint32_t> pageSize;
    Little<std::uint64_t> baseTxnid;
    Little<std::uint64_t> baseMetaChecksum;
    Little<std::uint64_t> targetTxnid;
    Little<std::uint64_t> pageCount;
    Little<std::uint64_t> payloadChecksum;
    Little<std::uint64_t> flags;
};
static_assert(sizeof(BundleHeader) == 72);

struct BundleIdentity
{
    Identity storeId;
    Identity baseCommitId;
};
static_assert(sizeof(BundleIdentity) == 32);

/// Followed by `byteLength` bytes of page image.
struct PageRecord
{
    Little<std::uint64_t> pgno;
    Little<std::uint32_t> byteLength;
    Little<std::uint32_t> reserved;
    Little<std::uint64_t> checksum;
};
static_assert(sizeof(PageRecord) == 24);

// BundleIdentity follows the header; target Meta is last.

/// One segment file: a header, then one entry per captured commit.
struct SegmentHeader
{
    char magic[16];  ///< kSegmentMagic
    Little<std::uint32_t> version;
    Little<std::uint32_t> pageSize;
    Little<std::uint64_t> firstTxnid;
};
static_assert(sizeof(SegmentHeader) == 32);

/// Followed by page records, Meta, and an XXH3-64 digest of the preceding entry bytes.
struct SegmentEntry
{
    Little<std::uint64_t> txnid;
    Little<std::uint64_t> pageCount;
    Little<std::uint64_t> byteLength;
};
static_assert(sizeof(SegmentEntry) == 24);

inline std::uint64_t segmentEntryBytes(std::uint64_t pageCount, std::size_t pageSize)
{
    return pageCount * (sizeof(PageRecord) + pageSize) + sizeof(Meta) + sizeof(std::uint64_t);
}

struct SegmentIndexRecord
{
    Little<std::uint64_t> offset;
    Little<std::uint64_t> pageCount;
    Meta meta;
    Little<std::uint64_t> frameChecksum;
    Little<std::uint64_t> checksum;
};
static_assert(sizeof(SegmentIndexRecord) == 512);

}  // namespace nosql::internal
