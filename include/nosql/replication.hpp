// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Shipping incremental changes from a checkpoint to a copy of the store.
//
// Coalesced deltas publish a fully flushed replacement file, never an in-place partial apply.
// See docs/replication.md for the reasoning behind every choice here.
#pragma once

#include <array>
#include <cstdint>
#include <filesystem>

#include "nosql/nosql.hpp"

namespace nosql {

/// Store and commit identities distinguish unrelated stores and divergent clones.
struct Checkpoint
{
    std::uint64_t txnid = 0;
    std::uint64_t metaChecksum = 0;
    std::array<std::uint64_t, 2> storeId{};
    std::array<std::uint64_t, 2> commitId{};

    /// A store that has never committed still has a valid checkpoint at txn 0;
    /// the checksum is never zero, so it is what marks an unset one.
    bool valid() const noexcept { return metaChecksum != 0; }
    explicit operator bool() const noexcept { return valid(); }
    friend bool operator==(const Checkpoint& left, const Checkpoint& right) noexcept
    {
        return left.txnid == right.txnid && left.metaChecksum == right.metaChecksum &&
               left.storeId == right.storeId && left.commitId == right.commitId;
    }
    friend bool operator!=(const Checkpoint& left, const Checkpoint& right) noexcept
    {
        return !(left == right);
    }
};

/// Newest valid checkpoint in a store file. Reads the two meta pages
/// directly, so the store may be open elsewhere and need not be this process's.
Checkpoint checkpointOf(const std::filesystem::path& store);

/// Copies the snapshot `txn` is reading to `dst`, which must not already
/// exist. The copy opens at the same checkpoint, so a delta chain continues
/// onto it directly -- this is how a replica is bootstrapped.
///
/// Holds the snapshot for the duration, which pins page reclamation on the
/// source: expect the source file to grow by roughly the write volume that
/// lands while the copy runs.
void copySnapshot(Txn& txn, const std::filesystem::path& dst);

// ----------------------------------------------------------------- bundle ---

/// What a bundle carries, read from its header alone.
struct BundleInfo
{
    std::uint32_t pageSize = 0;
    Checkpoint base;               ///< the checkpoint it must be applied onto
    std::uint64_t targetTxnid = 0; ///< the checkpoint it produces
    std::uint64_t pageCount = 0;
    std::uint64_t fileSize = 0;    ///< bytes the target store must be grown to
};

BundleInfo inspectBundle(const std::filesystem::path& bundle);

/// Applies `bundle` to `store`, which must be closed and must currently sit
/// on the bundle's base checkpoint. Returns the checkpoint it now holds.
///
/// Uses a flushed temporary copy and atomic replacement; requires temporary disk space.
///
/// Throws ErrorCode::Incompatible when the store is not on the expected base
/// -- which is the divergence check, and the reason it is worth having.
Checkpoint applyBundle(const std::filesystem::path& store,
                       const std::filesystem::path& bundle);

// --------------------------------------------------------------- shipping ---

enum class ShipStatus
{
    Ok = 0,     ///< a bundle was written
    UpToDate,   ///< the base is already the newest captured transaction
    NeedBase,   ///< the range has aged out; send a base image instead
};

const char* toString(ShipStatus) noexcept;

/// Reader for the segment directory a primary fills when it is opened with
/// `Env::Options::shipTo()`.
///
/// Segments are a shipping artefact, not a recovery one. Losing them costs a
/// resync, never a commit, which is why they are written without their own
/// fsync and why `NeedBase` is an ordinary answer rather than an error.
class ShipLog
{
public:
    /// The transaction range the retained segments can serve.
    struct Range
    {
        std::uint64_t oldest = 0;  ///< first transaction any retained segment carries
        std::uint64_t newest = 0;  ///< last
        bool empty() const noexcept { return oldest == 0; }
    };

    explicit ShipLog(std::filesystem::path directory);

    const std::filesystem::path& directory() const noexcept { return dir_; }
    Range available() const;

    /// Writes a bundle advancing `base` up to `through` (0 = as far as the
    /// retained segments go). Nothing is written unless the result is `Ok`.
    ///
    /// Coalesces: a page touched by every transaction in the range -- the tree
    /// root, always -- ships once, with its final contents. That is only sound
    /// because the bundle is applied as a unit and no intermediate meta is
    /// ever published, so a coalesced bundle must never be split.
    /// memoryBytes bounds the disposable coalescing index's dirty buffers, not total RSS.
    ShipStatus extract(const Checkpoint& base, const std::filesystem::path& bundle,
                       std::uint64_t through = 0, std::size_t memoryBytes = 8u << 20) const;

    /// Deletes whole segments that carry nothing at or after `keepFrom`.
    /// Returns how many files were removed.
    std::size_t prune(std::uint64_t keepFrom) const;

private:
    std::filesystem::path dir_;
};

}  // namespace nosql
