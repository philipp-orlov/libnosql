// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Commit capture. The environment owns one of these when it was opened with
// Env::Options::shipTo(); nothing else touches it.
#pragma once

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <utility>
#include <vector>

#include "nosql/internal/replication_format.hpp"
#include "nosql/internal/os.hpp"

namespace nosql::internal {

/// Appends each commit's changed pages to a segment file.
///
/// Called on the committing thread once the commit is already durable, so it
/// can never fail the transaction: any error closes the current segment and
/// the next commit starts a new one, leaving a gap that readers detect and
/// answer with NeedBase. A lost segment costs a resync, never a commit, which
/// is why segments are not fsynced.
class ShipWriter
{
public:
    ShipWriter(std::filesystem::path dir, std::size_t pageSize, std::uint64_t segmentBytes,
               unsigned retain);
    ~ShipWriter();

    ShipWriter(const ShipWriter&) = delete;
    ShipWriter& operator=(const ShipWriter&) = delete;

    /// `runs` are the inclusive page ranges the commit flushed and `mapBase`
    /// the mapping it flushed them through -- the bytes now on disk, not the
    /// dirty buffers, so what ships is exactly what the store holds.
    void capture(const Meta& meta, const std::vector<std::pair<PageNo, PageNo>>& runs,
                 const std::byte* mapBase) noexcept;
    std::uint64_t failures() const noexcept { return failures_.load(); }
    std::uint64_t capturedTxnid() const noexcept { return captured_.load(); }

private:
    void openSegment(TxnId firstTxnid);
    void closeSegment() noexcept;
    void pruneOld() noexcept;
    void append(const void* p, std::size_t n);
    void flushBuffer();

    std::filesystem::path dir_;
    std::size_t pageSize_;
    std::uint64_t segmentBytes_;
    unsigned retain_;
    os::FileHandle fd_ = os::kInvalidFile;
    os::FileHandle indexFd_ = os::kInvalidFile;
    std::uint64_t indexOffset_ = 0;
    std::uint64_t offset_ = 0;
    std::vector<std::byte> buf_;
    Checksum64 checksum_;
    std::atomic<std::uint64_t> failures_{0}, captured_{0};
};

/// "000000000042.seg" -- fixed width so the directory sorts by transaction id.
std::string segmentName(TxnId firstTxnid);

}  // namespace nosql::internal
