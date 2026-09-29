// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Blob storage in a side-car tar archive, indexed by a nosql sub-database.
//
// The archive is the source of truth and the index is derived: every entry can
// be recovered by walking tar headers, so a stale, missing or damaged index is
// repaired by rescanning rather than by a cross-file commit protocol. That is
// what lets the write path skip an fsync per blob, and it is why the archive
// stays readable by tar(1) -- add, list and extract with the tools you already
// have, then rebuild the index.
//
// Reads are zero-copy: a Blob points straight into a read-only mapping of the
// archive and keeps that mapping alive for as long as it exists.
//
// The layer is shaped for workloads that write a corpus once and then read it
// back at random many times over -- serving assets, or feeding a training
// loop. Writes coalesce into large appends, lookups can be batched against one
// snapshot, and the next batch's pages can be requested from the OS while the
// current one is still being consumed.
#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/slice.hpp"

namespace nosql {

namespace internal {
struct TarMapping;
}  // namespace internal

/// A borrowed view of one blob's bytes and its archive member name.
///
/// `data()` addresses a read-only mapping of the archive directly; nothing is
/// copied on the way out. A Blob holds a reference to the mapping it was read
/// through, so those bytes stay addressable for the Blob's whole lifetime --
/// including after the transaction that produced it has ended, and across
/// appends that grow the archive (growth remaps, it never resizes in place).
///
/// Holding many Blobs from before a growth keeps the older, smaller mappings
/// alive; that costs address space, not memory, since the pages are shared.
///
/// Copyable and cheap to copy. `name()` is owned by the Blob, so unlike a
/// Slice read from a transaction it does not dangle.
class Blob
{
public:
    Blob() = default;

    /// False for a lookup that found nothing; every accessor below is
    /// meaningless in that case.
    bool valid() const noexcept { return data_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

    /// The payload, pointing into the archive mapping. Valid while this Blob
    /// is alive; copy it if it must outlive the Blob.
    Slice data() const noexcept { return Slice(data_, std::size_t(size_)); }
    /// The tar member name, i.e. what `tar tvf` lists and what a rebuild sees.
    std::string_view name() const noexcept { return name_; }
    std::uint64_t size() const noexcept { return size_; }
    /// Byte offset of the payload inside the archive, i.e. what a plain
    /// `pread(fd, buf, size(), offset())` would need. Always 512-aligned,
    /// because tar payloads start on a block boundary. Useful for handing the
    /// range to sendfile(), io_uring or another process.
    std::uint64_t offset() const noexcept { return offset_; }
    /// XXH3-64 digest recorded at append; consult hasChecksum() after a header-only rebuild.
    std::uint64_t storedChecksum() const noexcept { return checksum_; }
    bool hasChecksum() const noexcept { return checksumKnown_; }
    /// Recomputes the checksum over the payload and compares. O(size). Returns
    /// true when no checksum was recorded, so it never reports a false alarm
    /// on a rebuilt index.
    bool verify() const;

private:
    friend class BlobStorage;
    std::shared_ptr<const internal::TarMapping> map_;
    const std::byte* data_ = nullptr;
    std::uint64_t size_ = 0;
    std::uint64_t offset_ = 0;
    std::uint64_t checksum_ = 0;
    bool checksumKnown_ = false;
    std::string name_;
};

/// One member as found by walking the archive's headers, independently of
/// whatever the index happens to say about it.
struct TarEntry
{
    std::string name;
    std::uint64_t offset = 0;  ///< payload offset, always 512-aligned
    std::uint64_t size = 0;
};

/// Blob storage in a tar archive beside the store, indexed by a sub-database.
///
/// Threading mirrors the store's own model: any number of threads may read
/// concurrently, and one may write. A Writer holds the store's write
/// transaction for its whole lifetime, so it blocks other writers -- keep
/// batches bounded if other work needs to commit.
///
/// Move-only, and it borrows the Env: the Env must outlive it.
class BlobStorage
{
public:
    /// How the payloads will be read, passed on to the OS as a paging hint for
    /// the archive mapping. Purely advisory; every mode returns the same bytes.
    enum class Access
    {
        /// The OS's default read-ahead heuristics. Right when nothing is known.
        Normal,
        /// Lookups land all over the archive, as a shuffled training epoch does.
        /// Turns kernel read-ahead off, so a 4 KiB blob costs 4 KiB of I/O
        /// instead of the 128 KiB window around it. Pair with prefetch() to
        /// get the pages you *do* want moving early.
        Random,
        /// The archive is consumed front to back. Read-ahead is widened, and
        /// pages behind the cursor may be dropped sooner.
        Sequential,
    };

    class Options
    {
    public:
        /// Directory holding the archive. Defaults to the store's own
        /// directory. Point it at another mount to split the two: the index is
        /// small and random-access (put it on NVMe), the archive is bulk and
        /// mostly sequential (put it on spinning or network storage).
        Options& directory(std::filesystem::path d)
        {
            dir_ = std::move(d);
            return *this;
        }
        /// Archive file name. Defaults to `<sub-database>.tar`, or
        /// `<store stem>.tar` when the index lives in the main tree.
        Options& fileName(std::string n)
        {
            file_ = std::move(n);
            return *this;
        }
        /// Open the archive without write access. Appends and index
        /// maintenance then throw ErrorCode::ReadOnly.
        Options& readOnly(bool on = true)
        {
            readOnly_ = on;
            return *this;
        }
        /// Enable for durable references: catch-up cannot recover a lost, unflushed archive payload.
        Options& syncOnAppend(bool on = true)
        {
            syncOnAppend_ = on;
            return *this;
        }
        /// Index any members the archive has gained since the index last
        /// looked -- from an abandoned batch, a crash, or `tar --append` run
        /// from a shell. On by default; the cost is proportional to the
        /// un-indexed tail, not to the archive.
        Options& catchUpOnOpen(bool on = true)
        {
            catchUp_ = on;
            return *this;
        }
        /// Paging hint for the archive mapping; see Access. Default Normal.
        Options& accessPattern(Access a)
        {
            access_ = a;
            return *this;
        }
        /// Bytes a Writer accumulates before it touches the file. Members
        /// smaller than a thirty-second of this are coalesced into one write;
        /// larger ones (128 KiB and up by default) go straight down as a
        /// gathered write, after anything queued ahead of them, since past
        /// that size copying costs more than the syscall it saves. Zero
        /// writes every member out immediately. Default 4 MiB, which turns
        /// a million 4 KiB appends into a thousand writes.
        Options& writeBuffer(std::size_t bytes)
        {
            writeBuffer_ = bytes;
            return *this;
        }

    private:
        friend class BlobStorage;
        std::filesystem::path dir_;
        std::string file_;
        bool readOnly_ = false;
        bool syncOnAppend_ = false;
        bool catchUp_ = true;
        Access access_ = Access::Normal;
        std::size_t writeBuffer_ = 4u << 20;
    };

    /// Out of line, like the destructor, so the pimpl stays incomplete here.
    BlobStorage() noexcept;
    BlobStorage(BlobStorage&&) noexcept;
    BlobStorage& operator=(BlobStorage&&) noexcept;
    BlobStorage(const BlobStorage&) = delete;
    BlobStorage& operator=(const BlobStorage&) = delete;
    ~BlobStorage();

    /// Opens (creating if needed) the archive and binds it to an index.
    ///
    /// `env` must outlive the storage. `indexDb` names the sub-database that
    /// holds the index; empty selects the main tree. A second, internal
    /// sub-database tracks how far the index has consumed the archive.
    ///
    /// On open the archive's headers are walked to find the append point,
    /// which also normalises a torn tail left by a crash mid-append, and --
    /// unless catchUpOnOpen was turned off -- anything past the index's
    /// high-water mark is indexed. Both cost one pass over the *headers*,
    /// never over the payloads.
    static BlobStorage open(Env& env, Slice indexDb, const Options& options);
    static BlobStorage open(Env& env, Slice indexDb = Slice());

    bool valid() const noexcept { return impl_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

    /// Where the archive actually landed, after defaults and overrides.
    const std::filesystem::path& archivePath() const;
    /// Offset of the append point: the end of the last member, and where the
    /// end-of-archive marker begins. Not the file size, which includes it.
    std::uint64_t archiveSize() const;
    /// The index's high-water mark into the archive. Below archiveSize() when
    /// members are present that the index has not adopted yet.
    std::uint64_t indexedUpTo() const;
    /// Indexed members. Counts index entries, so it excludes anything erased
    /// but still physically present in the archive.
    std::uint64_t count() const;

    // --- writes ---

    /// Appends `contents` as a member called `name` and indexes it under
    /// `key`, committing before returning.
    ///
    /// `name` must be 1..100 bytes -- the ustar limit, staying inside which is
    /// what keeps tar from emitting PAX extension members and keeps the
    /// one-header-per-member layout. Prefer encoding `key` into `name` so the
    /// archive stays self-describing and rebuildIndex() can recover the
    /// original keyspace.
    ///
    /// One index transaction per call; use a Writer to amortise that.
    void put(Slice key, std::string_view name, Slice contents);

    /// Batches many appends behind a single index transaction.
    ///
    /// Payloads are coalesced in memory (see Options::writeBuffer) and reach
    /// the archive in large writes as the buffer fills, or when the Writer
    /// commits or is destroyed; the index is committed once, at the end.
    /// Abandoning a Writer without committing leaves those bytes in the
    /// archive but absent from the index -- nothing is half-visible, and the
    /// next catchUp() adopts them. That is the same state a crash mid-batch
    /// produces, which is why there is no rollback to perform.
    ///
    /// Holds the store's single write transaction while alive, unless it was
    /// begun on a transaction the caller owns.
    class Writer
    {
    public:
        Writer() noexcept;
        Writer(Writer&&) noexcept;
        Writer& operator=(Writer&&) noexcept;
        Writer(const Writer&) = delete;
        Writer& operator=(const Writer&) = delete;
        /// Flushes anything still buffered to the archive. Never commits.
        /// (Overwriting a Writer by move-assignment flushes the old one too.)
        ~Writer();

        /// Queues header, payload and block padding. A payload too large for
        /// the buffer goes down as one gathered write, so a big blob is never
        /// copied just to prepend its 512-byte header. Throws
        /// ErrorCode::InvalidArgument on an unusable name.
        void add(Slice key, std::string_view name, Slice contents);
        /// Flushes the buffer, terminates the archive, optionally fsyncs it
        /// (see Options::syncOnAppend), then commits the index -- or, for a
        /// Writer begun on a caller's transaction, records the index in that
        /// transaction and leaves committing it to the caller. Idempotent.
        void commit();

    private:
        friend class BlobStorage;
        struct State;
        std::unique_ptr<State> st_;
    };
    Writer beginWrite();
    /// A Writer that records its index entries in `txn`, a write transaction
    /// the caller already holds, instead of opening one of its own. That puts
    /// blob appends and whatever else the transaction does under one commit:
    /// a row describing a sample and the sample's bytes land together or not
    /// at all. Call Writer::commit() before committing `txn`; aborting `txn`
    /// leaves the bytes in the archive for the next catchUp(), exactly as an
    /// abandoned Writer does.
    Writer beginWrite(Txn& txn);

    /// Drops the index entry only. The member stays in the archive, so this
    /// reclaims no space and a full rebuildIndex() would bring the key back.
    /// Real deletion means repacking the archive and rebuilding.
    bool erase(Slice key);

    /// Writes the end-of-archive marker, pads to tar(1)'s 10 KiB record size
    /// and fsyncs. Appending afterwards is still fine -- it overwrites the
    /// marker. Call this before handing the file to another tool, and after
    /// any live Writer has committed.
    void finalize();
    /// Flushes the archive to storage without padding or terminating it.
    void sync();

    // --- reads ---

    /// Zero-copy lookup; an invalid Blob when the key is absent. Opens and
    /// closes its own read snapshot, so it is self-contained but pays for a
    /// transaction per call.
    Blob find(Slice key) const;
    /// The same lookup against a snapshot you already hold -- the form for a
    /// serving loop, where it avoids a transaction per image. The returned
    /// Blob remains usable after `txn` ends.
    Blob find(Txn& txn, Slice key) const;
    /// find(), but throws ErrorCode::NotFound instead of returning invalid.
    Blob at(Slice key) const;
    /// Whether the index holds `key`, without mapping the payload.
    bool contains(Slice key) const;

    /// Looks `count` keys up against one snapshot and writes `out[i]` for
    /// `keys[i]`, invalid where the key is absent. Probing happens in key
    /// order, so a shuffled batch touches each index leaf once instead of
    /// hopping around the tree; results still come back in input order.
    /// Returns how many were found. This is the form for a training loop
    /// that draws a batch of samples per step.
    std::size_t findMany(Txn& txn, const Slice* keys, std::size_t count, Blob* out) const;
    std::vector<Blob> findMany(Txn& txn, const std::vector<Slice>& keys) const;
    /// findMany() against a snapshot of its own.
    std::vector<Blob> findMany(const std::vector<Slice>& keys) const;

    /// Asks the OS to start reading the payloads of `blobs` into memory and
    /// returns as soon as the requests are issued, without waiting for the
    /// data. Only the index has been consulted to produce a Blob, so this is
    /// how the *next* batch's pages get moving while the current one is being
    /// consumed: on a corpus larger than RAM it turns a blocking page fault
    /// per sample into I/O that overlaps compute. Adjacent members are merged
    /// into one request. Advisory; harmless where unsupported.
    void prefetch(const Blob* blobs, std::size_t count) const;
    /// findMany() followed by prefetch(), without handing the Blobs back.
    /// Returns how many keys were found.
    std::size_t prefetch(Txn& txn, const Slice* keys, std::size_t count) const;
    /// Requests the whole archive; the first pass over a corpus that fits in
    /// RAM then streams in sequentially instead of faulting page by page.
    void warm() const;

    /// Visits every indexed key in key order, or only those starting with
    /// `prefix`, until `fn` returns false. The Slice is borrowed from the
    /// transaction and valid for the duration of the call only. Returns how
    /// many keys were visited.
    std::uint64_t forEachKey(Txn& txn, Slice prefix, const std::function<bool(Slice)>& fn) const;

    // --- index maintenance ---

    /// Indexes members that appeared past the index's high-water mark, keyed
    /// by their member names. Returns how many were adopted. Cost is
    /// proportional to the un-indexed tail, not the archive. This is what
    /// makes `tar --append` from a shell script a supported way to load data,
    /// and what recovers an abandoned or crashed batch.
    std::uint64_t catchUp();
    /// Discards the index and rebuilds it from the whole archive, reading only
    /// headers. `keyOf` maps a member name back to its key; without it the
    /// name is the key -- so encode the key in the name if a rebuild should
    /// restore the original keyspace. Checksums cannot be recovered this way,
    /// so hasChecksum() reports false afterwards. Returns the member count.
    std::uint64_t rebuildIndex(const std::function<std::string(std::string_view)>& keyOf = {});
    /// Walks the archive's headers and reports what is physically present,
    /// ignoring the index entirely -- the ground truth to compare against when
    /// diagnosing a suspect index.
    std::vector<TarEntry> list() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace nosql
