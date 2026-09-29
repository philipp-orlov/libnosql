// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// libnosql -- a minimalistic, single-file, ACID key/value store.
//
//   * copy-on-write B+tree (shadow paging) -- no write-ahead log
//   * single writer / many concurrent readers, MVCC snapshot isolation
//   * named sub-databases, arbitrarily nested write transactions
//   * one file, no side-car lock files, Linux + Windows
//
#pragma once

#include <cstddef>
#include <cstdint>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "nosql/error.hpp"
#include "nosql/slice.hpp"

namespace nosql {

namespace internal {
struct EnvImpl;
struct TxnImpl;
struct CursorImpl;
}  // namespace internal

class Env;
class Txn;
class Db;
class Cursor;

// ---------------------------------------------------------------- enums ---

/// Sub-database creation / ordering flags.
enum class DbFlags : std::uint32_t
{
    None = 0,
    Create = 1u << 0,      ///< Create the sub-database if it does not exist
    IntegerKey = 1u << 1,  ///< fixed-width little-endian uint32_t or uint64_t keys
    ReverseKey = 1u << 2,  ///< compare keys back-to-front (good for suffixes)
};
constexpr DbFlags operator|(DbFlags a, DbFlags b) noexcept
{
    return DbFlags(std::uint32_t(a) | std::uint32_t(b));
}
constexpr bool operator&(DbFlags a, DbFlags b) noexcept
{
    return (std::uint32_t(a) & std::uint32_t(b)) != 0;
}

/// How `Db::put` treats a key that is already present.
enum class PutMode
{
    Upsert = 0,    ///< insert or overwrite (default)
    InsertUnique,  ///< fail with ErrorCode::KeyExists if present
    UpdateOnly,    ///< fail with ErrorCode::NotFound if absent
    Append,        ///< bulk-load fast path; key must be > every existing key
};

/// How much the store fsyncs on commit.
enum class Durability
{
    Safe = 0,    ///< flush data, then the meta page: crash-Safe and durable
    NoMetaSync,  ///< flush data only; a crash may lose the last commit(s),
                 ///< but the store always opens on a consistent snapshot
    None,        ///< flush nothing; consistent across process crash, not
                 ///< across power loss. For bulk import and scratch stores.
};

// ---------------------------------------------------------------- stats ---

struct TreeStats
{
    std::uint32_t pageSize = 0;
    std::uint32_t depth = 0;
    std::uint64_t branchPages = 0;
    std::uint64_t leafPages = 0;
    std::uint64_t overflowPages = 0;
    std::uint64_t entries = 0;
};

struct EnvStats
{
    std::uint32_t pageSize = 0;
    std::uint64_t fileSize = 0;      ///< bytes currently allocated to the file
    std::uint64_t mapSize = 0;       ///< bytes currently mapped
    std::uint64_t usedPages = 0;     ///< highest page in use + 1
    std::uint64_t freePages = 0;     ///< pages parked in the reclaim list
    std::uint64_t lastTxn = 0;       ///< id of the newest committed transaction
    std::uint64_t oldestReader = 0;  ///< snapshot pinning reclamation, if any
    std::uint32_t readers = 0;       ///< live read transactions
    std::uint64_t bufferBytes = 0;   ///< Page buffers held for reuse
    std::uint64_t captureFailures = 0;
    std::uint64_t capturedTxnid = 0;
    std::uint64_t dirtyBytes = 0;
};

// ------------------------------------------------------------------ db ----

/// A handle to one sub-database (B+tree), scoped to the transaction that
/// produced it. Copyable, and it keeps its transaction's bookkeeping alive:
/// using a handle after its transaction has finished raises
/// ErrorCode::BadTransaction instead of touching released memory.
class Db
{
public:
    Db() = default;

    bool valid() const noexcept { return txn_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }
    unsigned handle() const noexcept { return dbi_; }
    const std::string& name() const;
    DbFlags flags() const;

    // --- reads ---
    std::optional<Slice> get(Slice key) const;
    Slice at(Slice key) const;  ///< throws ErrorCode::NotFound
    bool contains(Slice key) const;
    std::uint64_t count() const;  ///< number of entries
    bool empty() const { return count() == 0; }
    TreeStats stats() const;

    // --- writes ---
    /// Returns false only for the non-throwing "expected" misses:
    /// insertUnique on an existing key, updateOnly on a missing key.
    bool put(Slice key, Slice value, PutMode mode = PutMode::Upsert) const;
    /// Allocate `size` bytes of value space in-place and return a writable view.
    WritableSlice reserve(Slice key, std::size_t size, PutMode mode = PutMode::Upsert) const;
    bool erase(Slice key) const;  ///< false if the key was absent
    void clear() const;           ///< drop every entry, keep the sub-database
    void drop() const;            ///< drop entries and the sub-database itself

    // --- iteration ---
    class Cursor cursor() const;

    /// A borrowed, forward-iterable window over the sub-database.
    /// `for (auto [k, v] : d.all())`
    class Range;
    Range all() const;
    Range from(Slice lo) const;               ///< [lo, end)
    Range upto(Slice hi) const;               ///< [begin, hi)
    Range between(Slice lo, Slice hi) const;  ///< [lo, hi)
    Range prefix(Slice p) const;              ///< every key starting with p

private:
    friend class Txn;
    friend class Cursor;
    Db(std::shared_ptr<internal::EnvImpl> e, std::shared_ptr<internal::TxnImpl> t, unsigned dbi)
        : env_(std::move(e)), txn_(std::move(t)), dbi_(dbi)
    {}
    /// Holding the store as well as the transaction is what lets a handle
    /// outlive both and still report cleanly instead of reading freed memory.
    std::shared_ptr<internal::EnvImpl> env_;
    std::shared_ptr<internal::TxnImpl> txn_;
    unsigned dbi_ = 0;
};

// -------------------------------------------------------------- cursor ----

/// A positioned iterator over one sub-database.
///
/// Cursors survive mutations made through the same transaction: they
/// re-anchor themselves on the recorded key when the tree shape changes.
class Cursor
{
public:
    Cursor() noexcept = default;
    Cursor(Cursor&&) noexcept;
    Cursor& operator=(Cursor&&) noexcept;
    Cursor(const Cursor&) = delete;
    Cursor& operator=(const Cursor&) = delete;
    ~Cursor();

    /// True while the cursor sits on a live entry.
    bool valid() const;
    explicit operator bool() const { return valid(); }

    Slice key() const;
    Slice value() const;
    std::pair<Slice, Slice> item() const { return {key(), value()}; }

    bool first();
    bool last();
    bool next();
    bool prev();
    /// Position on the first key >= `k`. False if no such key exists.
    bool seek(Slice k);
    /// Position on `k` exactly. False (and unpositioned) if absent.
    bool seekExact(Slice k);

    bool put(Slice key, Slice value, PutMode mode = PutMode::Upsert);
    /// Erase the current entry and advance to the next one. False at the end.
    bool erase();

    class Db db() const;

private:
    friend class Db;
    friend class Db::Range;
    explicit Cursor(internal::CursorImpl* c) noexcept : impl_(c) {}
    internal::CursorImpl* impl_ = nullptr;
};

/// Half-open key window, produced by `Db::all()` and friends.
class Db::Range
{
public:
    struct Entry
    {
        Slice key;
        Slice value;
    };

    class Iterator
    {
    public:
        using value_type = Entry;
        using difference_type = std::ptrdiff_t;
        using iterator_category = std::input_iterator_tag;

        Entry operator*() const { return owner_->current(); }
        Iterator& operator++()
        {
            atEnd_ = !owner_->step();
            return *this;
        }
        bool operator==(const Iterator& o) const noexcept { return atEnd_ == o.atEnd_; }
        bool operator!=(const Iterator& o) const noexcept { return !(*this == o); }

    private:
        friend class Range;
        Iterator() = default;
        Iterator(Range* r, bool atEnd) : owner_(r), atEnd_(atEnd) {}
        Range* owner_ = nullptr;
        bool atEnd_ = true;
    };

    Iterator begin() { return Iterator(this, !rewind()); }
    Iterator end() { return Iterator(this, true); }

private:
    friend class Db;
    Range(class Cursor c, std::optional<std::string> lo, std::optional<std::string> hi,
          std::optional<std::string> pfx)
        : cur_(std::move(c)), lo_(std::move(lo)), hi_(std::move(hi)), prefix_(std::move(pfx))
    {}

    bool rewind();  ///< position on the first entry of the window
    bool step();    ///< advance, staying inside the window
    Entry current() const;

    class Cursor cur_;
    std::optional<std::string> lo_, hi_, prefix_;
};

// ----------------------------------------------------------------- txn ----

/// A transaction. Move-only; aborts on destruction unless committed.
class Txn
{
public:
    bool belongsTo(const Env& env) const noexcept;
    Txn() noexcept = default;
    Txn(Txn&&) noexcept;
    Txn& operator=(Txn&&) noexcept;
    Txn(const Txn&) = delete;
    Txn& operator=(const Txn&) = delete;
    ~Txn();

    bool valid() const noexcept { return impl_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }
    bool isReadOnly() const;
    std::uint64_t id() const;

    /// Open (and optionally create) a named sub-database. `name` empty or
    /// omitted selects the unnamed main tree.
    class Db db(Slice name = Slice(), DbFlags flags = DbFlags::None);
    class Db mainDb() { return db(); }

    bool hasDb(Slice name) const;
    std::vector<std::string> listDbs() const;
    void dropDb(Slice name);

    /// Begin a child transaction on top of this one. The parent is frozen
    /// until the child commits (changes fold in) or aborts (changes vanish).
    Txn nested();
    /// Run `fn(child)` in a nested transaction: commit on return, abort on throw.
    template <class F>
    decltype(auto) nested(F&& fn)
    {
        Txn child = nested();
        if constexpr (std::is_void_v<decltype(fn(child))>) {
            fn(child);
            child.commit();
        } else {
            decltype(auto) r = fn(child);
            child.commit();
            return r;
        }
    }

    void commit();
    void abort();

private:
    friend class Env;
    friend void checkIntegrity(Txn&);
    friend void copySnapshot(Txn&, const std::filesystem::path&);
    Txn(std::shared_ptr<internal::EnvImpl> e, std::shared_ptr<internal::TxnImpl> t) noexcept
        : env_(std::move(e)), impl_(std::move(t))
    {}
    std::shared_ptr<internal::EnvImpl> env_;
    std::shared_ptr<internal::TxnImpl> impl_;
};

// ----------------------------------------------------------------- env ----

/// The store itself: one file, one memory map, one writer lock.
class Env
{
public:
    /// Fluent open builder: `Env::Options().pageSize(8192).open("data.db")`.
    class Options
    {
    public:
        /// Page size for a store being created. Opening an existing store adopts
        /// whatever it was created with; pinning a different value here is an
        /// error rather than a silent mismatch.
        Options& pageSize(std::size_t bytes)
        {
            pageSize_ = bytes;
            return *this;
        }
        /// Hard upper bound on the file. Address space is not reserved up front;
        /// the file grows on demand and the map follows. The default is 64 GiB,
        /// a ceiling for a database that owns its disk, not a budget: a consumer
        /// that shares a volume should set a bound from its own capacity so a
        /// runaway writer fails with MapFull instead of filling the volume.
        Options& maxSize(std::uint64_t bytes)
        {
            maxSize_ = bytes;
            return *this;
        }
        Options& initialSize(std::uint64_t bytes)
        {
            initialSize_ = bytes;
            return *this;
        }
        Options& growthStep(std::uint64_t bytes)
        {
            growth_ = bytes;
            return *this;
        }
        Options& maxDbs(unsigned n)
        {
            maxDbs_ = n;
            return *this;
        }
        Options& readOnly(bool on = true)
        {
            readOnly_ = on;
            return *this;
        }
        Options& createIfMissing(bool on = true)
        {
            create_ = on;
            return *this;
        }
        Options& errorIfExists(bool on = true)
        {
            exclusive_ = on;
            return *this;
        }
        /// Permission bits of a store file this open creates (POSIX; applied
        /// exactly, whatever the umask). Default 0600: the file holds
        /// application data. An existing file keeps the mode it has.
        Options& fileMode(unsigned mode)
        {
            fileMode_ = mode;
            return *this;
        }
        /// Reserve disk blocks when the file grows (fallocate), so a full disk
        /// is reported as ErrorCode::IoError at the growth instead of later, when
        /// a hole in the sparse file is touched. On by default. The file then
        /// occupies its whole size on disk; turn it off for a sparse one.
        Options& preallocate(bool on = true)
        {
            preallocate_ = on;
            return *this;
        }
        /// The store is memory mapped and protected by flock. On a network or
        /// user-space filesystem (NFS, SMB, FUSE, ...) flock is not dependable
        /// across hosts and a remote truncation or I/O error kills the process
        /// with SIGBUS, so opening one is refused (ErrorCode::Unsupported)
        /// unless this is set. Supported: local filesystems (ext4, xfs, btrfs,
        /// tmpfs, ...).
        Options& allowNetworkFilesystem(bool on = true)
        {
            allowNetworkFilesystem_ = on;
            return *this;
        }
        Options& sync(Durability d)
        {
            durability_ = d;
            return *this;
        }
        /// Let read transactions trust a page checksum this process has
        /// already verified. Copy-on-write makes a committed page immutable
        /// until a later commit recycles its number, and the writer clears the
        /// cached check for exactly the pages it rewrites, so the cache is
        /// exact with respect to this process's own writes and is shared by
        /// every reader for the life of the environment. What it cannot see is
        /// the file changing underneath the process -- media or memory faults
        /// -- so every remembered check is dropped after `revalidateAfter()`.
        /// Off by default: then every access verifies. Writers always verify.
        Options& cacheReadChecksums(bool on = true)
        {
            cacheReadChecksums_ = on;
            return *this;
        }
        /// How long a cached page check may be trusted before the next access
        /// verifies again; zero means for ever. Only meaningful with
        /// cacheReadChecksums(). Default one minute.
        Options& revalidateAfter(std::chrono::milliseconds interval)
        {
            revalidateAfter_ = interval;
            return *this;
        }
        /// Payload bytes of page buffers to keep recycled between transactions.
        /// Includes overflow runs whose rounded size fits the budget; zero
        /// disables retention. Raise it for the steady dirty working set.
        /// Allocation metadata and OS mapping granularity are additional costs.
        Options& bufferCache(std::size_t bytes)
        {
            bufferCache_ = bytes;
            return *this;
        }
        Options& dirtyLimit(std::uint64_t bytes)
        {
            dirtyLimit_ = bytes;
            return *this;
        }
        /// Capture every commit's changed pages into `dir` so a replica can be
        /// advanced from them. Off by default; roughly doubles the bytes a
        /// commit writes. See nosql/replication.hpp and docs/replication.md.
        Options& shipTo(std::filesystem::path dir)
        {
            ship_ = std::move(dir);
            return *this;
        }
        /// How many segment files to keep. A replica whose checkpoint has
        /// aged out of them needs a fresh base image instead of a delta, so
        /// this is really "how far behind a replica may fall".
        Options& shipRetain(unsigned segments)
        {
            shipRetain_ = segments;
            return *this;
        }
        /// Roll to a new segment once the current one passes this size.
        Options& shipSegmentSize(std::uint64_t bytes)
        {
            shipSegment_ = bytes;
            return *this;
        }

        Env open(const std::filesystem::path& path) const;

    private:
        friend class Env;
        std::size_t pageSize_ = 0;  // 0 = 4096 for a new store, adopt for an old one
        std::uint64_t maxSize_ = 64ull << 30;
        std::uint64_t initialSize_ = 0;
        std::uint64_t growth_ = 0;
        unsigned maxDbs_ = 128;
        bool readOnly_ = false;
        bool create_ = true;
        bool exclusive_ = false;
        unsigned fileMode_ = 0600;
        bool preallocate_ = true;
        bool allowNetworkFilesystem_ = false;
        Durability durability_ = Durability::Safe;
        bool cacheReadChecksums_ = false;
        std::chrono::milliseconds revalidateAfter_{60000};
        std::size_t bufferCache_ = 32u << 20;
        std::uint64_t dirtyLimit_ = 256ull << 20;
        std::filesystem::path ship_;
        std::uint64_t shipSegment_ = 64ull << 20;
        unsigned shipRetain_ = 16;
    };

    static Options configure() { return Options(); }

    Env() noexcept = default;
    Env(Env&&) noexcept = default;
    Env& operator=(Env&&) noexcept = default;
    Env(const Env&) = delete;
    Env& operator=(const Env&) = delete;
    ~Env();

    /// Convenience: open with all defaults.
    explicit Env(const std::filesystem::path& path) { *this = configure().open(path); }

    bool valid() const noexcept { return impl_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

    const std::filesystem::path& path() const;
    EnvStats stats() const;
    std::size_t pageSize() const;
    std::size_t maxKeySize() const;

    /// Begin an explicit transaction. Only one write transaction may be live
    /// at a time; `writeTxn()` blocks until the current one finishes.
    Txn writeTxn();
    Txn readTxn();

    /// Scoped form: commits when `fn` returns, aborts if it throws.
    template <class F>
    decltype(auto) write(F&& fn)
    {
        Txn t = writeTxn();
        if constexpr (std::is_void_v<decltype(fn(t))>) {
            fn(t);
            t.commit();
        } else {
            decltype(auto) r = fn(t);
            t.commit();
            return r;
        }
    }

    template <class F>
    decltype(auto) read(F&& fn) const
    {
        Txn t = const_cast<Env*>(this)->readTxn();
        if constexpr (std::is_void_v<decltype(fn(t))>) {
            fn(t);
            t.abort();
        } else {
            decltype(auto) r = fn(t);
            t.abort();
            return r;
        }
    }

    /// Force everything durable now (useful under Durability::None).
    void sync(bool force = true);

    /// Forget every cached page check (see Options::cacheReadChecksums), so
    /// the next access to each page verifies it again. A no-op without the
    /// cache.
    void invalidateReadCache();

    /// Id of the newest committed write transaction, which increases by one
    /// per commit. Zero for a store that has never been written to.
    std::uint64_t commitGeneration() const noexcept;

    /// Blocks until commitGeneration() differs from `seen`, or the timeout
    /// elapses, and returns the generation observed. Spurious wakeups are
    /// absorbed; a commit that lands between a caller's read of
    /// commitGeneration() and its call here returns immediately rather than
    /// being slept through.
    ///
    /// Waiting does not hold the writer slot, so a waiter never blocks the
    /// commit it is waiting for.
    std::uint64_t waitForCommit(std::uint64_t seen, std::chrono::milliseconds timeout) const;

    /// Release this handle. The store closes once the last transaction that
    /// referenced it has finished.
    void close();

    /// Another handle on the same open store: no second open, descriptor or
    /// lock. Every handle may begin transactions from its own thread (readers
    /// run concurrently, one writer at a time); the store closes when the last
    /// handle and transaction are gone.
    Env share() const;

private:
    friend class Options;
    friend class Txn;
    explicit Env(std::shared_ptr<internal::EnvImpl> e) noexcept : impl_(std::move(e)) {}
    /// Shared so that a transaction (and any handle it produced) keeps the file
    /// and its mapping alive even if the `Env` object is destroyed first.
    std::shared_ptr<internal::EnvImpl> impl_;
};

/// Walk every tree reachable from `t` and validate it: Page headers, key
/// ordering inside and across pages, branch separators, Page accounting, and
/// that no page is reachable twice. Throws ErrorCode::Corrupted describing the
/// first problem found. Intended for tests and for triaging a suspect store.
void checkIntegrity(Txn& t);

/// Rebuild `src` into a fresh, densely packed store at `dst`.
/// Reclaims space that the in-place free list cannot return to the file.
void compact(const std::filesystem::path& src, const std::filesystem::path& dst);

/// Library version, e.g. "0.1.0".
const char* version() noexcept;

}  // namespace nosql
