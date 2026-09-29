# Scope and known limitations

Recorded here so none of it gets mistaken for an oversight. Unfamiliar terms
(MVCC, WAL, shadow paging, torn write, write amplification, ...) are defined
in the "Terminology" section of `docs/design.md`.

## Out of scope by design

* **Multi-process access.** One writer process at a time; any number of threads
  inside it (`Env::share()` gives each thread its own handle on the one open
  store). A whole-file advisory lock enforces this on open: a read-write open
  takes it exclusively and a `readOnly()` open shares it, so read-only processes
  can open a store together while no writer has it, and anyone else gets
  `ErrorCode::Busy`. There is no cross-process reader table and no `.lck` file,
  because the requirement was a single file.
* **Duplicate keys (`DUPSORT`).** One value per key. Model multi-values with a
  composite key, as `examples/catalog.cpp` does.
* **Custom comparator callbacks.** Ordering comes from three built-in modes —
  byte-wise, reverse byte-wise, and little-endian integer — chosen per
  sub-database and recorded on disk, so a store can never be reopened with an
  ordering it was not built with. A user-supplied function pointer cannot be
  persisted, and getting it wrong silently corrupts the tree.
* **Encryption, compression, a query language.** Not this library's job. A query
  language is provided one layer up by [libsql](../../libsql), a SQL engine (tables,
  indexes, joins, a planner) that stores its data in a libnosql store; libnosql itself
  has no schema, no query parser and no planner.

## Real constraints

* **A write transaction's dirty set must fit in RAM.** There is no spilling of
  dirty pages to disk mid-transaction. Bulk loads of many gigabytes should be
  split across several commits. `dirtyLimit` defaults to 256 MiB of active
  page buffers, and a write that would exceed it fails with
  `ErrorCode::OutOfMemory`; zero disables the limit. Compaction uses a streaming
  direct-page builder.
* **`maxSize` is a hard ceiling**, set at open (default 64 GiB, a ceiling for a
  database that owns its disk rather than a budget). Exceeding it raises
  `ErrorCode::MapFull`; the store stays consistent on its last good commit.
  Raise it by reopening — it is not baked into the file.
* **Little-endian metadata, opaque user bytes.** Format 6 fields are explicitly
  little-endian. User values and integer keys written as native object bytes are
  not automatically converted. Big-endian runtime is not yet validated.
  Only store format 6 is supported.
* **Keys are capped** at roughly a quarter page (1004 bytes at 4 KiB); see
  `Env::maxKeySize()`. Values are capped at 4 GiB. Empty keys are rejected.
* **64-bit only.** The design maps the whole file, and keeps stale
  mappings alive across a growth, which needs a roomy address space. CMake
  refuses to configure for a 32-bit target.
* **Local filesystems only, by default.** The store is memory mapped and
  protected by `flock`. On a network or user-space filesystem (NFS, SMB, FUSE,
  Ceph, 9p and the like, recognised by `statfs` type on Linux) the lock is not
  dependable across hosts and a remote truncation or I/O error arrives as
  SIGBUS, so `open` refuses with `ErrorCode::Unsupported` unless
  `allowNetworkFilesystem()` is set. Detection is Linux-only; elsewhere nothing is
  refused. A file shortened behind the process is caught as a transaction begins
  (`ErrorCode::IoError`), which cannot close the race, only the persistent case.
* **Failures that poison the handle.** A failed data or meta flush during commit,
  a failed `Env::sync()`, or a failed archive fsync in `BlobStorage` makes the
  handle refuse further work (`ErrorCode::BadTransaction`) until it is closed and
  reopened, because the kernel may have dropped dirty pages and report success to
  a retry. Reopening recovers from the last checksummed meta page. Running into
  `maxSize` (or a full disk at growth) before the first page is written is
  different: only that transaction is lost.
* **Files are created owner-only.** A new store gets mode 0600 (`fileMode()`
  overrides it), is opened close-on-exec, is never opened through a symbolic
  link, and is refused if the effective user does not own it (root excepted).
  These checks are POSIX-only; the Windows path applies none of them.
* **Growth reserves disk blocks.** `preallocate()` is on by default and calls
  `fallocate` when the file grows, so a full disk is an `IoError` at growth rather
  than a fault later; the file then occupies its whole size on disk. Turn it off
  for a sparse file. Filesystems without `fallocate` are tolerated silently.

## Simplifications, with reasoning

* **A rebalance that cannot merge or shift gives up.** If moving a node would
  need a longer separator than the parent has room for, the page is left
  underfull rather than cascading a parent split out of a delete. This costs
  space in a rare case and is always correct; the integrity checker treats
  underfull pages as legal.
* **Memory budgets cover specific ownership, not total RSS.** The idle page pool
  is capped by `bufferCache`; active pages by `dirtyLimit`. Finished writers
  release buffers and retain at most 2 MiB of scratch container capacity per
  transaction; excess capacity is discarded. Page/run buffers of at least 64 KiB
  use OS mappings; smaller buffers still use the system allocator on cache misses.
  Payload budgets exclude allocator headers and OS mapping-rounding overhead.
  Arena scratch, OS cache, and shipping segment/transaction descriptors still
  consume memory. External coalescing bounds its dirty buffers, not total RSS.
  Warm allocation-free core workloads are tested, but cold starts, changing
  working sets and optional layers can allocate. Process-wide heap fragmentation
  is not a guarantee the library can make.
* **A very large free list costs commits a chunk, not the list.** Retired
  pages are recorded in chunks of at most one leaf node (250 pages at 4 KiB),
  and a commit absorbs one chunk at a time, so drawing a few pages from a list
  of a million rewrites about 2 KB rather than 8 MB. The list still occupies
  free-tree pages in proportion to its size until the pages are reused, and a
  commit that itself retires many pages writes many chunks; that cost is the
  commit's own, not the store's history.
* **Deferred free metadata is bounded.** Format 6 persists at most 26 late free-page
  numbers in Meta. Exceeding that bound fails the commit and requires reopen;
  the pages are never silently discarded after an acknowledged commit.
* **Overflow runs longer than one page are only reclaimed as whole runs, and a
  multi-page allocation only reuses a contiguous run from the free list.** A
  store dominated by large values of constantly changing size will fragment
  its free list and grow; `compact()` is the answer.
* **Main and named tree clear release whole trees.** The separate catalog is
  unaffected. Traversing the released tree still costs work proportional to its
  pages; it is not an O(1) deferred tree-drop scheme.
* **Sub-database handles are transaction-local.** `maxDbs` limits live named
  databases, not historical names. Snapshot slots may allocate and nested
  transactions copy that state; this is not a lock-free registry.
* **`Env::stats().freePages` opens a short read transaction** to total up the
  free list, so it is O(free-list size), not O(1).
* **Splits rebuild both pages** from a decoded entry list rather than shuffling
  bytes in place. Splits are a small fraction of inserts and this is where
  subtle bugs live; the cost is bounded by one page.

## Not yet implemented, but sensible to add

* Spilling dirty pages for transactions larger than memory.
* Returning free tail pages to the filesystem (shrinking the file in place).

## Compared to server RDBMS engines (Oracle, SQL Server)

The design trades away, deliberately, everything a client/server database
needs and an embedded library does not. Setting SQL itself aside (this is a
KV engine, not a relational one, so a missing query language isn't a fair
strike against it; libsql adds one on top), the storage-engine-level gaps are real:

* **One writer at a time, globally.** Enforced by a single mutex
  (`Env::writeTxn()` in `src/api.cpp`), not just one writer per key or per page. Oracle and SQL
  Server allow many concurrent writers via row/page locking (or optimistic
  concurrency), so write throughput scales with concurrent sessions and
  hardware. Any workload with many independent concurrent writers is
  throughput-bound here in a way it structurally isn't on either commercial
  engine.
* **Copy-on-write write amplification.** Every commit rewrites the full
  root-to-leaf path for every key touched, plus free-list maintenance (see
  "Reclamation and Memory" in `docs/design.md`). Engines with a buffer pool + WAL
  update pages in place and defer the expensive full-page write to a
  checkpoint, decoupled from commit latency. Skewed hot-key writes or large
  multi-row transactions cost structurally more here.
* **Checksum verification has a read cost.** Format 6 verifies leaf/branch XXH3-64 hashes
  and whole-overflow-run hashes on every committed-page access by default.
  `cacheReadChecksums()` opts readers into an environment-wide bitmap of
  successful checks that the writer keeps exact for its own writes and that is
  dropped every `revalidateAfter()` interval. External corruption after a check
  may be missed until that interval passes or `checkIntegrity` runs. The bitmap
  covers `maxSize` up to 4 MiB of bits; higher pages, writes, and explicit
  integrity checks always use strict validation.
  Large overflow values preserve contiguous slices by placing one checksum at
  the end of the run; an uncached read verifies the full run, not only the requested bytes.
  Vectorised XXH3-64 (NEON, SSE2, AVX2) reduces CPU cost but cannot eliminate
  the scan. XXH3-64 is not a cryptographic authentication mechanism.
  The implementation is dependency-free C++17 with compile-time kernel selection
  and a one-time AVX2 check on x86-64. Windows performance is unmeasured; the
  README's "Numbers" section says which machines the published figures come from.
* **No multi-process access, no network protocol.** A whole-file advisory
  lock permits exactly one writer process; there is no client/server
  protocol, connection pooling, authentication, authorization, or
  encryption at rest (see "Out of scope by design" above). A real DBMS
  serves many concurrent, mutually untrusted clients over a network; this
  serves one process's address space.
* **No automatic failover, HA, or point-in-time recovery.** Checkpoint
  shipping exists (`nosql/replication.hpp`, `docs/replication.md`): a replica
  can be bootstrapped from a live snapshot and advanced by deltas. What is
  absent is everything around it — no leader election, no automatic failover,
  no synchronous commit, no restore to an arbitrary point in time, and no
  streaming transport, only file drops. Data Guard, Always On and RMAN
  represent decades of engineering this project does not attempt.
* **Comparatively unproven under adversarial conditions.** The durability
  design is sound on paper and exercised by the `torture` and `steady`
  suites, but that is months, not decades, of fault injection and production
  incident history. Treat it accordingly for anything where the cost of
  being wrong is high.

None of this is a defect to fix — it is the other side of the trade that
buys the simplicity and footprint described in `docs/design.md`. Use a
server RDBMS when any of the above is a real requirement; use this when it
isn't and the operational simplicity is worth more.
