# Design

## Contract

libnosql is a C++17 embedded, single-file, copy-on-write B+tree with one writer,
many snapshot readers, named databases, and nested write transactions. It uses
no WAL and no third-party libraries or vendored code. Only format 6 is supported;
opening an unsupported format fails, including through compaction.

See [format.md](format.md) for persisted bytes, [mq.md](mq.md) for queues,
[replication.md](replication.md) for shipping, and [limitations.md](limitations.md)
for operational constraints. [estimates.md](estimates.md) sizes the work.

Blob storage, queues and replication are optional subsystems: `NOSQL_WITH_BLOB`,
`NOSQL_WITH_MQ` (which needs blob storage) and `NOSQL_WITH_REPLICATION` default to
ON and can be switched off so a consumer that only needs the key/value layer
(libsql does) builds and audits less code. Tests adapt to what is built; examples
and the command-line tool need all three, and are skipped otherwise. With
replication compiled out, `Env::Options::shipTo()` fails with `ErrorCode::Unsupported`.

## Repository Layout

All maintained `.hpp` files live beneath `include/`:

| Directory | Purpose |
|---|---|
| `include/nosql/` | Installed public API headers |
| `include/nosql/internal/` | Private engine, OS, format, queue and shipping headers |
| `include/tests/` | Test harness header |
| `include/examples/` | Example scratch-directory helper |
| `src/internal/` | OS and checksum implementations |
| `src/` | Engine and optional-layer implementations |
| `tests/` | Test translation units, the installed-consumer fixture (`InstalledConsumer/`) and `Ubuntu2004.Dockerfile` |
| `examples/` | Executable examples (quickstart, catalog, mq, replication, bench, baseline) |
| `tools/` | Command-line tool (`nosql`: stats, check, list, dump, compact, checkpoint, ship, apply) |
| `benchmarks/` | `bench_kv` and `bench_checksum`; built only with `-DNOSQL_BUILD_BENCHMARKS=ON` |
| `cmake/` | Package config template for `find_package(nosql)` |
| `docs/` | These documents |

Implementation declarations use `nosql::internal`. Public forward declarations
use that namespace too; it is not a stable public API. Only public headers are
installed. Builds include headers by paths rooted at `include`, such as
`nosql/internal/core.hpp` and `tests/test_util.hpp`, without source-relative paths.

Types use PascalCase, file names use snake_case, functions and members use camelCase,
private members have a trailing underscore, and constants use a `k` prefix.
Formatting uses four spaces and a 100-column target. Preserve existing public
APIs unless the requested change requires otherwise.

## Terminology

- **COW / shadow paging:** write replacement pages, then publish a new root.
- **MVCC:** transactions retain the committed snapshot they began with.
- **WAL:** a recovery log written before overwriting data; this engine has none.
- **Checkpoint:** transaction ID, meta checksum, store ID and commit ID together.
- **Checksum:** error detection over bytes; not authentication or history identity.
- **Barrier:** an OS durability operation completed before the next publication step.
- **Torn write:** a write that reaches storage only partially.
- **Write amplification:** physical bytes written per logical byte changed.
- **Page cache:** OS-managed file data; separate from the library's buffer budgets.
- **Reclamation:** making retired page numbers eligible for later allocations.

## File and Trees

Pages 0 and 1 contain alternating meta records. Each 480-byte record follows a
16-byte page header. It stores free, main and catalog tree roots, store/commit/
parent identities, allocation geometry, and up to 26 deferred free pages.
Open checks both meta slots across supported page sizes and chooses the newest
valid record. Unsupported versions are rejected rather than interpreted.

The catalog is separate from user data. Its keys are database names, and its
values are 48-byte `Tree` records marked `N_SUBDB`. A database and a user key may
share a name. Clearing the main tree leaves the catalog intact. Runtime named
database slots belong to the transaction, so readers never consult a writer's
mutable slot registry.

Leaf and branch pages use slots growing upward and packed nodes growing downward.
Branch slot zero is the empty-key, minus-infinity child. Nodes are aligned to
four bytes. The last eight bytes of each slotted page hold its XXH3-64 checksum;
an empty 64 KiB page therefore has `upper == 65528`, without a sentinel.
Large values use a contiguous overflow run with one checksum at the run's end.
The resulting value can still be returned as one borrowed `Slice`.

Persisted integers are explicitly little-endian. User values are opaque.
Integer-key trees select four- or eight-byte keys on first insertion and reject
mixed widths. Application encodings must honor this ordering contract.

## Opening a Store

`Env::Options::open` refuses to guess about the file. On POSIX it opens with
`O_NOFOLLOW` and `O_CLOEXEC`, rejects a file the effective user does not own
(root excepted), creates a new file with mode 0600 whatever the umask
(`fileMode()` overrides it; an existing file keeps its mode), and takes a
whole-file `flock`: exclusive for read-write, shared for `readOnly()`. The lock
replaces a `.lck` file; a conflicting opener gets `ErrorCode::Busy`. Windows uses
`LockFileEx` the same way but applies no mode, symlink or owner checks.

On Linux the file's filesystem type is checked: network and user-space
filesystems (NFS, SMB, FUSE, Ceph, 9p and others) are refused with
`ErrorCode::Unsupported` unless `allowNetworkFilesystem()` is set, because
`flock` is not dependable across hosts and a remote truncation or I/O error
reaches the process as SIGBUS through the mapping. As each transaction begins,
the file is checked to be at least as large as the mapping, turning a persistent
truncation into `ErrorCode::IoError`.

A new file is sized to `max(initialSize, 16 pages)` and grows geometrically (at
least `growthStep`, default `max(1 MiB, 64 pages)`, and an eighth of the current
size) up to `maxSize` (default 64 GiB; address space is not reserved up front).
With `preallocate()` (default on) growth calls `fallocate`, so a full disk fails at
growth with `ErrorCode::IoError`. An existing store's page size is adopted;
requesting a different one is `ErrorCode::Incompatible`, as is a store of another
format version.

`Env` is a shared handle. `Env::share()` returns another handle on the same open
store, with no second open, descriptor or lock, so each thread can hold its own;
the store closes when the last handle and transaction are gone.

## Transactions and Concurrency

The environment serializes writers with a mutex. Readers acquire short metadata
locks to register their snapshot and obtain a mapping, but do not hold the writer
mutex during reads. Each transaction retains its mapping; growth publishes a new
mapping while existing readers keep theirs alive.

Writes copy committed pages into private buffers and update parent links through
the root. Nested writers first see their own dirty pages, then ancestor dirty
pages, then the committed snapshot. Nested commit transfers state to the parent;
abort discards it. The parent is unavailable while its child is active.

Public handles keep transaction/environment ownership alive. A finished
transaction releases heavy resources; remaining handles report `BadTransaction`.
Pooled transactions clear all snapshot state before reuse. Cursors track structural
mutation and re-seek their anchor when necessary. Single-shot lookups avoid that
anchor bookkeeping.

The id of the newest published commit is mirrored into an atomic
(`Env::commitGeneration()`), and `Env::waitForCommit(seen, timeout)` blocks on the
metadata mutex, never on the writer mutex, until it changes. The commit path
notifies only after the meta is published, so a woken waiter's snapshot contains
the commit, and a commit landing between a caller's sample and its wait returns
immediately. Only top-level commits count; a nested commit publishes nothing.
The message queue's blocking receive is built on this.

## Commit and Recovery

1. Finalize named tree descriptors and the free tree.
2. Grow the file and mapping if the new allocation frontier needs it.
3. Sort dirty pages, zero slotted free gaps, seal final images, clear the affected
   read-cache bits, and write them into the file.
4. Complete the data durability barrier required by the selected mode.
5. Write the new meta into slot `txnid & 1`, then complete the meta durability barrier.
6. Publish the snapshot and commit generation under the metadata mutex, notify
   waiters, capture the commit for shipping if enabled, and release the writer.

### Durability and the memory map

By default (`NOSQL_WRITE_THROUGH_MAPPING`, on) pages are copied into the writable
mapping; building with it set to 0 writes each contiguous run with `pwritev`
instead, which avoids write-protect faults on already-clean pages at the price of
a system call per run. Either way the data barrier is a barrier, not a hint: on
Linux it is one `fdatasync`; on Windows it is `FlushViewOfFile` over each written
run followed by one `FlushFileBuffers`. The meta page is flushed separately
(`msync(MS_SYNC)` on its range, falling back to a file sync where the filesystem
rejects that; `FlushViewOfFile` plus `FlushFileBuffers` on Windows).
`Durability::Safe` requests both barriers; `NoMetaSync` omits the meta barrier;
`None` omits explicit synchronization. These are different durability guarantees,
not interchangeable benchmark modes. `Env::sync()` forces everything durable, for
use under `None`.

### The meta page's own durability

The meta record is written only after the data barrier has returned, so a meta
that reaches storage always points at pages that did too. Its own torn write is
detected by its checksum rather than prevented by ordering: two meta slots
alternate by transaction parity, open picks the newest valid one, and the
reclamation rule below guarantees the preceding snapshot's pages were never
overwritten, so a torn newest meta falls back to the previous snapshot.

### Failure

Pages reachable by a protected snapshot are not overwritten. The reclamation
frontier protects readers and the preceding committed snapshot needed for meta
fallback. Any commit failure, including a failed barrier, marks the environment
failed, with one exception below: further transactions raise
`BadTransaction` until it is closed and reopened, and an exception does not prove
the transaction never reached storage. A failed `Env::sync()` and a failed
blob-archive fsync poison their handles the same way, because the kernel may drop
dirty pages after a failed flush and report success to a retry. The exception is
running out of `maxSize` (`MapFull`) before any page is written: the file is
untouched, so only that transaction is lost.

## Reclamation and Memory

The free tree maps (retiring transaction id, chunk) keys to page-number arrays
of at most one inline leaf node each. Eligible chunks are absorbed one at a time
as allocations need them; contiguous runs service overflow values. At commit the
absorbed chunks are deleted and the retired pages plus the unspent remainder are
written under the new id, so the cost of a commit is bounded by what it retired
and one chunk, not by the size of the list. The free tree's own COW updates also
retire pages, so the update is repeated until a pass changes nothing, with the
sorted remainder merged in linear time. The loop is capped at eight rounds; on
the last one the reclaim list is frozen so the write describes a fixed set. Pages
freed too late to be recorded roll into the next writer, and are persisted in meta
meanwhile, not silently lost on restart. Exceeding its 26-page capacity fails commit.

`dirtyLimit` defaults to 256 MiB of active page buffers, including nested writers;
a write that would exceed it fails with `ErrorCode::OutOfMemory`, and zero disables
the limit. `bufferCache` defaults to 32 MiB and bounds idle buffer
payload bytes. Power-of-two classes cover small pages and overflow runs; runs
above 16 pages are rounded only when their class fits the cache budget. Uncached
runs are released immediately. Cache links occupy idle buffer bytes, so release
does not allocate bookkeeping, and acquire/release are constant-time under a mutex.

`os::allocPage` provides 64-byte alignment with a private 64-byte ownership header.
Requests of at least 64 KiB use anonymous `mmap` on POSIX or `VirtualAlloc` on
Windows; smaller requests use the aligned system allocator. Matching frees read
the header to select the correct release operation. Mapping-rounding overhead
and allocation headers are outside the payload budgets. Recycling also avoids
map/unmap calls after a cacheable working set warms up. No guarantee about a
system allocator's fragmentation or process-wide allocation behavior is implied.

The environment's arena retains split/rebalance chunks and rewinds scoped
allocations. Growth reserves descriptor capacity before acquiring a new chunk,
and replacement allocates before freeing the old chunk, preserving ownership
when either allocation fails. An open-addressed page table avoids per-entry nodes.

Each transaction retains up to 2 MiB of scratch container capacity across its
dirty-page table, ownership/free-list/flush vectors and optional read-validation
bitmap. Cleanup keeps capacities that fit and releases the rest without new
allocations. This avoids repeatedly discarding useful buffers at an arbitrary
element count. The 32 pooled transactions can retain at most 64 MiB of this
scratch; active or externally retained handles can add transaction objects.
Named-database slots, arena chunks, mappings and OS cache remain separate costs.

`tests/test_memory.cpp` replaces ordinary C++ allocation operators and, in Linux
static builds, wraps aligned allocation, mapping and release calls. It checks
allocation-free first release and warm recycling, arena failure ownership,
immediate uncached release, and scratch budgets. In the checked core workloads,
1,000 warmed 256 KiB overwrite/read transactions and 12 warmed 8,192-record
batches make zero instrumented allocation/free calls (the test prints `new=0
backing=0 frees=0` for both). Cold starts, changing
working sets, public cursors, queues and workloads beyond budgets can allocate.

## Integrity and Read Caching

The independent XXH3-64 implementation uses seed zero, the default secret, safe
unaligned little-endian loads, and an allocation-free 344-byte streaming state.
Inputs up to 240 bytes use portable scalar code everywhere. The long-input loop
selects a kernel at compile time: six NEON lanes plus two scalar lanes on ARM64,
SSE2 on x86-64 with an AVX2 variant chosen once at run time on GCC/Clang, and
portable C++17 otherwise. Every kernel produces the same digest, so files move
between ARM64 and x86-64 unchanged. There is no third-party checksum implementation.
Page/run, meta, blob, bundle, segment and index checks use full 64-bit digests;
tar header checks retain the additive checksum required by tar.

Every committed-page access validates by default. Dirty private pages are sealed
only before publication. Overflow bounds are checked before hashing a run.
`checkIntegrity` walks all roots and named trees, checks bounds, node overlap,
ordering and depth, and accounts for allocated pages as live, free or deferred.
It does not depend on public named handles already being open.

`Env::configure().cacheReadChecksums()` enables an environment-wide cache of
verified page numbers (`internal/validation_cache.hpp`). A successful check by a
read transaction sets the page's bit; the writer clears the bits of every page it
writes before publishing the meta that makes them visible, which is exact because
no live snapshot can reference a page the writer is allowed to recycle. Relaxed
atomics suffice: the writer's clears are ordered before the meta publication by
the environment mutex every transaction takes to read the meta. The bitmap is
sized for `maxSize` and capped at 4 MiB; higher page numbers verify on every
access. Overflow runs cache only their starting page after checking the whole run.
Failed validation never populates the cache.

Bits survive transactions and are shared by every thread, which is what makes the
cache useful to a caller that opens a transaction per request. What it cannot see
is the file changing underneath the process, so all bits are dropped once the
`revalidateAfter()` interval (default one minute) has elapsed, and on
`Env::invalidateReadCache()`. Writes and explicit integrity checks always use
strict validation. This detection tradeoff is why caching is opt-in.

## Compaction and Optional Layers

`compact(src, dst)` opens a format-6 source read-only and streams its main and
named trees through `BulkFile`/`BulkTree`. It retains one page per active tree
level and chunks overflow values, rather than buffering a destination transaction.
It builds a new catalog, checks the result, synchronizes it and atomically
publishes it. New identities require a new physical replication base. User values
are copied verbatim; sidecar archives are neither relocated nor compacted.

Replication capture uses bounded 64 KiB staging and checked segment indexes.
Extraction coalesces through a temporary disk index with an explicit dirty-buffer
budget. Apply validates the bundle and writes a complete temporary replica,
using reflink when available or a bounded full-copy fallback, before atomic
replacement. A multi-commit bundle must not overwrite pages in the live replica.
The 72-byte bundle header retains `REP-NOSQL-ORLOV` in its 16-byte magic field.

Queues persist queue identity and delivery tokens. Sweep revalidates token and
deadline under the writer lock. Stale transactional acknowledgement aborts
database-local effects. Tree counts replace redundant count metadata. Grouped
sends have a byte budget; external bodies are synchronized before references
commit. Database replication does not include archive payloads.

## Measurements

Measured on 2026-09-29 on the development host: a Jetson AGX Xavier (8 ARMv8
cores, `schedutil` governor, power mode MAXN), Ubuntu 20.04, GCC 9.4, Release, eMMC
storage, one core pinned with `taskset -c 2`, 500,000 entries of 100-byte values,
4 KiB pages, one run each (no repetitions, so treat differences under about 10%
as noise). The checksum backend was the in-tree NEON kernel.

| `bench` example | Every-read | Transaction cache |
|---|---:|---:|
| random get, 500,000 lookups | 1,179 ms (424 k/s) | 693 ms (721 k/s) |
| ordered scan | 31.0 ms | 25.8 ms |
| random overwrite (1 txn) | 541 ms | 532 ms |
| durable commits (fsync each), 200 commits | 1,048 /s | 907 /s |

`bench_kv --entries 200000 --sync none`, ns per operation (one run of each
read-validation mode; the cache-on run stopped after its first rows because the
host's disk filled, so later rows are every-read only):

| workload | every-read | `--cache` |
|---|---:|---:|
| get random, one transaction | 2,353 | 1,295 |
| get random, transaction each | 6,267 | 4,893 |
| get missing | 1,715 | 523 |
| scan ordered (cursor) | 44 | 47 |
| commit 1 put (no fsync) | 13,828 | 13,824 |
| commit 1 put (fsync, Safe) | 1,187,237 | 1,099,033 |
| overwrite random (batched) | 3,861 | not run |
| commit 1 put, loaded store, big free list | 25,172 | not run |

`bench_checksum`: XXH3-64 over a 4 KiB page takes 425 ns (about 9.2 GiB/s), a 64 KiB
run 6.5 µs (9.6 GiB/s), a 16-byte input 6.4 ns; streaming a page in 64-byte chunks
takes 1,270 ns. The transaction-cache gain comes from avoiding repeated hashing;
scans visit each page once and gain nothing. Durable commit rates are a property
of this storage, not of the engine: the fsync dominates.

The README's "Numbers" section carries figures from other machines (an x86 Xeon,
and two ARM64 cores of a different host); those were not reproduced here and
are not comparable with the tables above. The six-NEON-plus-two-scalar lane split
in the checksum kernel was chosen by measurement on those cores (a comment in
`src/internal/checksum.cpp`), not on this host. The SSE2 and AVX2 kernels are
tested for equal digests but were not run here.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DNOSQL_BUILD_BENCHMARKS=ON
cmake --build build
taskset -c 2 ./build/examples/bench 500000 100 4096 every-read
taskset -c 2 ./build/examples/bench 500000 100 4096 transaction
taskset -c 2 ./build/benchmarks/bench_kv --entries 200000 --sync none [--cache]
./build/benchmarks/bench_checksum
./build/examples/baseline 10000 100 3
```

`baseline` pre-generates keys and reports batched safe-write latency and
observed OS I/O; mmap reads are not counted as positional reads. `bench`
includes key formatting in its lookup loop. The example targets are named
`example_<name>` but the binaries are plain `bench`, `baseline`, `mq` and so on.
Record compiler, filesystem, CPU,
storage, cache state and repetitions before drawing comparative conclusions.

## Verification and Reconstruction

The dependency-free CTest suite covers database behavior, geometry, cursors,
named/nested transactions, overflow/reclamation, snapshot concurrency, long-run
workloads, queues, replication, fault injection, format and checksum fixtures,
and tar storage. `include/tests/test_util.hpp` supplies the test harness.

For reconstruction, implement and test primitives in dependency order:

1. OS I/O, endian codecs and checksum golden fixtures.
2. Page/node format, buffer ownership, arena and dirty-page table.
3. B+tree lookup, insertion, split/merge, overflow and cursor invariants.
4. Transaction publication, reclamation, nested state and snapshot retention.
5. Catalog, complete integrity checking and streaming compaction.
6. Blob storage, queues, shipping, CLI, examples and installed-package use.

Every stage needs narrow tests before relying on higher-level stress tests.
`internal::os::ScopedIoObserver` supplies thread-local deterministic I/O failures,
including partial write progress and publication barriers. It does not model
volatile device caches or every power-loss reordering.

The suite is 20 CTest binaries in a full build (14 core, `mq` and `mqsteady`,
`replication`, `replicationtorture` and `fault`, and `tar`); the optional-layer
tests are registered only when their subsystem is built, and `hardening` covers the
file-safety and failure-poisoning behavior of "Opening a Store" and "Failure".
On 2026-09-29 all 20 passed natively on aarch64 Ubuntu 20.04 with GCC 9.4 in
Release, and all 20 passed in a RelWithDebInfo build with `NOSQL_ENABLE_ASAN=ON`
(ASan and UBSan, leak detection on). `mqsteady`, whose
`abandonedAndRejectedWorkIsAlwaysRecovered` case failed an acknowledgement-count
assertion in an earlier sanitizer run, passed ten consecutive repetitions under
ASan/UBSan. That failure has not been reproduced, but its cause was never
identified (the test uses 50 ms leases and counts before calling `ack`, so a
delayed settlement can count a stale receipt), so this is absence of evidence, not
a fix.

`tests/Ubuntu2004.Dockerfile` builds with `-pedantic-errors` on stock Ubuntu 20.04
(CMake 3.16), runs the tests, installs the package and builds and runs
`tests/InstalledConsumer` against it. That container check was not re-run for this
revision. The installed consumer is not a CTest entry. None of this establishes
runtime correctness on any particular board or its vendor kernel.

Run examples in a disposable directory because several create databases/archives in
their working directory. Windows, big-endian runtime, TSan, exhaustive fault
schedules and physical power-loss testing remain separate validation gates.