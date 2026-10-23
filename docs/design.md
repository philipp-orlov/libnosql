# Design

## Contract

libnosql is a C++17 embedded, single-file, copy-on-write B+tree with one writer,
many snapshot readers, named databases, and nested write transactions. It uses
no WAL and no third-party libraries or vendored code. Only format 7 is supported;
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
Branch slot zero is the empty-key, minus-infinity child. A branch key is a
separator: keys routed to its left child sort below it, and keys routed to its
right child sort at or above it. Any key in that gap is valid. Wherever a
separator comes from two adjacent leaves (a leaf split, a rebalancing shift
between leaf siblings, and the bulk builder behind `compact()`), the tree stores
the shortest part of the right leaf's first key that still sorts after the left
leaf's last key (`separatorFor`): a prefix in byte order, a suffix in reverse
order. Integer-keyed trees keep the whole key, since a shorter one would have a
different width. Stores written
either way open the same, because the layout is unchanged. Nodes are aligned to
four bytes. The last eight bytes of each slotted page hold its XXH3-64 checksum;
an empty 64 KiB page therefore has `upper == 65528`, without a sentinel.
Large values use a contiguous overflow run with one checksum at the run's end.
The resulting value can still be returned as one borrowed `Slice`.

A sub-database created with `DbFlags::PrefixKeys` (byte or reverse order) may hold
`P_PREFIX` leaves (`internal/key_prefix.hpp`). Such a leaf stores the bytes all its
keys start with once, in a block at the top of the node area just under the
checksum: the prefix, then its length as a u16, padded to four bytes. Its nodes
hold only what follows. The block sits above every node, so deleting a node, which
compacts the node area from `upper`, never moves it. The prefix is what the page's
first and last key share, used only when that saves room; the working format is
the file format, so readers search such a page in place: the key is placed against
the prefix once and the binary search runs on the rest. Inserting a key that does
not start with the prefix, or that only fits compressed, lays the page out afresh
from its whole entries, or splits it. Splits choose a boundary by compressed size,
and rebalancing reads both siblings whole and lays them out again, merged or with
one entry moved. Branch pages compress the same way, `P_BRANCH | P_PREFIX`: slot 0,
the -infinity child, stores no key, so the prefix is what the separators in slots
1 and n-1 share, and a key below it goes to child 0 and one above it to the last.
Separators are still cut from whole keys; one that does not start with its page's
prefix (an insert, or a rebalance moving a separator) lays the branch out afresh,
and a rebalance checks the parent can take the new separator before it moves any
entry. A descent has no interpolation bounds below such a page, whose separators
are suffixes. A cursor assembles a key from such a leaf in its
own buffer. `compact()` stages each leaf's entries to pack it by compressed size;
its branch pages are written plain. Writes to a tree without the flag lay any
`P_PREFIX` page a damaged file holds out without a prefix, rather than editing a
suffix as a key.
Format version 8 adds this; a store is raised to it by the commit that creates its
first such sub-database and is otherwise written as 7, which older builds read.
A tree in reverse order compares keys from their last byte, so the same
argument holds for suffixes: its pages are `P_SUFFIX`, whose block holds the bytes
every key ends with and whose nodes hold what comes before them, and a key is
placed against the suffix from its end, then searched by the rest in reverse
order. Each page's flag says which end it shares, so readers need no tree flags;
`verify` checks each page shares at the end its tree's order calls for.
A build with `NOSQL_PREFIX_KEYS_DEFAULT` gives every byte- or reverse-ordered tree the flag,
main tree included, and the whole suite passes that way.

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
`LockFileEx` the same way but applies no mode, symlink or owner checks. Its locks
are mandatory, so it locks one byte past any data rather than the file: other
handles, a checkpoint or a snapshot copy in the same process among them, still
read the store. A replacement (compaction, a replica's apply) renames with POSIX
semantics where the volume has them, which may replace a file still open, and
falls back to `MoveFileEx`; ship-log segments are opened shared for deletion so
pruning can remove them while a scan holds them. CI builds and tests with MSVC.

On Linux the file's filesystem type is checked: network and user-space
filesystems (NFS, SMB, FUSE, Ceph, 9p and others) are refused with
`ErrorCode::Unsupported` unless `allowNetworkFilesystem()` is set, because
`flock` is not dependable across hosts and a remote truncation or I/O error
reaches the process as SIGBUS through the mapping. As each write transaction
begins, and at most every 100 ms as a read transaction begins, the file is
checked to be at least as large as the mapping, turning a persistent truncation
into `ErrorCode::IoError`.

On POSIX a writable store reserves `maxSize` of address space at open
(`PROT_NONE`, `MAP_NORESERVE`: address space only, no memory or swap) and maps
the file over the front of it. Growth maps the new extent in place with
`MAP_FIXED`, so the base never moves, pointers and slices handed out before a
growth stay valid, and pages already faulted in stay mapped. There is one
mapping for the life of the environment. Windows cannot reserve this way: there
growth publishes a new mapping, and every transaction keeps each mapping it has
used alive until it ends (a writer that grew keeps its old ones too).

A new file is sized to `max(initialSize, 16 pages)` and grows geometrically (at
least `growthStep`, default `max(1 MiB, 64 pages)`, and an eighth of the current
size) up to `maxSize` (default 64 GiB; reserved as address space on POSIX, see
below).
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
mutex during reads. On POSIX the mapping never moves; on Windows each transaction
retains the mappings it has seen, and growth publishes a new mapping while
existing transactions keep theirs alive.

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

### Optimistic transactions

`OptimisticTxn` (`src/optimistic.cpp`) runs its body on a read snapshot and
buffers its writes: an op list over one byte arena, with a hash index for
read-your-writes once a transaction has 32 writes. Each thread keeps one spare
set of buffers, so a warm stream of small transactions allocates nothing.
`scan()` merges the transaction's writes in range, sorted in the
sub-database's order, over a snapshot cursor; writing to the transaction during
a scan is refused, since it could move the bytes the scan is handing out.

Validation works on 64-bit hashes of (sub-database, key) (`occKey`). Every
write also records its sub-database's `occDbKey`, the hash with an empty key,
which no real key has; a scan records that instead of keys. So a point read or
write conflicts only with later writes of its key, and a scan with any later
write to its sub-database.

At commit the transaction gives back its reader slot, which would otherwise pin
page reclamation while it waits, and pins only its snapshot id
(`EnvImpl::occPins`), which keeps the validation log's entries after that id.
It then queues (`occQueue`). A committer that finds no group being applied
becomes the leader: it takes up to 256 queued commits (about 8 MiB of buffered
bytes), takes the writer slot, validates each against `occLog` (the keys each
commit since its snapshot wrote) and against the keys already accepted into the
group, replays the accepted ones into one write transaction, and commits it
once. Members whose writes fail to apply are dropped and the group retried
without them.

`publish()` records each commit's written keys under its transaction id. A write
transaction collects them (`TxnImpl::occWritten`, filled by the public write
calls) when `EnvImpl::occActive` says an optimistic transaction exists as it
begins; the leader's own writer always does. A nested transaction's keys join
its parent's when it commits. A writer that began with no optimistic
transaction about, cleared or dropped a sub-database, or wrote more than about
a million keys is recorded as writing unknown keys.

Waiters yield a few hundred times before sleeping on their own condition, and
the leader wakes only its group and the next leader: without device flushes a
group takes microseconds, about what a sleep and wake cost. Members are
validated in order of conflicts already lost; from the eighth lost attempt a
transaction holds back every other optimistic commit until it finishes, so a
hot key cannot starve it.

`EnvStats::optimisticCommits` counts `OptimisticTxn::commit()` calls in progress
and `optimisticQueued` those waiting behind the group being applied: the queue's
depth for monitoring, and what the tests wait on to line commits up in a known
order instead of sleeping and hoping the scheduler did.

## Commit and Recovery

1. Finalize named tree descriptors and the free tree.
2. Grow the file and mapping if the new allocation frontier needs it.
3. Sort dirty pages, zero slotted free gaps, seal final images, clear the affected
   read-cache bits, and write them into the file.
4. Complete the data durability barrier required by the selected mode.
5. Write the new meta into the slot that does not hold the newest durable meta,
   then complete the meta durability barrier, as the mode requires.
6. Publish the snapshot and commit generation under the metadata mutex, notify
   waiters, capture the commit for shipping if enabled, and release the writer.

Readers take their snapshot from the in-memory meta, never from the file, so
publication does not wait for the file. Under `Lazy` and `None` steps 4 and 5
are deferred to the next sync (below).

### Durability and the memory map

By default (`NOSQL_WRITE_THROUGH_MAPPING`, on) pages are copied into the writable
mapping; building with it set to 0 writes each contiguous run with `pwritev`
instead, which avoids write-protect faults on already-clean pages at the price of
a system call per run. Either way the data barrier is a barrier, not a hint: on
Linux it is one `fdatasync`; on Windows it is `FlushViewOfFile` over each written
run followed by one `FlushFileBuffers`. The meta page is flushed separately
(`msync(MS_SYNC)` on its range, falling back to a file sync where the filesystem
rejects that; `FlushViewOfFile` plus `FlushFileBuffers` on Windows).
`Durability::Safe` requests both barriers on every commit. `NoMetaSync` requests
the data barrier on every commit and writes the meta without its barrier; the
next commit's data barrier carries it to the device. These are different
durability guarantees, not interchangeable benchmark modes.

### Deferred meta: `Lazy` and `None`

Under `Lazy` and `None` a commit writes and seals its pages as usual but leaves
the newest meta in memory only: no barrier and no meta write. A *sync* then
makes everything up to the newest commit durable in three steps, each finished
before the next: the data barrier over the whole store, the meta write into the
slot not holding the newest durable meta, and the meta barrier. The file
therefore only ever holds metas whose pages are on the device. The protocol is
the same copy-on-write without a log; only when the meta is flushed changes.

What runs a sync: under `Lazy`, the first commit that finds
`Options::syncInterval()` (default one second) elapsed since the last sync
runs it before it returns, so a busy store makes two device flushes per
interval whatever its commit rate (group commit). `Env::sync()` runs one under
either mode, and so does the store closing (the last handle and transaction
going away; that sync cannot report a failure, so call `Env::sync()` first to see
one). By default there is no timer thread: a `Lazy` store that stops committing
keeps its newest commits in memory until one of those happens. With
`Options::backgroundSync()` the store starts one thread that wakes once an
interval and, if commits are waiting and none has synced within the interval,
runs the same sync `Env::sync()` would, so an idle store's commits reach the
device within about two intervals. A failure there poisons the store like any
failed sync; the thread stops and joins before the closing sync. `None` never
syncs on its own. A crash of either kind, process or power, reopens the
store on the last sync.

Reclamation is pinned at the newest durable commit (below), which is what keeps
the snapshot a crash falls back to intact. `Env::sync()` serializes with the
commit path on its own mutex and holds neither the metadata mutex nor the
writer slot across the I/O, so readers and the writer carry on during it.
`EnvStats::durableTxn` reports the newest commit known to be on stable storage.

### Pipelined commits: `Safe` with `pipelineCommits()`

`Env::Options::pipelineCommits()` keeps `Safe`'s promises (a commit returns
only once it is durable, and readers see only durable commits) while letting a
commit's meta page reach the device with the next commit's data, as under
`NoMetaSync`. Two metas are published separately: `EnvImpl::writerMeta`, the
newest commit, which the next writer builds on, and `EnvImpl::meta`, the newest
durable commit, which readers take.

A pipelined commit flushes its data together with the previous commit's
waiting meta page (one barrier, `landPipelined` then publishing that commit to
readers and waking its committer), writes its own meta with no barrier,
publishes it to writers, and hands back the writer slot. Its committer then
waits in `awaitDurable` for a later commit's data flush to carry the meta, for
at most about two data flushes, and only when a successor is in sight (a writer
holding or waiting for the slot, a queued optimistic commit, or the last meta
having been carried, which marks a stream); otherwise it flushes the meta
itself. A lone writer so pays nothing extra. Reclamation is pinned at the
newest durable commit, which lags by at most one, exactly as under
`NoMetaSync`, so a power cut that loses the waiting meta falls back to a whole
snapshot. A failed flush, by the carrier or by the commit itself, poisons the
environment and fails every commit still waiting. The group-commit leader
hands the lead on before waiting, or the next group could never carry its meta.

Off by default. Measured on a 4-core VM whose flushes take about 85 µs (data)
and 50 µs (meta), the saved meta flush is a small part of a commit and the
effect is within ±25%: up a little for plain writers and two-thread optimistic
streams, down about 13% for eight-thread optimistic groups, which form smaller
when the commit cycle is one flush rather than two. With each device flush
modelled at 1 ms, plain writers at two and eight threads gain 1.7×, two-thread
optimistic streams 1.55× and eight-thread groups 16%; one thread gains nothing,
having no successor to carry its meta.

### The meta page's own durability

The meta record is written only after the data barrier has returned, so a meta
that reaches storage always points at pages that did too. Its own torn write is
detected by its checksum rather than prevented by ordering: each meta goes into
the slot that does not hold the newest durable meta (under `Safe` that is plain
alternation by transaction parity), open picks the newest valid one, and the
reclamation rule below guarantees the durable snapshot's pages were never
overwritten, so a torn newest meta falls back to it.

### Failure

Pages reachable by a protected snapshot are not overwritten. The reclamation
frontier protects readers and the preceding committed snapshot needed for meta
fallback. Any commit failure, including a failed barrier, marks the environment
failed, with one exception below: further transactions raise
`BadTransaction` until it is closed and reopened, and an exception does not prove
the transaction never reached storage. A failed `Env::sync()` and a failed
blob-archive fsync poison their handles the same way, because the kernel may drop
dirty pages after a failed flush and report success to a retry; a poisoned
`Lazy` or `None` store never writes its in-memory meta. The exception is any
failure before the first page is written (`MapFull`, or an `IoError` such as
`ENOSPC` from growing the file): no page a snapshot names has been touched, so
only that transaction is lost.

## Reclamation and Memory

A free-list entry keyed by transaction T lists pages of snapshot T-1 that T
retired. A writer may recycle it once every snapshot that matters is T or
newer: every live reader's, and the newest durable one's. Under `Safe` the
newest durable snapshot is the newest commit; under the other modes it lags
(by one commit under `NoMetaSync`, by up to one sync interval under `Lazy`), and
pinning reclamation there is what keeps the snapshot a crash falls back to
intact. Pages handed out since the newest durable commit are an exception:
that snapshot never named them, so when a later commit retires them they go
into *exempt* chunks that only readers hold back. Each page of the durable
snapshot therefore waits at most once per sync, and the file stays within about
twice the data the last sync held, however long a `None` store runs unsynced.

The free tree maps (retiring transaction id, chunk) keys to page-number arrays
of at most one inline leaf node each. In a store at format 8 a chunk may instead
hold its pages as runs of consecutive pages (`internal/free_list.hpp`): a varint
count, then each run's gap from the end of the one before and its length less
one. An array's length is a multiple of eight and the runs' never is (a zero byte
pads it when it would be), so the two forms need no tag, and a chunk takes runs
only when they are shorter. What a commit frees is mostly runs: deleting 100,000
keys (some 5,600 pages) while a reader pins them added 18 pages to the file as
arrays and 6 as runs. A store at 7 keeps arrays, which older builds read; readers
check either form whole, and the count it claims against a chunk's limit, before
handing out any page. Eligible chunks are absorbed one at a time
as allocations need them; contiguous runs service overflow values. At commit the
absorbed chunks are deleted and the retired pages plus the unspent remainder are
written under the new id, so the cost of a commit is bounded by what it retired
and one chunk, not by the size of the list. Under `Lazy` and `None` the unspent
remainder goes back under the reclamation frontier instead of the new id, so it
does not wait for the next sync. The free tree's own COW updates also
retire pages, so the update is repeated until a pass changes nothing, with the
sorted remainder merged in linear time. The loop is capped at eight rounds; on
the last one the reclaim list is frozen so the write describes a fixed set. Pages
freed too late to be recorded roll into the next writer, and are persisted in meta
meanwhile, not silently lost on restart. Exceeding its 26-page capacity fails commit.

A commit that retired at most 16 pages lists those in the meta record instead
(an inline entry under its id) and writes only its unspent remainder to the
tree. The next writer absorbs the inline entry first, so a run of small
commits needs nothing from the tree and writes no free-list page; a reader
that pins the entry makes the next commit move it into the tree under a
reserved chunk number. Likewise the descriptors of up to six recently written
sub-databases live in the meta record (root slots), so such a commit does not
rewrite the catalog leaf either. A small commit then writes its tree path and
the meta page and nothing else.

Branch slots carry their subtree's entry count. Inserts and deletes adjust the
counts along the (already copied) path; splits set the two halves' counts from
the entries they divide; merges and shifts move counts with the nodes. Range
counts (`Db::countRange`) and `Cursor::skip` descend by them.

`dirtyLimit` defaults to 256 MiB of active page buffers, including nested writers;
zero disables the limit. A write that may pass it spills first (`TxnImpl::spill`,
called on entry to `put`, `reserve`, `erase` and the cursor writes, never inside a
descent, which holds page pointers): from this transaction and its ancestors, it
picks the least recently touched leaf and overflow pages until half the budget
would be free, then seals them and writes them, in page order, where they will
commit. Every write that touches a dirty buffer stamps it with a clock in the
checksum trailer, which sealing overwrites, so the order costs no memory. A spill
that wrote every page it could and still missed its aim (branch pages, which stay,
fill the rest) marks itself exhausted; none other is tried, and none rescans the
dirty pages on every write, until a write makes a leaf or overflow page dirty
again. `EnvStats::spilledPages` and `unspilledPages` count both directions. Those page numbers are free in every snapshot,
durable or visible, so the early write is as safe as the one at commit, and the
`power` suite replays power cuts through spilling commits. The buffer's memory
goes back to the OS (`discardMemory`) but its address stays owned, so a stale
pointer still reads mapped memory; the write's own key and value are copied first
in case they point into one, and open cursors re-anchor. A spilled page touched
again is read back, verified, into the same buffer under the same page number
(`unspill`), so a spilling transaction lays out exactly the pages an unbounded one
would; one that is freed, by the transaction or by an aborted child, is free at
once like any dirty page. At commit, spilled pages join the runs that are flushed,
invalidated in the validation cache and captured for shipping. Past the budget
with spilling off, or with nothing left to spill, a write fails with
`ErrorCode::OutOfMemory`. `bufferCache` defaults to 32 MiB and bounds idle buffer
payload bytes. Page buffers never come from the heap. Each request rounds up to
a power-of-two class of pages, and each class is carved from slabs the pool maps
from the OS itself (1 MiB, or 64 pages if that is more; a class larger than that
gets a slab of its own). A slab's bookkeeping (two bitmaps) lives in its first
system page. Up to `bufferCache` a released buffer stays resident on its class's
free list, whose links occupy the idle buffer bytes. Past the budget its memory
goes back to the OS (`madvise(MADV_DONTNEED)`, adjacent buffers in one call) while
the slab keeps the address, and a slab with nothing in use or kept is unmapped.
A transaction hands back its whole buffer list under one lock. After one large
transaction the process therefore keeps `bufferCache` plus the slabs' unreturned
address space, not a heap arena full of freed page-sized blocks.

The environment's arena retains split/rebalance chunks and rewinds scoped
allocations. Growth reserves descriptor capacity before acquiring a new chunk,
and replacement allocates before freeing the old chunk, preserving ownership
when either allocation fails. An open-addressed page table avoids per-entry nodes.

Each transaction retains up to 2 MiB of scratch container capacity across its
dirty-page table, ownership/free-list/flush vectors and optional read-validation
bitmap. Cleanup keeps capacities that fit and releases the rest without new
allocations. This avoids repeatedly discarding useful buffers at an arbitrary
element count. The 8 pooled transactions can retain at most 16 MiB of this
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

`tests/test_alloc_budget.cpp` holds the policy as a test: zero heap allocations
per warm write and read transaction (ten 512-byte rows and ten index entries
per write over a fixed key set; a get and a 100-step scan per read) under
`None`, `Lazy` and `Safe`; a heap whose total size and free space do not grow
across rounds that rewrite a fifth of a 50,000-row store; and a held slice that
survives a growth while a reader on another thread comes and goes.

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

A checksum proves a page holds the bytes that were written, not that their
writer told the truth: a buggy build, or anyone who re-seals a page they edited,
produces pages that pass it and lie about their layout. Every branch or leaf read
from the file has its header checked with the checksum (`validateHeader`: page
type, slot array, a 4-aligned `upper`, the prefix block), and a reader checks each
node it reaches before following it (`checkNode` in the searches, descents,
ranks and cursor reads), so no slot, key or value it follows leaves the page. That
costs a few comparisons per node visited. A writer checks the node it rewrites
or deletes, and every node where it reads them all (a split, a relayout, a
rebalance); a scan checks each leaf once as it enters it, and the read cache once
per page for many reads (`validateNodes`). A tree pointer that
lands on an overflow run, an overflow pointer that lands on a tree page, a value
longer than its run, and a free-list entry naming a page outside the store are all
`Corrupted`; dropping a cyclic tree stops at the depth limit. The `corrupt` suite
re-seals a thousand lying pages of a store holding every kind of page and requires
that opening, reading and writing each ends in an `Error`; under ASan with
undefined behaviour made fatal it is a fixed-seed fuzzer, and `fuzz/fuzz_pages.cpp`
(`NOSQL_BUILD_FUZZERS`, clang) is the open-ended libFuzzer one. Its findings, a
catalog flag read before its node was checked and a misaligned `upper` a writer
then placed nodes under, are fixed and replayed by the suite. Its sibling `fuzz/fuzz_ops.cpp` drives well-formed stores instead: random
puts, erases, cursor walks that write as they go, nested and optimistic
transactions, older snapshots, shrinks, reopens and compactions, over every tree
flavour and page size and a dirty budget small enough to spill, each read checked
against a `std::map`. CI runs both briefly on every push.

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
access. It is mapped from the OS, which hands it out zeroed, so only the words
covering pages actually checked become resident, and a reset clears only up to
the highest word ever set rather than the whole bitmap. Overflow runs cache only their starting page after checking the whole run.
Failed validation never populates the cache.

Bits survive transactions and are shared by every thread, which is what makes the
cache useful to a caller that opens a transaction per request. What it cannot see
is the file changing underneath the process, so all bits are dropped once the
`revalidateAfter()` interval (default one minute) has elapsed, and on
`Env::invalidateReadCache()`. Writes and explicit integrity checks always use
strict validation. This detection tradeoff is why caching is opt-in.

## Compaction and Optional Layers

`compact(src, dst)` opens a format-7 source read-only and streams its main and
named trees through `BulkFile`/`BulkTree`. It retains one page per active tree
level and chunks overflow values, rather than buffering a destination transaction.
It builds a new catalog, checks the result, synchronizes it and atomically
publishes it. New identities require a new physical replication base. User values
are copied verbatim; sidecar archives are neither relocated nor compacted.

`Env::shrink()` gives the space back in place, in three steps each made durable
before the next. First a write transaction absorbs every free-list entry nothing
holds back and picks a limit: the lowest page number below which the free pages
hold every live page above it, with slack for the copied paths' branch pages and
the commit's free-list chunks; then it walks every tree, the catalog and the free
tree included, leaf by leaf, copying each path that has a page from the limit up
(the allocator hands out the lowest free page first) and moving each overflow run
from there to a run below it, spilling between leaves under `dirtyLimit`, and
within one whose runs alone need more than the budget. What the budget cannot
take, spilling off, stays where it is: the move is maintenance and never fails a
`shrink()` for want of memory. Once
those old locations are free in every snapshot, a second transaction drops the run
of free pages that ends the file from the free list and lowers the frontier past
it, repeated while its own writes free more of the tail. Only then, with that meta
durable, does a third, under the write slot so no commit allocates there in
between, truncate the file and the mapping to the frontier; its meta records the
new size, which a replica's apply follows. A crash at any point recovers on a meta
that lists no page past the file's end: before the cut is durable the file is
whole, after it the pages are listed nowhere. Readers check the file against the
mapping under the same lock, so a truncation is never mistaken for one done
behind the process. The `power` suite replays power cuts through shrinking
commits in `Safe`, `Lazy` and `None`, and fails without the syncs between steps.
A store 80% erased at random shrank from 3,308 pages to 842, 778 of them live.
On Windows a mapped file cannot be cut: the frontier drops but the file keeps its
size.

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

### Optional features and the cost of checked pages

Measured on 2026-10-23 in a shared x86-64 cloud container (AVX2 checksum kernel,
Release, GCC 13), where wall-clock times moved 10-30% between identical runs; the
instruction counts below come from callgrind and do not move. `bench_features`
(300,000 entries, 48-byte values, one run):

| workload | plain keys | `PrefixKeys` |
|---|---:|---:|
| leaf pages for keys like `tenant/0001/orders/2026-10-17/000000012345` | 11,296 | 6,932 |
| get random, one transaction | 2,688 ns | 2,230 ns |
| scan ordered (cursor) | 37 ns | 52 ns |
| insert random, batches of 1,000 | 4,499 ns | 4,196 ns |

Prefix keys hold the same tree in 39% fewer leaves, so lookups touch fewer pages;
a scan pays for assembling each key. A transaction four times a 16 MiB
`dirtyLimit`, random keys: 1.74 us per put held in memory (80 MiB of dirty
buffers), 5.60 us spilling (dirty buffers never above 16 MiB; 5.96 us when spills
went in page order); random keys bring spilled leaves back again and again, so
spilling is a ceiling on memory, not free. Where writes have a hot set it pays
less: the `spill` suite's transaction of ascending inserts that keeps rewriting
60 keys read back 172 of 1,485 spilled pages in page order and none coldest first.

Checking every page a reader follows (see Integrity) against the code before it,
in instructions per operation over 100,000 entries of 100 bytes: insert in batches
of 1,000 7,505 to 8,170 (+9%), random get 6,949 to 7,433 (+7%), one-put commit
63,114 to 66,844 (+6%), cursor scan 145 to 190 per entry (+31%, mostly one check per
node as a scan enters each leaf). The first version, every node of every page on
every read, cost 25-50% on lookups; checking only the nodes visited is what remains.

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
including partial write progress and publication barriers. Copying the file at
one of those moments assumes every write before it landed; the `power` suite
drops that assumption. An `internal::os::WriteRecorder` sees every write, through
a system call or as a store into the mapping (`recordMappedWrite`), every resize
and every flush, in program order. The test replays the record against a model
of the device: bytes a flush covered are kept, and every sector written since may
hold any version written to it or the one before, the file size likewise. At each
point it builds images that keep none of that, all of it, random sectors and
random whole pages, and each must pass `checkIntegrity` and hold exactly a
committed snapshot no older than the newest the store reported durable. At every
whole-file flush the model must equal the real file, so a write the recorder
missed fails the test rather than weakening it. Removing the data barrier before
a `Safe` meta write, or before the meta write of a `Lazy` sync, fails most images;
replaying a correct record with its flushes taken out must fail some, and does.
The workloads write the main tree, a named tree and a `PrefixKeys` tree. A
replacement built beside a file and renamed over it (compaction, a replica's
apply) is checked at its rename: nothing written to any file may still wait for a
flush there, since the rename may reach the device first, and leaving out the
flush in `AtomicFile::publish` fails that check.
It models a device that honours flushes; one that acknowledges a flush it has not
performed is outside what any ordering can survive.

The suite is 28 CTest binaries in a full build (22 core, `mq` and `mqsteady`,
`replication`, `replicationtorture` and `fault`, and `tar`); the optional-layer
tests are registered only when their subsystem is built, and `hardening` covers the
file-safety and failure-poisoning behavior of "Opening a Store" and "Failure".
On 2026-09-29 all 20 then registered passed natively on aarch64 Ubuntu 20.04 with GCC 9.4 in
Release, and all 20 passed in a RelWithDebInfo build with `NOSQL_ENABLE_ASAN=ON`
(ASan and UBSan, leak detection on). `mqsteady`'s
`abandonedAndRejectedWorkIsAlwaysRecovered` case had once failed an
acknowledgement-count assertion under a sanitizer. The cause was the test, not the
queue: it counted a delivery as acked before calling `ack`, so a consumer held up
past its 50 ms lease counted a delivery that had meanwhile been swept back and
acked by another consumer, whose ack, not the stale one, settled it. Making one
consumer in fifty sleep 80 ms before counting reproduces the failure every run
(a count of 2); the test now counts only acks that settle, through the
transactional `ack`, which reports a stale receipt, and passes under the same
delay.

`tests/Ubuntu2004.Dockerfile` builds with `-pedantic-errors` on stock Ubuntu 20.04
(CMake 3.16), runs the tests, installs the package and builds and runs
`tests/InstalledConsumer` against it. That container check was not re-run for this
revision. The installed consumer is not a CTest entry. None of this establishes
runtime correctness on any particular board or its vendor kernel.

Run examples in a disposable directory because several create databases/archives in
their working directory. Big-endian runtime, exhaustive fault schedules and
physical power-loss testing remain separate validation gates; Windows is tested in
CI with MSVC (not `hardening` or `allocbudget`, which are POSIX- and glibc-specific).

Continuous integration (`.github/workflows/ci.yml`) runs the suite on every push and
pull request three ways: Release, ASan/UBSan, and ThreadSanitizer. The thread
sanitizer build (`-DNOSQL_ENABLE_TSAN=ON`) uses Clang, because GCC 13's runtime does
not intercept `pthread_mutex_clocklock` and misreports every timed writer's unlock,
and it leaves out the two suites that count allocations (`memory`, `alloc_budget`):
the sanitizer replaces the allocator, and its slowdown moves when containers reach
their high-water marks.