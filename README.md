# libnosql

A single-file, embedded, ACID key/value store in C++17.

**Format 6:** little-endian metadata, in-tree XXH3-64 page/run checksums, a separate database catalog,
persistent identities, deferred free-page state, and a free list kept in leaf-sized chunks.
Only format 6 is supported; a format-5 file is rejected as incompatible rather than misread.
`nosql compact source.db compacted.db` builds a separate compacted store;
archives must be handled separately and physical replication restarts from a new base.
`Blob::storedChecksum()` returns the 64-bit digest; use
`hasChecksum()` to distinguish a header-only rebuild from a known checksum.

* **Copy-on-write B+tree, no write-ahead log.** A commit writes new pages
  where nothing old points, flushes them, then flips a meta page. There is no
  log to replay, no recovery pass on open, and no torn state to clean up.
* **One writer, many readers, MVCC (Multi-Version Concurrency Control).**
  Readers take a snapshot and never block or get blocked; the writer never
  waits for a reader.
* **Named sub-databases** and **arbitrarily nested transactions**.
* **One file.** No `.lck` side files, no journals, no directories.
* **Optional blob storage** in a side-car tar archive, indexed by the store
  and readable by `tar(1)`. Reads are zero-copy.
* **Optional durable message queues** with leases, redelivery and
  dead-lettering, transactional end to end.
* **Optional replication**: ship a commit's changed pages to a replica, which
  applies them with no tree work at all.
* **Linux and Windows**, 64-bit.

```cpp
#include "nosql/nosql.hpp"

auto store = nosql::Env::configure()
                 .pageSize(4096)
                 .maxSize(8ull << 30)
                 .open("app.db");

store.write([](nosql::Txn& t) {
    auto users = t.db("users", nosql::DbFlags::Create);
    users.put("alice", "admin");
    t.nested([](nosql::Txn& savepoint) {       // rolls back on throw
        savepoint.db("users").put("bob", "editor");
    });
});

store.read([](nosql::Txn& t) {
    auto users = t.db("users");
    if (auto role = users.get("alice")) std::puts(role->chars());
    for (auto [name, role] : users.prefix("a")) { /* ordered */ }
});
```

## Building

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 4
cmake -E chdir build ctest --output-on-failure
```

Options: `NOSQL_BUILD_TESTS`, `NOSQL_BUILD_EXAMPLES`, `NOSQL_BUILD_TOOLS`,
`NOSQL_BUILD_BENCHMARKS` (off by default), `NOSQL_ENABLE_ASAN`, and the subsystem
switches `NOSQL_WITH_BLOB`, `NOSQL_WITH_MQ` (needs blob storage) and
`NOSQL_WITH_REPLICATION`, all on by default. A consumer that only needs the
key/value layer switches the rest off; examples and the command-line tool are then
skipped, and `shipTo()` fails with `ErrorCode::Unsupported` without replication.
The library needs a 64-bit target and CMake refuses to configure otherwise.
**No third-party libraries or vendored code.** The published XXH3-64 algorithm is
independently implemented in standard C++17, with allocation-free one-shot and
streaming paths. The long-input loop has a NEON kernel on ARM64 and SSE2 plus
run-time-selected AVX2 kernels on Intel/AMD x86-64, with portable scalar code as
the fallback; all produce identical digests and none needs compiler flags.
Builds need no downloads; only the standard library and OS threading/I/O
facilities are required.
All maintained `.hpp` files live under `include`: public headers in `include/nosql`,
private headers in `include/nosql/internal`, and helpers in `include/tests` and
`include/examples`. Internal code uses `nosql::internal`; OS and checksum sources
live in `src/internal`. Only public API headers are installed.
The Release suite passes 20/20 tests, and so does an ASan/UBSan build (both run
natively on aarch64 Ubuntu 20.04 with GCC 9.4 on 2026-09-29). `mqsteady` had failed an
acknowledgement-count assertion in an earlier sanitizer run; it did not reproduce in
that run or in ten repetitions, but its cause was never identified. See
[docs/design.md](docs/design.md).
ThreadSanitizer, Windows, and big-endian runtime validation remain separate gates.
Consume it from another project with `find_package(nosql)` and link
`nosql::nosql`, or just `add_subdirectory`.

### ARM64 (aarch64) Linux

C++17 and CMake 3.16 are sufficient. Ubuntu 20.04's default GCC 9 toolchain can
build the library, tests and examples; no compiler PPA and no source changes
are needed. Run on the target from the project root:

```sh
sudo apt update
sudo apt install build-essential cmake
cmake -S . -B build-arm64 -DCMAKE_BUILD_TYPE=Release
cmake --build build-arm64 --parallel 4
cmake -E chdir build-arm64 ctest --output-on-failure -j2
```

For a build tuned to one machine, optionally add `-DCMAKE_CXX_FLAGS=-mcpu=native`
when configuring. Use a fresh build directory rather than a cache from another
machine. All 20 suites pass natively on ARM64 Ubuntu 20.04 with GCC 9.4; this
checks the compiler and standard library, not any
particular board or its vendor kernel. `tests/Ubuntu2004.Dockerfile` builds with
`-pedantic-errors` on stock Ubuntu 20.04 (CMake 3.16), runs the suite, installs the
package and builds and runs `tests/InstalledConsumer` against it; that container
check has not been re-run for this revision. To run it:

```sh
tar -cf - CMakeLists.txt cmake include src tests examples tools | \
  docker build --platform linux/arm64 -f tests/Ubuntu2004.Dockerfile \
    -t libnosql-cpp17-ubuntu2004 -
```

`MessageQueue::sendBatch` accepts a `const Slice*` and count, or a contiguous
container such as `std::vector<Slice>`, `std::array<Slice, N>` or a C array.
Container forwarding does not allocate or copy the input. A null pointer is
valid only with a zero count. Rebuild consumers after this API change; persisted
format-6 bytes and checksum values are unchanged.

## Testing in CI/CD

The suite is plain CTest: no external test framework, no fixtures to install,
no network, and every case cleans up its own scratch directory under the
system temp path. `ctest` exits non-zero if anything fails, so a pipeline
needs nothing more than the three commands above.

Twenty test binaries are registered in a full build, each with a 900-second timeout
(the `mq`, `mqsteady`, replication and `tar` entries exist only when their subsystem
is built):

| test | what it covers |
|---|---|
| `basic` | open/put/get/erase, page-size handling, `Append` bulk loads |
| `env` | option validation, growth, reopening, compaction, sync modes |
| `cursor` | positioning, ranges, erase-while-iterating, re-anchoring |
| `named` | sub-databases, flags, directory hiding, drop |
| `nested` | savepoints, deep nesting, abort semantics |
| `overflow` | large values, overflow runs, reclaim of runs |
| `reclaim` | free list, GC rules, drop returning pages |
| `steady` | long-run plateau: page count and file size stop moving |
| `memory` | allocation-free warm reuse, bounded scratch, mapping ownership and allocation failures |
| `mvcc` | snapshot isolation, readers versus writer, reader pinning |
| `torture` | randomized operation mixes checked against a `std::map` oracle |
| `mq` | queue semantics: FIFO, leases, redelivery, dead-lettering |
| `mqsteady` | queues under sustained send/receive: no unbounded growth |
| `replication` | checkpoints, base images, bundle format, apply, divergence |
| `replicationtorture` | randomized workloads: a replica must match the primary exactly |
| `fault` | deterministic I/O and partial-write failures around commit/apply/compaction |
| `format` | little-endian bytes, 64-bit page/run hashes, catalog separation, compaction and unsupported-format rejection |
| `checksum` | golden digests, independent reference equality, streaming splits, alignment and concurrency |
| `tar` | the `BlobStorage` side-car archive and its index |
| `hardening` | owner-only file creation, symlink refusal, close-on-exec, network-filesystem detection, preallocation and disk-full at growth, truncation behind the process, handles poisoned by a failed sync or commit barrier, `MapFull` not poisoning, shared handles |

### Useful invocations

The `--test-dir` examples below require CMake 3.20 or newer. With CMake 3.16,
use `cmake -E chdir build ctest ...` as above.

```sh
ctest --test-dir build --output-on-failure          # the default gate
ctest --test-dir build -j8                          # tests are independent
ctest --test-dir build -R 'basic|cursor|named'      # fast subset for PRs
ctest --test-dir build -E 'torture|steady'          # skip the long ones
ctest --test-dir build -R torture --repeat until-fail:50   # flake hunting
ctest --test-dir build --output-junit results.xml   # CMake 3.21+, for reports
```

`steady` and `torture` are the long-running ones (a few seconds in Release,
considerably more under sanitizers); the rest finish in well under a second.

### Sanitizers

`NOSQL_ENABLE_ASAN=ON` puts `-fsanitize=address,undefined` on the library's
`PUBLIC` interface, so everything linking it is instrumented too. Thread
sanitizer has no dedicated option because it must not be combined with ASan;
pass the flags directly (`-DCMAKE_CXX_FLAGS=-fsanitize=thread
-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=thread`). Under sanitizers set
`ASAN_OPTIONS=detect_leaks=1` and `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`.

### Notes for pipeline authors

* Tests write to the system temp directory, so a container needs a writable
  `TMPDIR` — the default is fine, a read-only root filesystem is not.
* Nothing binds a port and nothing shares state between cases, so `ctest -j`
  is safe up to the parallelism the disk can take.
* `steady` asserts on page counts and file sizes; it is the test to watch for
  a regression in allocation or reclamation, and it is deterministic.
* `torture` is randomized but seeded, so a failure reproduces exactly.
* Under sanitizers, raise `--timeout` (the per-test default here is 900s) and
  expect the suite to take an order of magnitude longer.
* To smoke-test the examples in the same pipeline, run
  `./build/examples/example_quickstart` in a scratch directory; each example
  is self-contained, and a library error propagates out of `main` as an
  uncaught exception, so a broken build fails the step.

## The API in one page

### Opening

`Env::configure()` returns a fluent builder. Everything has a sensible
default, so `nosql::Env store("app.db")` works too.

| setting | default | meaning |
|---|---|---|
| `pageSize(n)` | 4096 (new store) | power of two in [512, 65536]. An existing store's page size is adopted automatically; pinning a different one is an error. |
| `maxSize(n)` | 64 GiB | hard ceiling; exceeding it raises `MapFull`. Address space is *not* reserved up front. A ceiling for a database that owns its disk, not a budget: set it from your own capacity if the volume is shared. |
| `initialSize(n)` | 16 pages | pre-size the file to avoid early growth. |
| `growthStep(n)` | max(1 MiB, 64 pages) | minimum growth increment; growth is also geometric. |
| `maxDbs(n)` | 128 | how many named sub-databases can be open at once. |
| `readOnly()` | off | map read-only, take a shared file lock. |
| `createIfMissing(b)` | on | |
| `errorIfExists(b)` | off | fail if the file is already there. |
| `fileMode(mode)` | 0600 | permission bits of a store file this open creates (POSIX, applied exactly whatever the umask); an existing file keeps its mode. |
| `preallocate(b)` | on | `fallocate` when the file grows, so a full disk is an `IoError` at growth instead of a fault later; the file then occupies its whole size on disk. |
| `allowNetworkFilesystem(b)` | off | NFS, SMB, FUSE and similar are refused with `ErrorCode::Unsupported` (Linux): `flock` is not dependable there and remote truncation arrives as SIGBUS. |
| `sync(Durability)` | `Durability::Safe` | see below. |
| `cacheReadChecksums(b)` | off | let readers trust a page check this process already made; see the integrity tradeoff below. |
| `revalidateAfter(d)` | 1 minute | how long a cached page check may be trusted; zero means for ever. |
| `bufferCache(n)` | 32 MiB | page buffers kept recycled between transactions. |
| `dirtyLimit(n)` | 256 MiB | active dirty page buffers; a write past it fails with `ErrorCode::OutOfMemory`; zero disables. |
| `shipTo(dir)`, `shipRetain(n)`, `shipSegmentSize(n)` | off, 16, 64 MiB | change shipping; see Replication. |

Store files are also opened close-on-exec, never through a symbolic link, and are
refused if the effective user does not own them (root excepted). `Env` is a shared
handle: `Env::share()` returns another handle on the same open store, with no second
open or lock, so each thread can hold its own.

`cacheReadChecksums()` is an opt-in read optimization. Copy-on-write makes a
committed page immutable until a later commit recycles its number, and the
writer clears the remembered check for exactly the pages it rewrites before it
publishes them, so a check made by any read transaction stays good for every
later one, on every thread, until the page really changes. What the cache cannot
see is the file changing underneath the process -- a media or memory fault --
so every remembered check is dropped after `revalidateAfter()` (default one
minute), and `Env::invalidateReadCache()` drops them on demand. Write
transactions and `checkIntegrity(txn)` always verify. The bitmap costs one bit
per page number of `maxSize` (2 MiB for the 64 GiB default at 4 KiB pages,
capped at 4 MiB); pages beyond the cap verify on every access. The default keeps
every-read verification. This option does not change format-6 files, checksum
values, durability, or replication.

### Transactions

`Env::write(fn)` and `Env::read(fn)` run `fn` in a transaction that commits on
return and rolls back on exception. `Env::writeTxn()` / `Env::readTxn()`
hand you a move-only `Txn` that aborts in its destructor unless you
`commit()`.

Only one write transaction exists at a time; `writeTxn()` blocks until the
current one finishes. `Txn::nested()` opens a child transaction — a savepoint.
The parent is frozen until the child commits (changes fold in) or aborts
(changes vanish). Nesting is unlimited in depth.

### Sub-databases

`Txn::db(name, flags)` opens a named B+tree; `Txn::db()` with no name is the
main tree. `DbFlags::Create` creates on demand; `DbFlags::IntegerKey` compares
keys as fixed-width little-endian `uint32_t`/`uint64_t` (the width is fixed by the
first insertion); `DbFlags::ReverseKey` compares back to
front.

Sub-database descriptors live in a separate catalog tree, not in the main tree, so
they never show up as rows in it and a user key and a sub-database may share a name.

### Reading and writing

```cpp
std::optional<Slice> get(key);   Slice at(key);        bool contains(key);
bool put(key, value, mode);      WritableSlice reserve(key, n, mode);
bool erase(key);                 void clear();         void drop();
std::uint64_t count();           TreeStats stats();
```

`PutMode` is `Upsert` (default), `InsertUnique`, `UpdateOnly`, or `Append`
(bulk-load fast path for ascending keys — it fills pages almost completely).
`put` returns `false`, rather than throwing, for the two expected misses:
`InsertUnique` on an existing key and `UpdateOnly` on a missing one.

`reserve` gives you the value bytes to fill in place, avoiding a copy.

Slices returned by the library point straight into the memory map. **They are
valid until the owning transaction ends** — copy anything you need to keep.

`Db` and `Cursor` handles are safe to outlive their transaction: they keep it
alive as a husk and report `ErrorCode::BadTransaction` rather than reading
released memory. The same goes for a `Txn` outliving its `Env`, which keeps
working on its snapshot until it finishes.

### Cursors and ranges

```cpp
auto c = d.cursor();
for (bool ok = c.last(); ok; ok = c.prev()) use(c.key(), c.value());

for (auto [k, v] : d.all())               { }
for (auto [k, v] : d.between("a", "m"))   { }   // half-open
for (auto [k, v] : d.prefix("user:"))     { }
```

`Cursor::erase()` removes the current entry and advances, so deleting while
iterating is a normal thing to do. Cursors also survive writes made through
*other* cursors in the same transaction: they re-anchor on their recorded key
when the tree shape changes underneath them.

### Durability

| mode | on commit | after a process crash | after power loss |
|---|---|---|---|
| `Durability::Safe` (default) | flush data, write meta, flush meta | last commit intact | last commit intact |
| `Durability::NoMetaSync` | flush data, write meta | last commit intact | consistent, may lose recent commits |
| `Durability::None` | nothing | consistent, may lose recent commits | consistent, may lose recent commits |

Every mode is *consistent* — the store always opens on some committed
snapshot, never on a half-written one. The modes differ only in how many
recent commits a crash can take with it. `Env::sync(true)` forces everything
out at any time, which is how you make a `Durability::None`-mode bulk load
durable.

### Maintenance

`Env::commitGeneration()` returns the id of the newest committed write
transaction, and `Env::waitForCommit(seen, timeout)` blocks until it moves.
Together they let one thread wait on another's commit without polling the
file; waiting takes only the metadata lock, never the writer slot, so a waiter
can never block the commit it is waiting for. `MessageQueue` is built on this.

`checkIntegrity(Txn&)` walks every tree and validates structure, ordering,
separators, page accounting, and that no page is reachable twice. It is used
throughout the test suite and is the right first thing to run against a
suspect store.

`compact(src, dst)` rewrites a store into a fresh, densely packed file.
Deleted pages are recycled in place by the free list, but a store that once
grew large stays large on disk; compaction is how you give the space back.

## Blob storage

Large immutable payloads — images, models, documents — do not belong in a
copy-on-write B+tree. Every touched page is copied on the way to the root, so
a multi-megabyte value makes commits expensive and the free list churn. The
`nosql/blob_storage.hpp` layer puts those bytes in a plain **tar archive beside
the store** and keeps only a small index entry in a sub-database.

The archive is the source of truth; the index is derived. Every member can be
recovered by walking tar headers, so a stale, missing or damaged index is
repaired by rescanning rather than by a two-file commit protocol. That is what
lets the append path skip an fsync per blob, and it is why the file stays a
normal tar — `tar tvf`, `tar xf` and `tar --append` all work on it.

```cpp
#include "nosql/blob_storage.hpp"

Env env = Env::configure().open("catalog.db");
BlobStorage blobs = BlobStorage::open(env, Slice("images"));   // -> images.tar

// One blob, one commit.
blobs.put(Slice("sku-1001"), "sku-1001.jpg", Slice(jpeg.data(), jpeg.size()));

// Many blobs, one commit.
{
    auto w = blobs.beginWrite();
    for (const auto& [key, name, bytes] : batch)
        w.add(Slice(key), name, Slice(bytes.data(), bytes.size()));
    w.commit();
}

// Zero-copy read: `b.data()` points into a mapping of the archive.
if (Blob b = blobs.find(Slice("sku-1001"))) {
    send(b.data().data(), b.size());
    // or hand the byte range to sendfile()/io_uring:
    // pread(fd, buf, b.size(), b.offset());
}
```

### Reading

`find(key)` opens its own read snapshot and returns a `Blob`; `find(txn, key)`
uses a snapshot you already hold, which is the form for a serving loop where a
transaction per request would dominate. `at(key)` throws
`ErrorCode::NotFound` instead of returning an invalid `Blob`, and
`contains(key)` checks the index without mapping the payload.

`findMany(txn, keys)` looks a whole batch up against one snapshot and returns
the Blobs in input order, invalid where a key is absent. It probes in key
order, so a shuffled batch touches each index leaf once instead of hopping
around the tree — the shape of a training step that draws N samples. Only
the index is consulted; the payload pages are not touched until you read them.

`prefetch(blobs)` asks the OS to start reading those payloads now and returns
without waiting, so the *next* batch's I/O overlaps the current batch's
compute. On a corpus larger than RAM it replaces a blocking page fault per
sample with read-ahead you scheduled. `prefetch(txn, keys)` does the lookup
and the request in one call; `warm()` requests the whole archive, so a corpus
that fits in RAM streams in sequentially on its first pass. Adjacent members
are merged into one request. Pair these with
`Options().accessPattern(Access::Random)`, which turns kernel read-ahead off
so a 4 KiB blob costs 4 KiB of I/O rather than the 128 KiB window around it.

`forEachKey(txn, prefix, fn)` walks the index in key order.

A `Blob` is a borrowed view that keeps its mapping alive, so the bytes stay
addressable after the transaction ends and across appends that grow the
archive — growth remaps, it never resizes in place. `name()` is owned and
does not dangle. `offset()` is the 512-aligned payload offset inside the file,
which is what you need to hand the range to `sendfile()`, `io_uring`, or
another process. `storedChecksum()` returns the XXH3-64 digest recorded at append time,
`hasChecksum()` reports whether it is known, and `verify()` recomputes it.
Header-only rebuilt entries have no known checksum; `verify()` still returns
true for those entries, so check `hasChecksum()` when verified content is required.

### Writing

`put()` is one archive append plus one index transaction. `beginWrite()`
returns a `Writer` that queues members in a buffer (`Options::writeBuffer`,
4 MiB by default) and writes it out in one go when it fills, so a million
4 KiB appends cost a thousand writes rather than a million; a member too big
to be worth copying goes straight down as one gathered write of header,
payload and block padding. The index is committed once, at the end, and its
entries are written in place rather than through a temporary per member. A
`Writer` holds the store's single write transaction for its whole lifetime,
so keep batches bounded if other work needs to commit.

`beginWrite(txn)` instead records the index in a write transaction you
already hold, so blob appends and whatever else that transaction does —
inserting the row that describes the sample, say — land under one commit.
Call `Writer::commit()` before committing `txn`; aborting `txn` leaves the
bytes in the archive for the next `catchUp()`, like an abandoned `Writer`.

Abandoning a `Writer` without committing leaves those bytes in the archive but
absent from the index. Nothing is half-visible, and the next `catchUp()`
adopts them — which is exactly the state a crash mid-batch produces, and why
there is no rollback to perform.

Member names must be 1..100 bytes, the ustar limit. Staying inside it is what
keeps tar from emitting PAX extension members and preserves the
one-header-per-member layout. Encode the key into the name if you want a
rebuild to restore the original keyspace.

### Index maintenance

| call | what it does |
|---|---|
| `catchUp()` | indexes members past the index's high-water mark, keyed by member name. Cost is proportional to the un-indexed tail. Runs on open by default. |
| `rebuildIndex(keyOf)` | discards the index and rebuilds it from the whole archive, reading only headers. `keyOf` maps a member name back to a key. Checksums are not recoverable this way. |
| `list()` | walks the headers and reports what is physically present, ignoring the index — the ground truth when diagnosing a suspect index. |
| `finalize()` | writes the end-of-archive marker, pads to tar's 10 KiB record size and fsyncs. Call it before handing the file to another tool; appending afterwards still works, it overwrites the marker. |

`catchUp()` is what makes `tar --append` from a shell script a supported way
to load data.

`erase(key)` drops the index entry only. The member stays in the archive, so
it reclaims no space and a full `rebuildIndex()` would bring the key back.
Real deletion means repacking the archive and rebuilding.

### Options

| option | default | notes |
|---|---|---|
| `directory(path)` | the store's own directory | point it at another mount to split the two: the index is small and random-access, the archive is bulk and mostly sequential |
| `fileName(name)` | `<sub-database>.tar`, or `<store stem>.tar` for the main tree | |
| `readOnly(bool)` | off | appends and index maintenance then throw `ErrorCode::ReadOnly` |
| `syncOnAppend(bool)` | off | fsync the archive before each batch's index commit. Turn it on when the archive is the only copy and you cannot re-ingest |
| `catchUpOnOpen(bool)` | on | adopt anything the archive gained since the index last looked |
| `accessPattern(Access)` | `Normal` | paging hint for the mapping: `Random` disables kernel read-ahead for shuffled access, `Sequential` widens it |
| `writeBuffer(bytes)` | 4 MiB | how much a `Writer` queues before writing; members of at least 1/32 of it (128 KiB by default) bypass the buffer; 0 writes through |

`archivePath()`, `archiveSize()` (the append point, excluding the
end-of-archive marker), `indexedUpTo()` and `count()` report where things
stand. `indexedUpTo() < archiveSize()` means there are members the index has
not adopted yet.

## Message queues

`nosql/message_queue.hpp` is a durable, MSMQ-shaped work queue built on the
same file: FIFO by sequence number, peek-lock delivery with leases, automatic
redelivery, dead-lettering, and blocking receive without polling.

```cpp
nosql::MessageQueue jobs = nosql::MessageQueue::open(
    store, "jobs",
    nosql::MessageQueue::Options()
        .leaseDuration(30s)       // how long a worker has before redelivery
        .maxDeliveries(5)         // then it is dead-lettered
        .groupCommit(256, 500us)  // amortize fsync across producers
);

jobs.send("render page 7");

nosql::Receipt r = jobs.receive(1s);   // blocks; woken by the producer's commit
if (r) {
    // Do the work and acknowledge it in one transaction. Either both happened
    // or neither did -- no outbox table, no two-phase commit.
    jobs.env().write([&](nosql::Txn& t) {
        t.db("results", nosql::DbFlags::Create).put(r.message().body, "ok");
        jobs.ack(t, std::move(r));
    });
}
```

A receipt that is dropped without `ack()` or `nack()` is not a loss: its lease
lapses and `sweep()` puts the message back. Delivery is therefore **at least
once**, and exactly-once *effects* come from doing the work and the ack in the
caller's transaction, as above.

| option | default | notes |
|---|---|---|
| `leaseDuration(ms)` | 30 s | how long a delivery is exclusive before `sweep()` may reclaim it |
| `maxDeliveries(n)` | 10 | attempts before the message is dead-lettered |
| `messageTtl(ms)` | none | messages older than this are dead-lettered instead of delivered |
| `externalBodyThreshold(n)` | 0 (never) | bodies at least this large go to a side-car `BlobStorage` archive; retrieve with `fetchBody()` |
| `groupCommit(maxBatch, linger)` | off | a writer thread merges concurrent sends into one commit, trading a little latency for far higher throughput than one fsync per message allows |

Each queue costs four sub-databases (`ready`, `lease`, `dead`, `meta`), so
raise `Env::Options::maxDbs` if you want many of them. `sweep()` is the only
thing that needs scheduling; call it periodically from anywhere. It scans
under a read snapshot and revalidates at most 256 due leases per call.
Queue counts use tree metadata, not separately cached counters. Grouped-send
intake is capped at 16 MiB by default with `intakeLimit(bytes)`; overflow returns
`Busy`. Stale transactional acknowledgements abort the caller's transaction.
External queue bodies are fsynced before publishing their references, but their
archive bytes must still be replicated separately.

`examples/mq.cpp` is a worked producer/consumer example, and `docs/mq.md` describes
what is implemented: data model, delivery semantics, the commit-generation wait, group
commit, and what is not built.

## Replication

`nosql/replication.hpp` ships incremental changes from a primary to a copy of
the store. Copy-on-write makes this cheap: a commit's changed page set is
exactly what it wrote, so a replica applies pre-formed pages and flips a meta
page, doing no tree work at all.

A snapshot is named by a `Checkpoint`: transaction ID, meta checksum, store ID,
and commit ID. The identities distinguish unrelated stores and divergent clones;
the meta checksum alone does not identify database contents.

```cpp
// On the primary: capture every commit's pages into a segment directory.
nosql::Env primary = nosql::Env::configure()
                         .shipTo("catalog.db.ship")
                         .shipRetain(16)          // how far behind a replica may fall
                         .open("catalog.db");

// Bootstrap a replica from a live snapshot.
{
    nosql::Txn rt = primary.readTxn();
    nosql::copySnapshot(rt, "replica.db");
    rt.abort();
}

// Then, whenever: build the delta that advances the replica and apply it.
nosql::ShipLog log("catalog.db.ship");
switch (log.extract(nosql::checkpointOf("replica.db"), "delta.bundle")) {
    case nosql::ShipStatus::Ok:       nosql::applyBundle("replica.db", "delta.bundle"); break;
    case nosql::ShipStatus::UpToDate: break;
    case nosql::ShipStatus::NeedBase: /* too far behind; take a fresh copySnapshot */ break;
}
```

A bundle is one self-contained file, so the transport can be anything that
moves bytes. The command line tool covers the file-drop case:

```console
$ nosql checkpoint replica.db
txnid    3
checksum 14541052056481583877
$ nosql ship catalog.db.ship replica.db delta.bundle
delta.bundle: txn 3 -> 6, 14 pages (56.0 KiB)
$ nosql apply replica.db delta.bundle
replica.db: txn 3 -> 6
```

`ship` exits 0 for a written bundle, 3 for "already up to date" and 4 for
"take a base image", so a scheduled job can tell them apart without parsing
messages.

Things worth knowing before relying on it:

* Capture is **off by default** and roughly doubles the bytes a commit writes.
* Bundle metadata is little-endian and page-size specific. User payload bytes
  are not schema-converted. The store must be format 6; bundles and segments carry
  their own version (5), independent of the store format.
* `applyBundle` needs the replica **closed** — it takes the same exclusive
  lock opening the store does — and validates the whole bundle before writing
  a temporary replacement. It copies the entire replica, applies pages to the
  copy, flushes, and atomically replaces the original. Linux attempts reflink
  first with a byte-copy fallback. Allow full-copy temporary space. Replay is idempotent.
* Segments are a shipping artifact, not a recovery one. Losing them costs a
  resync, never a commit, which is why they are not fsynced and why
  `NeedBase` is a return value rather than an error.
* Writing to a replica breaks the chain, and `extract` says so instead of
  producing a delta that would corrupt it.
* Database snapshots and deltas do not include tar archives. External queue
  bodies require separately coordinated archive transfer before using a replica.
* Capture staging is capped at 64 KiB. Monitor `EnvStats::captureFailures` and
  `capturedTxnid`; capture is not an archive durability acknowledgement.
* Checked `.seg.idx` files accelerate transaction discovery. Coalescing uses a
  temporary disk index; `extract(base, bundle, through, memoryBytes)` defaults
  to an 8 MiB dirty-buffer budget, not a total-process memory cap.

[examples/replication.cpp](examples/replication.cpp) runs a self-cleaning
bootstrap, delta apply, retention gap, and fresh-base recovery walkthrough.

`docs/replication.md` describes the implementation: bundle and segment formats,
coalescing, base-image options, divergence handling, and what was deliberately
left unbuilt.

## Design

Shadow paging, as in LMDB. `docs/design.md` has the full write-up; the short
version:

* Pages 0 and 1 are meta pages, alternating by transaction-id parity, each
  checksummed. Open picks the newest one that validates, so a torn meta write
  costs you the last commit and nothing else.
* A write transaction copies every page it touches to a new page number and
  rewrites the parent pointer, all the way up to the root. Nothing an older
  snapshot can reach is ever overwritten.
* Freed pages are recorded in a hidden B+tree keyed by the transaction that
  freed them, in chunks small enough to sit inline in a leaf (250 pages at
  4 KiB), so a commit that draws a few pages from the list rewrites one chunk
  rather than the whole entry. A page becomes reusable once no live snapshot
  predates the transaction that freed it — which is also exactly the rule that
  keeps the *previous* meta page valid, so a torn write always has something
  good to fall back on.
* The file is mapped read/write and, by default, written by `memcpy` into the map,
  then flushed with a data barrier before meta publication (compile with
  `-DNOSQL_WRITE_THROUGH_MAPPING=0` to write runs with `pwritev` instead). Growing the
  file publishes a new
  mapping and keeps the old ones alive until the transactions that saw them
  are gone.

`docs/design.md` also ends with a short reconstruction order: the primitives to
implement and test, in dependency order, and the fault-injection hooks the tests use.

## Numbers

The benchmarks are off by default; configure with `-DNOSQL_BUILD_BENCHMARKS=ON`.

`benchmarks/bench_kv` runs a fixed set of workloads and reports, per workload,
the median ns/op over several repetitions together with the heap allocations,
the allocator footprint change and the page faults the last repetition made.
`benchmarks/bench_checksum` measures page verification per page size. Both
write JSON or CSV with `--json` / `--csv`, take `--only name,name` to pick
workloads, and are meant to be run pinned to a core in Release:

```sh
taskset -c 2 ./build/benchmarks/bench_kv --entries 500000
taskset -c 2 ./build/benchmarks/bench_kv --entries 500000 --cache
taskset -c 2 ./build/benchmarks/bench_kv --sync safe --only commit
./build/benchmarks/bench_kv --threads 8 --only readers      # readers need their own cores
```

The tables below are from other machines and were not reproduced on the current
development host; `docs/design.md`, "Measurements", has fresh figures from a Jetson
AGX Xavier with the same tools.

On a 2.1 GHz Xeon Gold 6252 (AVX2, GCC 13.3, Release, one pinned core; 500,000
entries, 100-byte values, 4 KiB pages; `--sync none`), before and after the
work described below:

| workload | before | after | after, `--cache` |
|---|---:|---:|---:|
| random get, one transaction per lookup | 2,259 ns | 2,131 ns | 1,511 ns |
| random get, one transaction | 2,239 ns | 1,801 ns | 1,064 ns |
| 64 KiB overflow get | 12.4 µs | 11.7 µs | 0.19 µs |
| commit of 1 put, fresh store, no fsync | 7.65 µs | 4.49 µs | 4.53 µs |
| commit of 10 puts, fresh store, no fsync | 11.7 µs | 8.1 µs | 9.8 µs |
| commit of 1 put on the loaded store (500k entries, churned free list) | 141 µs | 12.7 µs | 6.2 µs |
| random get, 8 reader threads beside a committing writer, 12 cores | 411 ns | 433 ns | 243 ns |
| 100-entry range, one transaction each, heap allocations | 3 | 2 | 2 |

The commit figures come from three changes: commit identities are drawn from a
seeded generator rather than `std::random_device` (which cost about 2.5 µs per
commit), the dirty-page table walks only its occupied slots rather than the
capacity a pooled transaction inherited from an earlier bulk load, and the
free list is chunked so a small commit rewrites one leaf node instead of the
whole retired-page array. Page verification is the other half of a point
lookup's cost; the read cache removes it for pages the process has already
checked. `baseline 10000 100 3` still emits a seeded CSV baseline for
safe batched writes. Benchmark the intended workload before drawing speed
conclusions.

The opt-in read cache reduced CPU-0 random gets from 714 ms to 291 ms (about 2.5x)
for 500,000 entries with 100-byte values and 4 KiB pages; CPU 0 is a Cortex-A725.
On Cortex-X925 CPU 5, strict reads
took 367 ms and cached reads 201 ms. Scans and writes gain little because they
do not repeatedly verify the same committed pages in a read-only transaction.
These are representative individual runs, not cross-machine comparisons.
Switching the digest from XXH64 to XXH3-64 with a six-NEON-plus-two-scalar kernel
cut strict X925 reads from 464 ms to 367 ms; the A725, whose scalar multiplier
already ran XXH64 at 15 GB/s, is unchanged within noise. Compare policies on the
same core (the example targets are named `example_<name>`, the binaries are
`bench`, `baseline`, `mq` and so on):

```sh
taskset -c 0 ./build/examples/bench 500000 100 4096 every-read
taskset -c 0 ./build/examples/bench 500000 100 4096 transaction
```

## Running for a long time

Page recycling stores free-list links inside idle buffers, so returning a buffer
never allocates bookkeeping or swallows an allocation exception. Power-of-two
classes also recycle large overflow runs when their rounded size fits the budget.
Backing allocations of 64 KiB or more use anonymous OS mappings, separating them
from small heap objects. Smaller buffers use the system allocator on cache misses.

Transaction scratch retains up to 2 MiB of container capacity per transaction,
including its dirty-page table, whose occupancy bitmap keeps visiting and
clearing it proportional to the pages a transaction actually dirtied rather than
to the capacity an earlier bulk load grew it to. Capacity exceeding that budget
is released. Public cursors are recycled through a small per-thread pool, so a
range scan or a join's inner probe allocates no cursor object. The pool holds at most 32 transactions; active or
externally retained transaction handles can add further objects. Named-database
slots, arena chunks, public cursors and queue objects are separate allocations.

The Linux static-build allocation probes measure zero `new`, page-backing
allocation and direct free/unmap calls over 1,000 warmed 256 KiB overwrite/read
transactions, and over 12 warmed batches of 8,192 records (`tests/test_memory.cpp`
prints `new=0 backing=0 frees=0` for both). These are specific steady-state
regressions, not a promise that every workload or the whole process is allocation-free.

`Env::Options::dirtyLimit(bytes)` defaults to 256 MiB of active dirty buffers
(zero disables the limit); `bufferCache(bytes)` separately limits idle buffer
payload bytes, excluding allocation headers and OS mapping-rounding overhead.
Compaction uses a streaming page builder, retaining one page per active level
and chunking large values instead of buffering a destination transaction.
Source snapshot state, catalog names, and the integrity bitmap remain additional
costs. Neither option caps OS cache or total process RSS.

The store itself also has to plateau, and that is a tested property rather
than an aspiration: `tests/test_steady.cpp` runs tens of thousands of commits
against a fixed working set and asserts the page count and file size stop
moving. Twenty thousand rounds of rewrites against a 20 000-key store leave it
at exactly the 640 pages it warmed up to.

## Scope

Deliberately **not** implemented: multi-process writers, duplicate keys
(`DUPSORT`), custom comparator callbacks, encryption and compression.
`docs/limitations.md` records every simplification and its reasoning, so none
of them get mistaken for oversights.

The message queue and replication layers began as specifications and were then
built; `docs/mq.md` and `docs/replication.md` describe what was built, including what
from the original designs was not. Both needed the storage engine only to expose a
commit-generation wait (queues) and a capture hook in the commit path (replication),
not restructuring.

## License

Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
