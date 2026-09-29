# Engineering effort estimate

## Scope Update: 2026-09-29

Store format 6 implements XXH3-64 directly in C++17 with NEON, SSE2 and AVX2 kernels, with independent reference,
golden-vector, streaming-split, alignment, and concurrency tests.
Every-read verification is the default; read caching is explicit. The suite (20 CTest
binaries) passes in Release and under ASan/UBSan on aarch64 Linux with GCC 9.4;
native x86 (kernels tested for equal digests, not run in this review), Windows,
big-endian and ThreadSanitizer runs remain platform gates.
The no-third-party requirement is a constraint on future work, not an optional
tradeoff that can be waived to obtain a faster benchmark.

The implementation includes page/run integrity, fixed-endian format fields,
a separate catalog root, direct streaming bulk construction, checked segment
indexes, external bounded coalescing, reflink fallback, deterministic I/O fault
hooks, and a seeded CSV baseline. These are now implementation and maintenance
costs, not merely research proposals. The estimate below remains a
planning range; no measured effort or speedup is inferred from lines changed.

The implementation also includes store/commit identity and deferred-free
metadata, a chunked free list, atomic replica replacement, queue settlement/durability
fixes, a public commit-generation wait, transaction-local named handles, complete
read-snapshot page accounting, bounded dirty/capture/intake memory, batched data
barriers and compaction, package-consumer verification, and sanitizer regressions.
These are part of replication, testing, and integration hardening in the
existing phase estimate, not evidence that the hardening tail has disappeared.

A later review round added operational hardening that the phase estimate below
now lists on its own row: owner-only file creation, no symbolic links, an owner
check, close-on-exec, refusal of network and user-space filesystems, a truncation
check, disk-block preallocation with a clean failure at growth, handles poisoned
after a failed flush (including the blob archive), `MapFull` that does not poison
the store, shareable `Env` handles, and build switches for the blob, queue and
replication subsystems.

The estimate below is not a quote for every research proposal raised in review.
Compressed keys, extent allocation, new queue layouts, and alternative engines
remain separate work requiring measurements and revised estimates. Windows and
physical power-loss verification also remain release work. An earlier sanitizer
run failed one `mqsteady` assertion; it did not reproduce in a full ASan/UBSan
run or in ten repetitions on 2026-09-29, but its cause was never identified.

A conservative estimate of the time for one senior systems/C++ engineer --
already experienced with B+trees, MVCC (Multi-Version Concurrency Control),
mmap-based storage and crash consistency -- to design, build and test this
solution from scratch. Add 30-50% if that expertise has to be built along the
way. Unfamiliar terms are defined in the "Terminology" section of
`docs/design.md`.

## Why this isn't a LOC-based estimate

~11,000 lines of library code and ~6,000 of tests (~17,000 together, ~19,000 with
the tools, examples and benchmarks) is small by line count, but nearly every line of
the engine sits in a
correctness-critical hot path: torn-write recovery, self-referential free-list
reclamation, cursor invalidation under in-place mutation, cross-platform mmap
lifetime, page-splitting invariants. This is the same category of system as
LMDB/SQLite's storage layer -- where the bulk of real-world time historically
goes to the *last 20%*: the crash-recovery edge cases and concurrency races
that don't show up until stress-tested for days.

The two layers built on top of the engine -- message queues and replication --
are cheaper per line, because they inherit atomicity and snapshot isolation
rather than implementing them. They are not free, though: each has one genuinely
hard idea in it (lease expiry that cannot lose or duplicate a message; capturing
a commit's changed page set, which is knowable only while the commit is
happening) and each is a place where an implementation that looks right is
quietly wrong.

## Phase breakdown (solo, focused effort)

| Phase | Conservative estimate | Why |
|---|---|---|
| Design & on-disk format | 3-5 weeks | Choosing shadow-paging over WAL, meta-page/checksum scheme, page/node layout, deciding the durability-level API (`include/nosql/nosql.hpp`) |
| Core B+tree (COW, splits, merges, rebalancing, overflow pages, cursors) | 7-11 weeks | `src/b_tree.cpp` is 1,220 lines alone; split-point selection, the "-infinity slot" invariant, cursor re-descent on mutation are the kind of bugs that take days each to chase |
| Transactions, MVCC, nested txns, GC/free-list reclamation | 5-7 weeks | The free-list update is explicitly self-referential (freeing pages allocates pages) -- that circularity, and the "oldest reader" reclamation rule, is genuinely hard to get right and easy to get subtly wrong |
| Durability & crash safety (torn meta writes, msync/fsync ordering, cross-platform I/O) | 4-6 weeks | Getting the barrier semantics right (data flush strictly before meta flush) and validating it under actual power-loss-style testing, plus separate Windows/POSIX I/O paths |
| Allocation discipline (buffer pool, arena, open-addressed page table, txn pooling) | 2-4 weeks | Optimizing to zero steady-state allocations is a deliberate, non-default design goal, not incidental |
| Operational hardening (file mode/owner/symlink/close-on-exec, network-filesystem refusal, truncation check, preallocation, failure poisoning, shareable handles, subsystem build switches) | 1-2 weeks | Each item is small; the cost is in deciding what a failed fsync or a full disk must leave behind and testing it with injected faults (`test_hardening.cpp`) |
| Blob storage layer (tar-indexed side-car archive) | 2-3 weeks | Crash-safe append, index rebuild/catch-up from headers only |
| Durable message queues (leases, redelivery, dead-lettering, group commit, blocking receive) | 3-5 weeks | The data model is easy; at-least-once delivery under lease expiry is not, and the group-commit writer thread plus commit-generation notification add a second concurrency problem on top of the engine's |
| Replication (commit capture, segments, bundles, apply, base images, retention) | 4-6 weeks | Apply is cheap on a copy-on-write store, but capture touches the commit path, page numbers get recycled so a delta is not a suffix of the file, and proving a replica matches its primary needs randomised end-to-end equivalence tests |
| CLI tool + examples | 1-2 weeks | Straightforward once the API is stable |
| Testing (unit + mvcc + torture/steady stress tests) | 6-9 weeks | `test_torture.cpp`/`test_steady.cpp` imply deliberate crash-injection and long-running stress runs -- this is where storage engines actually get validated, and it's iterative with the phase above |
| Documentation (design.md, format.md, limitations.md, mq.md, replication.md, estimates.md) | 2-3 weeks | Writing this down accurately (as opposed to just having it work) takes real time. The queue and replication documents began as specifications *before* the code, which is cheaper up front and is also how they drifted from what was built; keeping them true is a recurring cost |
| Integration hardening / bug-fix tail | 6-9 weeks | The long tail of concurrency and crash-consistency bugs that only surface under combined load -- historically the most underestimated phase in this domain, and two of the upper layers reach into the commit path |

**Total: roughly 46-72 person-weeks &asymp; 11-17 months of focused solo work.**

## Caveats that matter

* **Team size doesn't scale this well.** The core engine (B+tree + txn +
  durability) is one tightly-coupled subsystem where a second engineer mostly
  adds coordination overhead rather than parallel throughput. Two engineers
  might get this to ~8-12 months, not half the time.
* **This assumes no false starts.** Real projects in this space (LMDB,
  SQLite's B-tree layer) went through multiple design revisions before
  landing on their final on-disk format; this estimate assumes the design in
  `docs/design.md` is roughly the first one that works, which is optimistic
  even though it's being called "conservative" on the execution side.
* **Excludes fuzzing infrastructure / CI hardening over months of real-world
  use**, which is how bugs in this class of software are actually found in
  practice (e.g., SQLite's OSS-Fuzz history). A "done" storage engine that's
  only been tested for a few months of dev time should still be considered
  pre-production.
* **The upper layers are the only part that parallelises.** Blob storage,
  message queues and replication sit on the engine's API and barely touch each
  other, so they are the one place a second or third engineer adds real
  throughput -- roughly 9-14 of the weeks above. The engine underneath them
  does not divide.

So: conservatively, **11-17 months for one expert engineer working alone**,
with real risk of running longer given how often crash-consistency bugs in
this exact design pattern (shadow paging + mmap) surface only under
production-scale stress.
