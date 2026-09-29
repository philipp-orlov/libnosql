# Replication

Replication ships committed page images from a primary to a closed replica.
Only store format 6 is supported; bundles and segments carry their own
version field (5), which is independent of the store format. The public interface is
[replication.hpp](../include/nosql/replication.hpp); wire declarations are in
[replication_format.hpp](../include/nosql/internal/replication_format.hpp).
No network transport, leader election, automatic failover or archive transfer
is included. The subsystem is built by default; configuring with
`-DNOSQL_WITH_REPLICATION=OFF` compiles it out, after which
`Env::Options::shipTo()` fails with `ErrorCode::Unsupported`.

## Checkpoints

`Checkpoint` contains `txnid`, `metaChecksum`, `storeId[2]` and `commitId[2]`.
All four must match. Meta also records parent commit identity/checksum, allowing
the base immediately before the oldest retained entry to be checked. A checkpoint
ahead of the log or from a different history is not reported as up to date.
Store/commit IDs distinguish histories; XXH3-64 detects damage, not malicious forgery.

The empty store has a valid transaction-zero checkpoint. A zero meta checksum
marks an unset checkpoint; persisted meta maps a zero digest to one.
Format fields are little-endian and page sizes must agree. Application payloads
are opaque bytes, not schema-converted data.

## Workflow

1. Open the primary with `Env::configure().shipTo(directory)` to enable capture.
2. Hold a read transaction and call `copySnapshot(txn, replicaPath)` to bootstrap.
3. Obtain the replica checkpoint with `checkpointOf(replicaPath)`.
4. Use `ShipLog(directory).extract(base, bundlePath)` to create a delta.
5. Close the replica before `applyBundle(replicaPath, bundlePath)`.
6. If extraction returns `NeedBase`, take a new snapshot and restart the chain.

`copySnapshot` requires a live read transaction and a destination that does not
exist. It sizes the file to the snapshot's recorded `fileSize`, writes pages up to
the allocation frontier in 4 MiB chunks, and stamps the snapshot's Meta verbatim
into both meta slots so the copy opens at the same checkpoint. It ends with an
fsync and deletes the destination if anything fails.
The source snapshot pins reclamation during the copy, so concurrent writes may
grow the source. Compaction is different: it changes physical page numbers and
identities and therefore requires a fresh replication base.

`ShipStatus::Ok` means a bundle was written; `UpToDate` means the base is already
the latest captured checkpoint; `NeedBase` means history cannot serve the range.
`extract(base, path, through, memoryBytes)` accepts a target transaction, with zero
meaning the latest available. No bundle is written unless status is `Ok`.

`inspectBundle` reports header metadata. `applyBundle` performs full validation,
requires the expected base, and returns the target checkpoint. Reapplying the
exact target is a no-op. Independent writes to a replica create a divergent
history, which is rejected rather than merged.

[examples/replication.cpp](../examples/replication.cpp) runs a self-cleaning
bootstrap, delta apply, retention-gap and fresh-base recovery example.

## Bundle Format

The `BundleHeader` is **72 bytes**, with `magic[16]` containing
`REP-NOSQL-ORLOV` and bundle version 5 (`kReplicationVersion`, not the store format). It is followed by:

- A 32-byte `BundleIdentity`: 16-byte store ID and 16-byte base commit ID.
- `pageCount` records, each a 24-byte `PageRecord` and one page image.
- A 480-byte target Meta, as specified in [format.md](format.md).

Page records are strictly increasing by page number and contain
`pgno:u64, byteLength:u32, reserved:u32=0, checksum:u64`. The image checksum is
XXH3-64 over the complete page image, including its page trailer. The header's
`payloadChecksum` covers the header with that field zeroed, identities, every
page-record header/image, and target Meta. Every digest is the full 64-bit value.

Counts, exact file length, geometry, identities, ordering, flags, reserved fields
and hashes are validated before constructing a replacement replica. Coalescing
keeps the final image of each touched page; the resulting bundle must be applied
as a unit, without publishing intermediate checkpoints.

## Segments and Indexes

Segments have a 32-byte header with `SEG-NOSQL-ORLOV` magic and 24-byte entry
headers. Each entry contains page records, target Meta, and an eight-byte XXH3-64
over entry header, records/images and Meta. Entry `byteLength` includes the
trailer. The frame hash protects addresses as well as page images.

Readers stop at torn or invalid entries and select a contiguous history with
checked ancestry. Segments are disposable shipping history, not a recovery log:
losing them requires a new base, not rolling back a committed primary transaction.

A `.seg.idx` sidecar stores one 512-byte row per complete transaction:
`offset:u64, pageCount:u64, Meta:480, frameChecksum:u64, checksum:u64`.
XXH3-64 covers the first 504 bytes. Rows follow frame publication. Length,
contiguous offsets, row checksums and ancestry are checked; missing, torn or
corrupt indexes fall back to frame scanning. Selected frames are still verified
before extraction, so an index never substitutes for data validation.

`shipRetain(segments)` (default 16) and `shipSegmentSize(bytes)` (default 64 MiB)
control automatic retention and rolling; segment files are named by their first
transaction id, such as `000000000042.seg`, so the directory sorts in history
order. `ShipLog::available()` reports the retained transaction range.
`prune(keepFrom)` removes whole segments containing nothing at or after the cutoff
and returns the number removed. Retention must cover expected replica lag.

## Apply Publication

1. Open, lock and validate the bundle. Input bytes must remain immutable.
2. Exclusively open the replica and check its full checkpoint.
3. Create a unique `.tmp-<id>-<id>` file beside the replica. Linux attempts `FICLONE`;
   unsupported clones fall back to copying in 1 MiB chunks. The replica's
   permissions carry over to the copy.
4. Resize the temporary file, apply page images, and stamp target Meta into both
   slots. Published replica pages remain untouched.
5. Flush the replacement and atomically replace the directory entry. POSIX also
   synchronizes the parent directory; Windows uses replacement/write-through.

An interruption before replacement leaves the original unchanged. After
replacement, the target is complete, subject to filesystem durability guarantees.
A directory-sync error after replacement has an indeterminate publication
outcome; inspect the checkpoint before retrying. Exceptions clean temporary files;
a process crash may leave an unreferenced `.tmp-*` file. Confirm no active apply
owns it before removing it.

Coalesced apply cannot use the ordinary single-commit COW proof to overwrite an
arbitrarily lagging replica. Pages reused on the primary may still be live in the
replica's base snapshot. Complete temporary-file publication avoids that hazard.

Without reflink, cost is a full replica copy plus bundle writes. With reflink,
unchanged extents are shared until written. Provision fallback disk space and
test the deployment filesystem. No online-reader handover or distributed-locking
guarantee is claimed.

## Memory and Capture Failures

Capture is synchronous under the writer lock with at most 64 KiB of staging.
It cannot fail an already-published primary commit. Monitor
`EnvStats::captureFailures` and `capturedTxnid`; captured does not mean fsynced.
Storage latency and checksumming can still extend writer occupancy.

Extraction defaults to an 8 MiB coalescing dirty-buffer budget, with a 64 KiB
minimum. A temporary disk-backed B+tree coalesces in small batches and streams
sorted page numbers. This bounds dirty buffers, not RSS: fixed scratch,
transaction/segment descriptors, a separately capped idle pool and OS cache add
memory. Very long retained histories also require descriptors and open files.

Apply uses two sequential passes and a page buffer, not an offsets vector for
every image. The second pass rechecks bounds, ordering, image digests and the
aggregate hash before publication. Both passes can hit the OS cache; do not
equate application bytes read with physical device traffic.

## Archives and Operations

Snapshots and deltas cover only the database file. Blob payloads and external
queue bodies must be transferred separately, with coordinated archive identity
and durable offsets, before use on the replica. No archive-manifest protocol or
automatic sidecar repack is implemented.

CLI commands `checkpoint`, `ship` and `apply` expose the delta workflow;
`copySnapshot` and `inspectBundle` are library APIs. `ship` exits 0 for a bundle,
3 for up-to-date and 4 for a fresh-base
requirement. File drops are the transport; authentication, encryption and network
retry protocols belong to the deployment.

Regression tests cover divergent histories, retention gaps, frame/index damage,
partial writes, interrupted replacement and logical replica equivalence. Native
Windows execution, physical power-loss schedules and a complete volatile-device
model remain separate gates. See [design.md](design.md) for current verification
status.