# A message queue on libnosql

A durable, MSMQ-shaped local queue built on the store's public API: FIFO delivery,
peek-lock receive with leases and redelivery, dead-lettering, blocking receive
without polling, optional group commit, and side-car storage for large bodies.
This document describes what is implemented. The API is
[message_queue.hpp](../include/nosql/message_queue.hpp), the logic is in
`src/message_queue.cpp` and `src/mq_writer.cpp`, the record codec is
[mq_record.hpp](../include/nosql/internal/mq_record.hpp), the worked example is
[examples/mq.cpp](../examples/mq.cpp), and the tests are `tests/test_mq.cpp` and
`tests/test_mq_steady.cpp`. The subsystem is built by default; `NOSQL_WITH_MQ=OFF`
removes it (it needs `NOSQL_WITH_BLOB`).

## Contract

- Delivery is at-least-once. A message is in exactly one of the ready, lease or
  dead trees at any time, because every move is a delete plus an insert in one
  transaction. Exactly-once effects are possible only when the consumer's side
  effects commit in the same store and transaction as the dequeue or ack
  (`receive(Txn&)`, `ack(Txn&, ...)`).
- Ordering is by sequence number and survives restarts, because sequences come from a
  counter in the store, never a clock. A nacked or swept message returns under its
  original sequence, so it keeps its place rather than overtaking fresh traffic.
- Queue records and sequence keys use little-endian integers; bodies and the opaque
  queue identity are not converted. Queue records are the same in store formats 5 and 6;
  only store format 6 is supported.
- One writer at a time, in one process, as for the store itself. Any number of
  threads may send and receive; writes serialise on the store's writer.
- Options are per handle and are not persisted. Applications must open every handle
  on a queue with compatible policies. There is no injected clock.
- Invalid options are rejected at open with `InvalidArgument`: lease duration must be
  positive, TTL and linger nonnegative, maximum deliveries nonzero. A queue name is
  1 to 48 bytes and must not contain `/`.

## Data model

One queue is four sub-databases named after it. For a queue called `orders`:

| sub-database | key | value |
|---|---|---|
| `mq/orders/ready` | u64 sequence | message record |
| `mq/orders/lease` | u64 sequence | lease header + message record |
| `mq/orders/dead` | u64 sequence | dead header + message record |
| `mq/orders/meta` | ASCII names | little-endian scalars and the queue identity |

The three message trees use `DbFlags::IntegerKey`, so the key is a little-endian
`uint64_t` compared numerically. Sends arrive in ascending order and use
`PutMode::Append`, the dense bulk-load path. Each queue costs four sub-database
handles (more for external bodies) against the default `maxDbs` of 128; raise
`Env::Options::maxDbs` for many queues. Handles are reopened per transaction.

Meta keys: `queueId` (128-bit identity, generated once), `nextSeq` (starts at 1) and
`nextToken` (delivery tokens, strictly increasing across handles and restarts;
if absent it is initialised above every existing lease token on open). Ready, lease
and dead counts are `Db::count()` of the corresponding tree, which is constant time;
older `readyCount`/`leaseCount`/`deadCount` meta keys are erased on open.

Message record (28-byte header, then the correlation id, then the body):

```
offset size field
  0     8   enqueuedAt          u64 nanoseconds since the Unix epoch
  8     8   expiresAt           u64 (0 = never)
 16     4   deliveryCount       u32
 20     4   bodyLength          u32 (the blob key length when external)
 24     2   correlationIdLength u16
 26     1   flags               u8  (bit 0: body lives in the blob archive)
 27     1   reserved            u8
 28     N   correlationId
 28+N   M   body, or the blob key when bit 0 is set
```

A correlation id is at most 65,535 bytes and a body at most 4 GiB minus one.
A lease record is a 16-byte header (`leaseExpiresAt` u64, `consumerToken` u64)
followed by the message record verbatim; a dead record is a 16-byte header (reason
u8, seven zero bytes, `deadAt` u64) followed by the message record verbatim. Keeping
the record verbatim makes nack and lease expiry a move, not a re-encode, and a
crash mid-lease loses nothing.

## Semantics

**Send.** `send(body, correlationId)` reads and bumps `nextSeq`, appends to `ready`
and commits, so the counter and the message can never disagree. With group commit
enabled it instead hands the message to the writer thread and blocks until that
batch commits. `send(Txn&, ...)` appends inside the caller's transaction, so the
message becomes visible exactly when the caller's other writes do; it is never
batched and rejects external bodies with `Unsupported`. `sendBatch` appends many
bodies in one transaction (one fsync); it does not use the group-commit writer and
takes no correlation ids. A TTL, if set, is stamped on the message at send.

**Peek.** A read snapshot and a cursor on `ready`. Nothing is leased or hidden and
the write lock is never taken. It returns an owning copy.

**Receive (peek-lock).** `tryReceive` first counts `ready` under a read snapshot and
returns an invalid `Receipt` if it is empty, so consumers that lose a race cost
nothing. Otherwise, in one write transaction, it takes the head message and:
1. if the message has expired, moves it to `dead` (reason `Expired`) and continues;
2. if `deliveryCount + 1` would exceed `maxDeliveries`, moves it to `dead` (reason
   `TooManyDeliveries`) and continues;
3. otherwise increments `deliveryCount`, takes the next delivery token, writes a
   lease with deadline `now + leaseDuration`, and erases it from `ready`.
The transaction commits only if it changed something. Retiring the last message is
a write even though nothing is returned, so the commit is keyed to "changed", not to
"got a message"; a poll that changed nothing aborts and does not commit, because a
commit would wake every waiter in the process for nothing.

`receiveBatch(max, timeout)` repeats the take inside one transaction, and
`ackAll` settles a batch in one transaction; acking or receiving one message at a
time is fsync-bound at the same rate as committing one message at a time.
`receive(Txn&)` takes inside the caller's transaction; aborting that transaction puts
the message back as though it was never delivered.

**Blocking receive.** `receive(timeout)` and `receiveBatch` sample
`Env::commitGeneration()`, tries, and if nothing came waits with
`Env::waitForCommit(seen, remaining)`. Sampling before the attempt is what prevents a
lost wakeup when a send lands between the attempt and the wait. Any commit to the
store wakes every waiter (there are no per-queue condition variables); each woken
consumer re-probes with a read snapshot before reaching for the write lock, which is
the herd mitigation. Timeout returns an invalid `Receipt` (or the messages gathered).
There is no cross-process wakeup: the store takes an exclusive file lock.

**Ack and nack.** Every `Receipt` carries the delivery token, the queue identity and
its environment. `ack` erases the lease. `nack` moves the message back to `ready`
under its original sequence, or straight to `dead` with reason `Rejected` when
`deadLetter` is set. Settling a receipt whose lease has expired and been swept, or
re-leased to someone else, is harmless: the token no longer matches, nothing changes
and nothing is committed. The exception is `ack(Txn&, ...)`, which aborts the
caller's transaction and throws `BadTransaction` for a stale receipt, so the caller's
other effects cannot commit on a dequeue that no longer holds. A receipt from another
queue or environment throws `BadTransaction`. Dropping a `Receipt` unsettled is a
redelivery after the lease expires, not a loss.

**Lease expiry.** `sweep()` collects, under a read snapshot, up to 256 leases whose
deadline has passed (scanning the lease tree from the lowest sequence), then in one
write transaction re-checks each one's token and deadline and moves it back to
`ready`. A sweep with nothing due commits nothing, so a periodic sweeper does not wake
waiters. It returns the number moved; with more than 256 due, call it again. Scanning
is linear in the number of leased messages. TTL expiry is not done by `sweep`: an
expired message is retired when it reaches the head of the queue, so it occupies space
until a consumer walks past it.

**Dead letters.** `deadLetters(max)` returns owning copies; `purgeDeadLetters()`
clears the tree and returns the count.

**External bodies.** With `externalBodyThreshold(n)`, bodies of at least `n` bytes go
to a side-car tar archive (`BlobStorage`, opened with `syncOnAppend(true)`, named
`<store stem>-mq-<queue>.tar` in the store's directory) and the message record holds
only the blob key (`<queue>-<open time>-<counter>`). The blob is committed in its own
transaction before the queue transaction, because `BlobStorage` takes the store's
writer lock and the writer is not reentrant. A crash in between leaves unreferenced
archive bytes, never a message pointing at bytes that are not there. `fetchBody`
returns the body zero-copy. Acking does not reclaim the blob: the archive is
append-only and only a repack shrinks it, so an external-body queue carries a
compaction chore. Database replication does not include archive payloads.

## Group commit

`Options::groupCommit(maxBatch, linger)` starts a dedicated writer thread for
`send()`. Producers append to an in-memory intake and block until their batch
commits. The writer drains up to `maxBatch` messages per transaction, lingering up
to `linger` (default 1 ms) only while the batch is still small and more may be coming;
a full batch or a shutdown goes straight out. The fsync is per transaction, not per
message, which is the difference between a per-message commit rate and a per-batch
one. If the commit throws, every message in that batch receives the same exception.
`intakeLimit(bytes)` (default 16 MiB, body plus correlation id, including the batch
being committed) makes an over-full intake fail with `Busy` rather than grow. Sends
after shutdown begins throw `BadTransaction`. The writer serves sends only: acks and
receives commit on their own, or in the caller's `ackAll`/`receiveBatch` batch.

Do not use `Durability::None` to make a queue look faster: acknowledged messages could
vanish. `Durability::NoMetaSync` is the defensible middle, exposed only to power loss.

## Cost model

There are no queue throughput figures in this repository, and the older figures
in this document were extrapolations, not measurements. What is measured
(`design.md`, "Measurements") is the store: on this development host one durable
single-put commit costs about 1.1-1.2 ms in `bench_kv` (roughly 840 to 1,050
commits per second across the two benchmarks), almost all of it the fsync. A queue that commits once per message is therefore
storage-bound at that rate for sends, and again for every ack and every unbatched
receive; batching (`sendBatch`, group commit, `receiveBatch`, `ackAll`) amortises the
fsync across the batch. Peek is a read snapshot. The non-durable rate of the tree is
far above any of this, so measure the intended workload on the intended storage.

## Operations

- **File growth.** Head-insert with tail-delete plateaus: `mqsteady` asserts a drained
  queue does not grow the file, and the store's `steady` and `torture` tests cover the
  underlying pattern.
- **Reclamation pinning.** Pages freed by a commit become reusable only when no live
  reader holds an older snapshot. A long read transaction in a queue process (a
  monitoring thread iterating the queue) pins reclamation for the whole store.
  `Message` therefore owns its bytes instead of borrowing from a snapshot: a consumer
  holds a receipt for as long as the work takes, and a borrowed view would pin a
  snapshot that long.
- **Compaction.** A drained queue keeps its pages on the free list; `compact(src, dst)`
  returns the space but needs the store closed. `checkIntegrity(Txn&)` validates the
  trees and is worth running after any torture scenario.
- **Slow sweeps.** The lease scan is linear in leased messages; a large in-flight
  population would need an expiry-ordered index, which does not exist.

## Not implemented

These appeared in earlier drafts of this document and are not in the code:
priorities (keys are plain sequences); delayed delivery or nack backoff
(`visibleAfter`); a lease-expiry index; a post-commit callback (`onCommit`);
per-queue condition variables or `notify_one` selection; a shared writer for acks;
persisted queue policy; an injected clock; a queue-specific integrity audit. The
public commit-generation wait (`Env::commitGeneration`, `Env::waitForCommit`) that
the design called the one required library change is implemented and is what blocking
receive uses.

## Tests

`tests/test_mq.cpp` covers FIFO order and ascending sequences, counters surviving
reopen, peek not leasing, an empty receive not committing, nack under the original
sequence, dead-lettering by nack, delivery limit and TTL, abandoned-lease redelivery,
sweeping with nothing due costing no commit, a blocked consumer woken by a send, timeout,
no lost wakeup when a send races the receive, batched receive and send (including
C++17 containers and empty ranges), transactional receive and send, group commit
preserving every send and their order and its byte budget, external bodies, queue
isolation, bad names, stale receipts, and foreign transactions and receipts.
`tests/test_mq_steady.cpp` runs concurrent producers and consumers asserting nothing is
lost or delivered twice while leased, that abandoned and rejected work is always
recovered, that a drained queue does not grow the file, and that dead letters
accumulate and purge cleanly.

Two bugs those tests caught, both invisible to a single-threaded check:

- A stale receipt could cancel someone else's delivery. Consumer A stalls past its
  lease, the sweeper returns the message, consumer B leases it, and A's late ack then
  erased B's lease. The receipt now carries the lease's `consumerToken`, and a settle
  whose token no longer matches is ignored.
- A receive that returned nothing could still have work to commit. Skipping an expired
  or over-delivered message is a write; when it was the last message, aborting the
  transaction threw the dead-lettering away and the next call redid it. `take` now
  reports whether it changed anything and the caller commits on that.
