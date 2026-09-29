# On-disk Format 6

Format 6 is the only store format this code reads or writes. It uses independently
implemented XXH3-64 checksums, little-endian metadata,
a separate catalog, and a free list kept in leaf-sized chunks under 16-byte keys.
No third-party checksum code or library is included. Meta is 480 bytes and the
slotted-page header is 16 bytes. Opening a store of any other format version,
format 5 included, fails with `ErrorCode::Incompatible` for both read-only
and writable open; nothing is converted.

Three version numbers appear in the persisted bytes and are independent of each other:

| where | value | constant |
|---|---|---|
| store meta `version` (this document) | 6 | `kFormatVersion` in `format.hpp` |
| replication bundle and segment headers | 5 | `kReplicationVersion` in `replication_format.hpp` |
| blob index record prefix | 5 | the literal in `blob_storage.cpp` |

The replication and blob-index versions did not change when the store format moved
from 5 to 6, so a "5" next to "bundle" or "blob index" is not a stale store format.

`nosql compact source.db compacted.db` builds a separate format-6 store with
new store/commit identities. It does not copy archives or convert application
schemas. Replication restarts from a fresh snapshot of the compacted store.

All format-owned integer fields use little-endian encoding through `Little<T>`,
`readLittle`, and `writeLittle`. Application values remain opaque bytes. Integer
keys supplied to `IntegerKey` trees must be encoded little-endian by the caller;
`Slice::ref(nativeInteger)` is portable only on little-endian hosts. Queue keys,
records/scalars, free-page records, blob index records/high-water marks, and
replication framing use explicit little-endian encoding. Identity arrays are
opaque 16-byte tokens, copied verbatim, not interpreted as portable numbers.

All page, run, meta, blob and frame digests use the full unsigned 64-bit result of the
published XXH3 algorithm (`XXH3_64bits`, seed zero, default secret). The code is implemented
in-tree in standard C++17, not the xxHash library and not a newly invented hash
algorithm. The long-input loop has NEON, SSE2 and AVX2 kernels beside the portable
scalar code; every kernel produces the same digest, so files move between AArch64
and x86-64 hosts unchanged. Streaming produces the identical result regardless of
chunk boundaries. Digests are stored little-endian. Page, blob,
and frame hashes preserve zero as a legitimate digest; meta maps zero to one
because Checkpoint uses a nonzero meta checksum as its validity marker.

Golden values (identical to xxHash 0.8.2): empty input is `0x2d06800538d394c2`;
ASCII `a` is `0xe6c632b61e964e1f`; `abc` is `0x78af5f94892f3950`; `123456789` is
`0x72dcb18b67a17dff`; `The quick brown fox jumps over the lazy dog` is
`0xce7d19a5418fb365`. These are error-detection hashes, not authentication or
cryptographic collision resistance.

Earlier development formats, format 5 included, are not read: they are rejected
as incompatible, never converted.

### Checksum Algorithm

All arithmetic is unsigned modulo 2^64 unless noted; `rotl` is 64-bit rotation
left. `r64(p)` and `r32(p)` decode little-endian words at byte position `p`
regardless of host byte order, `bswap` reverses the eight bytes of a word,
`lo32`/`hi32` select word halves, and `fold(a, b)` is the low 64 bits XOR the
high 64 bits of the 128-bit product `a * b`. `sec` is the 192-byte constant.

```text
P32_1 = 0x9e3779b1          P32_2 = 0x85ebca77          P32_3 = 0xc2b2ae3d
P64_1 = 0x9e3779b185ebca87  P64_2 = 0xc2b2ae3d27d4eb4f  P64_3 = 0x165667b19e3779f9
P64_4 = 0x85ebca77c2b2ae63  P64_5 = 0x27d4eb2f165667c5
MX1   = 0x165667919e3779f9  MX2   = 0x9fb21c651e98df25

sec = b8fe6c39 23a44bbe 7c01812c f721ad1c ded46de9 839097db 7240a4a4 b7b3671f
      cb79e64e ccc0e578 825ad07d ccff7221 b8084674 f743248e e03590e6 813a264c
      3c2852bb 91c300cb 88d0658b 1b532ea3 71644897 a20df94e 3819ef46 a9deacd8
      a8fa763f e39c343f f9dcbbc7 c70b4f1d 8a51e04b cdb45931 c89f7ec9 d9787364
      eac5ac83 34d3ebc3 c581a0ff fa1363eb 170ddd51 b7f0da49 d3165526 29d4689e
      2b16be58 7d47a1fc 8ff8b8d1 7ad031ce 45cb3a8f 95160428 afd7fbca bb4b407e

avalanche64(h) = h ^= h >> 33; h *= P64_2; h ^= h >> 29; h *= P64_3; h ^= h >> 32
avalanche(h)   = h ^= h >> 37; h *= MX1; h ^= h >> 32
rrmxmx(h, len) = h ^= rotl(h, 49) ^ rotl(h, 24); h *= MX2; h ^= (h >> 35) + len;
                 h *= MX2; h ^= h >> 28
mix16(p, s)    = fold(r64(p) ^ r64(sec + s), r64(p + 8) ^ r64(sec + s + 8))
```

Input `in` of length `len` is hashed by length class:

* `len = 0`: `avalanche64(r64(sec+56) ^ r64(sec+64))`.
* `1..3`: 32-bit `c = in[0] << 16 | in[len/2] << 24 | in[len-1] | len << 8`;
  `avalanche64(c ^ (r32(sec) ^ r32(sec+4)))`.
* `4..8`: `v = r32(in+len-4) + (r32(in) << 32)`;
  `rrmxmx(v ^ (r64(sec+8) ^ r64(sec+16)), len)`.
* `9..16`: `lo = r64(in) ^ (r64(sec+24) ^ r64(sec+32))`,
  `hi = r64(in+len-8) ^ (r64(sec+40) ^ r64(sec+48))`;
  `avalanche(len + bswap(lo) + hi + fold(lo, hi))`.
* `17..128`: `acc = len * P64_1`; if `len > 96` add `mix16(in+48, 96)` and
  `mix16(in+len-64, 112)`; if `len > 64` add `mix16(in+32, 64)` and
  `mix16(in+len-48, 80)`; if `len > 32` add `mix16(in+16, 32)` and
  `mix16(in+len-32, 48)`; always add `mix16(in, 0)` and `mix16(in+len-16, 16)`;
  result `avalanche(acc)`.
* `129..240`: `acc = len * P64_1`; add `mix16(in+16i, 16i)` for `i = 0..7`;
  `acc = avalanche(acc)`; add `mix16(in+16i, 16(i-8)+3)` for `i = 8..len/16-1`;
  add `mix16(in+len-16, 119)`; result `avalanche(acc)`.
* `241..`: eight 64-bit accumulators start as
  `(P32_3, P64_1, P64_2, P64_3, P64_4, P32_2, P64_5, P32_1)`. The input is
  consumed in 64-byte stripes; stripe `s` of a 16-stripe block uses secret
  position `8s`: for lane `i = 0..7`, `d = r64(stripe + 8i)`,
  `k = d ^ r64(sec + 8s + 8i)`, `acc[i^1] += d`, `acc[i] += lo32(k) * hi32(k)`.
  After every sixteenth stripe each lane is scrambled:
  `a ^= a >> 47; a ^= r64(sec + 128 + 8i); a *= P32_1`. Exactly `(len-1)/64`
  stripes are consumed this way. Then the final 64 bytes of the input, which may
  overlap the last consumed stripe, are accumulated once with secret position 121
  and no scramble. Merge: `h = len * P64_1`; for `j = 0..3`,
  `h += fold(acc[2j] ^ r64(sec + 11 + 16j), acc[2j+1] ^ r64(sec + 19 + 16j))`;
  result `avalanche(h)`.

Streaming retains the eight accumulators, a 256-byte buffer, the stripe position
inside the current block, and the total length: 344 bytes, no allocation.
Digest does not mutate state, so updates after intermediate digests remain valid.
Tests compare against a separate byte-decoding reference and all split points
for short inputs, in addition to golden vectors and unaligned long inputs.

## Current Meta Layout

Offsets below are relative to the Meta record, which begins 16 bytes into a
meta page. Identity fields are opaque 16-byte tokens.

| Offset | Bytes | Field |
|---|---|---|
| 0 | 16 | magic, `NOSQL-ORLOV`, NUL padded |
| 16 | 4 | version = 6 |
| 20 | 4 | pageSize |
| 24 | 8 | txnid |
| 32 | 8 | lastPgno |
| 40 | 8 | fileSize |
| 48 | 8 | namedDbs |
| 56 | 48 | freeTree |
| 104 | 48 | mainTree |
| 152 | 8 | checksum |
| 160 | 16 | storeId |
| 176 | 16 | commitId |
| 192 | 16 | parentId |
| 208 | 8 | parentChecksum |
| 216 | 8 | deferredCount, at most 26 |
| 224 | 208 | deferredPages[26], unused entries zero |
| 432 | 48 | catalogTree |

Meta checksum: copy the 480-byte record, zero bytes 152..159, and hash all
480 bytes with XXH3-64. Replace a zero result with 1.
Checksums detect errors; store/commit identities distinguish histories.

New stores receive independent 128-bit store and initial commit identities.
Every local commit records the previous identity/checksum and draws a new
commit identity from `newIdentity()`: a splitmix64 stream seeded once per thread
from two `std::random_device` words, the clock and the thread id, never
returning zero. Copies/replay preserve identities; independent writes to a
clone do not. Identities are probabilistic identifiers, not content hashes,
signatures, or authorization tokens, and the generator is not cryptographic.

Late free-page records that exceed the normal freelist convergence budget are
persisted in meta and loaded on reopen. Exceeding 26 deferred pages fails the
commit rather than leaking them silently. A commit failure requires closing and
reopening the environment because its publication outcome may be indeterminate.

`Tree::flags` low 16 bits retain public ordering flags; bits 16..31 encode the
integer key width, 4 or 8, selected on first integer insertion. Mixed widths
and other integer key lengths are rejected. The free tree has flags zero: its
keys are 16 bytes under the default byte-wise ordering (see Free list).

Slotted pages reserve their last eight bytes for the XXH3-64 digest, so an empty 64 KiB
page has `upper == 65528`; no sentinel is needed.
Geometry probing includes both meta slots at every supported page size.

## Page Integrity

Leaf and branch hashes cover bytes 0..pageSize-9, including page number, flags,
slots, free gap, and nodes. The little-endian digest occupies the final eight bytes.
Writers zero free gaps and seal final images before copying to the mapping.
By default readers verify committed mapped pages on every access before interpreting nodes;
private dirty pages are not yet sealed.

An overflow value remains contiguous after its 16-byte run header. The final
eight bytes of the entire run hold its hash, covering every other byte in the run.
Run length is bounds-checked before verification. This is one hash per overflow
run, not one trailer in each continuation page, preserving the contiguous Slice
API. Large overflow reads therefore checksum the whole run. With the explicit
`cacheReadChecksums()` option, read-only transactions cache successful page/run
checks in a bounded bitmap. Later accesses may miss external corruption until
another transaction or an explicit integrity check. The default does not cache
checks. This is a runtime policy only; format-6 bytes and checksum values are unchanged.

The node limit is `((pageSize - 24) / 2) & ~3` (2036 at 4 KiB); key limits and
node header sizes are unchanged. Overflow allocation rounds `16 + valueBytes + 8`
upward to pages. Meta keeps its separate 64-bit checksum, not a page trailer.
Checksums detect damage, not malicious forgery.

## Separate Catalog

Meta's catalog root references a third internal B+tree. Bytewise keys are DB
names; values are 48-byte little-endian Tree descriptors marked `N_SUBDB`.
The main tree is exclusively user data: a key and a DB may share a name, and
main-tree clear can release the whole tree. Integrity walks all three roots
and every catalog descriptor.

The free tree stores page-ID arrays, not extents. Compaction streams the
main and named trees through the bulk builder, creates a new catalog, and starts
a new history. Application-owned values are preserved verbatim.

## Blob Index Records

The blob index value prefix is 32 bytes: version:u32=5, flags:u32, payloadOffset:u64,
payloadSize:u64, checksum:u64, followed by the member-name bytes. This record version
is independent of the store format (see the table at the top). Flag bit 0 means
the checksum is known; other bits are rejected. A header-only rebuild writes
flags=0 and checksum=0. `Blob::hasChecksum()` distinguishes an unknown digest
from a legitimate zero; `storedChecksum()` returns the 64-bit digest.
`verify()` returns true when no digest is known.
The tar file and its required additive header checksum are unchanged.

## File

| pages | contents |
|---|---|
| 0 | meta page A |
| 1 | meta page B |
| 2 … `lastPgno` | branch, leaf and overflow pages |

The two meta pages alternate by transaction-id parity: transaction *n* writes
slot *n* mod 2. Open takes the valid one with the higher `txnid`.

## Tree Descriptor

All integer fields below are encoded little-endian.

```c
struct Tree {            // 48 bytes
  uint64_t root;         // page number, or ~0 for an empty tree
  uint64_t branchPages, leafPages, overflowPages, entries;
  uint32_t depth;
  uint32_t flags;        // ordering flags and integer-key width
};
```

## Page header

```c
struct Page {            // 16 bytes
  uint64_t pgno;
  uint16_t flags;        // 1 branch, 2 leaf, 4 overflow, 8 meta
  uint16_t nkeys;
  uint16_t lower;        // end of the slot array
  uint16_t upper;        // start of the node data
};
```

On an overflow page the last three fields are unused; a `uint32_t` at offset
12 holds the run length in pages.

A `uint16_t` slot array starts at offset 16 and holds byte offsets of the
nodes, ordered by key. Node bodies are packed downward from the end of the
usable page area, before the checksum trailer, and are 4-byte aligned.

## Nodes

```c
struct LeafNode   { uint16_t flags; uint16_t ksize; uint32_t vsize; };
struct BranchNode { uint32_t childLo; uint16_t childHi; uint16_t ksize; };
```

Both are 8 bytes and are followed by `ksize` key bytes.

A leaf node's payload follows the key: the value itself, or — when
`flags & N_BIGDATA` — an 8-byte page number naming an overflow run, with
`vsize` still holding the true value length.

`flags & N_SUBDB` marks a catalog leaf record whose 48-byte value is a `Tree`
naming a sub-database. These records do not occupy the main tree's keyspace.

A branch node's child page number is `childLo | (childHi << 32)` — 48 bits.
**Slot 0 of a branch page has `ksize == 0`**: it is the -infinity child and
its key is never compared.

## Limits

| quantity | rule | at 4 KiB pages |
|---|---|---|
| largest node | `((pageSize - 24) / 2) & ~3` | 2036 B |
| largest key | `(((pageSize - 16) / 4) & ~3) - 16` | 1004 B |
| largest value | `2^32 - 1` | 4 GiB |
| page numbers | 48 bits | 2^48 pages |
| tree depth | 48 levels | — |

## Free list

The free tree is a normal B+tree under the default byte-wise ordering. Keys are
16 bytes: the retiring transaction id then a chunk number, both big-endian so
that byte order is numeric order. Values are packed little-endian arrays of
`uint64_t` page numbers. An entry keyed `(K, c)` lists the `c`-th chunk of the
pages that transaction `K` retired.

A chunk holds at most `(maxNodeSize - 32) / 8` pages -- 250 at 4 KiB, 26 at
512 B -- which is as many as fit a leaf node held inline, so a chunk is never an
overflow run. The writer absorbs one chunk at a time as it needs pages, deletes
the chunks it absorbed at commit, and records what it retired plus the unspent
remainder under its own id in fresh chunks. Drawing a handful of pages from a
list of a million therefore rewrites one node, not an 8 MB value.
