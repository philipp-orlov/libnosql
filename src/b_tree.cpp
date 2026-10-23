// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Slotted-page B+tree with copy-on-write updates.
//
// Every page touched by a write transaction is first copied to a freshly
// allocated Page number ("touch"); the original stays untouched so older
// snapshots keep reading it. That is what makes commits atomic without a log:
// nothing reachable from the previous meta page is ever overwritten.
//
// Slot 0 of a branch page is the -infinity child: searches start comparing at
// slot 1, so that slot's key is never consulted and is stored empty. Keeping
// it empty (rather than letting a stale key linger) makes "branch keys are
// strictly increasing" a checkable invariant. The two places the placeholder
// has to become a real key -- folding a page into its left sibling, and
// shifting a node across -- substitute the parent's separator for that child,
// which is a genuine lower bound on the subtree.

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <vector>

#include <new>

#include "nosql/internal/core.hpp"
#include "nosql/internal/key_prefix.hpp"

namespace nosql::internal {
namespace {

// ------------------------------------------------------------ page prims ---

void pageInit(Page* p, PageNo no, std::uint16_t flags, std::size_t ps) noexcept
{
    p->pgno = no;
    p->flags = flags;
    p->nkeys = 0;
    p->lower = std::uint16_t(kPageHdr);
    p->upper = std::uint16_t(ps - kPageChecksumBytes);
    // The unused gap is not scrubbed here -- commit does it, where the gap is
    // exactly known and usually far smaller than a whole page.
}

void slotInsert(Page* p, unsigned i, std::uint16_t off) noexcept
{
    auto* s = slots(p);
    std::memmove(s + i + 1, s + i, (p->nkeys - i) * sizeof(std::uint16_t));
    s[i] = off;
    ++p->nkeys;
    p->lower += 2;
}

/// Appends the node body and its slot; returns the payload area.
std::byte* nodeAddLeaf(Page* p, unsigned i, Slice key, std::uint32_t vsize,
                       std::uint16_t flags) noexcept
{
    const bool big = (flags & N_BIGDATA) != 0;
    const std::size_t sz = leafNodeSize(key.size(), vsize, big);
    p->upper = std::uint16_t(p->upper - sz);
    auto* n = reinterpret_cast<LeafNode*>(reinterpret_cast<std::byte*>(p) + p->upper);
    n->flags = flags;
    n->ksize = std::uint16_t(key.size());
    n->vsize = vsize;
    auto* body = reinterpret_cast<std::byte*>(n) + sizeof(LeafNode);
    if (key.size())
        std::memcpy(body, key.data(), key.size());
    slotInsert(p, i, p->upper);
    return body + key.size();
}

void nodeAddBranch(Page* p, unsigned i, Slice key, PageNo child, std::uint64_t count) noexcept
{
    const std::size_t sz = branchNodeSize(key.size());
    p->upper = std::uint16_t(p->upper - sz);
    auto* n = reinterpret_cast<BranchNode*>(reinterpret_cast<std::byte*>(p) + p->upper);
    n->ksize = std::uint16_t(key.size());
    n->childLo = std::uint32_t(child);
    n->childHi = std::uint16_t(child >> 32);
    n->countLo = std::uint32_t(count);
    n->countHi = std::uint32_t(count >> 32);
    if (key.size())
        std::memcpy(reinterpret_cast<std::byte*>(n) + sizeof(BranchNode), key.data(), key.size());
    slotInsert(p, i, p->upper);
}

/// Removes node `i`, compacting the data area so free space stays contiguous.
void nodeDel(Page* p, unsigned i) noexcept
{
    const std::size_t sz = nodeSize(p, i);
    const std::uint16_t off = slots(p)[i];
    auto* base = reinterpret_cast<std::byte*>(p);
    std::memmove(base + p->upper + sz, base + p->upper, off - p->upper);
    auto* s = slots(p);
    for (unsigned j = 0; j < p->nkeys; ++j)
        if (s[j] < off)
            s[j] = std::uint16_t(s[j] + sz);
    std::memmove(s + i, s + i + 1, (p->nkeys - i - 1) * sizeof(std::uint16_t));
    --p->nkeys;
    p->lower -= 2;
    p->upper = std::uint16_t(p->upper + sz);
}

/// Rewrite the key of a branch node in place, keeping its child pointer.
/// Returns false when the page cannot accommodate a longer key.
bool branchSetKey(Page* p, unsigned i, Slice key) noexcept
{
    BranchNode* n = bnode(p, i);
    if (n->ksize == key.size()) {
        if (key.size())
            std::memcpy(reinterpret_cast<std::byte*>(n) + sizeof(BranchNode), key.data(),
                        key.size());
        return true;
    }
    const PageNo child = branchChild(p, i);
    const std::uint64_t count = branchCount(p, i);
    if (pageRoom(p) + nodeSize(p, i) < branchNodeSize(key.size()))
        return false;
    nodeDel(p, i);
    nodeAddBranch(p, i, key, child, count);
    return true;
}

// -------------------------------------------------------------- searches ---

// The binary searches below run several comparisons per page and three or
// four pages per lookup, so an indirect call per comparison is worth getting
// rid of. Both searches are templated on the comparison and dispatched once,
// which lets the overwhelmingly common byte-wise ordering inline.

/// `x` with its bytes in memory order as the most significant first.
inline std::uint64_t memoryOrder(std::uint64_t x) noexcept
{
    if constexpr (kLittleEndian) {
#if defined(_MSC_VER) && !defined(__clang__)
        return _byteswap_uint64(x);
#else
        return __builtin_bswap64(x);
#endif
    }
    return x;
}

/// Byte-wise ordering, as a functor the compiler can inline. Keys are short,
/// so eight bytes at a time beats a call to memcmp per probe.
struct LexCmp
{
    int operator()(Slice a, Slice b) const noexcept
    {
        const std::byte* pa = a.data();
        const std::byte* pb = b.data();
        const std::size_t n = std::min(a.size(), b.size());
        if (n >= 8) {
            std::uint64_t x, y;
            for (std::size_t i = 0; i + 8 <= n; i += 8) {
                std::memcpy(&x, pa + i, 8);
                std::memcpy(&y, pb + i, 8);
                if (x != y)
                    return memoryOrder(x) < memoryOrder(y) ? -1 : 1;
            }
            // The last eight bytes, overlapping ones already found equal.
            std::memcpy(&x, pa + n - 8, 8);
            std::memcpy(&y, pb + n - 8, 8);
            if (x != y)
                return memoryOrder(x) < memoryOrder(y) ? -1 : 1;
        } else {
            for (std::size_t i = 0; i < n; ++i) {
                if (pa[i] != pb[i])
                    return pa[i] < pb[i] ? -1 : 1;
            }
        }
        return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
    }
};
/// Fallback for the two non-default orderings.
struct FnCmp
{
    CompareFn f;
    int operator()(Slice a, Slice b) const noexcept { return f(a, b); }
};

/// Largest slot whose key is <= `key`; slot 0 is -infinity.
template <class Cmp>
unsigned branchSearchWith(const Page* p, Slice key, Cmp cmp, std::size_t ps)
{
    const std::size_t top = nodeTop(p, ps);
    unsigned lo = 1, hi = p->nkeys, best = 0;
    while (lo < hi) {
        const unsigned mid = lo + (hi - lo) / 2;
        if (cmp(checkedKey(p, mid, top), key) <= 0) {
            best = mid;
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return best;
}

unsigned branchSearch(const Page* p, Slice key, CompareFn cmp, std::size_t ps)
{
    if (cmp == &cmpLexicographic)
        return branchSearchWith(p, key, LexCmp{}, ps);
    return branchSearchWith(p, key, FnCmp{cmp}, ps);
}

/// First slot whose key is >= `key` (may equal nkeys).
template <class Cmp>
unsigned leafLowerBoundWith(const Page* p, Slice key, Cmp cmp, bool* exact, std::size_t ps)
{
    const std::size_t top = nodeTop(p, ps);
    unsigned lo = 0, hi = p->nkeys;
    *exact = false;
    while (lo < hi) {
        const unsigned mid = lo + (hi - lo) / 2;
        const int c = cmp(checkedKey(p, mid, top), key);
        if (c == 0) {
            *exact = true;
            return mid;
        }
        if (c < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

unsigned leafLowerBound(const Page* p, Slice key, CompareFn cmp, bool* exact, std::size_t ps)
{
    if (p->flags & P_PREFIX) {
        // Byte order only: the page's keys all start with its prefix, so the
        // key is placed against that once and the search runs on the rest.
        Slice rest;
        const int side = againstPrefix(leafPrefix(p, ps), key, &rest);
        if (side) {
            *exact = false;
            return side < 0 ? 0u : unsigned(p->nkeys);
        }
        return leafLowerBoundWith(p, rest, LexCmp{}, exact, ps);
    }
    if (cmp == &cmpLexicographic)
        return leafLowerBoundWith(p, key, LexCmp{}, exact, ps);
    return leafLowerBoundWith(p, key, FnCmp{cmp}, exact, ps);
}

/// `key` against leaf `p`'s key `i`, prefix included.
int againstLeafKey(const Page* p, unsigned i, Slice key, CompareFn cmp, std::size_t ps)
{
    const Slice stored = checkedKey(p, i, nodeTop(p, ps));
    if (!(p->flags & P_PREFIX))
        return cmp(key, stored);
    Slice rest;
    const int side = againstPrefix(leafPrefix(p, ps), key, &rest);
    return side ? side : rest.compare(stored);
}

/// The keys a descent knows lie around the page it enters: its parent's
/// separators either side of the child taken (or further up at an edge).
struct Bounds
{
    Slice low, high;
    bool known() const noexcept { return low.size() && high.size(); }
};

/// Where `key` likely sits among `n` slots whose keys spread evenly over
/// [b.low, b.high): the eight bytes past the bounds' common prefix, read as
/// a number. Rowid keys and most index keys are near enough to that.
unsigned guessSlot(const Bounds& b, Slice key, unsigned n) noexcept
{
    const std::byte* lo = b.low.data();
    const std::byte* hi = b.high.data();
    const std::size_t m = std::min(b.low.size(), b.high.size());
    std::size_t at = 0;
    while (at < m && lo[at] == hi[at])
        ++at;
    auto word = [at](Slice s) {
        std::uint64_t v = 0;
        for (std::size_t j = 0; j < 8; ++j)
            v = (v << 8) |
                (at + j < s.size() ? std::to_integer<std::uint64_t>(s.data()[at + j]) : 0u);
        return v;
    };
    const std::uint64_t a = word(b.low), z = word(b.high), k = word(key);
    if (z <= a || k <= a)
        return 0;
    if (k >= z)
        return n - 1;
    const unsigned g = unsigned(double(k - a) / double(z - a) * double(n));
    return g < n ? g : n - 1;
}

/// First slot in [lo, hi) for which `before` is false (it holds for a prefix
/// of the range), probing `guess` first and galloping out from it. A good
/// guess costs two probes; a bad one about twice a binary search's.
template <class Before>
unsigned searchFrom(unsigned lo, unsigned hi, unsigned guess, Before before) noexcept
{
    unsigned a = lo, z = hi;
    if (before(guess)) {
        a = guess + 1;
        for (unsigned step = 1;; step *= 2) {
            const unsigned probe = a + step - 1;
            if (probe >= hi)
                break;
            if (!before(probe)) {
                z = probe;
                break;
            }
            a = probe + 1;
        }
    } else {
        z = guess;
        for (unsigned step = 1;; step *= 2) {
            if (z < lo + step)
                break;
            const unsigned probe = z - step;
            if (before(probe)) {
                a = probe + 1;
                break;
            }
            z = probe;
        }
    }
    while (a < z) {
        const unsigned mid = a + (z - a) / 2;
        if (before(mid))
            a = mid + 1;
        else
            z = mid;
    }
    return a;
}

/// branchSearch and leafLowerBound for a descent that knows the page's
/// bounds: the binary search's chain of dependent cache misses becomes one
/// or two probes near the interpolated slot.
unsigned branchSearchIn(const Page* p, Slice key, CompareFn cmp, const Bounds& b, std::size_t ps)
{
    const unsigned n = p->nkeys;
    if (cmp != &cmpLexicographic || !b.known() || n < 8)
        return branchSearch(p, key, cmp, ps);
    const LexCmp lex;
    const std::size_t top = nodeTop(p, ps);
    const unsigned guess = 1 + guessSlot(b, key, n - 1);
    return searchFrom(1, n, guess,
                      [&](unsigned i) { return lex(checkedKey(p, i, top), key) <= 0; }) -
           1;
}

unsigned leafLowerBoundIn(const Page* p, Slice key, CompareFn cmp, const Bounds& b, bool* exact,
                          std::size_t ps)
{
    const unsigned n = p->nkeys;
    if (cmp != &cmpLexicographic || !b.known() || n < 8 || (p->flags & P_PREFIX))
        return leafLowerBound(p, key, cmp, exact, ps);
    const LexCmp lex;
    const std::size_t top = nodeTop(p, ps);
    const unsigned i = searchFrom(0, n, guessSlot(b, key, n),
                                  [&](unsigned j) { return lex(checkedKey(p, j, top), key) < 0; });
    *exact = i < n && lex(checkedKey(p, i, top), key) == 0;
    return i;
}

// -------------------------------------------------------------- overflow ---

unsigned overflowRun(std::size_t ps, std::size_t vsize) noexcept
{
    return unsigned((kPageHdr + vsize + kPageChecksumBytes + ps - 1) / ps);
}

std::byte* overflowCreate(TxnImpl* t, std::size_t vsize, PageNo* out)
{
    const std::size_t ps = t->pageSize();
    const unsigned n = overflowRun(ps, vsize);
    const PageNo p0 = t->allocPages(n);
    Page* h = t->allocBuffer(p0, P_OVERFLOW, n);
    h->pgno = p0;
    h->flags = P_OVERFLOW;
    h->nkeys = 0;
    setOvPages(h, n);
    std::byte* data = reinterpret_cast<std::byte*>(h) + kPageHdr;
    // The caller writes the first `vsize` bytes; zero only the tail, so a run
    // costs one small memset rather than a full-run one.
    std::memset(data + vsize, 0, std::size_t(n) * ps - kPageHdr - vsize);
    *out = p0;
    return data;
}

void overflowRelease(TxnImpl* t, Tree& tr, PageNo p0)
{
    const unsigned n = ovPages(t->getPage(p0, false, TxnImpl::PageKind::Overflow));
    t->freePage(p0, n);
    tr.overflowPages -= n;
}

// ---------------------------------------------------------------- touch ----

/// Ensure the page at stack level `lvl` belongs to this transaction.
Page* touchLevel(CursorImpl& c, int lvl)
{
    TxnImpl* t = c.txn;
    Page* p = c.pg[lvl];
    if (t->isOwnDirty(p->pgno))
        return p;
    if (Page* back = t->unspill(p->pgno)) {
        c.pg[lvl] = back;  // the same page number: the parent needs no update
        return back;
    }
    // Every node, before a writer edits the copy by the sizes they claim.
    validateNodes(p, t->pageSize());
    const PageNo np = t->allocPages(1);
    Page* d = t->allocBuffer(np, p->flags, 1);
    std::memcpy(d, p, t->pageSize());
    d->pgno = np;
    t->freePage(p->pgno);
    c.pg[lvl] = d;
    if (lvl == 0) {
        c.tree().root = np;
        t->markDirty(c.dbi);
    } else {
        setBranchChild(c.pg[lvl - 1], c.idx[lvl - 1], np);
    }
    return d;
}

void touchPath(CursorImpl& c)
{
    for (int i = 0; i <= c.top; ++i)
        touchLevel(c, i);
    c.txn->markDirty(c.dbi);
}

// ----------------------------------------------------------------- split ---

/// A node in transit: read out of a page being rebuilt, or freshly inserted.
struct Entry
{
    Slice key;
    const std::byte* pay = nullptr;  ///< nullptr => zero-fill (reserve)
    std::uint32_t paylen = 0;        ///< bytes stored inside the node
    std::uint32_t vsize = 0;         ///< logical value length
    std::uint16_t flags = 0;
    PageNo child = 0;
    std::uint64_t count = 0;  ///< branch entries: leaf entries under `child`
};

/// Entry `i` of `p`, its whole key assembled in scratch on a P_PREFIX leaf.
Entry readEntry(const Page* p, unsigned i, TxnImpl* t)
{
    Entry e;
    e.key = nodeKey(p, i);
    if (p->flags & P_PREFIX) {
        const Slice prefix = leafPrefix(p, t->pageSize());
        std::byte* whole = t->scratch().alloc(prefix.size() + e.key.size());
        std::memcpy(whole, prefix.data(), prefix.size());
        if (e.key.size())
            std::memcpy(whole + prefix.size(), e.key.data(), e.key.size());
        e.key = Slice(whole, prefix.size() + e.key.size());
    }
    if (isLeaf(p)) {
        const LeafNode* n = lnode(p, i);
        e.flags = n->flags;
        e.vsize = n->vsize;
        e.paylen = (n->flags & N_BIGDATA) ? std::uint32_t(sizeof(PageNo)) : std::uint32_t(n->vsize);
        e.pay = reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode) + n->ksize;
    } else {
        e.child = branchChild(p, i);
        e.count = branchCount(p, i);
    }
    return e;
}

std::size_t entryCost(const Entry& e, bool leaf) noexcept
{
    return 2 + (leaf ? leafNodeSize(e.key.size(), e.vsize, (e.flags & N_BIGDATA) != 0)
                     : branchNodeSize(e.key.size()));
}

void emit(Page* p, unsigned at, const Entry& e, bool leaf) noexcept
{
    if (!leaf) {
        nodeAddBranch(p, at, e.key, e.child, e.count);
        return;
    }
    std::byte* dst = nodeAddLeaf(p, at, e.key, e.vsize, e.flags);
    if (e.pay)
        std::memcpy(dst, e.pay, e.paylen);
    else
        std::memset(dst, 0, e.paylen);
}

/// Pick a boundary so both halves fit, preferring a balanced split but
/// honouring the append/prepend fast paths that keep bulk loads dense.
unsigned chooseSplit(const Entry* e, unsigned n, unsigned newindx, bool leaf, std::size_t usable,
                     Arena& scratch)
{
    std::size_t* pre = scratch.allocArray<std::size_t>(std::size_t(n) + 1);
    pre[0] = 0;
    for (unsigned i = 0; i < n; ++i)
        pre[i + 1] = pre[i] + entryCost(e[i], leaf);
    const std::size_t total = pre[n];

    // 0 means "balanced"; otherwise aim for this boundary if it is legal.
    const unsigned want = (newindx == n - 1) ? n - 1 : (newindx == 0 ? 1u : 0u);

    unsigned best = 0;
    std::size_t bestDiff = ~std::size_t(0);
    for (unsigned s = 1; s < n; ++s) {
        if (pre[s] > usable)
            break;
        if (total - pre[s] > usable)
            continue;
        const std::size_t diff =
            want ? (s > want ? s - want : want - s)
                 : (pre[s] * 2 > total ? pre[s] * 2 - total : total - pre[s] * 2);
        if (diff < bestDiff) {
            bestDiff = diff;
            best = s;
        }
    }
    if (!best)
        throw Error(ErrorCode::Corrupted, "page split found no legal boundary");
    return best;
}

/// Whether `tr` stores its leaves' shared key prefixes once (PrefixKeys).
bool compresses(const Tree& tr) noexcept
{
    return (tr.flags & std::uint32_t(DbFlags::PrefixKeys)) != 0;
}

/// Whether `n` sorted entries fit one leaf of a prefix-compressing tree.
bool fitsPrefixed(const Entry* e, unsigned n, std::size_t usable) noexcept
{
    return leafLayoutBytes(e, n, leafPrefixFor(e, n)) <= usable;
}

/// chooseSplit for a leaf of a prefix-compressing tree, where what an entry
/// costs depends on the keys it shares a page with. Tries boundaries outwards
/// from the one chooseSplit would prefer, so it usually checks one or two.
unsigned chooseSplitPrefixed(const Entry* e, unsigned n, unsigned newindx, std::size_t usable,
                             Arena& scratch)
{
    unsigned want;
    if (newindx == n - 1) {
        want = n - 1;
    } else if (newindx == 0) {
        want = 1;
    } else {
        std::size_t* pre = scratch.allocArray<std::size_t>(std::size_t(n) + 1);
        pre[0] = 0;
        for (unsigned i = 0; i < n; ++i)
            pre[i + 1] = pre[i] + entryCost(e[i], true);
        want = 1;
        std::size_t bestDiff = ~std::size_t(0);
        for (unsigned s = 1; s < n; ++s) {
            const std::size_t diff =
                pre[s] * 2 > pre[n] ? pre[s] * 2 - pre[n] : pre[n] - pre[s] * 2;
            if (diff < bestDiff) {
                bestDiff = diff;
                want = s;
            }
        }
    }
    for (unsigned d = 0; d < n; ++d) {
        for (const unsigned s : {want + d, want - d}) {
            if (s >= 1 && s < n && fitsPrefixed(e, s, usable) && fitsPrefixed(e + s, n - s, usable))
                return s;
            if (!d)
                break;
        }
    }
    throw Error(ErrorCode::Corrupted, "page split found no legal boundary");
}

/// Copies an inserted entry's key and payload into scratch: they may point
/// into the very page about to be laid out again.
void detach(Entry& e, Arena& scratch)
{
    std::byte* key = scratch.alloc(e.key.size() ? e.key.size() : 1);
    if (e.key.size())
        std::memcpy(key, e.key.data(), e.key.size());
    e.key = Slice(key, e.key.size());
    if (e.pay && e.paylen) {
        std::byte* pay = scratch.alloc(e.paylen);
        std::memcpy(pay, e.pay, e.paylen);
        e.pay = pay;
    }
}

void splitPage(CursorImpl& c, int level, unsigned newindx, const Entry& ne);

/// Adds `delta` to the counts of the branch slots on the cursor's path
/// above `top` (the path must already be ours).
void adjustPathCounts(CursorImpl& c, std::int64_t delta) noexcept
{
    for (int i = 0; i < c.top; ++i)
        setBranchCount(c.pg[i], c.idx[i], std::uint64_t(std::int64_t(branchCount(c.pg[i], c.idx[i])) + delta));
}

/// Insert (key -> child, holding `count` entries) just after the child the
/// cursor descended through at `level`, splitting that branch page if full.
void branchInsert(CursorImpl& c, int level, Slice key, PageNo child, std::uint64_t count)
{
    Page* p = c.pg[level];
    const unsigned at = c.idx[level] + 1;
    if (pageRoom(p) >= branchNodeSize(key.size()) + 2) {
        nodeAddBranch(p, at, key, child, count);
        return;
    }
    Entry ne;
    ne.key = key;
    ne.child = child;
    ne.count = count;
    splitPage(c, level, at, ne);
}

void splitPage(CursorImpl& c, int level, unsigned newindx, const Entry& ne)
{
    TxnImpl* t = c.txn;
    const std::size_t ps = t->pageSize();
    const std::size_t usable = ps - kPageHdr - kPageChecksumBytes;
    Tree& tr = c.tree();
    Page* mp = c.pg[level];
    const bool leaf = isLeaf(mp);

    // Everything below is scratch; the arena hands it out with a pointer bump
    // and the scope rewinds it on the way out. Splits recurse, so the arena's
    // chunks must never move -- see internal/Arena.hpp.
    Arena::Scope rewind(t->scratch());

    // Snapshot the page: the entry list points into `copy`, which outlives
    // every step below, so the originals can be overwritten freely.
    std::byte* copy = t->scratch().alloc(ps, 64);
    std::memcpy(copy, mp, ps);
    const Page* src = reinterpret_cast<const Page*>(copy);

    const unsigned count = unsigned(src->nkeys) + 1;
    Entry* ents = reinterpret_cast<Entry*>(
        t->scratch().alloc(std::size_t(count) * sizeof(Entry), alignof(Entry)));
    for (unsigned i = 0; i < newindx; ++i)
        new (&ents[i]) Entry(readEntry(src, i, t));
    new (&ents[newindx]) Entry(ne);
    detach(ents[newindx], t->scratch());
    for (unsigned i = newindx; i < src->nkeys; ++i)
        new (&ents[i + 1]) Entry(readEntry(src, i, t));

    // Growing a new root pushes the whole stack one level down.
    if (level == 0) {
        if (c.top + 1 >= kMaxDepth)
            throw Error(ErrorCode::Corrupted, "b-tree exceeded maximum depth");
        const PageNo rn = t->allocPages(1);
        Page* rp = t->allocBuffer(rn, P_BRANCH, 1);
        pageInit(rp, rn, P_BRANCH, ps);
        nodeAddBranch(rp, 0, Slice(), mp->pgno, 0);  // -infinity child; counted below
        for (int i = c.top; i >= 0; --i) {
            c.pg[i + 1] = c.pg[i];
            c.idx[i + 1] = c.idx[i];
        }
        c.pg[0] = rp;
        c.idx[0] = 0;
        ++c.top;
        tr.root = rn;
        ++tr.depth;
        ++tr.branchPages;
        t->markDirty(c.dbi);
        level = 1;
    }

    const bool prefixed = leaf && compresses(tr);
    const unsigned split = prefixed
                               ? chooseSplitPrefixed(ents, count, newindx, usable, t->scratch())
                               : chooseSplit(ents, count, newindx, leaf, usable, t->scratch());

    const PageNo sn = t->allocPages(1);
    const std::uint16_t kind = std::uint16_t(mp->flags & ~P_PREFIX);
    Page* np = t->allocBuffer(sn, kind, 1);
    pageInit(np, sn, kind, ps);
    if (leaf)
        ++tr.leafPages;
    else
        ++tr.branchPages;

    // Entry counts of the two halves. The parent's slot for `mp` gets the left
    // one now, while the cursor still points at it; the right one travels
    // with the new slot, through any cascade of splits above.
    std::uint64_t leftCount = 0, rightCount = 0;
    for (unsigned i = 0; i < count; ++i)
        (i < split ? leftCount : rightCount) += leaf ? 1 : ents[i].count;
    setBranchCount(c.pg[level - 1], c.idx[level - 1], leftCount);

    // Promote the separator first. This may cascade all the way to a new root,
    // but it never touches `mp` or `np`, so the pointers below stay good.
    // A leaf's first key is more than a separator needs: byte and reverse
    // order need only enough of it to sort after the left leaf's last key.
    const Slice sep =
        leaf ? separatorFor(comparatorFor(tr.flags), ents[split - 1].key, ents[split].key)
             : ents[split].key;
    branchInsert(c, level - 1, sep, sn, rightCount);

    if (prefixed) {
        // Each half shares what its own first and last key share.
        layoutLeaf(mp, mp->pgno, ents, split, leafPrefixFor(ents, split), ps);
        layoutLeaf(np, sn, ents + split, count - split, leafPrefixFor(ents + split, count - split),
                   ps);
        return;
    }
    pageInit(mp, mp->pgno, mp->flags, ps);
    for (unsigned i = 0; i < count; ++i) {
        Entry e = ents[i];
        // Whichever entry lands in slot 0 of a branch page becomes its
        // -infinity child; the key it carried is what we promoted upwards.
        if (!leaf && (i == 0 || i == split))
            e.key = Slice();
        if (i < split)
            emit(mp, i, e, leaf);
        else
            emit(np, i - split, e, leaf);
    }
}

/// Lays the cursor's leaf out afresh with `ne` inserted at `at` and the prefix
/// that suits the result, when that fits on one page: for a key without the
/// page's prefix, or a page that only holds one more entry compressed.
/// False when it needs a split instead.
bool relayoutLeaf(CursorImpl& c, unsigned at, const Entry& ne)
{
    TxnImpl* t = c.txn;
    const std::size_t ps = t->pageSize();
    Arena::Scope rewind(t->scratch());
    Page* p = c.pg[c.top];
    std::byte* copy = t->scratch().alloc(ps, 64);
    std::memcpy(copy, p, ps);
    const Page* src = reinterpret_cast<const Page*>(copy);
    const unsigned count = unsigned(src->nkeys) + 1;
    Entry* ents = reinterpret_cast<Entry*>(
        t->scratch().alloc(std::size_t(count) * sizeof(Entry), alignof(Entry)));
    for (unsigned i = 0; i < at; ++i)
        new (&ents[i]) Entry(readEntry(src, i, t));
    new (&ents[at]) Entry(ne);
    detach(ents[at], t->scratch());
    for (unsigned i = at; i < src->nkeys; ++i)
        new (&ents[i + 1]) Entry(readEntry(src, i, t));
    const std::size_t plen = leafPrefixFor(ents, count);
    if (leafLayoutBytes(ents, count, plen) > ps - kPageHdr - kPageChecksumBytes)
        return false;
    layoutLeaf(p, p->pgno, ents, count, plen, ps);
    return true;
}

// ------------------------------------------------------------- rebalance ---

void rebalance(CursorImpl& c);

/// rebalance() for two sibling leaves of a prefix-compressing tree, where an
/// entry that moves can change what every key on the page it joins costs:
/// both are read whole and laid out afresh, merged when one page holds them
/// all, or else with one entry moved towards the underfull page. Bailing out
/// is always safe, as there.
void rebalancePrefixed(CursorImpl& c, Page* left, Page* right, unsigned ridx, bool pIsLeft)
{
    TxnImpl* t = c.txn;
    Tree& tr = c.tree();
    const std::size_t ps = t->pageSize();
    const std::size_t usable = ps - kPageHdr - kPageChecksumBytes;
    Page* parent = c.pg[c.top - 1];

    // The entries point into copies: both pages are rewritten below.
    std::byte* copies = t->scratch().alloc(2 * ps, 64);
    std::memcpy(copies, left, ps);
    std::memcpy(copies + ps, right, ps);
    const Page* l = reinterpret_cast<const Page*>(copies);
    const Page* r = reinterpret_cast<const Page*>(copies + ps);
    const unsigned nl = l->nkeys, nr = r->nkeys, n = nl + nr;
    Entry* ents = reinterpret_cast<Entry*>(
        t->scratch().alloc(std::size_t(n ? n : 1) * sizeof(Entry), alignof(Entry)));
    for (unsigned i = 0; i < nl; ++i)
        new (&ents[i]) Entry(readEntry(l, i, t));
    for (unsigned i = 0; i < nr; ++i)
        new (&ents[nl + i]) Entry(readEntry(r, i, t));

    if (fitsPrefixed(ents, n, usable)) {
        setBranchCount(parent, ridx - 1, branchCount(parent, ridx - 1) + branchCount(parent, ridx));
        layoutLeaf(left, left->pgno, ents, n, leafPrefixFor(ents, n), ps);
        t->freePage(right->pgno);
        --tr.leafPages;
        nodeDel(parent, ridx);
        c.pg[c.top] = left;
        c.idx[c.top - 1] = std::uint16_t(ridx - 1);
        --c.top;
        t->markDirty(c.dbi);
        rebalance(c);
        return;
    }

    // Right's first entry moves left, or left's last moves right.
    if (pIsLeft ? nr <= 1 : nl <= 1)
        return;
    const unsigned split = pIsLeft ? nl + 1 : nl - 1;
    if (!fitsPrefixed(ents, split, usable) || !fitsPrefixed(ents + split, n - split, usable))
        return;
    const Slice sep = separatorFor(comparatorFor(tr.flags), ents[split - 1].key, ents[split].key);
    if (pageRoom(parent) + nodeSize(parent, ridx) < branchNodeSize(sep.size()))
        return;
    layoutLeaf(left, left->pgno, ents, split, leafPrefixFor(ents, split), ps);
    layoutLeaf(right, right->pgno, ents + split, n - split, leafPrefixFor(ents + split, n - split),
               ps);
    setBranchCount(parent, ridx - 1, split);
    setBranchCount(parent, ridx, n - split);
    branchSetKey(parent, ridx, sep);
    t->markDirty(c.dbi);
}

void rebalance(CursorImpl& c)
{
    TxnImpl* t = c.txn;
    Arena::Scope rewind(t->scratch());
    const std::size_t ps = t->pageSize();
    const std::size_t usable = ps - kPageHdr - kPageChecksumBytes;
    const std::size_t minFill = usable / 4;
    Tree& tr = c.tree();

    Page* p = c.pg[c.top];
    const bool leaf = isLeaf(p);
    // Leaves take the shortest separator their order allows when a shift moves one.
    const CompareFn cmp = comparatorFor(tr.flags);
    const unsigned minkeys = leaf ? 1u : 2u;
    if (p->nkeys >= minkeys && pageUsed(p, ps) >= minFill)
        return;

    if (c.top == 0) {
        if (leaf) {
            if (p->nkeys == 0) {
                t->freePage(p->pgno);
                --tr.leafPages;
                tr.root = kInvalidPage;
                tr.depth = 0;
                c.top = -1;
                c.positioned = false;
                t->markDirty(c.dbi);
            }
            return;
        }
        // A single-child root is pure overhead: pull the child up.
        while (isBranch(p) && p->nkeys == 1) {
            const PageNo ch = branchChild(p, 0);
            t->freePage(p->pgno);
            --tr.branchPages;
            tr.root = ch;
            --tr.depth;
            p = t->getPage(ch);
        }
        c.top = -1;
        c.positioned = false;
        t->markDirty(c.dbi);
        return;
    }

    Page* parent = c.pg[c.top - 1];
    const unsigned pidx = c.idx[c.top - 1];
    const unsigned sidx = (pidx == 0) ? pidx + 1 : pidx - 1;
    if (sidx >= parent->nkeys)
        return;  // nothing to balance against

    // The sibling is about to be modified, so it must become ours first.
    PageNo spgno = branchChild(parent, sidx);
    Page* sib = t->getPage(spgno);
    if (Page* back = t->isOwnDirty(spgno) ? nullptr : t->unspill(spgno)) {
        sib = back;
    } else if (!t->isOwnDirty(spgno)) {
        validateNodes(sib, ps);
        const PageNo np = t->allocPages(1);
        Page* d = t->allocBuffer(np, sib->flags, 1);
        std::memcpy(d, sib, ps);
        d->pgno = np;
        t->freePage(spgno);
        setBranchChild(parent, sidx, np);
        sib = d;
    }

    const bool pIsLeft = (pidx < sidx);
    Page* left = pIsLeft ? p : sib;
    Page* right = pIsLeft ? sib : p;
    const unsigned ridx = pIsLeft ? sidx : pidx;  // parent slot of `right`
    if (leaf && compresses(tr)) {
        rebalancePrefixed(c, left, right, ridx, pIsLeft);
        return;
    }

    // `right`'s slot 0 key is a -infinity placeholder on branch pages; whenever
    // it stops being slot 0 it must become the parent's separator, which is a
    // genuine lower bound for that subtree. Copy it: editing the parent below
    // would invalidate a slice pointing into it.
    Slice rsep;
    if (!leaf) {
        const Slice s = nodeKey(parent, ridx);
        std::byte* buf = t->scratch().alloc(s.size() ? s.size() : 1);
        std::memcpy(buf, s.data(), s.size());
        rsep = Slice(buf, s.size());
    }

    const std::ptrdiff_t sepDelta =
        leaf ? 0
             : std::ptrdiff_t(branchNodeSize(rsep.size())) -
                   std::ptrdiff_t(branchNodeSize(nodeKey(right, 0).size()));

    const std::ptrdiff_t merged =
        std::ptrdiff_t(pageUsed(left, ps)) + std::ptrdiff_t(pageUsed(right, ps)) + sepDelta;

    if (merged <= std::ptrdiff_t(usable)) {
        setBranchCount(parent, ridx - 1, branchCount(parent, ridx - 1) + branchCount(parent, ridx));
        for (unsigned i = 0; i < right->nkeys; ++i) {
            Entry e = readEntry(right, i, t);
            if (!leaf && i == 0)
                e.key = rsep;
            emit(left, left->nkeys, e, leaf);
        }
        t->freePage(right->pgno);
        if (leaf)
            --tr.leafPages;
        else
            --tr.branchPages;
        nodeDel(parent, ridx);
        c.pg[c.top] = left;
        c.idx[c.top - 1] = std::uint16_t(ridx - 1);
        --c.top;
        t->markDirty(c.dbi);
        rebalance(c);
        return;
    }

    // Merging would overflow, so shift a single node across instead. Bailing
    // out here is always safe -- an underfull page costs space, never meaning.
    const unsigned donorMin = leaf ? 1u : 2u;
    if (pIsLeft) {
        if (right->nkeys <= donorMin)
            return;
        Entry e = readEntry(right, 0, t);
        if (!leaf)
            e.key = rsep;
        if (pageRoom(left) < entryCost(e, leaf))
            return;
        // The new separator is right's slot 1 key, already a valid lower bound;
        // for leaves, only enough of it to sort after the entry moving left.
        Slice nk = nodeKey(right, 1);
        if (leaf)
            nk = separatorFor(cmp, e.key, nk);
        std::byte* nkbuf = t->scratch().alloc(nk.size() ? nk.size() : 1);
        std::memcpy(nkbuf, nk.data(), nk.size());
        const Slice newsep(nkbuf, nk.size());
        if (pageRoom(parent) + nodeSize(parent, ridx) < branchNodeSize(newsep.size()))
            return;
        emit(left, left->nkeys, e, leaf);
        nodeDel(right, 0);
        if (!leaf)
            branchSetKey(right, 0, Slice());  // new -infinity child
        const std::uint64_t moved = leaf ? 1 : e.count;
        setBranchCount(parent, ridx - 1, branchCount(parent, ridx - 1) + moved);
        setBranchCount(parent, ridx, branchCount(parent, ridx) - moved);
        branchSetKey(parent, ridx, newsep);
    } else {
        if (left->nkeys <= donorMin)
            return;
        Entry e = readEntry(left, left->nkeys - 1, t);
        std::byte* kbuf = t->scratch().alloc(e.key.size() ? e.key.size() : 1);
        std::memcpy(kbuf, e.key.data(), e.key.size());
        const Slice movedKey(kbuf, e.key.size());
        if (e.pay && e.paylen) {
            std::byte* pbuf = t->scratch().alloc(e.paylen);
            std::memcpy(pbuf, e.pay, e.paylen);
            e.pay = pbuf;
        } else {
            e.pay = nullptr;
        }
        e.key = movedKey;
        const std::ptrdiff_t need = std::ptrdiff_t(entryCost(e, leaf)) + sepDelta;
        if (std::ptrdiff_t(pageRoom(right)) < need)
            return;
        // The moved key becomes right's lower bound; for leaves, only enough of
        // it to sort after what is left behind as left's last key.
        const Slice sep =
            leaf ? separatorFor(cmp, nodeKey(left, left->nkeys - 2), movedKey) : movedKey;
        if (pageRoom(parent) + nodeSize(parent, ridx) < branchNodeSize(sep.size()))
            return;
        if (!leaf && !branchSetKey(right, 0, rsep))
            return;
        if (!leaf)
            e.key = Slice();  // it becomes the new -infinity child
        emit(right, 0, e, leaf);
        nodeDel(left, left->nkeys - 1);
        const std::uint64_t moved = leaf ? 1 : e.count;
        setBranchCount(parent, ridx - 1, branchCount(parent, ridx - 1) - moved);
        setBranchCount(parent, ridx, branchCount(parent, ridx) + moved);
        branchSetKey(parent, ridx, sep);
    }
    t->markDirty(c.dbi);
}

// --------------------------------------------------------------- descent ---

// Both helpers take (pg[top], idx[top]) as the subtree entry point and walk
// down to its extreme leaf entry, leaving every level below fully positioned.
// They return false when the walk ends on an empty page.

bool descendFirst(CursorImpl& c)
{
    while (isBranch(c.pg[c.top])) {
        if (c.idx[c.top] >= c.pg[c.top]->nkeys)
            return false;
        c.pg[c.top + 1] =
            c.txn->getPage(checkedChild(c.pg[c.top], c.idx[c.top], c.txn->pageSize()));
        ++c.top;
        c.idx[c.top] = 0;
    }
    return c.pg[c.top]->nkeys > 0;
}

bool descendLast(CursorImpl& c)
{
    while (isBranch(c.pg[c.top])) {
        if (c.idx[c.top] >= c.pg[c.top]->nkeys)
            return false;
        c.pg[c.top + 1] =
            c.txn->getPage(checkedChild(c.pg[c.top], c.idx[c.top], c.txn->pageSize()));
        ++c.top;
        const unsigned n = c.pg[c.top]->nkeys;
        c.idx[c.top] = std::uint16_t(n ? n - 1 : 0);
    }
    return c.pg[c.top]->nkeys > 0;
}

void setAnchor(CursorImpl& c)
{
    c.seqSeen = c.txn->root()->seq;
    // A read snapshot never moves, and a throwaway cursor never outlives the
    // call that made it -- neither needs an anchor.
    if (c.txn->readOnly || !c.track)
        return;
    const Page* p = c.pg[c.top];
    checkNode(p, c.idx[c.top], c.txn->pageSize());
    if (p->flags & P_PREFIX) {
        c.anchor.clear();
        appendLeafKey(p, c.idx[c.top], c.txn->pageSize(), c.anchor);
        return;
    }
    const Slice k = nodeKey(p, c.idx[c.top]);
    c.anchor.assign(k.data(), k.data() + k.size());
}

/// Step back to the last real entry at or before the current position.
bool normalizeBackward(CursorImpl& c)
{
    for (;;) {
        if (c.pg[c.top]->nkeys > 0) {
            if (c.idx[c.top] >= c.pg[c.top]->nkeys)
                c.idx[c.top] = std::uint16_t(c.pg[c.top]->nkeys - 1);
            return true;
        }
        int lvl = c.top;
        while (lvl > 0 && c.idx[lvl - 1] == 0)
            --lvl;
        if (lvl == 0)
            return false;
        c.top = lvl - 1;
        --c.idx[c.top];
        descendLast(c);  // may land on another empty leaf; the loop retries
    }
}

/// Move forward to a real entry when the search landed past the end of a leaf
/// (or on an empty one).
bool normalizeForward(CursorImpl& c)
{
    for (;;) {
        if (c.idx[c.top] < c.pg[c.top]->nkeys)
            return true;
        int lvl = c.top;
        while (lvl > 0 && c.idx[lvl - 1] + 1u >= c.pg[lvl - 1]->nkeys)
            --lvl;
        if (lvl == 0)
            return false;
        c.top = lvl - 1;
        ++c.idx[c.top];
        descendFirst(c);  // may land on an empty leaf; the loop retries
    }
}

}  // namespace

// -------------------------------------------------------------- exported ---

int cmpLexicographic(Slice a, Slice b) noexcept
{
    return a.compare(b);
}

int cmpReverse(Slice a, Slice b) noexcept
{
    const std::size_t n = std::min(a.size(), b.size());
    const auto* pa = a.data() + a.size();
    const auto* pb = b.data() + b.size();
    for (std::size_t i = 1; i <= n; ++i) {
        const int d = int(std::to_integer<unsigned char>(pa[-std::ptrdiff_t(i)])) -
                      int(std::to_integer<unsigned char>(pb[-std::ptrdiff_t(i)]));
        if (d)
            return d < 0 ? -1 : 1;
    }
    return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
}

int cmpInteger(Slice a, Slice b) noexcept
{
    if (a.size() == b.size()) {
        if (a.size() == sizeof(std::uint64_t)) {
            const auto x = readLittle<std::uint64_t>(a.data());
            const auto y = readLittle<std::uint64_t>(b.data());
            return x < y ? -1 : (x > y ? 1 : 0);
        }
        if (a.size() == sizeof(std::uint32_t)) {
            const auto x = readLittle<std::uint32_t>(a.data());
            const auto y = readLittle<std::uint32_t>(b.data());
            return x < y ? -1 : (x > y ? 1 : 0);
        }
    }
    return a.compare(b);
}

CompareFn comparatorFor(std::uint32_t flags) noexcept
{
    if (flags & std::uint32_t(DbFlags::IntegerKey))
        return &cmpInteger;
    if (flags & std::uint32_t(DbFlags::ReverseKey))
        return &cmpReverse;
    return &cmpLexicographic;
}

Slice leafValueOverflow(TxnImpl* t, const Page* p, unsigned i)
{
    const LeafNode* n = lnode(p, i);
    const auto* body = reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode) + n->ksize;
    if (!(n->flags & N_BIGDATA))
        return Slice(body, n->vsize);
    Page* ov = t->getOverflow(bigPgno(n), n->vsize);
    return Slice(reinterpret_cast<const std::byte*>(ov) + kPageHdr, n->vsize);
}

bool treeSearch(CursorImpl& c, Slice key, bool* exact)
{
    *exact = false;
    Tree& tr = c.tree();
    if (tr.root == kInvalidPage) {
        c.top = -1;
        return false;
    }
    const CompareFn cmp = c.cmp();
    if ((tr.flags & std::uint32_t(DbFlags::IntegerKey)) != 0) {
        const unsigned width = tr.flags >> 16;
        if ((key.size() != 4 && key.size() != 8) || (width && key.size() != width))
            throw Error(ErrorCode::InvalidArgument, "integer key width differs from the tree");
    }
    c.top = 0;
    c.pg[0] = c.txn->getPage(tr.root);
    Bounds b;
    while (isBranch(c.pg[c.top])) {
        const Page* p = c.pg[c.top];
        if (p->nkeys == 0)
            throw Error(ErrorCode::Corrupted, "empty branch page");
        const std::size_t ps = c.txn->pageSize();
        const unsigned i = branchSearchIn(p, key, cmp, b, ps);
        c.idx[c.top] = std::uint16_t(i);
        if (c.top + 1 >= kMaxDepth)
            throw Error(ErrorCode::Corrupted, "b-tree exceeded maximum depth");
        // The child's keys lie between the separators either side of it.
        const std::size_t top = nodeTop(p, ps);
        if (i > 0)
            b.low = checkedKey(p, i, top);
        if (i + 1u < p->nkeys)
            b.high = checkedKey(p, i + 1, top);
        c.pg[c.top + 1] = c.txn->getPage(checkedChild(p, i, ps));
        ++c.top;
    }
    c.idx[c.top] =
        std::uint16_t(leafLowerBoundIn(c.pg[c.top], key, cmp, b, exact, c.txn->pageSize()));
    return true;
}

namespace {

void checkIntegerKey(const Tree& tr, Slice key)
{
    if ((tr.flags & std::uint32_t(DbFlags::IntegerKey)) != 0) {
        const unsigned width = tr.flags >> 16;
        if ((key.size() != 4 && key.size() != 8) || (width && key.size() != width))
            throw Error(ErrorCode::InvalidArgument, "integer key width differs from the tree");
    }
}

}  // namespace

std::uint64_t treeRank(CursorImpl& c, Slice key)
{
    Tree& tr = c.tree();
    if (tr.root == kInvalidPage)
        return 0;
    checkIntegerKey(tr, key);
    const CompareFn cmp = c.cmp();
    std::uint64_t rank = 0;
    const Page* p = c.txn->getPage(tr.root);
    for (int depth = 0; isBranch(p); ++depth) {
        if (p->nkeys == 0)
            throw Error(ErrorCode::Corrupted, "empty branch page");
        if (depth + 1 >= kMaxDepth)
            throw Error(ErrorCode::Corrupted, "b-tree exceeded maximum depth");
        // Every subtree left of the one `key` belongs in sorts before it.
        const std::size_t ps = c.txn->pageSize();
        const unsigned i = branchSearch(p, key, cmp, ps);
        for (unsigned j = 0; j < i; ++j)
            rank += checkedCount(p, j, ps);
        p = c.txn->getPage(checkedChild(p, i, ps));
    }
    bool exact = false;
    return rank + leafLowerBound(p, key, cmp, &exact, c.txn->pageSize());
}

bool treeSelect(CursorImpl& c, std::uint64_t rank)
{
    c.seqSeen = c.txn->root()->seq;
    Tree& tr = c.tree();
    if (tr.root == kInvalidPage || rank >= tr.entries) {
        c.unposition();
        return false;
    }
    c.top = 0;
    c.pg[0] = c.txn->getPage(tr.root);
    while (isBranch(c.pg[c.top])) {
        const Page* p = c.pg[c.top];
        unsigned j = 0;
        for (; j < p->nkeys; ++j) {
            const std::uint64_t n = checkedCount(p, j, c.txn->pageSize());
            if (rank < n)
                break;
            rank -= n;
        }
        if (j == p->nkeys)
            throw Error(ErrorCode::Corrupted, "branch entry counts fall short of the tree's");
        if (c.top + 1 >= kMaxDepth)
            throw Error(ErrorCode::Corrupted, "b-tree exceeded maximum depth");
        c.idx[c.top] = std::uint16_t(j);
        c.pg[c.top + 1] = c.txn->getPage(checkedChild(p, j, c.txn->pageSize()));
        ++c.top;
    }
    if (rank >= c.pg[c.top]->nkeys)
        throw Error(ErrorCode::Corrupted, "branch entry count exceeds its leaf");
    c.idx[c.top] = std::uint16_t(rank);
    c.positioned = true;
    setAnchor(c);
    return true;
}

std::uint64_t cursorRank(CursorImpl& c)
{
    cursorRevalidate(c);
    if (!c.positioned)
        throw Error(ErrorCode::NotFound, "cursor is not positioned");
    std::uint64_t rank = c.idx[c.top];
    for (int lvl = 0; lvl < c.top; ++lvl) {
        for (unsigned j = 0; j < c.idx[lvl]; ++j)
            rank += checkedCount(c.pg[lvl], j, c.txn->pageSize());
    }
    return rank;
}

void cursorRevalidate(CursorImpl& c)
{
    TxnImpl* r = c.txn->root();
    if (c.seqSeen == r->seq)
        return;
    if (!c.track) {
        c.seqSeen = r->seq;
        return;
    }
    c.seqSeen = r->seq;
    if (!c.positioned) {
        c.top = -1;
        return;
    }
    // Swap rather than move: both buffers stay alive, so a cursor that
    // re-anchors on every mutation never touches the allocator again.
    c.anchorSpare.swap(c.anchor);
    c.anchor.clear();
    const std::vector<std::byte>& key = c.anchorSpare;
    bool exact = false;
    if (!treeSearch(c, Slice(key.data(), key.size()), &exact) || !normalizeForward(c)) {
        c.positioned = false;
        c.top = -1;
        return;
    }
    setAnchor(c);
}

bool cursorFirst(CursorImpl& c)
{
    c.seqSeen = c.txn->root()->seq;
    Tree& tr = c.tree();
    if (tr.root == kInvalidPage) {
        c.unposition();
        return false;
    }
    c.top = 0;
    c.pg[0] = c.txn->getPage(tr.root);
    c.idx[0] = 0;
    if (!descendFirst(c) && !normalizeForward(c)) {
        c.unposition();
        return false;
    }
    c.positioned = true;
    setAnchor(c);
    return true;
}

bool cursorLast(CursorImpl& c)
{
    c.seqSeen = c.txn->root()->seq;
    Tree& tr = c.tree();
    if (tr.root == kInvalidPage) {
        c.unposition();
        return false;
    }
    c.top = 0;
    c.pg[0] = c.txn->getPage(tr.root);
    c.idx[0] = std::uint16_t(c.pg[0]->nkeys ? c.pg[0]->nkeys - 1 : 0);
    if (!descendLast(c) && !normalizeBackward(c)) {
        // Only reachable with empty pages left behind by a bailed-out rebalance.
        c.unposition();
        return false;
    }
    c.positioned = true;
    setAnchor(c);
    return true;
}

bool cursorNext(CursorImpl& c)
{
    cursorRevalidate(c);
    if (!c.positioned)
        return false;
    for (;;) {
        if (c.idx[c.top] + 1u < c.pg[c.top]->nkeys) {
            ++c.idx[c.top];
            break;
        }
        int lvl = c.top;
        while (lvl > 0 && c.idx[lvl - 1] + 1u >= c.pg[lvl - 1]->nkeys)
            --lvl;
        if (lvl == 0) {
            c.unposition();
            return false;
        }
        c.top = lvl - 1;
        ++c.idx[c.top];
        if (descendFirst(c))
            break;
    }
    setAnchor(c);
    return true;
}

bool cursorPrev(CursorImpl& c)
{
    cursorRevalidate(c);
    if (!c.positioned)
        return false;
    for (;;) {
        if (c.idx[c.top] > 0) {
            --c.idx[c.top];
            break;
        }
        int lvl = c.top;
        while (lvl > 0 && c.idx[lvl - 1] == 0)
            --lvl;
        if (lvl == 0) {
            c.unposition();
            return false;
        }
        c.top = lvl - 1;
        --c.idx[c.top];
        if (descendLast(c))
            break;
    }
    setAnchor(c);
    return true;
}

Slice cursorKey(CursorImpl& c)
{
    cursorRevalidate(c);
    if (!c.positioned)
        throw Error(ErrorCode::NotFound, "cursor is not positioned");
    return leafKeyAt(c, c.pg[c.top], c.idx[c.top]);
}

Slice leafKeyAt(CursorImpl& c, const Page* p, unsigned i)
{
    checkNode(p, i, c.txn->pageSize());
    if (!(p->flags & P_PREFIX))
        return nodeKey(p, i);
    c.keyBuf.clear();
    appendLeafKey(p, i, c.txn->pageSize(), c.keyBuf);
    return Slice(c.keyBuf.data(), c.keyBuf.size());
}

Slice cursorValue(CursorImpl& c)
{
    cursorRevalidate(c);
    if (!c.positioned)
        throw Error(ErrorCode::NotFound, "cursor is not positioned");
    return leafValue(c.txn, c.pg[c.top], c.idx[c.top]);
}

bool treePut(CursorImpl& c, Slice key, Slice value, PutMode mode, WritableSlice* out)
{
    TxnImpl* t = c.txn;
    const std::size_t ps = t->pageSize();
    Arena::Scope rewind(t->scratch());  // copies of arguments that alias the leaf
    if (key.size() > maxKeySize(ps) || key.empty())
        throw Error(ErrorCode::KeyTooLarge,
                    key.empty() ? "empty keys are not allowed" : "key exceeds the page-size limit");
    if (value.size() > 0xffffffffull)
        throw Error(ErrorCode::ValueTooLarge, "value exceeds 4 GiB");

    Tree& tr = c.tree();
    bool exact = false;

    if ((tr.flags & std::uint32_t(DbFlags::IntegerKey)) != 0) {
        if (key.size() != 4 && key.size() != 8)
            throw Error(ErrorCode::InvalidArgument, "integer keys must be four or eight bytes");
        const unsigned width = tr.flags >> 16;
        if (width && width != key.size())
            throw Error(ErrorCode::InvalidArgument, "mixed integer key widths are not supported");
        if (!width) {
            tr.flags |= std::uint32_t(key.size()) << 16;
            t->markDirty(c.dbi);
        }
    }

    if (tr.root == kInvalidPage) {
        if (mode == PutMode::UpdateOnly)
            return false;
        const PageNo np = t->allocPages(1);
        Page* p = t->allocBuffer(np, P_LEAF, 1);
        pageInit(p, np, P_LEAF, ps);
        tr.root = np;
        tr.depth = 1;
        tr.leafPages = 1;
        t->markDirty(c.dbi);
        c.top = 0;
        c.pg[0] = p;
        c.idx[0] = 0;
    } else {
        treeSearch(c, key, &exact);
    }

    if (exact && !c.allowSubdb && (lnode(c.pg[c.top], c.idx[c.top])->flags & N_SUBDB))
        throw Error(ErrorCode::Incompatible,
                    "that key names a sub-database; use Txn::db() / Txn::dropDb()");
    if (mode == PutMode::Append && exact)
        throw Error(ErrorCode::InvalidArgument, "append mode saw a duplicate key");
    if (exact && mode == PutMode::InsertUnique)
        return false;
    if (!exact && mode == PutMode::UpdateOnly)
        return false;
    if (mode == PutMode::Append && !exact) {
        // The key has to belong past the end of the right-most leaf, which means
        // the descent followed the right edge of every branch page and then ran
        // off the end of the leaf.
        bool rightmost = c.idx[c.top] == c.pg[c.top]->nkeys;
        for (int i = 0; rightmost && i < c.top; ++i)
            rightmost = c.idx[i] + 1u == c.pg[i]->nkeys;
        if (!rightmost)
            throw Error(ErrorCode::InvalidArgument, "append mode requires strictly ascending keys");
    }
    touchPath(c);

    const bool big = leafNodeSize(key.size(), value.size(), false) > maxNodeSize(ps);
    Page* p = c.pg[c.top];
    unsigned at = c.idx[c.top];

    if (exact) {
        LeafNode* n = lnode(p, at);
        const bool oldbig = (n->flags & N_BIGDATA) != 0;
        auto* body = reinterpret_cast<std::byte*>(n) + sizeof(LeafNode) + n->ksize;

        if (!oldbig && !big && n->vsize == value.size()) {
            if (value.data())
                std::memcpy(body, value.data(), value.size());
            else
                std::memset(body, 0, value.size());
            if (out)
                *out = WritableSlice(body, value.size());
            ++t->root()->seq;
            c.positioned = true;
            setAnchor(c);
            return true;
        }
        if (oldbig && big) {
            const PageNo oldp = bigPgno(n);
            Page* ovp = t->getOverflow(oldp, n->vsize);
            const bool fits = ovPages(ovp) == overflowRun(ps, value.size());
            if (fits && !t->isOwnDirty(oldp))
                if (Page* back = t->unspill(oldp))
                    ovp = back;
            if (fits && t->isOwnDirty(oldp)) {
                const std::size_t span = std::size_t(ovPages(ovp)) * ps - kPageHdr;
                n->vsize = std::uint32_t(value.size());
                auto* dst = reinterpret_cast<std::byte*>(ovp) + kPageHdr;
                if (value.data())
                    std::memcpy(dst, value.data(), value.size());
                else
                    std::memset(dst, 0, value.size());
                std::memset(dst + value.size(), 0, span - value.size());
                if (out)
                    *out = WritableSlice(dst, value.size());
                ++t->root()->seq;
                c.positioned = true;
                setAnchor(c);
                return true;
            }
        }
        // The caller's key or value may sit in this leaf (a cursor's key)
        // or in the run about to be freed; both move or go away below.
        const auto inside = [](Slice s, const void* base, std::size_t bytes) {
            const auto* b = static_cast<const std::byte*>(base);
            return s.data() && s.data() < b + bytes && s.data() + s.size() > b;
        };
        const auto keep = [&](Slice& s) {
            std::byte* copy = t->scratch().alloc(s.size() ? s.size() : 1);
            std::memcpy(copy, s.data(), s.size());
            s = Slice(copy, s.size());
        };
        const Page* oldRun =
            oldbig ? t->getPage(bigPgno(n), false, TxnImpl::PageKind::Overflow) : nullptr;
        const std::size_t runBytes = oldRun ? std::size_t(ovPages(oldRun)) * ps : 0;
        if (inside(key, p, ps) || (oldRun && inside(key, oldRun, runBytes)))
            keep(key);
        if (inside(value, p, ps) || (oldRun && inside(value, oldRun, runBytes)))
            keep(value);
        if (oldbig)
            overflowRelease(t, tr, bigPgno(n));
        nodeDel(p, at);
        --tr.entries;
    }

    // Build the node to insert.
    Entry ne;
    ne.key = key;
    ne.vsize = std::uint32_t(value.size());
    PageNo ovp = 0;
    Little<PageNo> encodedOverflow{};
    if (big) {
        std::byte* dst = overflowCreate(t, value.size(), &ovp);
        tr.overflowPages += overflowRun(ps, value.size());
        if (value.data())
            std::memcpy(dst, value.data(), value.size());
        else
            std::memset(dst, 0, value.size());
        ne.flags = N_BIGDATA;
        encodedOverflow = ovp;
        ne.pay = reinterpret_cast<const std::byte*>(&encodedOverflow);
        ne.paylen = sizeof(PageNo);
    } else {
        ne.pay = value.data();
        ne.paylen = std::uint32_t(value.size());
    }

    // Re-read: overflowCreate may have handed out pages, but never moved ours.
    p = c.pg[c.top];
    at = c.idx[c.top];
    // A new key adds one entry under every branch slot on the way down; a
    // split below fixes up the slots it divides.
    if (!exact)
        adjustPathCounts(c, 1);
    std::byte* payload;
    // A P_PREFIX leaf stores only what follows its prefix, so a key without
    // it cannot go in as it stands.
    Slice stored = key;
    bool fits;
    if (p->flags & P_PREFIX) {
        fits = againstPrefix(leafPrefix(p, ps), key, &stored) == 0 &&
               pageRoom(p) >= 2 + leafNodeSize(stored.size(), ne.vsize, big);
    } else {
        fits = pageRoom(p) >= entryCost(ne, true);
    }
    if (fits) {
        payload = nodeAddLeaf(p, at, stored, ne.vsize, ne.flags);
        if (ne.pay)
            std::memcpy(payload, ne.pay, ne.paylen);
        else
            std::memset(payload, 0, ne.paylen);
    } else {
        if (!compresses(tr) || !relayoutLeaf(c, at, ne))
            splitPage(c, c.top, at, ne);
        bool found = false;
        treeSearch(c, key, &found);
        if (!found)
            throw Error(ErrorCode::Corrupted, "inserted entry lost during split");
        LeafNode* n = lnode(c.pg[c.top], c.idx[c.top]);
        payload = reinterpret_cast<std::byte*>(n) + sizeof(LeafNode) + n->ksize;
    }

    ++tr.entries;
    t->markDirty(c.dbi);
    ++t->root()->seq;
    c.positioned = true;
    setAnchor(c);
    if (out) {
        if (big) {
            Page* ov = t->getPage(ovp, false, TxnImpl::PageKind::Overflow);
            payload = reinterpret_cast<std::byte*>(ov) + kPageHdr;
        }
        *out = WritableSlice(payload, value.size());
    }
    return true;
}

void treeDelCurrent(CursorImpl& c)
{
    TxnImpl* t = c.txn;
    Tree& tr = c.tree();
    touchPath(c);  // idempotent: already-owned pages are left alone
    Page* p = c.pg[c.top];
    const unsigned i = c.idx[c.top];
    LeafNode* n = lnode(p, i);
    if (n->flags & N_BIGDATA)
        overflowRelease(t, tr, bigPgno(n));
    nodeDel(p, i);
    adjustPathCounts(c, -1);
    --tr.entries;
    t->markDirty(c.dbi);
    rebalance(c);
    ++t->root()->seq;
    c.positioned = false;
    c.top = -1;
}

bool treeDel(CursorImpl& c, Slice key)
{
    Tree& tr = c.tree();
    if (tr.root == kInvalidPage)
        return false;
    bool exact = false;
    treeSearch(c, key, &exact);
    if (!exact)
        return false;
    if (!c.allowSubdb && (lnode(c.pg[c.top], c.idx[c.top])->flags & N_SUBDB))
        throw Error(ErrorCode::Incompatible, "that key names a sub-database; use Txn::dropDb()");
    touchPath(c);
    treeDelCurrent(c);
    return true;
}

namespace {

/// Repositions within the cursor's current leaf when `key` belongs there:
/// the cursor is positioned, nothing has changed the tree since, and `key`
/// lies between the leaf's first and last key. Saves the descent from the
/// root that a seek near the previous one would otherwise repeat.
bool seekInLeaf(CursorImpl& c, Slice key, bool* exact)
{
    // Probes that keep missing are not local: stop paying for the check for
    // a while, then look again.
    if (c.hintSkip) {
        --c.hintSkip;
        return false;
    }
    if (!c.positioned || c.top < 0 || c.seqSeen != c.txn->root()->seq)
        return false;
    const Page* p = c.pg[c.top];
    const unsigned n = p->nkeys;
    if (n == 0 || !isLeaf(p))
        return false;
    const Tree& tr = c.tree();
    if ((tr.flags & std::uint32_t(DbFlags::IntegerKey)) != 0)
        return false;
    const CompareFn cmp = comparatorFor(tr.flags);
    const std::size_t ps = c.txn->pageSize();
    if (againstLeafKey(p, n - 1, key, cmp, ps) > 0 || againstLeafKey(p, 0, key, cmp, ps) < 0) {
        if (++c.hintMisses >= 2) {
            c.hintMisses = 0;
            c.hintSkip = 32;
        }
        return false;
    }
    c.hintMisses = 0;
    c.idx[c.top] = std::uint16_t(leafLowerBound(p, key, cmp, exact, ps));
    return true;
}

}  // namespace

bool cursorSeek(CursorImpl& c, Slice key)
{
    bool exact = false;
    if (seekInLeaf(c, key, &exact)) {
        setAnchor(c);
        return true;
    }
    c.seqSeen = c.txn->root()->seq;
    if (!treeSearch(c, key, &exact) || !normalizeForward(c)) {
        c.unposition();
        return false;
    }
    c.positioned = true;
    setAnchor(c);
    return true;
}

bool cursorSeekExact(CursorImpl& c, Slice key)
{
    bool exact = false;
    if (seekInLeaf(c, key, &exact)) {
        if (!exact) {
            c.unposition();
            return false;
        }
        setAnchor(c);
        return true;
    }
    c.seqSeen = c.txn->root()->seq;
    if (!treeSearch(c, key, &exact) || !exact) {
        c.unposition();
        return false;
    }
    c.positioned = true;
    setAnchor(c);
    return true;
}

// ------------------------------------------------------ sub-db directory ---

bool treeGetSubdb(TxnImpl* t, Slice name, Tree* out)
{
    CursorImpl c;
    c.txn = t;
    c.dbi = kCatalogDbi;
    c.allowSubdb = true;
    if (!cursorSeekExact(c, name))
        return false;
    const LeafNode* n = lnode(c.pg[c.top], c.idx[c.top]);
    if (!(n->flags & N_SUBDB))
        throw Error(ErrorCode::Corrupted, "catalog record is not a sub-database");
    if (n->vsize != sizeof(Tree))
        throw Error(ErrorCode::Corrupted, "malformed sub-database record");
    std::memcpy(out, leafValue(t, c.pg[c.top], c.idx[c.top]).data(), sizeof(Tree));
    if (const RootSlot* s = findRootSlot(t->root()->meta, name))
        *out = s->tree;
    return true;
}

RootSlot* findRootSlot(Meta& m, Slice name) noexcept
{
    if (name.empty() || name.size() > kRootNameMax)
        return nullptr;
    for (RootSlot& s : m.roots) {
        if (s.length == name.size() && std::memcmp(s.name, name.data(), name.size()) == 0)
            return &s;
    }
    return nullptr;
}

void treeFlushSubdb(TxnImpl* t, Slice name, const Tree& tr)
{
    TxnImpl* r = t->root();
    Meta& m = r->meta;
    RootSlot* slot = findRootSlot(m, name);
    if (!slot && !name.empty() && name.size() <= kRootNameMax) {
        // An empty slot, else the one written longest ago, whose record goes
        // back to the catalog.
        for (RootSlot& s : m.roots) {
            if (s.length == 0) {
                slot = &s;
                break;
            }
            if (!slot || s.written < slot->written)
                slot = &s;
        }
        if (slot->length)
            treePutSubdb(t, Slice(slot->name, slot->length), slot->tree);
        std::memset(slot->name, 0, sizeof slot->name);
        std::memcpy(slot->name, name.data(), name.size());
        slot->length = std::uint8_t(name.size());
    }
    if (!slot) {
        treePutSubdb(t, name, tr);
        return;
    }
    slot->tree = tr;
    slot->written = r->txnid;
}

void treePutSubdb(TxnImpl* t, Slice name, const Tree& tr)
{
    CursorImpl c;
    c.txn = t;
    c.dbi = kCatalogDbi;
    c.allowSubdb = true;
    WritableSlice w;
    treePut(c, name, Slice(nullptr, sizeof(Tree)), PutMode::Upsert, &w);
    std::memcpy(w.data(), &tr, sizeof(Tree));
    lnode(c.pg[c.top], c.idx[c.top])->flags |= N_SUBDB;
}

bool treeDelSubdb(TxnImpl* t, Slice name)
{
    if (RootSlot* s = findRootSlot(t->root()->meta, name))
        *s = RootSlot{};
    CursorImpl c;
    c.txn = t;
    c.dbi = kCatalogDbi;
    c.allowSubdb = true;
    return treeDel(c, name);
}

std::vector<std::string> treeListSubdbs(TxnImpl* t)
{
    std::vector<std::string> out;
    CursorImpl c;
    c.txn = t;
    c.dbi = kCatalogDbi;
    c.allowSubdb = true;
    for (bool ok = cursorFirst(c); ok; ok = cursorNext(c)) {
        checkNode(c.pg[c.top], c.idx[c.top], t->pageSize());
        if (lnode(c.pg[c.top], c.idx[c.top])->flags & N_SUBDB)
            out.push_back(cursorKey(c).string());
    }
    return out;
}

namespace {

void freeSubtree(TxnImpl* t, Tree& tr, PageNo pn, int depth = 0)
{
    // A damaged child pointer can make a cycle, which must not recurse forever.
    if (depth >= kMaxDepth)
        throw Error(ErrorCode::Corrupted, "b-tree exceeded maximum depth");
    Page* p = t->getPage(pn);
    validateNodes(p, t->pageSize());
    if (isBranch(p)) {
        for (unsigned i = 0; i < p->nkeys; ++i)
            freeSubtree(t, tr, branchChild(p, i), depth + 1);
        --tr.branchPages;
    } else {
        for (unsigned i = 0; i < p->nkeys; ++i) {
            const LeafNode* n = lnode(p, i);
            if (n->flags & N_BIGDATA) {
                const PageNo ov = bigPgno(n);
                const unsigned run = ovPages(t->getPage(ov, false, TxnImpl::PageKind::Overflow));
                t->freePage(ov, run);
                tr.overflowPages -= run;
            }
        }
        --tr.leafPages;
    }
    t->freePage(pn);
}

}  // namespace

void treeDrop(TxnImpl* txn, unsigned dbi)
{
    Tree& tr = txn->dbs[dbi].tree;
    if (tr.root != kInvalidPage)
        freeSubtree(txn, tr, tr.root);
    const std::uint32_t flags = tr.flags;
    tr = emptyTree(flags);
    txn->markDirty(dbi);
    ++txn->root()->seq;
}

}  // namespace nosql::internal
