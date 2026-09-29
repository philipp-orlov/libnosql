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
#include <cstring>
#include <vector>

#include <new>

#include "nosql/internal/core.hpp"

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

void nodeAddBranch(Page* p, unsigned i, Slice key, PageNo child) noexcept
{
    const std::size_t sz = branchNodeSize(key.size());
    p->upper = std::uint16_t(p->upper - sz);
    auto* n = reinterpret_cast<BranchNode*>(reinterpret_cast<std::byte*>(p) + p->upper);
    n->ksize = std::uint16_t(key.size());
    n->childLo = std::uint32_t(child);
    n->childHi = std::uint16_t(child >> 32);
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
    if (pageRoom(p) + nodeSize(p, i) < branchNodeSize(key.size()))
        return false;
    nodeDel(p, i);
    nodeAddBranch(p, i, key, child);
    return true;
}

// -------------------------------------------------------------- searches ---

// The binary searches below run several comparisons per page and three or
// four pages per lookup, so an indirect call per comparison is worth getting
// rid of. Both searches are templated on the comparison and dispatched once,
// which lets the overwhelmingly common byte-wise ordering inline.

/// Byte-wise ordering, as a functor the compiler can inline.
struct LexCmp
{
    int operator()(Slice a, Slice b) const noexcept { return a.compare(b); }
};
/// Fallback for the two non-default orderings.
struct FnCmp
{
    CompareFn f;
    int operator()(Slice a, Slice b) const noexcept { return f(a, b); }
};

/// Largest slot whose key is <= `key`; slot 0 is -infinity.
template <class Cmp>
unsigned branchSearchWith(const Page* p, Slice key, Cmp cmp) noexcept
{
    unsigned lo = 1, hi = p->nkeys, best = 0;
    while (lo < hi) {
        const unsigned mid = lo + (hi - lo) / 2;
        if (cmp(nodeKey(p, mid), key) <= 0) {
            best = mid;
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return best;
}

unsigned branchSearch(const Page* p, Slice key, CompareFn cmp) noexcept
{
    if (cmp == &cmpLexicographic)
        return branchSearchWith(p, key, LexCmp{});
    return branchSearchWith(p, key, FnCmp{cmp});
}

/// First slot whose key is >= `key` (may equal nkeys).
template <class Cmp>
unsigned leafLowerBoundWith(const Page* p, Slice key, Cmp cmp, bool* exact) noexcept
{
    unsigned lo = 0, hi = p->nkeys;
    *exact = false;
    while (lo < hi) {
        const unsigned mid = lo + (hi - lo) / 2;
        const int c = cmp(nodeKey(p, mid), key);
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

unsigned leafLowerBound(const Page* p, Slice key, CompareFn cmp, bool* exact) noexcept
{
    if (cmp == &cmpLexicographic)
        return leafLowerBoundWith(p, key, LexCmp{}, exact);
    return leafLowerBoundWith(p, key, FnCmp{cmp}, exact);
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
    const unsigned n = ovPages(t->getPage(p0));
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
};

Entry readEntry(const Page* p, unsigned i)
{
    Entry e;
    e.key = nodeKey(p, i);
    if (isLeaf(p)) {
        const LeafNode* n = lnode(p, i);
        e.flags = n->flags;
        e.vsize = n->vsize;
        e.paylen = (n->flags & N_BIGDATA) ? std::uint32_t(sizeof(PageNo)) : std::uint32_t(n->vsize);
        e.pay = reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode) + n->ksize;
    } else {
        e.child = branchChild(p, i);
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
        nodeAddBranch(p, at, e.key, e.child);
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

void splitPage(CursorImpl& c, int level, unsigned newindx, const Entry& ne);

/// Insert (key -> child) just after the child the cursor descended through
/// at `level`, splitting that branch page if it is full.
void branchInsert(CursorImpl& c, int level, Slice key, PageNo child)
{
    Page* p = c.pg[level];
    const unsigned at = c.idx[level] + 1;
    if (pageRoom(p) >= branchNodeSize(key.size()) + 2) {
        nodeAddBranch(p, at, key, child);
        return;
    }
    Entry ne;
    ne.key = key;
    ne.child = child;
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
        new (&ents[i]) Entry(readEntry(src, i));
    new (&ents[newindx]) Entry(ne);
    for (unsigned i = newindx; i < src->nkeys; ++i)
        new (&ents[i + 1]) Entry(readEntry(src, i));

    // Growing a new root pushes the whole stack one level down.
    if (level == 0) {
        if (c.top + 1 >= kMaxDepth)
            throw Error(ErrorCode::Corrupted, "b-tree exceeded maximum depth");
        const PageNo rn = t->allocPages(1);
        Page* rp = t->allocBuffer(rn, P_BRANCH, 1);
        pageInit(rp, rn, P_BRANCH, ps);
        nodeAddBranch(rp, 0, Slice(), mp->pgno);  // -infinity child
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

    const unsigned split = chooseSplit(ents, count, newindx, leaf, usable, t->scratch());

    const PageNo sn = t->allocPages(1);
    Page* np = t->allocBuffer(sn, mp->flags, 1);
    pageInit(np, sn, mp->flags, ps);
    if (leaf)
        ++tr.leafPages;
    else
        ++tr.branchPages;

    // Promote the separator first. This may cascade all the way to a new root,
    // but it never touches `mp` or `np`, so the pointers below stay good.
    branchInsert(c, level - 1, ents[split].key, sn);

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

// ------------------------------------------------------------- rebalance ---

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
    if (!t->isOwnDirty(spgno)) {
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
        for (unsigned i = 0; i < right->nkeys; ++i) {
            Entry e = readEntry(right, i);
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
        Entry e = readEntry(right, 0);
        if (!leaf)
            e.key = rsep;
        if (pageRoom(left) < entryCost(e, leaf))
            return;
        // The new separator is right's slot 1 key, already a valid lower bound.
        const Slice nk = nodeKey(right, 1);
        std::byte* nkbuf = t->scratch().alloc(nk.size() ? nk.size() : 1);
        std::memcpy(nkbuf, nk.data(), nk.size());
        const Slice newsep(nkbuf, nk.size());
        if (pageRoom(parent) + nodeSize(parent, ridx) < branchNodeSize(newsep.size()))
            return;
        emit(left, left->nkeys, e, leaf);
        nodeDel(right, 0);
        if (!leaf)
            branchSetKey(right, 0, Slice());  // new -infinity child
        branchSetKey(parent, ridx, newsep);
    } else {
        if (left->nkeys <= donorMin)
            return;
        Entry e = readEntry(left, left->nkeys - 1);
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
        if (pageRoom(parent) + nodeSize(parent, ridx) < branchNodeSize(movedKey.size()))
            return;
        if (!leaf && !branchSetKey(right, 0, rsep))
            return;
        if (!leaf)
            e.key = Slice();  // it becomes the new -infinity child
        emit(right, 0, e, leaf);
        nodeDel(left, left->nkeys - 1);
        branchSetKey(parent, ridx, movedKey);
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
        c.pg[c.top + 1] = c.txn->getPage(branchChild(c.pg[c.top], c.idx[c.top]));
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
        c.pg[c.top + 1] = c.txn->getPage(branchChild(c.pg[c.top], c.idx[c.top]));
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
    const Slice k = nodeKey(c.pg[c.top], c.idx[c.top]);
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

Slice leafValue(TxnImpl* t, const Page* p, unsigned i)
{
    const LeafNode* n = lnode(p, i);
    const auto* body = reinterpret_cast<const std::byte*>(n) + sizeof(LeafNode) + n->ksize;
    if (!(n->flags & N_BIGDATA))
        return Slice(body, n->vsize);
    Page* ov = t->getPage(bigPgno(n));
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
    while (isBranch(c.pg[c.top])) {
        if (c.pg[c.top]->nkeys == 0)
            throw Error(ErrorCode::Corrupted, "empty branch page");
        c.idx[c.top] = std::uint16_t(branchSearch(c.pg[c.top], key, cmp));
        if (c.top + 1 >= kMaxDepth)
            throw Error(ErrorCode::Corrupted, "b-tree exceeded maximum depth");
        c.pg[c.top + 1] = c.txn->getPage(branchChild(c.pg[c.top], c.idx[c.top]));
        ++c.top;
    }
    c.idx[c.top] = std::uint16_t(leafLowerBound(c.pg[c.top], key, cmp, exact));
    return true;
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
    return nodeKey(c.pg[c.top], c.idx[c.top]);
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
            Page* ovp = t->getPage(oldp);
            if (t->isOwnDirty(oldp) && ovPages(ovp) == overflowRun(ps, value.size())) {
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
    std::byte* payload;
    if (pageRoom(p) >= entryCost(ne, true)) {
        payload = nodeAddLeaf(p, at, key, ne.vsize, ne.flags);
        if (ne.pay)
            std::memcpy(payload, ne.pay, ne.paylen);
        else
            std::memset(payload, 0, ne.paylen);
    } else {
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
            Page* ov = t->getPage(ovp);
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

bool cursorSeek(CursorImpl& c, Slice key)
{
    c.seqSeen = c.txn->root()->seq;
    bool exact = false;
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
    c.seqSeen = c.txn->root()->seq;
    bool exact = false;
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
    return true;
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
        if (lnode(c.pg[c.top], c.idx[c.top])->flags & N_SUBDB)
            out.push_back(cursorKey(c).string());
    }
    return out;
}

namespace {

void freeSubtree(TxnImpl* t, Tree& tr, PageNo pn)
{
    Page* p = t->getPage(pn);
    if (isBranch(p)) {
        for (unsigned i = 0; i < p->nkeys; ++i)
            freeSubtree(t, tr, branchChild(p, i));
        --tr.branchPages;
    } else {
        for (unsigned i = 0; i < p->nkeys; ++i) {
            const LeafNode* n = lnode(p, i);
            if (n->flags & N_BIGDATA) {
                const PageNo ov = bigPgno(n);
                const unsigned run = ovPages(t->getPage(ov));
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
