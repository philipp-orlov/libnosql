// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Structural validation. Nothing here is on a hot path; it exists so tests
// (and a human staring at a suspect store) can assert that the tree really is
// a well-formed B+tree and that every page is accounted for exactly once.

#include <algorithm>
#include <vector>
#include <string>

#include "nosql/internal/core.hpp"

namespace nosql {

using namespace internal;

namespace {

struct Checker
{
    TxnImpl* t;
    std::size_t ps;
    std::vector<std::uint64_t> seen;
    std::vector<std::pair<std::string, Tree>> named;
    bool catalog = false;

    [[noreturn]] void bad(const std::string& what, PageNo p)
    {
        throw Error(ErrorCode::Corrupted,
                    "integrity: " + what + " (page " + std::to_string(p) + ")");
    }

    void claim(PageNo p, unsigned run = 1)
    {
        for (unsigned k = 0; k < run; ++k) {
            if (p + k > t->lastPgno)
                bad("page beyond the allocation frontier", p + k);
            const auto page = p + k;
            const auto mask = std::uint64_t(1) << (page % 64);
            if (seen[page / 64] & mask)
                bad("page reachable more than once", p + k);
            seen[page / 64] |= mask;
        }
    }

    void checkPageHeader(const Page* p)
    {
        if (p->flags != P_BRANCH && p->flags != P_LEAF)
            bad("invalid tree page flags", p->pgno);
        if (p->lower < kPageHdr || pageUpper(p) > ps || p->lower > pageUpper(p))
            bad("slot/data boundaries out of range", p->pgno);
        if (std::size_t(p->lower) != kPageHdr + std::size_t(p->nkeys) * 2)
            bad("slot count disagrees with the low-water mark", p->pgno);
        std::vector<std::pair<std::size_t, std::size_t>> spans;
        for (unsigned i = 0; i < p->nkeys; ++i) {
            const std::uint16_t off = slots(p)[i];
            if (off < pageUpper(p) || off > ps || ps - off < sizeof(LeafNode) || off % 4 != 0)
                bad("node offset out of range", p->pgno);
            if (off + nodeSize(p, i) > ps)
                bad("node runs past the end of the page", p->pgno);
            if (nodeKey(p, i).size() > maxKeySize(ps))
                bad("key exceeds page limit", p->pgno);
            spans.emplace_back(off, off + nodeSize(p, i));
        }
        std::sort(spans.begin(), spans.end());
        for (std::size_t index = 1; index < spans.size(); ++index)
            if (spans[index - 1].second > spans[index].first)
                bad("overlapping node bodies", p->pgno);
    }

    struct Span
    {
        std::string lo, hi;
        std::uint64_t entries = 0;
    };

    Span walk(PageNo pgno, Tree& tr, CompareFn cmp, unsigned depth, std::uint64_t& branchPages,
               std::uint64_t& leafPages, std::uint64_t& overflowPages)
    {
        if (depth > kMaxDepth)
            bad("tree deeper than the descent limit", pgno);
        claim(pgno);
        const Page* p = t->getPage(pgno, true);
        if (p->pgno != pgno)
            bad("page header carries the wrong page number", pgno);
        checkPageHeader(p);

        // Branch slot 0 is the -infinity child and must carry no key at all;
        // every other slot has to be strictly greater than the one before it.
        const unsigned firstReal = isBranch(p) ? 1u : 0u;
        if (isBranch(p) && p->nkeys && nodeKey(p, 0).size() != 0)
            bad("branch slot 0 is not an empty -infinity placeholder", pgno);
        for (unsigned i = firstReal + 1; i < p->nkeys; ++i)
            if (cmp(nodeKey(p, i - 1), nodeKey(p, i)) >= 0)
                bad("keys are not strictly increasing within the page", pgno);

        Span span;
        if (isLeaf(p)) {
            if (depth != tr.depth)
                bad("leaf depth differs from tree descriptor", pgno);
            ++leafPages;
            if (p->nkeys) {
                span.lo = nodeKey(p, 0).string();
                span.hi = nodeKey(p, p->nkeys - 1).string();
            }
            span.entries = p->nkeys;
            for (unsigned i = 0; i < p->nkeys; ++i) {
                const LeafNode* n = lnode(p, i);
                if ((n->flags & ~(N_BIGDATA | N_SUBDB)) != 0 || n->ksize == 0)
                    bad("invalid leaf flags or empty key", pgno);
                if (n->flags & N_SUBDB) {
                    if (!catalog || (n->flags & N_BIGDATA) || n->vsize != sizeof(Tree))
                        bad("invalid named tree descriptor", pgno);
                    Tree subtree{};
                    std::memcpy(&subtree, leafValue(t, p, i).data(), sizeof subtree);
                    named.emplace_back(nodeKey(p, i).string(), subtree);
                }
                if (n->flags & N_BIGDATA) {
                    const PageNo ov = bigPgno(n);
                    const Page* op = t->getPage(ov, true);
                    if (op->flags != P_OVERFLOW || op->pgno != ov)
                        bad("large value does not point at an overflow page", ov);
                    const unsigned run = ovPages(op);
                    if (run == 0 || std::size_t(run) * ps < kPageHdr + n->vsize)
                        bad("overflow run is too short for the value", ov);
                    claim(ov, run);
                    overflowPages += run;
                }
            }
            return span;
        }

        if (!isBranch(p))
            bad("page is neither a leaf nor a branch", pgno);
        ++branchPages;
        if (p->nkeys == 0)
            bad("branch page has no children", pgno);

        bool first = true;
        for (unsigned i = 0; i < p->nkeys; ++i) {
            const Span sub =
                walk(branchChild(p, i), tr, cmp, depth + 1, branchPages, leafPages, overflowPages);
            span.entries += sub.entries;
            if (sub.lo.empty() && sub.hi.empty())
                continue;  // empty subtree

            // Slot 0 is the -infinity placeholder and is never compared, so only
            // slots >= 1 have to bound their subtree from below.
            if (i >= 1 && cmp(Slice(sub.lo), nodeKey(p, i)) < 0)
                bad("subtree holds a key below its separator", pgno);
            if (i + 1 < p->nkeys && cmp(Slice(sub.hi), nodeKey(p, i + 1)) >= 0)
                bad("subtree holds a key at or above the next separator", pgno);

            if (first) {
                span.lo = sub.lo;
                first = false;
            }
            span.hi = sub.hi;
        }
        return span;
    }

    void checkTree(Tree tr, const char* label)
    {
        if (tr.root == kInvalidPage) {
            if (tr.entries || tr.depth || tr.branchPages || tr.leafPages || tr.overflowPages)
                throw Error(ErrorCode::Corrupted,
                            std::string("integrity: ") + label + " is rootless but claims content");
            return;
        }
        std::uint64_t branch = 0, leaf = 0, over = 0;
        const CompareFn cmp = comparatorFor(tr.flags);
        const Span span = walk(tr.root, tr, cmp, 1, branch, leaf, over);

        auto mismatch = [&](const char* field, std::uint64_t got, std::uint64_t want) {
            throw Error(ErrorCode::Corrupted, std::string("integrity: ") + label + " " + field +
                                                  " says " + std::to_string(want) +
                                                  ", walk found " + std::to_string(got));
        };
        if (span.entries != tr.entries)
            mismatch("entry count", span.entries, tr.entries);
        if (branch != tr.branchPages)
            mismatch("branch page count", branch, tr.branchPages);
        if (leaf != tr.leafPages)
            mismatch("leaf page count", leaf, tr.leafPages);
        if (over != tr.overflowPages)
            mismatch("overflow page count", over, tr.overflowPages);
    }
};

}  // namespace

void checkIntegrity(Txn& t)
{
    TxnImpl* impl = t.impl_.get();
    if (!impl || impl->finished)
        throw Error(ErrorCode::BadTransaction, "transaction is not usable");

    Checker c{impl, impl->pageSize(), std::vector<std::uint64_t>((impl->lastPgno + 64) / 64), {}, false};
    c.claim(0, 2);
    c.checkTree(impl->dbs[kFreeDbi].tree, "free list");
    c.checkTree(impl->dbs[kMainDbi].tree, "main tree");
    c.catalog = true;
    c.checkTree(impl->dbs[kCatalogDbi].tree, "catalog");
    c.catalog = false;
    if (c.named.size() != impl->namedDbs)
        throw Error(ErrorCode::Corrupted, "named database count mismatch");
    for (auto& [name, tree] : c.named) {
        for (const DbSlot& slot : impl->dbs)
            if (slot.loaded && slot.name == name)
                tree = slot.tree;
        c.checkTree(tree, name.c_str());
    }

    // Free-list pages must not overlap anything the trees reach.
    CursorImpl fc;
    fc.txn = impl;
    fc.dbi = kFreeDbi;
    for (bool ok = cursorFirst(fc); ok; ok = cursorNext(fc)) {
        // A live writer has already handed out the pages of the entries it
        // absorbed; those keys are deleted at commit, so ignore them here.
        const Slice k = cursorKey(fc);
        if (k.size() != kFreeKeyBytes)
            throw Error(ErrorCode::Corrupted, "invalid free-list key length");
        const TxnImpl::GcKey key(freeKeyTxn(k), freeKeyChunk(k));
        if (std::find(impl->consumed.begin(), impl->consumed.end(), key) != impl->consumed.end())
            continue;
        const Slice v = cursorValue(fc);
        if (v.size() % sizeof(PageNo) != 0)
            throw Error(ErrorCode::Corrupted, "invalid free-list value length");
        for (std::size_t i = 0; i < v.size() / sizeof(PageNo); ++i) {
            PageNo page = 0;
            page = readLittle<PageNo>(v.data() + i * sizeof page);
            c.claim(page);
        }
    }
    if (impl->readOnly) {
        if (impl->meta.deferredCount > impl->meta.deferredPages.size())
            throw Error(ErrorCode::Corrupted, "invalid deferred free-page count");
        for (std::uint64_t index = 0; index < impl->meta.deferredCount; ++index)
            c.claim(impl->meta.deferredPages[index]);
        for (PageNo page = 0; page <= impl->lastPgno; ++page)
            if (!(c.seen[page / 64] & (std::uint64_t(1) << (page % 64))))
                c.bad("allocated page is neither live nor free", page);
    }
}

}  // namespace nosql
