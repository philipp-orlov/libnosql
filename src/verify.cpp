// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Structural validation. Nothing here is on a hot path; it exists so tests
// (and a human staring at a suspect store) can assert that the tree really is
// a well-formed B+tree and that every page is accounted for exactly once.

#include <algorithm>
#include <vector>
#include <string>

#include "nosql/internal/core.hpp"
#include "nosql/internal/free_list.hpp"
#include "nosql/internal/key_prefix.hpp"

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

    void checkPageHeader(const Page* p, const Tree& tr)
    {
        const std::uint16_t kind = std::uint16_t(p->flags & ~P_SHARED);
        const std::uint16_t shared = std::uint16_t(p->flags & P_SHARED);
        const bool prefixed = shared != 0;
        if ((kind != P_BRANCH && kind != P_LEAF) || shared == P_SHARED)
            bad("invalid tree page flags", p->pgno);
        if (prefixed && !(tr.flags & std::uint32_t(DbFlags::PrefixKeys)))
            bad("prefix-compressed page in a tree that does not compress", p->pgno);
        // A tree in reverse order shares suffixes, any other prefixes.
        const bool reverse = (tr.flags & std::uint32_t(DbFlags::ReverseKey)) != 0;
        if (prefixed && (shared == P_SUFFIX) != reverse)
            bad("page shares key bytes at the wrong end for its tree's order", p->pgno);
        // Nodes end below the checksum, and on a page with shared bytes below them.
        std::size_t top = ps - kPageChecksumBytes;
        std::size_t plen = 0;
        if (prefixed) {
            plen = readLittle<std::uint16_t>(reinterpret_cast<const std::byte*>(p) + top - 2);
            if (plen == 0 || prefixBlock(plen) > top - pageUpper(p))
                bad("key prefix block out of range", p->pgno);
            top -= prefixBlock(plen);
        }
        if (p->lower < kPageHdr || pageUpper(p) > top || p->lower > pageUpper(p))
            bad("slot/data boundaries out of range", p->pgno);
        if (std::size_t(p->lower) != kPageHdr + std::size_t(p->nkeys) * 2)
            bad("slot count disagrees with the low-water mark", p->pgno);
        std::vector<std::pair<std::size_t, std::size_t>> spans;
        for (unsigned i = 0; i < p->nkeys; ++i) {
            const std::uint16_t off = slots(p)[i];
            if (off < pageUpper(p) || off > top || top - off < sizeof(LeafNode) || off % 4 != 0)
                bad("node offset out of range", p->pgno);
            if (off + nodeSize(p, i) > top)
                bad("node runs past the end of the page", p->pgno);
            if (plen + nodeKey(p, i).size() > maxKeySize(ps))
                bad("key exceeds page limit", p->pgno);
            spans.emplace_back(off, off + nodeSize(p, i));
        }
        std::sort(spans.begin(), spans.end());
        for (std::size_t index = 1; index < spans.size(); ++index)
            if (spans[index - 1].second > spans[index].first)
                bad("overlapping node bodies", p->pgno);
    }

    /// Key `i` of `p` whole, its page's prefix included; empty for branch
    /// slot 0, which stores none.
    std::string wholeKey(const Page* p, unsigned i) const
    {
        if (isBranch(p) && i == 0)
            return std::string();
        if (p->flags & P_SUFFIX)
            return nodeKey(p, i).string() + pageShared(p, ps).string();
        return pageShared(p, ps).string() + nodeKey(p, i).string();
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
        validateNodes(p, ps);
        checkPageHeader(p, tr);

        // Branch slot 0 is the -infinity child and must carry no key at all;
        // every other slot has to be strictly greater than the one before it.
        const unsigned firstReal = isBranch(p) ? 1u : 0u;
        if (isBranch(p) && p->nkeys && nodeKey(p, 0).size() != 0)
            bad("branch slot 0 is not an empty -infinity placeholder", pgno);
        std::vector<std::string> keys(p->nkeys);
        for (unsigned i = 0; i < p->nkeys; ++i)
            keys[i] = wholeKey(p, i);
        for (unsigned i = firstReal + 1; i < p->nkeys; ++i)
            if (cmp(Slice(keys[i - 1]), Slice(keys[i])) >= 0)
                bad("keys are not strictly increasing within the page", pgno);

        Span span;
        if (isLeaf(p)) {
            if (depth != tr.depth)
                bad("leaf depth differs from tree descriptor", pgno);
            ++leafPages;
            if (p->nkeys) {
                span.lo = keys[0];
                span.hi = keys[p->nkeys - 1];
            }
            span.entries = p->nkeys;
            for (unsigned i = 0; i < p->nkeys; ++i) {
                const LeafNode* n = lnode(p, i);
                if ((n->flags & ~(N_BIGDATA | N_SUBDB)) != 0 ||
                    n->ksize + pageShared(p, ps).size() == 0)
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
                    const Page* op = t->getPage(ov, true, TxnImpl::PageKind::Overflow);
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
            if (branchCount(p, i) != sub.entries)
                bad("branch slot's entry count differs from its subtree", pgno);
            span.entries += sub.entries;
            if (sub.lo.empty() && sub.hi.empty())
                continue;  // empty subtree

            // Slot 0 is the -infinity placeholder and is never compared, so only
            // slots >= 1 have to bound their subtree from below.
            if (i >= 1 && cmp(Slice(sub.lo), Slice(keys[i])) < 0)
                bad("subtree holds a key below its separator", pgno);
            if (i + 1 < p->nkeys && cmp(Slice(sub.hi), Slice(keys[i + 1])) >= 0)
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
    // A meta root slot's record overrides the catalog's, which is stale; a
    // slot must name a tree the catalog has.
    std::size_t slotted = 0;
    for (auto& [name, tree] : c.named) {
        if (const RootSlot* s = findRootSlot(impl->root()->meta, Slice(name))) {
            tree = s->tree;
            ++slotted;
        }
    }
    std::size_t slots = 0;
    for (const RootSlot& s : impl->root()->meta.roots)
        slots += s.length != 0;
    if (slotted != slots)
        throw Error(ErrorCode::Corrupted, "integrity: a meta root slot names no catalog entry");
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
    std::uint64_t listed = 0;
    for (bool ok = cursorFirst(fc); ok; ok = cursorNext(fc)) {
        // A live writer has already handed out the pages of the entries it
        // absorbed; those keys are deleted at commit, so ignore them here.
        const Slice k = cursorKey(fc);
        if (k.size() != kFreeKeyBytes)
            throw Error(ErrorCode::Corrupted, "invalid free-list key length");
        const TxnImpl::GcKey key(freeKeyTxn(k), freeKeyChunk(k));
        if (std::find(impl->consumed.begin(), impl->consumed.end(), key) != impl->consumed.end())
            continue;
        std::size_t n = 0;
        if (!forEachFreePage(cursorValue(fc), freeChunkLimit(impl->pageSize()), n,
                             [&](PageNo page) { c.claim(page); }))
            throw Error(ErrorCode::Corrupted, "invalid free-list value");
        listed += n;
    }
    if (impl->readOnly) {
        if (std::uint64_t(impl->meta.deferredCount) + impl->meta.inlineCount > impl->meta.deferredPages.size())
            throw Error(ErrorCode::Corrupted, "invalid deferred free-page count");
        for (std::uint64_t index = 0; index < std::uint64_t(impl->meta.deferredCount) + impl->meta.inlineCount; ++index)
            c.claim(impl->meta.deferredPages[index]);
        for (PageNo page = 0; page <= impl->lastPgno; ++page)
            if (!(c.seen[page / 64] & (std::uint64_t(1) << (page % 64))))
                c.bad("allocated page is neither live nor free", page);
        // The count EnvStats reports without a walk must match one. It follows
        // the commit writers build on, which under pipelined commits can be
        // ahead of the newest one readers see.
        std::lock_guard<std::mutex> lk(impl->env->mtx);
        if (impl->env->writerMeta.txnid == impl->meta.txnid && impl->env->freeTreePages != listed)
            throw Error(ErrorCode::Corrupted,
                        "integrity: free-page count differs from the free list");
    }
}

}  // namespace nosql
