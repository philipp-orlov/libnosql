// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/inspect.hpp"

#include <algorithm>
#include <cstring>

#include "nosql/internal/core.hpp"
#include "nosql/internal/free_list.hpp"
#include "nosql/internal/key_prefix.hpp"

namespace nosql::inspect {

using namespace nosql::internal;

const char* toString(PageKind kind) noexcept
{
    switch (kind) {
        case PageKind::Meta: return "meta";
        case PageKind::Branch: return "branch";
        case PageKind::Leaf: return "leaf";
        case PageKind::Overflow: return "overflow";
        case PageKind::Continued: return "overflow-continued";
        case PageKind::Free: return "free";
        case PageKind::Unknown: return "unknown";
    }
    return "unknown";
}

const char* toString(TreeRole role) noexcept
{
    switch (role) {
        case TreeRole::Free: return "free";
        case TreeRole::Main: return "main";
        case TreeRole::Catalog: return "catalog";
        case TreeRole::Named: return "named";
    }
    return "named";
}

/// The friend of Txn: everything that needs the transaction's internals lives in its members.
struct Access
{
    static TxnImpl& impl(Txn& t)
    {
        if (!t.impl_ || t.impl_->finished)
            throw Error(ErrorCode::BadTransaction, "transaction is not usable");
        return *t.impl_;
    }
    static std::vector<std::string> names(Txn& t) { return t.listDbs(); }
    static Tree treeOf(Txn& t, const std::string& name)
    {
        Db d = t.db(Slice(name));
        return impl(t).dbs[d.handle()].tree;
    }
};

namespace {

TreeInfo describe(const Tree& tr, TreeRole role, std::string name = {})
{
    TreeInfo i;
    i.name = std::move(name);
    i.role = role;
    i.root = tr.root;
    i.depth = tr.depth;
    i.branchPages = tr.branchPages;
    i.leafPages = tr.leafPages;
    i.overflowPages = tr.overflowPages;
    i.entries = tr.entries;
    i.flags = tr.flags;
    return i;
}

std::string hex16(const Identity& id)
{
    static const char digits[] = "0123456789abcdef";
    std::string out;
    for (std::uint64_t word : id)
        for (int shift = 60; shift >= 0; shift -= 4)
            out += digits[(word >> shift) & 15];
    return out;
}

const Page* rawPage(TxnImpl& t, std::uint64_t pgno)
{
    if (pgno > t.lastPgno)
        throw Error(ErrorCode::InvalidArgument, "page " + std::to_string(pgno) + " is beyond the last page (" + std::to_string(t.lastPgno) + ")");
    return t.env->mapPage(*t.region, pgno);
}

std::uint64_t storedHashOf(const std::byte* base, std::size_t bytes)
{
    return readLittle<std::uint64_t>(base + bytes - kPageChecksumBytes);
}

/// Pages of an overflow run starting at `p`, or 0 when the header makes no sense.
std::uint32_t runOf(TxnImpl& t, const Page* p, std::uint64_t pgno)
{
    const std::uint32_t run = ovPages(p);
    return run != 0 && run <= t.lastPgno - pgno + 1 ? run : 0;
}

}  // namespace

std::vector<TreeInfo> trees(Txn& t)
{
    TxnImpl& im = Access::impl(t);
    std::vector<TreeInfo> out;
    out.push_back(describe(im.dbs[kFreeDbi].tree, TreeRole::Free));
    out.push_back(describe(im.dbs[kMainDbi].tree, TreeRole::Main));
    out.push_back(describe(im.dbs[kCatalogDbi].tree, TreeRole::Catalog));
    for (const std::string& name : Access::names(t))
        out.push_back(describe(Access::treeOf(t, name), TreeRole::Named, name));
    return out;
}

std::vector<MetaInfo> metas(Txn& t)
{
    TxnImpl& im = Access::impl(t);
    std::vector<MetaInfo> out;
    std::uint64_t newest = 0;
    for (unsigned slot = 0; slot < 2; ++slot) {
        const Page* page = im.env->mapPage(*im.region, slot);
        const Meta& m = *metaOf(page);
        MetaInfo i;
        i.slot = slot;
        i.valid = metaValid(m, 0);
        i.version = m.version;
        i.pageSize = m.pageSize;
        i.txnid = m.txnid;
        i.lastPgno = m.lastPgno;
        i.fileSize = m.fileSize;
        i.namedDbs = m.namedDbs;
        i.checksum = m.checksum;
        i.computedChecksum = metaChecksum(m);
        i.storeId = hex16(m.storeId);
        i.commitId = hex16(m.commitId);
        i.parentId = hex16(m.parentId);
        i.parentChecksum = m.parentChecksum;
        i.deferredCount = m.deferredCount;
        i.inlineCount = m.inlineCount;
        i.inlineTxn = m.inlineTxn;
        i.trees.push_back(describe(m.freeTree, TreeRole::Free));
        i.trees.push_back(describe(m.mainTree, TreeRole::Main));
        i.trees.push_back(describe(m.catalogTree, TreeRole::Catalog));
        for (const RootSlot& s : m.roots)
            if (s.length != 0)
                i.trees.push_back(describe(s.tree, TreeRole::Named, std::string(s.name, std::min<std::size_t>(s.length, sizeof s.name))));
        if (i.valid)
            newest = std::max<std::uint64_t>(newest, i.txnid);
        out.push_back(std::move(i));
    }
    for (MetaInfo& i : out)
        i.current = i.valid && i.txnid == newest;
    return out;
}

PageInfo page(Txn& t, std::uint64_t pgno)
{
    TxnImpl& im = Access::impl(t);
    const std::size_t ps = im.pageSize();
    const Page* p = rawPage(im, pgno);
    const auto* base = reinterpret_cast<const std::byte*>(p);
    PageInfo i;
    i.pgno = pgno;
    i.pageSize = static_cast<std::uint32_t>(ps);
    i.flags = p->flags;
    if (pgno < 2) {
        const Meta& m = *metaOf(p);
        i.kind = PageKind::Meta;
        i.storedHash = m.checksum;
        i.computedHash = metaChecksum(m);
        i.hashOk = metaValid(m, 0);
        i.headerOk = i.hashOk;
        i.usedBytes = static_cast<std::uint32_t>(kPageHdr + sizeof(Meta));
        i.freeBytes = static_cast<std::uint32_t>(ps - i.usedBytes);
        return i;
    }
    i.nkeys = p->nkeys;
    i.lower = p->lower;
    i.upper = p->upper;
    const bool overflow = (p->flags & P_OVERFLOW) != 0;
    if (overflow) {
        i.kind = PageKind::Overflow;
        const std::uint32_t run = runOf(im, p, pgno);
        i.runPages = run ? run : 1;
        const std::size_t bytes = std::size_t(i.runPages) * ps;
        i.runBytes = static_cast<std::uint32_t>(bytes - kPageHdr - kPageChecksumBytes);
        i.storedHash = storedHashOf(base, bytes);
        i.computedHash = checksum64(base, bytes - kPageChecksumBytes);
        i.hashOk = run != 0 && i.storedHash == i.computedHash;
        i.headerOk = run != 0 && p->pgno == pgno && p->flags == P_OVERFLOW;
        i.usedBytes = static_cast<std::uint32_t>(bytes);
        return i;
    }
    i.kind = (p->flags & P_BRANCH) ? PageKind::Branch : (p->flags & P_LEAF) ? PageKind::Leaf : PageKind::Unknown;
    i.prefixKeys = (p->flags & P_PREFIX) != 0;
    i.suffixKeys = (p->flags & P_SUFFIX) != 0;
    i.storedHash = storedHashOf(base, ps);
    i.computedHash = checksum64(base, ps - kPageChecksumBytes);
    i.hashOk = i.storedHash == i.computedHash;
    bool sane = i.kind != PageKind::Unknown && p->pgno == pgno && p->lower >= kPageHdr && p->lower <= p->upper && p->upper <= ps - kPageChecksumBytes;
    if (sane) {
        std::size_t top = ps - kPageChecksumBytes;
        if (p->flags & P_SHARED) {
            const std::size_t plen = readLittle<std::uint16_t>(base + top - 2);
            i.sharedBytes = static_cast<std::uint32_t>(plen);
            i.sharedBlock = static_cast<std::uint32_t>(prefixBlock(plen));
            sane = plen != 0 && i.sharedBlock <= top - p->upper;
        }
    }
    i.headerOk = sane;
    if (sane) {
        i.freeBytes = static_cast<std::uint32_t>(p->upper - p->lower);
        i.usedBytes = static_cast<std::uint32_t>(ps - kPageChecksumBytes - i.freeBytes);
        if (i.kind == PageKind::Leaf) {
            i.entries = p->nkeys;
        } else if (std::size_t(p->lower) == kPageHdr + std::size_t(p->nkeys) * 2) {
            for (unsigned n = 0; n < p->nkeys; ++n) {
                const std::size_t off = slots(p)[n];
                if (off >= p->upper && off + sizeof(BranchNode) <= ps - kPageChecksumBytes)
                    i.entries += branchCount(p, n);
            }
        }
    }
    return i;
}

std::vector<NodeInfo> nodes(Txn& t, std::uint64_t pgno, std::size_t preview)
{
    TxnImpl& im = Access::impl(t);
    const std::size_t ps = im.pageSize();
    if (pgno < 2)
        throw Error(ErrorCode::InvalidArgument, "a meta page has no nodes");
    const Page* p = rawPage(im, pgno);
    if (p->flags & P_OVERFLOW)
        throw Error(ErrorCode::InvalidArgument, "an overflow page has no nodes");
    try {
        validateHeader(p, ps);
        validateNodes(p, ps);
    } catch (const Error& e) {
        throw Error(ErrorCode::Corrupted, std::string("page ") + std::to_string(pgno) + " cannot be read: " + e.what());
    }
    const bool leaf = isLeaf(p);
    const Slice shared = pageShared(p, ps);
    const bool suffix = (p->flags & P_SUFFIX) != 0;
    std::vector<NodeInfo> out;
    out.reserve(p->nkeys);
    for (unsigned n = 0; n < p->nkeys; ++n) {
        NodeInfo i;
        i.index = n;
        i.offset = slots(p)[n];
        i.size = static_cast<std::uint32_t>(nodeSize(p, n));
        const bool slotZero = !leaf && n == 0;  // the -infinity child stores no key (and shares none)
        std::string key;
        if (!slotZero) {
            const Slice part = nodeKey(p, n);
            key = suffix ? part.string() + shared.string() : shared.string() + part.string();
        }
        i.keyBytes = static_cast<std::uint32_t>(key.size());
        if (key.size() > preview)
            key.resize(preview);
        i.key = std::move(key);
        if (leaf) {
            const LeafNode* ln = lnode(p, n);
            i.big = (ln->flags & N_BIGDATA) != 0;
            i.subdb = (ln->flags & N_SUBDB) != 0;
            i.valueBytes = ln->vsize;
            const std::byte* payload = reinterpret_cast<const std::byte*>(ln) + sizeof(LeafNode) + ln->ksize;
            if (i.big) {
                i.overflow = bigPgno(ln);
                if (i.overflow >= 2 && i.overflow <= im.lastPgno) {
                    const Page* op = im.env->mapPage(*im.region, i.overflow);
                    const std::uint32_t run = (op->flags & P_OVERFLOW) ? runOf(im, op, i.overflow) : 0;
                    if (run && std::uint64_t(ln->vsize) + kPageHdr + kPageChecksumBytes <= std::uint64_t(run) * ps) {
                        const auto* v = reinterpret_cast<const char*>(op) + kPageHdr;
                        i.value.assign(v, std::min<std::size_t>(ln->vsize, preview));
                    }
                }
            } else {
                i.value.assign(reinterpret_cast<const char*>(payload), std::min<std::size_t>(ln->vsize, preview));
            }
        } else {
            i.child = branchChild(p, n);
            i.count = branchCount(p, n);
        }
        out.push_back(std::move(i));
    }
    return out;
}

std::string pageBytes(Txn& t, std::uint64_t pgno, unsigned pages)
{
    TxnImpl& im = Access::impl(t);
    const std::size_t ps = im.pageSize();
    if (pages == 0 || pages > 1024)
        throw Error(ErrorCode::InvalidArgument, "pages must be from 1 to 1024");
    const Page* p = rawPage(im, pgno);
    if (pgno + pages - 1 > im.lastPgno)
        throw Error(ErrorCode::InvalidArgument, "the run goes beyond the last page");
    return std::string(reinterpret_cast<const char*>(p), std::size_t(pages) * ps);
}

namespace {

/// Calls fn(txn, chunk, value-slice) for every entry of the free tree.
template <class Fn>
void forEachFreeEntry(TxnImpl& im, Fn fn)
{
    CursorImpl fc;
    fc.txn = &im;
    fc.dbi = kFreeDbi;
    for (bool ok = cursorFirst(fc); ok; ok = cursorNext(fc)) {
        const Slice k = cursorKey(fc);
        if (k.size() != kFreeKeyBytes)
            throw Error(ErrorCode::Corrupted, "invalid free-list key length");
        fn(freeKeyTxn(k), freeKeyChunk(k), cursorValue(fc));
    }
}

}  // namespace

std::vector<MapRun> map(Txn& t, const std::vector<TreeInfo>& treeList)
{
    TxnImpl& im = Access::impl(t);
    const std::size_t ps = im.pageSize();
    const std::uint64_t total = im.lastPgno + 1;
    if (total > kMapPageLimit)
        throw Error(ErrorCode::Unsupported, "the store has too many pages to map (" + std::to_string(total) + ")");
    std::vector<std::uint8_t> kind(total, std::uint8_t(PageKind::Unknown));
    std::vector<std::int16_t> owner(total, -1);
    kind[0] = kind[1] = std::uint8_t(PageKind::Meta);

    auto mark = [&](std::uint64_t pg, PageKind k, int tree) {
        if (pg < total && kind[pg] == std::uint8_t(PageKind::Unknown)) {
            kind[pg] = std::uint8_t(k);
            owner[pg] = std::int16_t(tree);
        }
    };

    std::vector<std::uint64_t> stack;
    for (std::size_t ti = 0; ti < treeList.size() && ti < 32000; ++ti) {
        const TreeInfo& tr = treeList[ti];
        if (tr.root == kInvalidPage)
            continue;
        stack.assign(1, tr.root);
        while (!stack.empty()) {
            const std::uint64_t pg = stack.back();
            stack.pop_back();
            if (pg < 2 || pg >= total || kind[pg] != std::uint8_t(PageKind::Unknown))
                continue;
            const Page* p = im.env->mapPage(*im.region, pg);
            if (p->flags & P_OVERFLOW)
                continue;
            try {
                validateHeader(p, ps);
                validateNodes(p, ps);
            } catch (const Error&) {
                continue;  // damaged: left Unknown, check() names it
            }
            if (isBranch(p)) {
                mark(pg, PageKind::Branch, int(ti));
                for (unsigned n = 0; n < p->nkeys; ++n)
                    stack.push_back(branchChild(p, n));
            } else if (isLeaf(p)) {
                mark(pg, PageKind::Leaf, int(ti));
                for (unsigned n = 0; n < p->nkeys; ++n) {
                    const LeafNode* ln = lnode(p, n);
                    if (!(ln->flags & N_BIGDATA))
                        continue;
                    const std::uint64_t ov = bigPgno(ln);
                    if (ov < 2 || ov >= total)
                        continue;
                    const Page* op = im.env->mapPage(*im.region, ov);
                    if (!(op->flags & P_OVERFLOW))
                        continue;
                    const std::uint32_t run = runOf(im, op, ov);
                    if (!run)
                        continue;
                    mark(ov, PageKind::Overflow, int(ti));
                    for (std::uint32_t k = 1; k < run; ++k)
                        mark(ov + k, PageKind::Continued, int(ti));
                }
            }
        }
    }

    // Free and deferred pages: never reachable, and their bytes are whatever they last held.
    auto markFree = [&](std::uint64_t pg) { mark(pg, PageKind::Free, -1); };
    try {
        forEachFreeEntry(im, [&](std::uint64_t, std::uint64_t, Slice v) {
            std::size_t n = 0;
            forEachFreePage(v, freeChunkLimit(ps), n, markFree);
        });
    } catch (const Error&) {
    }
    const std::uint64_t listed = std::min<std::uint64_t>(std::uint64_t(im.meta.deferredCount) + im.meta.inlineCount, im.meta.deferredPages.size());
    for (std::uint64_t k = 0; k < listed; ++k)
        markFree(im.meta.deferredPages[k]);

    std::vector<MapRun> runs;
    for (std::uint64_t pg = 0; pg < total; ++pg) {
        if (!runs.empty() && runs.back().kind == PageKind(kind[pg]) && runs.back().tree == owner[pg] && runs.back().first + runs.back().count == pg)
            ++runs.back().count;
        else
            runs.push_back(MapRun{pg, 1, PageKind(kind[pg]), owner[pg]});
    }
    return runs;
}

std::vector<FreeEntry> freeList(Txn& t, std::size_t maxPages)
{
    TxnImpl& im = Access::impl(t);
    const std::size_t ps = im.pageSize();
    std::vector<FreeEntry> out;
    forEachFreeEntry(im, [&](std::uint64_t txn, std::uint64_t chunk, Slice v) {
        FreeEntry e;
        e.txn = txn;
        e.chunk = chunk;
        std::size_t n = 0;
        if (!forEachFreePage(v, freeChunkLimit(ps), n, [&](PageNo pg) {
                if (e.first.size() < 32 && maxPages > 0) {
                    e.first.push_back(pg);
                    --maxPages;
                }
            }))
            throw Error(ErrorCode::Corrupted, "invalid free-list value");
        e.pages = n;
        out.push_back(std::move(e));
    });
    const Meta& m = im.meta;
    if (m.deferredCount != 0) {
        FreeEntry e;
        e.deferred = true;
        e.pages = m.deferredCount;
        for (std::uint32_t k = 0; k < m.deferredCount && k < m.deferredPages.size() && e.first.size() < 32; ++k)
            e.first.push_back(m.deferredPages[k]);
        out.push_back(std::move(e));
    }
    if (m.inlineCount != 0) {
        FreeEntry e;
        e.deferred = true;
        e.txn = m.inlineTxn;
        e.chunk = kInlineChunk;
        e.pages = m.inlineCount;
        for (std::uint32_t k = 0; k < m.inlineCount && m.deferredCount + k < m.deferredPages.size() && e.first.size() < 32; ++k)
            e.first.push_back(m.deferredPages[m.deferredCount + k]);
        out.push_back(std::move(e));
    }
    std::stable_sort(out.begin(), out.end(), [](const FreeEntry& a, const FreeEntry& b) { return a.txn > b.txn; });
    return out;
}

CheckResult check(Txn& t, std::size_t maxBad)
{
    TxnImpl& im = Access::impl(t);
    CheckResult r;
    auto note = [&](std::uint64_t pg) {
        ++r.badCount;
        if (r.badPages.size() < maxBad)
            r.badPages.push_back(pg);
    };
    for (const MetaInfo& m : metas(t)) {
        ++r.pagesChecked;
        if (!m.valid && m.txnid != 0)
            note(m.slot);
    }
    const std::vector<TreeInfo> list = trees(t);
    for (const MapRun& run : map(t, list)) {
        if (run.kind == PageKind::Branch || run.kind == PageKind::Leaf || run.kind == PageKind::Overflow) {
            for (std::uint64_t pg = run.first; pg < run.first + run.count; ++pg) {
                ++r.pagesChecked;
                if (!page(t, pg).hashOk)
                    note(pg);
            }
        } else if (run.kind == PageKind::Unknown) {
            // Not reachable and not free: a writer's pages, or damage that hid a subtree.
            for (std::uint64_t pg = run.first; pg < run.first + run.count; ++pg)
                note(pg);
        }
    }
    try {
        checkIntegrity(t);
    } catch (const Error& e) {
        r.integrity = e.what();
    }
    (void)im;
    return r;
}

}  // namespace nosql::inspect
