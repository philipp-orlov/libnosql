// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <vector>

#include "nosql/internal/core.hpp"
#include "nosql/internal/key_prefix.hpp"

namespace nosql::internal {

class BulkFile
{
public:
    BulkFile(os::FileHandle file, std::size_t pageSize) : file_(file), pageSize_(pageSize) {}
    PageNo allocate(unsigned count = 1)
    {
        if (count > kMaxPgno - next_)
            throw Error(ErrorCode::MapFull, "bulk page frontier exceeded");
        const PageNo result = next_;
        next_ += count;
        const auto bytes = next_ * pageSize_;
        if (bytes > capacity_) {
            capacity_ = (bytes + (1u << 20) - 1) & ~std::uint64_t((1u << 20) - 1);
            os::resizeFile(file_, capacity_);
        }
        return result;
    }
    PageNo writePage(Page* page)
    {
        const auto number = allocate();
        page->pgno = number;
        sealPage(page, pageSize_);
        os::writeAt(file_, number * pageSize_, page, pageSize_);
        return number;
    }
    PageNo writeOverflow(Slice value, unsigned& count)
    {
        count = unsigned((kPageHdr + value.size() + kPageChecksumBytes + pageSize_ - 1) / pageSize_);
        const auto number = allocate(count);
        Page header{};
        header.pgno = number;
        header.flags = P_OVERFLOW;
        setOvPages(&header, count);
        os::writeAt(file_, number * pageSize_, &header, sizeof header);
        Checksum64 checksum;
        checksum.update(&header, sizeof header);
        for (std::size_t offset = 0; offset < value.size();) {
            const auto bytes = std::min<std::size_t>(1u << 20, value.size() - offset);
            os::writeAt(file_, number * pageSize_ + kPageHdr + offset, value.data() + offset, bytes);
            checksum.update(value.data() + offset, bytes);
            offset += bytes;
        }
        std::vector<std::byte> zeros(pageSize_, std::byte{});
        const auto padding = std::size_t(count) * pageSize_ - kPageHdr - value.size() - kPageChecksumBytes;
        os::writeAt(file_, number * pageSize_ + kPageHdr + value.size(), zeros.data(), padding);
        checksum.update(zeros.data(), padding);
        Little<std::uint64_t> encoded(checksum.digest());
        os::writeAt(file_, (number + count) * pageSize_ - sizeof encoded, &encoded, sizeof encoded);
        return number;
    }
    void finish(Meta meta)
    {
        meta.lastPgno = next_ - 1;
        meta.fileSize = next_ * pageSize_;
        meta.checksum = metaChecksum(meta);
        os::resizeFile(file_, meta.fileSize);
        std::vector<std::uint64_t> bytes(pageSize_ / 8);
        for (unsigned slot = 0; slot < 2; ++slot) {
            auto* page = reinterpret_cast<Page*>(bytes.data());
            page->pgno = slot;
            page->flags = P_META;
            *metaOf(page) = meta;
            os::writeAt(file_, slot * pageSize_, bytes.data(), pageSize_);
        }
        os::syncFile(file_, true);
    }
    std::size_t pageSize() const noexcept { return pageSize_; }

private:
    os::FileHandle file_;
    std::size_t pageSize_;
    PageNo next_ = 2;
    std::uint64_t capacity_ = 0;
};

class BulkTree
{
    struct Buffer
    {
        std::vector<std::uint64_t> bytes;
        std::string minimum;
        std::uint64_t entries = 0;  ///< leaf entries under this page
        explicit Buffer(std::size_t pageSize, std::uint16_t flags) : bytes(pageSize / 8) { reset(flags); }
        Page* page() { return reinterpret_cast<Page*>(bytes.data()); }
        void reset(std::uint16_t flags)
        {
            std::fill(bytes.begin(), bytes.end(), 0);
            page()->flags = flags;
            page()->lower = std::uint16_t(kPageHdr);
            page()->upper = std::uint16_t(bytes.size() * 8 - kPageChecksumBytes);
            minimum.clear();
            entries = 0;
        }
        void* node(std::size_t size)
        {
            auto* header = page();
            header->upper -= std::uint16_t(size);
            slots(header)[header->nkeys++] = header->upper;
            header->lower += 2;
            return reinterpret_cast<std::byte*>(header) + header->upper;
        }
    };

public:
    BulkTree(BulkFile& file, std::uint32_t flags)
        : file_(file), tree_(emptyTree(flags)), leaf_(file.pageSize(), P_LEAF) {}

    void add(Slice key, Slice value, std::uint16_t flags = 0)
    {
        if (key.empty() || key.size() > maxKeySize(file_.pageSize()) || value.size() > UINT32_MAX)
            throw Error(ErrorCode::InvalidArgument, "bulk entry exceeds format limits");
        if (!previous_.empty() && comparatorFor(tree_.flags)(previous_, key) >= 0)
            throw Error(ErrorCode::InvalidArgument, "bulk keys must be strictly ordered");
        if (tree_.flags & std::uint32_t(DbFlags::IntegerKey)) {
            if ((key.size() != 4 && key.size() != 8) || ((tree_.flags >> 16) && (tree_.flags >> 16) != key.size()))
                throw Error(ErrorCode::InvalidArgument, "bulk integer key width mismatch");
            tree_.flags |= std::uint32_t(key.size()) << 16;
        }
        const bool big = leafNodeSize(key.size(), value.size(), false) > maxNodeSize(file_.pageSize());
        if (tree_.flags & std::uint32_t(DbFlags::PrefixKeys)) {
            stage(key, value, flags, big);
            return;
        }
        const auto bytes = leafNodeSize(key.size(), value.size(), big);
        if (pageRoom(leaf_.page()) < bytes + 2)
            flushLeaf();  // previous_ is still the flushed leaf's last key
        previous_ = key.string();
        if (leaf_.page()->nkeys == 0)
            leaf_.minimum = key.string();
        auto* node = static_cast<LeafNode*>(leaf_.node(bytes));
        node->flags = flags | (big ? N_BIGDATA : 0);
        node->ksize = std::uint16_t(key.size());
        node->vsize = std::uint32_t(value.size());
        auto* body = reinterpret_cast<std::byte*>(node) + sizeof(*node);
        std::memcpy(body, key.data(), key.size());
        if (big) {
            unsigned count = 0;
            setBigPgno(node, file_.writeOverflow(value, count));
            tree_.overflowPages += count;
        } else if (!value.empty()) {
            std::memcpy(body + key.size(), value.data(), value.size());
        }
        ++tree_.entries;
    }

    Tree finish()
    {
        if (!staged_.empty())
            layoutStaged();
        if (leaf_.page()->nkeys)
            flushLeaf();
        for (std::size_t level = 0; level < levels_.size(); ++level) {
            auto& buffer = levels_[level];
            if (!buffer.page()->nkeys)
                continue;
            const bool last = level + 1 == levels_.size();
            if (last && buffer.page()->nkeys == 1) {
                tree_.root = branchChild(buffer.page(), 0);
                tree_.depth = std::uint32_t(level + 1);
                return tree_;
            }
            const auto minimum = buffer.minimum;
            const std::uint64_t entries = buffer.entries;
            const auto number = file_.writePage(buffer.page());
            ++tree_.branchPages;
            buffer.reset(P_BRANCH);
            if (last) {
                tree_.root = number;
                tree_.depth = std::uint32_t(level + 2);
                return tree_;
            }
            child(level + 1, minimum, number, entries);
        }
        return tree_;
    }

private:
    /// A PrefixKeys tree's leaf entry, held until its leaf is full: what an
    /// entry costs depends on the prefix of the keys it ends up with.
    struct Staged
    {
        std::string key, pay;
        std::uint32_t vsize = 0;
        std::uint16_t flags = 0;
    };
    struct View
    {
        Slice key;
        std::uint32_t vsize;
        std::uint16_t flags;
        const std::byte* pay;
        std::uint32_t paylen;
    };

    /// A tree in reverse order shares suffixes rather than prefixes.
    bool suffix() const noexcept { return (tree_.flags & std::uint32_t(DbFlags::ReverseKey)) != 0; }

    static std::size_t nodeBytes(const Staged& s, std::size_t plen) noexcept
    {
        return 2 + leafNodeSize(s.key.size() - plen, s.vsize, (s.flags & N_BIGDATA) != 0);
    }

    void stage(Slice key, Slice value, std::uint16_t flags, bool big)
    {
        Staged s;
        s.key = key.string();
        s.vsize = std::uint32_t(value.size());
        s.flags = std::uint16_t(flags | (big ? N_BIGDATA : 0));
        // The leaf's keys so far share stagedPrefix_ bytes; with this one they
        // share what it has in common with the first. Sizes at that prefix are
        // recomputed only when it shrinks, which it does at most once per byte.
        const std::size_t plen =
            staged_.empty()
                ? key.size()
                : std::min(stagedPrefix_, commonShared(Slice(staged_[0].key), key, suffix()));
        if (plen != stagedPrefix_) {
            stagedAt_ = 0;
            for (const Staged& e : staged_)
                stagedAt_ += nodeBytes(e, plen);
        }
        const std::size_t usable = file_.pageSize() - kPageHdr - kPageChecksumBytes;
        const std::size_t whole = stagedWhole_ + nodeBytes(s, 0);
        const std::size_t shared = stagedAt_ + nodeBytes(s, plen) + prefixBlock(plen);
        if (!staged_.empty() && std::min(whole, shared) > usable) {
            layoutStaged();
            flushLeaf();  // previous_ is still the flushed leaf's last key
            stage(key, value, flags, big);
            return;
        }
        if (big) {
            unsigned count = 0;
            const PageNo first = file_.writeOverflow(value, count);
            tree_.overflowPages += count;
            Little<PageNo> encoded(first);
            s.pay.assign(reinterpret_cast<const char*>(&encoded), sizeof encoded);
        } else {
            s.pay = value.string();
        }
        if (staged_.empty())
            leaf_.minimum = key.string();
        previous_ = key.string();
        stagedPrefix_ = plen;
        stagedWhole_ = whole;
        stagedAt_ += nodeBytes(s, plen);
        staged_.push_back(std::move(s));
        ++tree_.entries;
    }

    /// Lays the staged entries out in the leaf buffer, ready for flushLeaf().
    void layoutStaged()
    {
        std::vector<View> views;
        views.reserve(staged_.size());
        for (const Staged& s : staged_)
            views.push_back({Slice(s.key), s.vsize, s.flags,
                             reinterpret_cast<const std::byte*>(s.pay.data()),
                             std::uint32_t(s.pay.size())});
        const unsigned n = unsigned(views.size());
        layoutLeaf(leaf_.page(), 0, views.data(), n, leafPrefixFor(views.data(), n, suffix()),
                   file_.pageSize(), suffix());
        staged_.clear();
        stagedPrefix_ = 0;
        stagedWhole_ = 0;
        stagedAt_ = 0;
    }

    void child(std::size_t level, const std::string& minimum, PageNo number, std::uint64_t entries)
    {
        if (level >= kMaxDepth - 1)
            throw Error(ErrorCode::Corrupted, "bulk tree depth exceeded");
        if (levels_.size() <= level)
            levels_.emplace_back(file_.pageSize(), P_BRANCH);
        if (pageRoom(levels_[level].page()) < branchNodeSize(minimum.size()) + 2) {
            const auto parentMinimum = levels_[level].minimum;
            const std::uint64_t parentEntries = levels_[level].entries;
            const auto parentNumber = file_.writePage(levels_[level].page());
            ++tree_.branchPages;
            levels_[level].reset(P_BRANCH);
            child(level + 1, parentMinimum, parentNumber, parentEntries);
        }
        auto& buffer = levels_[level];
        const bool first = buffer.page()->nkeys == 0;
        if (first) buffer.minimum = minimum;
        const Slice key = first ? Slice() : Slice(minimum);
        auto* node = static_cast<BranchNode*>(buffer.node(branchNodeSize(key.size())));
        node->childLo = std::uint32_t(number);
        node->childHi = std::uint16_t(number >> 32);
        node->ksize = std::uint16_t(key.size());
        node->countLo = std::uint32_t(entries);
        node->countHi = std::uint32_t(entries >> 32);
        buffer.entries += entries;
        if (!key.empty()) std::memcpy(reinterpret_cast<std::byte*>(node) + sizeof(*node), key.data(), key.size());
    }
    void flushLeaf()
    {
        const std::uint64_t entries = leaf_.page()->nkeys;
        const auto number = file_.writePage(leaf_.page());
        ++tree_.leafPages;
        // The separator needs only enough of this leaf's first key to sort after
        // the previous leaf's last (a prefix in byte order, a suffix in reverse
        // order); it serves every level above, where it bounds the same two
        // subtrees. The first leaf's is never stored.
        if (!lastOfPrevious_.empty()) {
            const Slice sep =
                separatorFor(comparatorFor(tree_.flags), lastOfPrevious_, leaf_.minimum);
            const auto from =
                std::size_t(sep.data() - reinterpret_cast<const std::byte*>(leaf_.minimum.data()));
            const std::size_t len = sep.size();
            leaf_.minimum.erase(0, from);
            leaf_.minimum.resize(len);
        }
        child(0, leaf_.minimum, number, entries);
        lastOfPrevious_ = previous_;
        leaf_.reset(P_LEAF);
    }

    BulkFile& file_;
    Tree tree_;
    Buffer leaf_;
    std::vector<Buffer> levels_;
    std::string previous_;
    std::string lastOfPrevious_;  ///< last key of the leaf flushed before this one
    std::vector<Staged> staged_;
    std::size_t stagedPrefix_ = 0;  ///< bytes the staged keys share
    std::size_t stagedWhole_ = 0;   ///< their leaf bytes stored whole
    std::size_t stagedAt_ = 0;      ///< their leaf bytes at stagedPrefix_, block excluded
};

}  // namespace nosql::internal