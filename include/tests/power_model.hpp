// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// A record of every write to a store's file and a model of what a device may
// hold after a power cut at any point in it, shared by the power suite and
// the power-cut fuzzer.
//
// An os::WriteRecorder logs every write (system calls and stores into the
// mapping alike), resize and flush in program order. Replayed into a Device,
// what was flushed is kept, and an image can be built from it plus any subset
// of what was not, a sector at a time.
#pragma once

#include <cstdint>
#include <cstring>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <vector>

#include "nosql/internal/os.hpp"

namespace tst::power {

namespace os = nosql::internal::os;

constexpr std::size_t kSector = 512;
constexpr std::size_t kPage = 4096;

struct Op
{
    enum Kind
    {
        Write,
        Resize,
        Flush,
        Durable
    } kind;
    std::uint64_t offset = 0;  // Write, Flush; the size for Resize; the txnid for Durable
    std::uint64_t length = 0;  // Flush: 0 is the whole file
    std::string bytes;         // Write: the data; a whole-file Flush: the file as it was
};

class Recorder final : public os::WriteRecorder
{
public:
    Recorder() { os::writeRecorder.store(this); }
    ~Recorder() override { os::writeRecorder.store(nullptr); }
    Recorder(const Recorder&) = delete;
    Recorder& operator=(const Recorder&) = delete;

    void written(os::FileHandle fd, std::uint64_t offset, const void* data,
                 std::size_t length) override
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!mine(fd))
            return;
        log_.push_back({Op::Write, offset, 0, std::string(static_cast<const char*>(data), length)});
    }
    void resized(os::FileHandle fd, std::uint64_t bytes) override
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (mine(fd))
            log_.push_back({Op::Resize, bytes, 0, {}});
    }
    void flushed(os::FileHandle fd, std::uint64_t offset, std::uint64_t length) override
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!mine(fd))
            return;
        Op op{Op::Flush, offset, length, {}};
        if (!length) {
            // What the file holds now, to check the record left nothing out.
            op.bytes.resize(std::size_t(os::fileSize(fd)));
            os::readAt(fd, 0, op.bytes.data(), op.bytes.size());
        }
        log_.push_back(std::move(op));
    }
    /// The store has reported `txnid` durable.
    void durable(std::uint64_t txnid)
    {
        std::lock_guard<std::mutex> lk(mtx_);
        log_.push_back({Op::Durable, txnid, 0, {}});
    }

    std::vector<Op> take()
    {
        std::lock_guard<std::mutex> lk(mtx_);
        return std::move(log_);
    }
    /// Whether a second file was written: its writes are not in the log.
    bool foreign()
    {
        std::lock_guard<std::mutex> lk(mtx_);
        return foreign_;
    }

private:
    // The store under test writes one file; a second one would be recorded
    // into the same image.
    bool mine(os::FileHandle fd)
    {
        if (fd_ == os::kInvalidFile)
            fd_ = fd;
        foreign_ = foreign_ || fd != fd_;
        return fd == fd_;
    }

    std::mutex mtx_;
    std::vector<Op> log_;
    os::FileHandle fd_ = os::kInvalidFile;
    bool foreign_ = false;
};

/// What the device may hold: the bytes it surely has, and for every sector
/// written since its last flush, each version it may have instead.
class Device
{
public:
    explicit Device(std::string image)
        : latest_(std::move(image)), durable_(latest_), size_(latest_.size())
    {}

    /// Applies one recorded op; false if a whole-file flush found the file
    /// holding bytes the record never saw written.
    bool apply(const Op& op)
    {
        switch (op.kind) {
            case Op::Write: {
                const std::uint64_t end = op.offset + op.bytes.size();
                if (end > latest_.size())
                    grow(end);
                std::memcpy(latest_.data() + op.offset, op.bytes.data(), op.bytes.size());
                for (std::uint64_t s = op.offset / kSector; s * kSector < end; ++s)
                    pending_[s].push_back(latest_.substr(std::size_t(s * kSector), kSector));
                break;
            }
            case Op::Resize:
                if (op.offset > latest_.size())
                    grow(op.offset);
                else
                    latest_.resize(std::size_t(op.offset));
                sizes_.push_back(op.offset);
                break;
            case Op::Flush:
                if (!op.length) {
                    if (latest_ != op.bytes)
                        return false;
                    durable_ = latest_;
                    size_ = latest_.size();
                    pending_.clear();
                    sizes_.clear();
                } else {
                    const std::uint64_t first = op.offset / kSector;
                    const std::uint64_t last = (op.offset + op.length + kSector - 1) / kSector;
                    for (auto it = pending_.lower_bound(first);
                         it != pending_.end() && it->first < last;) {
                        land(it->first, it->second.size());
                        it = pending_.erase(it);
                    }
                }
                break;
            case Op::Durable: break;
        }
        return true;
    }

    enum class Lose
    {
        All,
        None,
        AnySector,
        WholePages
    };

    /// One state the device may be in after a power cut now.
    std::string image(Lose lose, std::mt19937_64& rng) const
    {
        std::string out = durable_;
        out.resize(latest_.size());
        std::uint64_t page = ~0ull;
        bool keepPage = false;
        for (const auto& [sector, versions] : pending_) {
            if (sector * kSector >= out.size())
                break;             // past a truncated end
            std::size_t pick = 0;  // 0 is the durable bytes; i the i-th write since
            switch (lose) {
                case Lose::All: break;
                case Lose::None: pick = versions.size(); break;
                case Lose::AnySector: pick = std::size_t(rng() % (versions.size() + 1)); break;
                case Lose::WholePages:
                    if (sector * kSector / kPage != page) {
                        page = sector * kSector / kPage;
                        keepPage = rng() & 1;
                    }
                    pick = keepPage ? versions.size() : 0;
                    break;
            }
            if (pick)
                std::memcpy(
                    out.data() + sector * kSector, versions[pick - 1].data(),
                    std::min<std::size_t>(kSector, out.size() - std::size_t(sector * kSector)));
        }
        // The size may be the last one flushed or any set since.
        std::uint64_t size = size_;
        if (!sizes_.empty() && lose != Lose::All)
            size = lose == Lose::None ? sizes_.back()
                                      : (rng() % 2 ? size_ : sizes_[rng() % sizes_.size()]);
        out.resize(std::size_t(size));
        return out;
    }

    bool settled() const { return pending_.empty() && sizes_.empty(); }

private:
    void grow(std::uint64_t to)
    {
        latest_.resize(std::size_t(to));
        if (durable_.size() < to)
            durable_.resize(std::size_t(to));
    }
    void land(std::uint64_t sector, std::size_t)
    {
        const std::size_t at = std::size_t(sector * kSector);
        if (at >= latest_.size())
            return;
        const std::size_t n = std::min(kSector, latest_.size() - at);
        if (durable_.size() < at + n)
            durable_.resize(at + n);
        std::memcpy(durable_.data() + at, latest_.data() + at, n);
    }

    std::string latest_;   // what the page cache holds
    std::string durable_;  // what the device surely holds, bytes past size_ included
    std::uint64_t size_;   // the file size the device surely holds
    std::map<std::uint64_t, std::vector<std::string>> pending_;
    std::vector<std::uint64_t> sizes_;  // sizes set since the last whole-file flush
};

}  // namespace tst::power
