// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Power cuts replayed from a record of every write.
//
// The fault suite cuts power by copying the file at a chosen moment, which
// assumes every write issued so far has landed. A device promises less: until
// a flush returns, any part of what was written since the last one may be on
// it or not, in any order, a sector at a time. Here an os::WriteRecorder logs
// every write (system calls and stores into the mapping alike), resize and
// flush in program order, and the log is replayed against a model of the
// device: what was flushed is kept, and at every point in the log several
// images are built from it plus different subsets of what was not. Each image
// must open, pass checkIntegrity, and hold exactly a snapshot that was
// committed, no older than the one the store had reported durable.

#include <cstring>
#include <map>
#include <mutex>
#include <random>

#include "nosql/nosql.hpp"
#include "nosql/internal/os.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
namespace os = internal::os;

namespace {

using State = std::map<std::string, std::string>;

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
        CHECK(!foreign_);
        return std::move(log_);
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

    void apply(const Op& op)
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
                    CHECK(latest_ == op.bytes);  // the record missed a write if not
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

std::string readFile(const std::filesystem::path& file)
{
    const auto fd = os::openFile(file, true, false, false, false);
    std::string bytes(std::size_t(os::fileSize(fd)), '\0');
    os::readAt(fd, 0, bytes.data(), bytes.size());
    os::closeFile(fd);
    return bytes;
}

void writeFile(const std::filesystem::path& file, const std::string& bytes)
{
    const auto fd = os::openFile(file, false, true, false, false);
    os::resizeFile(fd, 0);
    os::writeAt(fd, 0, bytes.data(), bytes.size());
    os::closeFile(fd);
}

/// The store at `file` as it opens: its newest snapshot, checked.
State contentsOf(const std::filesystem::path& file, std::uint64_t& txnid)
{
    Env env = Env::configure().readOnly().open(file);
    State state;
    env.read([&](Txn& txn) {
        checkIntegrity(txn);
        txnid = txn.id();
        for (auto [key, value] : txn.mainDb().all())
            state[key.string()] = value.string();
    });
    return state;
}

struct Replay
{
    unsigned images = 0;
    unsigned failures = 0;
    std::string first;  // what the first failure was
};

/// Replays `log` from `base` and checks images at every step against the
/// snapshots in `committed`. With `honourDurable` off, an image may open on
/// any committed snapshot, as long as it opens on one.
Replay replay(const tst::Scratch& scratch, const std::string& base, const std::vector<Op>& log,
              const std::map<std::uint64_t, State>& committed, bool honourDurable)
{
    Replay r;
    Device device(base);
    std::uint64_t durable = 0;
    std::mt19937_64 rng(42);
    const auto cut = scratch.file("cut.db");
    using Lose = Device::Lose;
    for (std::size_t at = 0; at <= log.size(); ++at) {
        if (at) {
            device.apply(log[at - 1]);
            if (log[at - 1].kind == Op::Durable && honourDurable)
                durable = log[at - 1].offset;
        }
        // Writes come in runs; between two of them nothing new can be lost.
        if (at && at < log.size() && log[at].kind == Op::Write && log[at - 1].kind == Op::Write)
            continue;
        std::vector<Lose> ways = {Lose::All, Lose::None};
        if (!device.settled()) {
            ways.insert(ways.end(), 8, Lose::AnySector);
            ways.insert(ways.end(), 4, Lose::WholePages);
        }
        for (const Lose lose : ways) {
            ++r.images;
            writeFile(cut, device.image(lose, rng));
            std::string why;
            try {
                std::uint64_t txnid = 0;
                const State state = contentsOf(cut, txnid);
                const auto it = committed.find(txnid);
                if (it == committed.end())
                    why = "opened on txn " + std::to_string(txnid) + ", never committed";
                else if (txnid < durable)
                    why = "opened on txn " + std::to_string(txnid) + ", but " +
                          std::to_string(durable) + " was durable";
                else if (state != it->second)
                    why = "txn " + std::to_string(txnid) + " has the wrong contents";
            } catch (const std::exception& ex) {
                why = std::string("open failed: ") + ex.what();
            }
            if (!why.empty() && !r.failures++)
                r.first = "after op " + std::to_string(at) + " of " + std::to_string(log.size()) +
                          ", losing way " + std::to_string(int(lose)) + ": " + why;
        }
    }
    return r;
}

struct Workload
{
    Durability mode;
    bool pipelined;
    std::string name;
    std::uint64_t dirtyLimit = 0;  // small enough to spill, or none
};

/// Commits that grow, rewrite and shrink the tree, with values from a few
/// bytes to several pages, so pages split, merge, overflow and get reused.
void step(Env& env, int round, State& expect)
{
    std::mt19937_64 rng(std::uint64_t(round) * 7919u + 1);
    env.write([&](Txn& txn) {
        Db db = txn.mainDb();
        const int puts = round % 5 == 4 ? 8 : 40;
        for (int i = 0; i < puts; ++i) {
            const std::string key = tst::keyOf(rng() % 300);
            const std::size_t size = rng() % 16 == 0 ? 3 * kPage + rng() % kPage : 8 + rng() % 400;
            const std::string value = tst::blob(size, rng());
            db.put(key, value);
            expect[key] = value;
        }
        const int dels = round % 5 == 4 ? 60 : 6;
        for (int i = 0; i < dels; ++i) {
            const std::string key = tst::keyOf(rng() % 300);
            if (expect.erase(key))
                db.erase(key);
        }
    });
}

struct Recorded
{
    std::string base;
    std::vector<Op> log;
    std::map<std::uint64_t, State> committed;
};

Recorded record(const tst::Scratch& scratch, const Workload& w)
{
    const auto file = scratch.file();
    auto options = [&] {
        return Env::configure()
            .pageSize(kPage)
            .sync(w.mode)
            .pipelineCommits(w.pipelined)
            .dirtyLimit(w.dirtyLimit)
            .syncInterval(std::chrono::hours(1));
    };
    Recorded out;
    State expect;
    {
        Env env = options().open(file);
        for (int round = 0; round < 4; ++round)
            step(env, round, expect);
        env.sync();
    }
    out.base = readFile(file);
    Recorder recorder;
    {
        Env env = options().open(file);
        out.committed[env.commitGeneration()] = expect;
        recorder.durable(env.stats().durableTxn);
        for (int round = 4; round < 40; ++round) {
            step(env, round, expect);
            out.committed[env.commitGeneration()] = expect;
            if (round % 7 == 6)
                env.sync();
            recorder.durable(env.stats().durableTxn);
        }
    }
    out.log = recorder.take();
    return out;
}

const Workload kWorkloads[] = {
    {Durability::Safe, false, "Safe"},
    {Durability::Safe, true, "Safe, pipelined"},
    {Durability::NoMetaSync, false, "NoMetaSync"},
    {Durability::Lazy, false, "Lazy"},
    {Durability::None, false, "None"},
    // Pages written to the file before their commit: as safe as at commit.
    {Durability::Safe, false, "Safe, spilling", 48u << 10},
    {Durability::NoMetaSync, false, "NoMetaSync, spilling", 48u << 10},
};

}  // namespace

/// Every durability mode, cut at every point with what was not flushed lost
/// in several ways: each image opens whole on a committed snapshot no older
/// than the newest one reported durable.
TEST(everyPowerCutOpensOnADurableSnapshot)
{
    for (const Workload& w : kWorkloads) {
        tst::Scratch scratch("power-replay");
        const Recorded rec = record(scratch, w);
        CHECK(rec.log.size() > 100);
        const Replay r = replay(scratch, rec.base, rec.log, rec.committed, true);
        if (r.failures)
            std::fprintf(stderr, "%s: %u of %u images failed; first %s\n", w.name.c_str(),
                         r.failures, r.images, r.first.c_str());
        CHECK_EQ(r.failures, 0u);
        CHECK(r.images > 1000);
    }
}

/// The replay can tell: with the flushes taken out of the same record, so the
/// device may keep any subset of every write, some image opens on a meta page
/// whose pages never landed.
TEST(replayWithoutFlushesFindsTornSnapshots)
{
    tst::Scratch scratch("power-unordered");
    Recorded rec = record(scratch, kWorkloads[0]);
    std::vector<Op> unordered;
    for (Op& op : rec.log)
        if (op.kind != Op::Flush)
            unordered.push_back(std::move(op));
    const Replay r = replay(scratch, rec.base, unordered, rec.committed, false);
    CHECK(r.failures > 0);
}

int main()
{
    return tst::runAll("power");
}
