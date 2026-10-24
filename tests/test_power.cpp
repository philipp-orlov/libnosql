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
#include <set>

#include "nosql/nosql.hpp"
#include "nosql/replication.hpp"
#include "nosql/internal/os.hpp"
#include "tests/power_model.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
namespace os = internal::os;

namespace {

using State = std::map<std::string, std::string>;
using tst::power::Device;
using tst::power::kPage;
using tst::power::kSector;
using tst::power::Op;
using tst::power::Recorder;

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
        for (const std::string& name : txn.listDbs())
            for (auto [key, value] : txn.db(name).all())
                state[name + "/" + key.string()] = value.string();
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
            CHECK(device.apply(log[at - 1]));  // the record missed a write if not
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
    bool shrinks = false;          // Env::shrink() every few rounds
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
        // A named tree and a prefix-compressed one, their catalog entries and
        // P_PREFIX leaves under the same cuts.
        for (const auto& [name, flags] : {std::pair<const char*, DbFlags>{"named", DbFlags::None},
                                          {"prefixed", DbFlags::PrefixKeys}}) {
            Db d = txn.db(name, flags | DbFlags::Create);
            for (int i = 0; i < 25; ++i) {
                const std::string key = "tenant/0042/orders/" + tst::keyOf(rng() % 400, "");
                const std::string value = tst::blob(16 + rng() % 200, rng());
                d.put(key, value);
                expect[std::string(name) + "/" + key] = value;
            }
            for (int i = 0; i < 5; ++i) {
                const std::string key = "tenant/0042/orders/" + tst::keyOf(rng() % 400, "");
                if (expect.erase(std::string(name) + "/" + key))
                    d.erase(key);
            }
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
            if (w.shrinks && round % 9 == 8) {
                // Its commits move pages and cut the file, the contents stay.
                const std::uint64_t from = env.commitGeneration();
                (void)env.shrink();
                for (std::uint64_t g = from + 1; g <= env.commitGeneration(); ++g)
                    out.committed[g] = expect;
            }
            if (round % 7 == 6)
                env.sync();
            recorder.durable(env.stats().durableTxn);
        }
    }
    CHECK(!recorder.foreign());
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
    // Pages moved down and the file cut short under the same cuts.
    {Durability::Safe, false, "Safe, shrinking", 0, true},
    {Durability::Lazy, false, "Lazy, shrinking", 0, true},
    {Durability::None, false, "None, shrinking", 0, true},
};

/// Watches every file written while a replacement is built: at the rename
/// that publishes it, nothing written to any file may still be waiting for a
/// flush, or a power cut after the rename could find the new name on a file
/// whose bytes never landed.
class ReplaceWatcher final : public os::WriteRecorder
{
public:
    ReplaceWatcher() { os::writeRecorder.store(this); }
    ~ReplaceWatcher() override { os::writeRecorder.store(nullptr); }
    ReplaceWatcher(const ReplaceWatcher&) = delete;
    ReplaceWatcher& operator=(const ReplaceWatcher&) = delete;

    void written(os::FileHandle fd, std::uint64_t offset, const void*, std::size_t length) override
    {
        std::lock_guard<std::mutex> lk(mtx_);
        auto& waiting = pending_[fd];
        for (std::uint64_t s = offset / kSector; s * kSector < offset + length; ++s)
            waiting.insert(s);
    }
    void resized(os::FileHandle fd, std::uint64_t) override
    {
        std::lock_guard<std::mutex> lk(mtx_);
        sized_.insert(fd);
    }
    void flushed(os::FileHandle fd, std::uint64_t offset, std::uint64_t length) override
    {
        std::lock_guard<std::mutex> lk(mtx_);
        auto& waiting = pending_[fd];
        if (!length) {
            waiting.clear();
            sized_.erase(fd);
            return;
        }
        waiting.erase(waiting.lower_bound(offset / kSector),
                      waiting.lower_bound((offset + length + kSector - 1) / kSector));
    }
    void replacing(const std::filesystem::path&, const std::filesystem::path&) override
    {
        std::lock_guard<std::mutex> lk(mtx_);
        ++renames_;
        for (const auto& [fd, waiting] : pending_)
            unflushed_ += waiting.size();
        unflushed_ += sized_.size();
    }
    unsigned renames() const { return renames_; }
    std::size_t unflushed() const { return unflushed_; }

private:
    std::mutex mtx_;
    std::map<os::FileHandle, std::set<std::uint64_t>> pending_;
    std::set<os::FileHandle> sized_;
    unsigned renames_ = 0;
    std::size_t unflushed_ = 0;
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

/// Compaction and a replica's apply build a new file and rename it over the
/// old: the old stays whole until the rename, and every byte of the new must
/// be on the device before it, since the rename may land first.
TEST(replacementsAreFlushedBeforeTheirRename)
{
    tst::Scratch scratch("power-replace");
    const auto store = scratch.file(), ship = scratch.file("ship");
    const auto replica = scratch.file("replica.db"), bundle = scratch.file("delta.bundle");
    State expect;
    Checkpoint primary{};
    {
        Env env =
            Env::configure().pageSize(kPage).sync(Durability::NoMetaSync).shipTo(ship).open(store);
        for (int round = 0; round < 6; ++round)
            step(env, round, expect);
        {
            Txn rt = env.readTxn();
            copySnapshot(rt, replica);
            rt.abort();
        }
        const Checkpoint base = checkpointOf(replica);
        for (int round = 6; round < 12; ++round)
            step(env, round, expect);
        env.sync();
        CHECK(ShipLog(ship).extract(base, bundle) == ShipStatus::Ok);
        primary = checkpointOf(store);
    }

    {
        ReplaceWatcher watch;
        const Checkpoint at = applyBundle(replica, bundle);
        CHECK_EQ(watch.renames(), 1u);
        CHECK_EQ(watch.unflushed(), std::size_t(0));
        CHECK_EQ(at, primary);
    }
    std::uint64_t txnid = 0;
    CHECK(contentsOf(replica, txnid) == expect);

    {
        ReplaceWatcher watch;
        compact(store, scratch.file("compact.db"));
        CHECK_EQ(watch.renames(), 1u);
        CHECK_EQ(watch.unflushed(), std::size_t(0));
    }
    CHECK(contentsOf(scratch.file("compact.db"), txnid) == expect);
}

int main()
{
    return tst::runAll("power");
}
