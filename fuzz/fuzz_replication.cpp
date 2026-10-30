// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// libFuzzer target: shipping and applying deltas through damage the input
// picks. A primary ships into a segment directory small enough to roll often
// while it commits puts, erases, clears and shrinks; a replica starts from a
// snapshot. Between commits the input extracts bundles from any checkpoint,
// flips, truncates, extends, deletes and swaps segment, index and bundle
// files, prunes, and applies any bundle it made, in any order, to the
// replica. An extract may refuse or fail with an Error; an apply may fail
// with an Error and leave the replica exactly as it was, or succeed and leave
// it whole on the checkpoint it returns, holding exactly what the primary had
// committed there. Anything else, a crash or a sanitizer report included, is
// a bug.
//
//   cmake -S . -B build-fuzz -DCMAKE_CXX_COMPILER=clang++ -DNOSQL_BUILD_FUZZERS=ON
//   cmake --build build-fuzz --target fuzz_replication
//   build-fuzz/fuzz/fuzz_replication -max_total_time=600

#include <fuzzer/FuzzedDataProvider.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/replication.hpp"

namespace {

namespace fs = std::filesystem;
using State = std::map<std::string, std::string>;  // "tree/key" -> value

[[noreturn]] void fail(const std::string& what)
{
    std::fprintf(stderr, "replication: %s\n", what.c_str());
    std::abort();
}

struct Paths
{
    fs::path dir;
    Paths()
    {
        const fs::path shm = "/dev/shm";
        const auto base = fs::exists(shm) ? shm : fs::temp_directory_path();
        dir = base / ("nosql-fuzz-replication-" + std::to_string(::getpid()));
    }
    ~Paths() { fs::remove_all(dir); }
};

/// The replica as it opens: its checkpoint and contents, checked.
State contentsOf(const fs::path& store)
{
    nosql::Env env = nosql::Env::configure().readOnly().open(store);
    State state;
    env.read([&](nosql::Txn& txn) {
        nosql::checkIntegrity(txn);
        for (auto [key, value] : txn.mainDb().all())
            state[std::string("/") + key.string()] = value.string();
        for (const std::string& name : txn.listDbs())
            for (auto [key, value] : txn.db(name).all())
                state[name + "/" + key.string()] = value.string();
    });
    return state;
}

std::string readAll(const fs::path& p)
{
    std::ifstream f(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), {});
}

void writeAll(const fs::path& p, const std::string& bytes)
{
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(bytes.data(), std::streamsize(bytes.size()));
}

class Run
{
public:
    Run(FuzzedDataProvider& in, const Paths& p)
        : in_(in), store_(p.dir / "p.db"), ship_(p.dir / "ship"), replica_(p.dir / "r.db")
    {
        fs::remove_all(p.dir);
        fs::create_directories(p.dir);
        dir_ = p.dir;
    }

    void go()
    {
        const std::size_t sizes[] = {1024, 4096};
        nosql::Env env = nosql::Env::configure()
                             .pageSize(sizes[in_.ConsumeIntegralInRange<int>(0, 1)])
                             .sync(nosql::Durability::NoMetaSync)
                             .shipTo(ship_)
                             .shipSegmentSize(in_.ConsumeIntegralInRange<std::uint64_t>(4, 64) << 10)
                             .shipRetain(in_.ConsumeIntegralInRange<unsigned>(1, 6))
                             .open(store_);
        record(env);
        for (int i = in_.ConsumeIntegralInRange<int>(0, 3); i > 0; --i)
            commit(env);
        rebase(env);
        for (int step = 0; step < 40 && in_.remaining_bytes() > 0; ++step) {
            switch (in_.ConsumeIntegralInRange<int>(0, 9)) {
                case 0:
                case 1: commit(env); break;
                case 2:
                case 3: extract(); break;
                case 4:
                case 5: apply(); break;
                case 6: damage(pick(segments())); break;
                case 7: damage(bundles_.empty() ? fs::path() : bundles_[pickIndex(bundles_.size())]); break;
                case 8: (void)nosql::ShipLog(ship_).prune(in_.ConsumeIntegral<std::uint8_t>()); break;
                case 9: rebase(env); break;
            }
        }
    }

private:
    void record(nosql::Env& env)
    {
        const nosql::Checkpoint at = nosql::checkpointOf(store_);
        if (at.txnid != env.commitGeneration())
            fail("the primary's file is not on its newest commit");
        committed_[at.txnid] = {at, model_};
    }

    std::string value()
    {
        const auto n = in_.ConsumeIntegralInRange<int>(0, 11) == 0
                           ? in_.ConsumeIntegralInRange<std::size_t>(1000, 9000)
                           : in_.ConsumeIntegralInRange<std::size_t>(0, 120);
        return std::string(n, char('a' + in_.ConsumeIntegralInRange<int>(0, 25)));
    }

    void commit(nosql::Env& env)
    {
        if (in_.ConsumeIntegralInRange<int>(0, 15) == 0) {
            // Its commits move pages down and cut the file; replicas follow.
            // Only the last one's checkpoint can be read back after.
            const std::uint64_t from = env.commitGeneration();
            (void)env.shrink();
            for (std::uint64_t g = from + 1; g < env.commitGeneration(); ++g)
                committed_[g] = {nosql::Checkpoint{}, model_};
            record(env);
            return;
        }
        State model = model_;
        env.write([&](nosql::Txn& t) {
            const int n = in_.ConsumeIntegralInRange<int>(1, 60);
            for (int i = 0; i < n && in_.remaining_bytes() > 0; ++i) {
                const bool named = in_.ConsumeBool();
                nosql::Db db = named ? t.db("named", nosql::DbFlags::Create) : t.mainDb();
                const std::string tree = named ? "named" : "";
                const std::string key = "k" + std::to_string(in_.ConsumeIntegralInRange<int>(0, 300));
                switch (in_.ConsumeIntegralInRange<int>(0, 9)) {
                    case 0:
                        if (model.erase(tree + "/" + key))
                            db.erase(key);
                        break;
                    case 1:
                        if (in_.ConsumeIntegralInRange<int>(0, 7) == 0) {
                            db.clear();
                            for (auto it = model.lower_bound(tree + "/");
                                 it != model.end() && it->first.compare(0, tree.size() + 1, tree + "/") == 0;)
                                it = model.erase(it);
                        }
                        break;
                    default: {
                        const std::string v = value();
                        db.put(key, v);
                        model[tree + "/" + key] = v;
                    }
                }
            }
        });
        model_ = std::move(model);
        record(env);
    }

    /// A fresh base: what a replica past the retention window is told to take.
    void rebase(nosql::Env& env)
    {
        fs::remove(replica_);
        nosql::Txn rt = env.readTxn();
        nosql::copySnapshot(rt, replica_);
        rt.abort();
        replicaAt_ = nosql::checkpointOf(replica_);
        check();
    }

    std::vector<fs::path> segments() const
    {
        std::vector<fs::path> out;
        std::error_code ec;
        for (const auto& e : fs::directory_iterator(ship_, ec))
            out.push_back(e.path());
        std::sort(out.begin(), out.end());
        return out;
    }
    std::size_t pickIndex(std::size_t n) { return in_.ConsumeIntegralInRange<std::size_t>(0, n - 1); }
    fs::path pick(const std::vector<fs::path>& v) { return v.empty() ? fs::path() : v[pickIndex(v.size())]; }

    void extract()
    {
        // From the replica's checkpoint mostly, else from any committed one.
        nosql::Checkpoint base = replicaAt_;
        if (in_.ConsumeIntegralInRange<int>(0, 3) == 0) {
            auto it = committed_.begin();
            std::advance(it, pickIndex(committed_.size()));
            if (it->second.at.valid())
                base = it->second.at;
        }
        std::uint64_t through = 0;
        if (in_.ConsumeBool())
            through = base.txnid + in_.ConsumeIntegralInRange<std::uint64_t>(0, 8);
        const fs::path bundle = dir_ / ("b" + std::to_string(bundles_.size()) + ".bundle");
        try {
            const nosql::ShipStatus st = nosql::ShipLog(ship_).extract(
                base, bundle, through, in_.ConsumeBool() ? 64u << 10 : 8u << 20);
            if (st == nosql::ShipStatus::Ok) {
                if (!fs::exists(bundle))
                    fail("extract said Ok and wrote nothing");
                bundles_.push_back(bundle);
            } else if (fs::exists(bundle)) {
                fail("extract wrote a bundle it did not report");
            }
        } catch (const nosql::Error&) {
            // Damaged history may be refused; nothing may be left behind.
            if (fs::exists(bundle))
                fail("a failed extract left a bundle behind");
        }
    }

    void apply()
    {
        if (bundles_.empty())
            return;
        const fs::path bundle = bundles_[pickIndex(bundles_.size())];
        const nosql::Checkpoint before = replicaAt_;
        try {
            const nosql::Checkpoint at = nosql::applyBundle(replica_, bundle);
            if (nosql::checkpointOf(replica_) != at)
                fail("apply returned a checkpoint the replica does not hold");
            replicaAt_ = at;
        } catch (const nosql::Error&) {
            if (nosql::checkpointOf(replica_) != before)
                fail("a failed apply moved the replica");
        }
        check();
        // No temporary may outlive an apply that returned.
        for (const auto& e : fs::directory_iterator(dir_))
            if (e.path().filename().string().rfind(".tmp-", 0) == 0 ||
                e.path().filename().string().find(".tmp-") != std::string::npos)
                fail("an apply left a temporary file: " + e.path().string());
    }

    /// The replica holds exactly what the primary committed at its checkpoint.
    void check()
    {
        const auto it = committed_.find(replicaAt_.txnid);
        if (it == committed_.end())
            fail("the replica is on txn " + std::to_string(replicaAt_.txnid) +
                 ", which the primary never committed");
        if (it->second.at.valid() && it->second.at != replicaAt_)
            fail("the replica is on a checkpoint the primary never had");
        State state;
        try {
            state = contentsOf(replica_);
        } catch (const std::exception& ex) {
            fail(std::string("the replica does not open whole: ") + ex.what());
        }
        if (state != it->second.state)
            fail("the replica holds other contents than its checkpoint's");
    }

    void damage(const fs::path& file)
    {
        if (file.empty() || !fs::exists(file))
            return;
        std::string bytes = readAll(file);
        switch (in_.ConsumeIntegralInRange<int>(0, 5)) {
            case 0:
            case 1:
                if (!bytes.empty())
                    bytes[pickIndex(bytes.size())] ^= char(in_.ConsumeIntegralInRange<int>(1, 255));
                break;
            case 2: bytes.resize(bytes.empty() ? 0 : pickIndex(bytes.size())); break;
            case 3: bytes += in_.ConsumeBytesAsString(in_.ConsumeIntegralInRange<std::size_t>(1, 64)); break;
            case 4: fs::remove(file); return;
            case 5: {
                // Another file's bytes under this one's name: history reordered.
                const std::vector<fs::path> all = segments();
                const fs::path other = pick(all);
                if (!other.empty() && other != file)
                    bytes = readAll(other);
                break;
            }
        }
        writeAll(file, bytes);
    }

    struct Committed
    {
        nosql::Checkpoint at;
        State state;
    };

    FuzzedDataProvider& in_;
    fs::path dir_, store_, ship_, replica_;
    State model_;
    std::map<std::uint64_t, Committed> committed_;
    nosql::Checkpoint replicaAt_;
    std::vector<fs::path> bundles_;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    static const Paths paths;
    FuzzedDataProvider in(data, size);
    Run(in, paths).go();
    return 0;
}
