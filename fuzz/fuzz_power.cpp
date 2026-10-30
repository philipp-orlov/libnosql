// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// libFuzzer target: power cuts at every point of a workload the input picks.
// The input chooses a durability, pipelined commits, a dirty limit small
// enough to spill and the flags of a few trees, then a run of write
// transactions (puts from a few bytes to several pages, erases, nested
// children committed or aborted, whole transactions aborted), syncs and
// shrinks. Every write, resize and flush is recorded, and the record is
// replayed into the device model of the power suite: at each point several
// images keep what was flushed plus different subsets of what was not. Each
// must open, pass checkIntegrity and hold exactly a committed snapshot no
// older than the newest one the store had reported durable.
//
//   cmake -S . -B build-fuzz -DCMAKE_CXX_COMPILER=clang++ -DNOSQL_BUILD_FUZZERS=ON
//   cmake --build build-fuzz --target fuzz_power
//   build-fuzz/fuzz/fuzz_power -max_total_time=600

#include <fuzzer/FuzzedDataProvider.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "tests/power_model.hpp"

namespace {

using State = std::map<std::string, std::string>;  // "tree/key" -> value
namespace os = nosql::internal::os;
namespace power = tst::power;

[[noreturn]] void fail(const std::string& what)
{
    std::fprintf(stderr, "power cut: %s\n", what.c_str());
    std::abort();
}

struct Paths
{
    std::filesystem::path dir, file, cut;
    Paths()
    {
        const std::filesystem::path shm = "/dev/shm";
        const auto base = std::filesystem::exists(shm) ? shm : std::filesystem::temp_directory_path();
        dir = base / ("nosql-fuzz-power-" + std::to_string(::getpid()));
        std::filesystem::create_directories(dir);
        file = dir / "s.db";
        cut = dir / "cut.db";
    }
    ~Paths() { std::filesystem::remove_all(dir); }
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

const char* const kTrees[] = {"", "plain", "prefixed", "reversed"};
const nosql::DbFlags kFlags[] = {nosql::DbFlags::None, nosql::DbFlags::None,
                                 nosql::DbFlags::PrefixKeys, nosql::DbFlags::ReverseKey};

State contentsOf(const std::filesystem::path& file, std::uint64_t& txnid)
{
    nosql::Env env = nosql::Env::configure().readOnly().open(file);
    State state;
    env.read([&](nosql::Txn& txn) {
        nosql::checkIntegrity(txn);
        txnid = txn.id();
        for (auto [key, value] : txn.mainDb().all())
            state[std::string("/") + key.string()] = value.string();
        for (const std::string& name : txn.listDbs())
            for (auto [key, value] : txn.db(name).all())
                state[name + "/" + key.string()] = value.string();
    });
    return state;
}

class Run
{
public:
    Run(FuzzedDataProvider& in, const Paths& p) : in_(in), p_(p) {}

    void go()
    {
        const nosql::Durability modes[] = {nosql::Durability::Safe, nosql::Durability::NoMetaSync,
                                           nosql::Durability::Lazy, nosql::Durability::None};
        mode_ = modes[in_.ConsumeIntegralInRange<int>(0, 3)];
        pipelined_ = mode_ == nosql::Durability::Safe && in_.ConsumeBool();
        spill_ = in_.ConsumeBool();
        trees_ = in_.ConsumeIntegralInRange<unsigned>(1, 4);

        std::filesystem::remove(p_.file);
        {
            nosql::Env env = options().open(p_.file);
            env.write([&](nosql::Txn& t) {
                for (unsigned i = 1; i < trees_; ++i)
                    t.db(kTrees[i], kFlags[i] | nosql::DbFlags::Create);
            });
            env.sync();
        }
        const std::string base = readFile(p_.file);

        std::map<std::uint64_t, State> committed;
        std::vector<power::Op> log;
        {
            power::Recorder recorder;
            {
                nosql::Env env = options().open(p_.file);
                committed[env.commitGeneration()] = model_;
                recorder.durable(env.stats().durableTxn);
                for (int step = 0; step < 24 && in_.remaining_bytes() > 0; ++step) {
                    switch (in_.ConsumeIntegralInRange<int>(0, 7)) {
                        case 0: env.sync(); break;
                        case 1: {  // pages moved down and the file cut short
                            const std::uint64_t from = env.commitGeneration();
                            (void)env.shrink();
                            for (std::uint64_t g = from + 1; g <= env.commitGeneration(); ++g)
                                committed[g] = model_;
                            break;
                        }
                        default: transaction(env); break;
                    }
                    committed[env.commitGeneration()] = model_;
                    recorder.durable(env.stats().durableTxn);
                }
            }
            if (recorder.foreign())
                fail("a second file was written");
            log = recorder.take();
        }
        replay(base, log, committed);
    }

private:
    nosql::Env::Options options() const
    {
        return nosql::Env::configure()
            .pageSize(power::kPage)
            .sync(mode_)
            .pipelineCommits(pipelined_)
            .dirtyLimit(spill_ ? 48u << 10 : 0)
            .syncInterval(std::chrono::hours(1));
    }

    std::string value()
    {
        const auto n = in_.ConsumeIntegralInRange<int>(0, 15) == 0
                           ? in_.ConsumeIntegralInRange<std::size_t>(power::kPage, 4 * power::kPage)
                           : in_.ConsumeIntegralInRange<std::size_t>(0, 300);
        std::string v(n, '\0');
        std::uint64_t x = in_.ConsumeIntegral<std::uint32_t>() | 1;
        for (char& c : v)
            c = char('a' + (x = x * 6364136223846793005ull + 1442695040888963407ull) % 26);
        return v;
    }

    /// Writes into `t` and `model`, which the caller keeps or drops with it.
    void writes(nosql::Txn& t, State& model, int n)
    {
        for (int i = 0; i < n && in_.remaining_bytes() > 0; ++i) {
            const unsigned tree = in_.ConsumeIntegralInRange<unsigned>(0, trees_ - 1);
            nosql::Db db = tree ? t.db(kTrees[tree]) : t.mainDb();
            const std::string key =
                "k" + std::to_string(in_.ConsumeIntegralInRange<unsigned>(0, 400));
            const std::string at = std::string(kTrees[tree]) + "/" + key;
            if (in_.ConsumeIntegralInRange<int>(0, 3) == 0) {
                if (model.erase(at))
                    db.erase(key);
            } else {
                const std::string v = value();
                db.put(key, v);
                model[at] = v;
            }
        }
    }

    void transaction(nosql::Env& env)
    {
        nosql::Txn t = env.writeTxn();
        State model = model_;
        writes(t, model, in_.ConsumeIntegralInRange<int>(1, 120));
        if (in_.ConsumeBool()) {
            nosql::Txn child = t.nested();
            State inner = model;
            writes(child, inner, in_.ConsumeIntegralInRange<int>(1, 60));
            if (in_.ConsumeBool()) {
                child.commit();
                model = std::move(inner);
            } else {
                child.abort();
            }
            writes(t, model, in_.ConsumeIntegralInRange<int>(0, 20));
        }
        if (in_.ConsumeIntegralInRange<int>(0, 7) == 0) {
            t.abort();
            return;
        }
        t.commit();
        model_ = std::move(model);
    }

    void replay(const std::string& base, const std::vector<power::Op>& log,
                const std::map<std::uint64_t, State>& committed)
    {
        using Lose = power::Device::Lose;
        power::Device device(base);
        std::uint64_t durable = 0;
        std::mt19937_64 rng(log.size());
        for (std::size_t at = 0; at <= log.size(); ++at) {
            if (at) {
                if (!device.apply(log[at - 1]))
                    fail("the record missed a write");
                if (log[at - 1].kind == power::Op::Durable)
                    durable = log[at - 1].offset;
            }
            // Writes come in runs; between two of them nothing new can be lost.
            if (at && at < log.size() && log[at].kind == power::Op::Write &&
                log[at - 1].kind == power::Op::Write)
                continue;
            std::vector<Lose> ways = {Lose::All, Lose::None};
            if (!device.settled())
                ways.insert(ways.end(), {Lose::AnySector, Lose::AnySector, Lose::AnySector,
                                         Lose::WholePages, Lose::WholePages});
            for (const Lose lose : ways) {
                writeFile(p_.cut, device.image(lose, rng));
                std::string why;
                try {
                    std::uint64_t txnid = 0;
                    const State state = contentsOf(p_.cut, txnid);
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
                if (!why.empty())
                    fail("after op " + std::to_string(at) + " of " + std::to_string(log.size()) +
                         ", losing way " + std::to_string(int(lose)) + ": " + why);
            }
        }
    }

    FuzzedDataProvider& in_;
    const Paths& p_;
    nosql::Durability mode_ = nosql::Durability::Safe;
    bool pipelined_ = false, spill_ = false;
    unsigned trees_ = 1;
    State model_;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    static const Paths paths;
    FuzzedDataProvider in(data, size);
    Run(in, paths).go();
    return 0;
}
