// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Everything at once, for the thread sanitizer to watch: a plain writer whose
// transactions spill, optimistic writers whose commits group, a thread that
// shrinks the file whenever it can, and readers checking every snapshot they
// take while holding slices into it across all of that.

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/optimistic.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

namespace {

constexpr int kKeys = 200;     // generation-stamped keys, all rewritten per round
constexpr int kBig = 6;        // overflow values among them
constexpr int kCounters = 3;   // one per optimistic thread, plus their total
constexpr int kIncrements = 150;

std::string stamp(int gen, std::size_t size)
{
    std::string v = std::to_string(gen) + ":";
    v.resize(size, char('a' + gen % 26));
    return v;
}

int genOf(Slice v)
{
    int g = 0;
    for (std::size_t i = 0; i < v.size() && v.chars()[i] != ':'; ++i)
        g = g * 10 + (v.chars()[i] - '0');
    return g;
}

long long number(const std::optional<Slice>& v)
{
    return v ? std::stoll(v->string()) : 0;
}

struct Failure
{
    std::mutex mtx;
    std::string text;
    std::atomic<bool> stop{false};

    void set(const std::string& what)
    {
        std::lock_guard<std::mutex> lk(mtx);
        if (text.empty())
            text = what;
        stop.store(true);
    }
};

/// One snapshot, checked: every stamped key is from one round, the counters
/// add up to their total, and none went back since this reader's last look.
void checkSnapshot(Env& env, std::vector<long long>& last, Failure& f)
{
    Txn t = env.readTxn();
    Db db = t.mainDb();
    const std::optional<Slice> first = db.get(tst::keyOf(0));
    const int gen = first ? genOf(*first) : 0;
    // Held across the scan, the other threads' commits and any shrink.
    const std::string held = first ? first->string() : std::string();
    for (int i = 0; i < kKeys; ++i) {
        const std::optional<Slice> v = db.get(tst::keyOf(i));
        if (bool(v) != bool(first) || (v && genOf(*v) != gen)) {
            f.set("torn snapshot at key " + std::to_string(i));
            return;
        }
    }
    long long sum = 0;
    for (int c = 0; c < kCounters; ++c) {
        const long long n = number(db.get(tst::keyOf(c, "count")));
        if (n < last[c]) {
            f.set("counter " + std::to_string(c) + " went back");
            return;
        }
        last[c] = n;
        sum += n;
    }
    if (sum != number(db.get("total")))
        f.set("counters do not add up to their total");
    std::this_thread::yield();
    if (first && first->string() != held)
        f.set("a held slice changed");
    checkIntegrity(t);
    t.abort();
}

void stress(const Env::Options& options)
{
    tst::Scratch scratch("stress");
    Env env = options.open(scratch.file());
    Failure f;
    std::atomic<int> optimistsLeft{kCounters};
    std::atomic<unsigned> snapshots{0};

    std::vector<std::thread> threads;
    for (int r = 0; r < 3; ++r)
        threads.emplace_back([&] {
            std::vector<long long> last(kCounters, 0);
            try {
                while (!f.stop.load()) {
                    checkSnapshot(env, last, f);
                    snapshots.fetch_add(1);
                }
            } catch (const std::exception& ex) {
                f.set(std::string("reader: ") + ex.what());
            }
        });
    for (int c = 0; c < kCounters; ++c)
        threads.emplace_back([&, c] {
            try {
                for (int i = 0; i < kIncrements && !f.stop.load(); ++i)
                    optimistic(env, [&](OptimisticTxn& t) {
                        const std::string key = tst::keyOf(c, "count");
                        t.put("", key, std::to_string(number(t.get("", key)) + 1));
                        t.put("", "total", std::to_string(number(t.get("", "total")) + 1));
                    });
            } catch (const std::exception& ex) {
                f.set(std::string("optimistic writer: ") + ex.what());
            }
            optimistsLeft.fetch_sub(1);
        });
    threads.emplace_back([&] {
        try {
            while (!f.stop.load()) {
                (void)env.shrink();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        } catch (const std::exception& ex) {
            f.set(std::string("shrink: ") + ex.what());
        }
    });

    // The plain writer: each round rewrites every stamped key, some of them
    // pages long, in one transaction many times the dirty limit, and every
    // other round frees a tail of filler for the shrinker to cut.
    try {
        for (int gen = 1; !f.stop.load() && (gen <= 24 || optimistsLeft.load() > 0); ++gen) {
            env.write([&](Txn& t) {
                Db db = t.mainDb();
                for (int i = 0; i < kKeys; ++i)
                    db.put(tst::keyOf(i), stamp(gen, i < kBig ? 3 * 4096 + 100 : 300));
                for (int i = 0; i < 40; ++i) {
                    if (gen % 2)
                        db.put(tst::keyOf(i, "fill"), stamp(gen, 2000));
                    else
                        db.erase(tst::keyOf(i, "fill"));
                }
            });
        }
    } catch (const std::exception& ex) {
        f.set(std::string("writer: ") + ex.what());
    }
    while (optimistsLeft.load() > 0 && !f.stop.load())
        std::this_thread::yield();
    f.stop.store(true);
    for (auto& th : threads)
        th.join();

    if (!f.text.empty())
        std::printf("        %s\n", f.text.c_str());
    CHECK(f.text.empty());
    CHECK(snapshots.load() > 0);
    env.read([&](Txn& t) {
        checkIntegrity(t);
        CHECK_EQ(number(t.mainDb().get("total")), (long long)(kCounters * kIncrements));
    });
    const EnvStats st = env.stats();
    CHECK(st.spilledPages > 0);
    CHECK_EQ(st.dirtyBytes, std::uint64_t(0));
}

}  // namespace

TEST(readersHoldUpUnderSpillsGroupCommitsAndShrinks)
{
    stress(Env::configure().sync(Durability::None).dirtyLimit(64u << 10));
}

TEST(pipelinedGroupCommitsUnderSpillsAndShrinks)
{
    stress(Env::configure().sync(Durability::Safe).pipelineCommits().dirtyLimit(64u << 10));
}

TEST(noMetaSyncUnderSpillsAndShrinks)
{
    stress(Env::configure().sync(Durability::NoMetaSync).dirtyLimit(64u << 10));
}

int main()
{
    return tst::runAll("stress");
}
