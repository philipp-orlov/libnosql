// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// libFuzzer target: the store against a model. The input picks a page size, a
// durability, a dirty limit and the flags of a few trees, then a sequence of
// writes, nested transactions, aborts, commits, cursor walks that write as
// they go, reads from an older snapshot, shrinks, reopens and compactions.
// Every read is checked against std::map; a mismatch, an Error the operation
// does not allow, a sanitizer report or a crash is a bug.
//
//   cmake -S . -B build-fuzz -DCMAKE_CXX_COMPILER=clang++ -DNOSQL_BUILD_FUZZERS=ON
//   cmake --build build-fuzz --target fuzz_ops
//   build-fuzz/fuzz/fuzz_ops -max_total_time=600

#include <fuzzer/FuzzedDataProvider.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/optimistic.hpp"

namespace {

using Map = std::map<std::string, std::string>;

[[noreturn]] void mismatch(const char* what, int line)
{
    std::fprintf(stderr, "model mismatch at line %d: %s\n", line, what);
    std::abort();
}
#define CHECK(c)                     \
    do {                             \
        if (!(c))                    \
            mismatch(#c, __LINE__);  \
    } while (0)

enum class Order { Bytes, Reverse, Integer };

struct TreeSpec
{
    std::string name;  // empty: the main tree
    nosql::DbFlags flags = nosql::DbFlags::None;
    Order order = Order::Bytes;
};

// The model keys each tree by a form whose byte order is the tree's order.
std::string toModel(const TreeSpec& t, const std::string& k)
{
    switch (t.order) {
        case Order::Bytes: return k;
        case Order::Reverse: return std::string(k.rbegin(), k.rend());
        case Order::Integer: return std::string(k.rbegin(), k.rend());  // little-endian u64
    }
    return k;
}
std::string fromModel(const TreeSpec& t, const std::string& m)
{
    return t.order == Order::Bytes ? m : std::string(m.rbegin(), m.rend());
}
std::string str(nosql::Slice s)
{
    return std::string(s.chars(), s.size());
}

struct Paths
{
    std::filesystem::path dir, file, copy;
    Paths()
    {
        const std::filesystem::path shm = "/dev/shm";
        const auto base = std::filesystem::exists(shm) ? shm : std::filesystem::temp_directory_path();
        dir = base / ("nosql-fuzz-ops-" + std::to_string(::getpid()));
        std::filesystem::create_directories(dir);
        file = dir / "s.db";
        copy = dir / "c.db";
    }
    ~Paths() { std::filesystem::remove_all(dir); }
};

class Run
{
public:
    explicit Run(FuzzedDataProvider& in) : in_(in) {}

    void go(const Paths& p)
    {
        std::filesystem::remove(p.file);
        std::filesystem::remove(p.copy);
        file_ = p.file;
        copy_ = p.copy;
        const std::size_t sizes[] = {1024, 4096, 8192};
        pageSize_ = sizes[in_.ConsumeIntegralInRange<int>(0, 2)];
        const nosql::Durability modes[] = {nosql::Durability::None, nosql::Durability::Lazy,
                                           nosql::Durability::Safe, nosql::Durability::NoMetaSync};
        durability_ = modes[in_.ConsumeIntegralInRange<int>(0, 3)];
        // A small dirty limit makes big transactions spill.
        smallDirty_ = pageSize_ >= 4096 && in_.ConsumeBool();
        const unsigned trees = in_.ConsumeIntegralInRange<unsigned>(0, 3);
        specs_.push_back({});
        for (unsigned i = 0; i < trees; ++i) {
            TreeSpec s;
            s.name = "t" + std::to_string(i);
            switch (in_.ConsumeIntegralInRange<int>(0, 4)) {
                case 0: break;
                case 1: s.flags = nosql::DbFlags::PrefixKeys; break;
                case 2:
                    s.flags = nosql::DbFlags::ReverseKey;
                    s.order = Order::Reverse;
                    break;
                case 3:
                    s.flags = nosql::DbFlags::ReverseKey | nosql::DbFlags::PrefixKeys;
                    s.order = Order::Reverse;
                    break;
                case 4:
                    s.flags = nosql::DbFlags::IntegerKey;
                    s.order = Order::Integer;
                    break;
            }
            specs_.push_back(s);
        }
        committed_.assign(specs_.size(), Map());
        open();
        env_.write([&](nosql::Txn& t) {
            for (const TreeSpec& s : specs_)
                if (!s.name.empty())
                    t.db(s.name, s.flags | nosql::DbFlags::Create);
        });

        for (int step = 0; step < 3000 && in_.remaining_bytes() > 0; ++step)
            op();
        while (!txns_.empty())
            commitTop();
        snapshot_.reset();
        verifyAll();
        check();
    }

private:
    void open()
    {
        auto o = nosql::Env::configure().pageSize(pageSize_).sync(durability_).maxDbs(8);
        if (smallDirty_)
            o.dirtyLimit(strictBudget() ? 512 * 1024 : 64 * 1024);
        env_ = o.open(file_);
        maxKey_ = env_.maxKeySize();
    }

    std::string key(const TreeSpec& s)
    {
        if (s.order == Order::Integer) {
            std::uint64_t v = in_.ConsumeIntegralInRange<std::uint64_t>(0, 600);
            if (in_.ConsumeIntegralInRange<int>(0, 15) == 0)
                v = in_.ConsumeIntegral<std::uint64_t>();
            std::string k(8, '\0');
            std::memcpy(k.data(), &v, 8);  // little-endian hosts only
            return k;
        }
        // Long shared stems on either end, so pages share prefixes and
        // suffixes; a small alphabet in between, so keys repeat.
        static const char* const stems[] = {"", "user/", "a", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz/",
                                            "\xff\xff", "com.example.www."};
        std::string k = stems[in_.ConsumeIntegralInRange<int>(0, 5)];
        const int mid = in_.ConsumeIntegralInRange<int>(1, in_.ConsumeBool() ? 3 : 24);
        for (int i = 0; i < mid; ++i)
            k.push_back(char('a' + in_.ConsumeIntegralInRange<int>(0, 5)));
        k += stems[in_.ConsumeIntegralInRange<int>(0, 5)];
        if (in_.ConsumeIntegralInRange<int>(0, 31) == 0)
            k.append(in_.ConsumeIntegralInRange<std::size_t>(0, maxKey_), 'q');
        if (k.size() > maxKey_)
            k.resize(maxKey_);
        return k;
    }

    std::string value()
    {
        std::size_t n;
        switch (in_.ConsumeIntegralInRange<int>(0, 9)) {
            case 0: n = in_.ConsumeIntegralInRange<std::size_t>(pageSize_ / 2, 6 * pageSize_); break;
            case 1: n = in_.ConsumeIntegralInRange<std::size_t>(100, pageSize_ / 2); break;
            default: n = in_.ConsumeIntegralInRange<std::size_t>(0, 40); break;
        }
        // Strict about the budget: no value whose buffer alone could not fit.
        if (smallDirty_ && strictBudget())
            n = std::min<std::size_t>(n, 64 * 1024 - 64);
        const char c = char(in_.ConsumeIntegral<std::uint8_t>());
        std::string v(n, c);
        for (std::size_t i = 0; i < n; i += 61)
            v[i] = char(i ^ std::size_t(c));
        return v;
    }

    Map& model(std::size_t tree) { return levels_.empty() ? committed_[tree] : levels_.back()[tree]; }

    nosql::Txn& writer()
    {
        if (txns_.empty()) {
            txns_.push_back(env_.writeTxn());
            levels_.push_back(committed_);
        }
        return txns_.back();
    }

    nosql::Db db(nosql::Txn& t, std::size_t tree)
    {
        return specs_[tree].name.empty() ? t.mainDb() : t.db(specs_[tree].name);
    }

    void commitTop()
    {
        txns_.back().commit();
        txns_.pop_back();
        if (levels_.size() >= 2)
            levels_[levels_.size() - 2] = levels_.back();
        else
            committed_ = levels_.back();
        levels_.pop_back();
    }

    void abortTop()
    {
        txns_.back().abort();
        txns_.pop_back();
        levels_.pop_back();
    }

    void endWrites()
    {
        while (!txns_.empty())
            commitTop();
    }

    // Only under a small dirty limit, where a write can need more than
    // spilling frees (branch pages stay). FUZZ_STRICT_BUDGET=1 makes it a
    // finding, to look at by hand: the budget is then 512 KiB and no value
    // takes more than an eighth of it, so dirty branch pages cannot fill it.
    static bool strictBudget()
    {
        static const bool strict = std::getenv("FUZZ_STRICT_BUDGET") != nullptr;
        return strict;
    }
    bool overBudget(const nosql::Error& e) const
    {
        return smallDirty_ && !strictBudget() && e.code() == nosql::ErrorCode::OutOfMemory;
    }

    // A write the store may refuse for want of memory (overBudget()): it
    // takes the whole transaction with it.
    template <class Fn>
    void write(Fn fn)
    {
        try {
            fn();
        } catch (const nosql::Error& e) {
            if (!overBudget(e))
                throw;
            while (!txns_.empty())
                abortTop();
        }
    }

    void op()
    {
        const std::size_t tree = in_.ConsumeIntegralInRange<std::size_t>(0, specs_.size() - 1);
        const TreeSpec& s = specs_[tree];
        switch (in_.ConsumeIntegralInRange<int>(0, 21)) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4: {  // put
                const std::string k = key(s), v = value();
                const int mode = in_.ConsumeIntegralInRange<int>(0, 5);
                write([&] {
                    nosql::Db d = db(writer(), tree);
                    Map& m = model(tree);
                    const bool present = m.count(toModel(s, k)) != 0;
                    if (mode == 4) {
                        CHECK(d.put(k, v, nosql::PutMode::InsertUnique) == !present);
                        if (!present)
                            m[toModel(s, k)] = v;
                    } else if (mode == 5) {
                        CHECK(d.put(k, v, nosql::PutMode::UpdateOnly) == present);
                        if (present)
                            m[toModel(s, k)] = v;
                    } else if (mode == 3) {
                        nosql::WritableSlice w = d.reserve(k, v.size());
                        std::memcpy(w.data(), v.data(), v.size());
                        m[toModel(s, k)] = v;
                    } else {
                        CHECK(d.put(k, v));
                        m[toModel(s, k)] = v;
                    }
                });
                break;
            }
            case 5:
            case 6: {  // erase
                const std::string k = key(s);
                write([&] {
                    nosql::Db d = db(writer(), tree);
                    CHECK(d.erase(k) == (model(tree).erase(toModel(s, k)) != 0));
                });
                break;
            }
            case 7: {  // get, in the writer or a fresh reader
                const std::string k = key(s);
                if (!txns_.empty() && in_.ConsumeBool()) {
                    checkGet(db(txns_.back(), tree), model(tree), s, k);
                } else {
                    nosql::Txn r = env_.readTxn();
                    checkGet(db(r, tree), committed_[tree], s, k);
                }
                break;
            }
            case 8:
                if (!txns_.empty())
                    commitTop();
                break;
            case 9:
                if (!txns_.empty())
                    abortTop();
                break;
            case 10:  // nested
                if (txns_.size() < 4) {
                    nosql::Txn child = writer().nested();
                    txns_.push_back(std::move(child));
                    levels_.push_back(levels_.back());
                }
                break;
            case 11: {  // seek and walk, both ways
                const std::string k = key(s);
                const bool inWriter = !txns_.empty() && in_.ConsumeBool();
                nosql::Txn r;
                if (!inWriter)
                    r = env_.readTxn();
                nosql::Txn& t = inWriter ? txns_.back() : r;
                walk(db(t, tree), inWriter ? model(tree) : committed_[tree], s, k);
                break;
            }
            case 12: {  // a cursor that erases and overwrites as it goes
                const std::string k = key(s);
                write([&] { cursorWrites(db(writer(), tree), model(tree), s, k); });
                break;
            }
            case 13: {  // countRange
                std::string lo = in_.ConsumeBool() ? key(s) : std::string();
                std::string hi = in_.ConsumeBool() ? key(s) : std::string();
                const bool inWriter = !txns_.empty();
                const Map& m = inWriter ? model(tree) : committed_[tree];
                nosql::Txn r;
                if (!inWriter)
                    r = env_.readTxn();
                nosql::Db d = db(inWriter ? txns_.back() : r, tree);
                auto b = lo.empty() ? m.begin() : m.lower_bound(toModel(s, lo));
                auto e = hi.empty() ? m.end() : m.lower_bound(toModel(s, hi));
                std::uint64_t want = 0;
                if (hi.empty() || lo.empty() || toModel(s, lo) < toModel(s, hi))
                    want = std::uint64_t(std::distance(b, e));
                CHECK(d.countRange(lo, hi) == want);
                CHECK(d.count() == m.size());
                break;
            }
            case 14:  // keep or drop an older snapshot
                if (snapshot_ && in_.ConsumeBool()) {
                    verifySnapshot();
                    snapshot_.reset();
                } else if (!snapshot_) {
                    snapshot_.emplace(env_.readTxn());
                    snapModel_ = committed_;
                } else {
                    verifySnapshot();
                }
                break;
            case 15: {  // shrink
                endWrites();
                if (in_.ConsumeBool() && snapshot_) {
                    verifySnapshot();
                    snapshot_.reset();
                }
                env_.shrink();
                if (snapshot_)
                    verifySnapshot();
                verifyAll();
                break;
            }
            case 16:  // reopen
                endWrites();
                snapshot_.reset();
                env_.close();
                open();
                verifyAll();
                break;
            case 17:  // compact and continue on the copy
                endWrites();
                snapshot_.reset();
                env_.close();
                nosql::compact(file_, copy_);
                std::filesystem::rename(copy_, file_);
                open();
                verifyAll();
                break;
            case 18:  // clear one tree
                write([&] {
                    db(writer(), tree).clear();
                    model(tree).clear();
                });
                break;
            case 19:
                if (txns_.empty())
                    check();
                else
                    env_.sync(in_.ConsumeBool());
                break;
            case 20:
                endWrites();
                optimistic();
                break;
            case 21: {  // Db::Range windows
                if (s.order == Order::Integer)
                    break;
                const bool inWriter = !txns_.empty() && in_.ConsumeBool();
                nosql::Txn r;
                if (!inWriter)
                    r = env_.readTxn();
                ranges(db(inWriter ? txns_.back() : r, tree), inWriter ? model(tree) : committed_[tree], s);
                break;
            }
        }
    }

    void ranges(nosql::Db d, const Map& m, const TreeSpec& s)
    {
        std::vector<std::pair<std::string, std::string>> got, want;
        const std::string lo = key(s), hi = key(s);
        switch (in_.ConsumeIntegralInRange<int>(0, 2)) {
            case 0: {  // a prefix in the tree's order: under ReverseKey, an ending
                std::string p = lo.substr(0, in_.ConsumeIntegralInRange<std::size_t>(0, lo.size()));
                if (s.order == Order::Reverse)
                    p = lo.substr(lo.size() - p.size());
                for (auto [k, v] : d.prefix(p))
                    got.emplace_back(str(k), str(v));
                for (const auto& [mk, v] : m) {
                    const std::string k = fromModel(s, mk);
                    const bool match = s.order == Order::Reverse
                                           ? k.size() >= p.size() && k.compare(k.size() - p.size(), p.size(), p) == 0
                                           : k.compare(0, p.size(), p) == 0;
                    if (match)
                        want.emplace_back(k, v);
                }
                break;
            }
            case 1: {
                for (auto [k, v] : d.between(lo, hi))
                    got.emplace_back(str(k), str(v));
                const std::string a = toModel(s, lo), b = toModel(s, hi);
                for (auto it = m.lower_bound(a); a < b && it != m.end() && it->first < b; ++it)
                    want.emplace_back(fromModel(s, it->first), it->second);
                break;
            }
            default:
                for (auto [k, v] : d.from(lo))
                    got.emplace_back(str(k), str(v));
                for (auto it = m.lower_bound(toModel(s, lo)); it != m.end(); ++it)
                    want.emplace_back(fromModel(s, it->first), it->second);
                break;
        }
        CHECK(got == want);
    }

    // An optimistic transaction, with plain commits landing between its
    // begin and its commit. It must conflict when one of them wrote a key it
    // read or wrote, or any key of a tree it scanned; it may conflict
    // otherwise. Whatever it read must be what its snapshot held.
    void optimistic()
    {
        const std::vector<Map> base = committed_;
        std::vector<std::map<std::string, std::optional<std::string>>> own(specs_.size());
        std::set<std::pair<std::size_t, std::string>> used, changed;  // model keys
        std::set<std::size_t> scanned;
        nosql::OptimisticTxn o(env_);
        auto seen = [&](std::size_t tree, const std::string& mk) -> std::optional<std::string> {
            const auto w = own[tree].find(mk);
            if (w != own[tree].end())
                return w->second;
            const auto it = base[tree].find(mk);
            return it == base[tree].end() ? std::nullopt : std::optional<std::string>(it->second);
        };
        const int steps = in_.ConsumeIntegralInRange<int>(1, 12);
        for (int i = 0; i < steps; ++i) {
            const std::size_t tree = in_.ConsumeIntegralInRange<std::size_t>(0, specs_.size() - 1);
            const TreeSpec& s = specs_[tree];
            const std::string k = key(s), mk = toModel(s, k);
            switch (in_.ConsumeIntegralInRange<int>(0, 4)) {
                case 0: {
                    const std::optional<std::string> got = o.get(s.name, k);
                    CHECK(got == seen(tree, mk));
                    used.insert({tree, mk});
                    break;
                }
                case 1: {
                    const std::string v = value();
                    o.put(s.name, k, v);
                    own[tree][mk] = v;
                    used.insert({tree, mk});
                    break;
                }
                case 2:
                    o.erase(s.name, k);
                    own[tree][mk] = std::nullopt;
                    used.insert({tree, mk});
                    break;
                case 3: {  // a scan of [k, hi)
                    const std::string hi = in_.ConsumeBool() ? key(s) : std::string();
                    Map want;
                    for (const auto& [m, v] : base[tree])
                        want[m] = v;
                    for (const auto& [m, v] : own[tree]) {
                        if (v)
                            want[m] = *v;
                        else
                            want.erase(m);
                    }
                    auto b = want.lower_bound(mk);
                    auto e = hi.empty() ? want.end() : want.lower_bound(toModel(s, hi));
                    Map got;
                    o.scan(s.name, k, hi, [&](nosql::Slice key, nosql::Slice v) {
                        got[toModel(s, str(key))] = str(v);
                    });
                    Map expect;
                    if (hi.empty() || mk < toModel(s, hi))
                        expect.insert(b, e);
                    CHECK(got == expect);
                    scanned.insert(tree);
                    break;
                }
                default: {  // a plain commit in between
                    Map after = committed_[tree];
                    std::set<std::pair<std::size_t, std::string>> wrote;
                    try {
                        nosql::Txn w = env_.writeTxn();
                        nosql::Db d = db(w, tree);
                        const int n = in_.ConsumeIntegralInRange<int>(1, 3);
                        for (int j = 0; j < n; ++j) {
                            const std::string wk = in_.ConsumeBool() ? k : key(s);
                            const std::string wm = toModel(s, wk);
                            if (in_.ConsumeBool()) {
                                const std::string v = value();
                                d.put(wk, v);
                                after[wm] = v;
                            } else if (d.erase(wk)) {
                                after.erase(wm);
                            } else {
                                continue;  // nothing changed
                            }
                            wrote.insert({tree, wm});
                        }
                        w.commit();
                    } catch (const nosql::Error& e) {
                        if (!overBudget(e))
                            throw;
                        break;
                    }
                    committed_[tree] = std::move(after);
                    changed.insert(wrote.begin(), wrote.end());
                    break;
                }
            }
        }
        bool mustConflict = false;
        for (const auto& c : changed)
            mustConflict = mustConflict || used.count(c) || scanned.count(c.first);
        try {
            o.commit();
        } catch (const nosql::Error& e) {
            if (overBudget(e))
                return;  // nothing applied
            if (e.code() != nosql::ErrorCode::Conflict)
                std::fprintf(stderr, "optimistic commit: %s (%d)\n", e.what(), int(e.code()));
            CHECK(e.code() == nosql::ErrorCode::Conflict);
            return;
        }
        CHECK(!mustConflict);
        for (std::size_t tree = 0; tree < specs_.size(); ++tree)
            for (const auto& [m, v] : own[tree]) {
                if (v)
                    committed_[tree][m] = *v;
                else
                    committed_[tree].erase(m);
            }
    }

    void checkGet(nosql::Db d, const Map& m, const TreeSpec& s, const std::string& k)
    {
        const auto it = m.find(toModel(s, k));
        const std::optional<nosql::Slice> got = d.get(k);
        CHECK(got.has_value() == (it != m.end()));
        if (got)
            CHECK(str(*got) == it->second);
        CHECK(d.contains(k) == (it != m.end()));
    }

    void walk(nosql::Db d, const Map& m, const TreeSpec& s, const std::string& k)
    {
        nosql::Cursor c = d.cursor();
        auto it = m.lower_bound(toModel(s, k));
        CHECK(c.seek(k) == (it != m.end()));
        const int steps = in_.ConsumeIntegralInRange<int>(0, 40);
        bool on = it != m.end();
        for (int i = 0; i < steps && on; ++i) {
            CHECK(str(c.key()) == fromModel(s, it->first));
            CHECK(str(c.value()) == it->second);
            if (in_.ConsumeBool()) {
                ++it;
                on = c.next();
                CHECK(on == (it != m.end()));
            } else if (it == m.begin()) {
                CHECK(!c.prev());
                on = false;
            } else {
                --it;
                CHECK(c.prev());
            }
        }
        if (in_.ConsumeIntegralInRange<int>(0, 3) == 0) {
            const std::uint64_t n = in_.ConsumeIntegralInRange<std::uint64_t>(0, 50);
            c = d.cursor();
            const bool first = c.first();
            CHECK(first == !m.empty());
            if (first) {
                const bool ok = c.skip(n);
                CHECK(ok == (n < m.size()));
                if (ok)
                    CHECK(str(c.key()) == fromModel(s, std::next(m.begin(), std::ptrdiff_t(n))->first));
            }
        }
    }

    void cursorWrites(nosql::Db d, Map& m, const TreeSpec& s, const std::string& k)
    {
        nosql::Cursor c = d.cursor();
        auto it = m.lower_bound(toModel(s, k));
        bool on = c.seek(k);
        CHECK(on == (it != m.end()));
        const int steps = in_.ConsumeIntegralInRange<int>(0, 30);
        for (int i = 0; i < steps && on; ++i) {
            CHECK(str(c.key()) == fromModel(s, it->first));
            switch (in_.ConsumeIntegralInRange<int>(0, 3)) {
                case 0:
                    it = m.erase(it);
                    on = c.erase();
                    CHECK(on == (it != m.end()));
                    break;
                case 1: {
                    const std::string v = value();
                    CHECK(c.put(fromModel(s, it->first), v));
                    it->second = v;
                    CHECK(str(c.key()) == fromModel(s, it->first));
                    CHECK(str(c.value()) == v);
                    break;
                }
                case 2: {  // a write elsewhere in the tree, then carry on
                    const std::string other = key(s), v = value();
                    CHECK(d.put(other, v));
                    const std::string at = it->first;
                    m[toModel(s, other)] = v;
                    it = m.find(at);
                    CHECK(str(c.key()) == fromModel(s, it->first));
                    break;
                }
                default:
                    ++it;
                    on = c.next();
                    CHECK(on == (it != m.end()));
                    break;
            }
        }
    }

    void verifyTree(nosql::Txn& t, std::size_t tree, const Map& m)
    {
        const TreeSpec& s = specs_[tree];
        nosql::Db d = db(t, tree);
        CHECK(d.count() == m.size());
        nosql::Cursor c = d.cursor();
        auto it = m.begin();
        for (bool ok = c.first(); ok; ok = c.next(), ++it) {
            CHECK(it != m.end());
            CHECK(str(c.key()) == fromModel(s, it->first));
            CHECK(str(c.value()) == it->second);
        }
        CHECK(it == m.end());
        auto rit = m.rbegin();
        for (bool ok = c.last(); ok; ok = c.prev(), ++rit) {
            CHECK(rit != m.rend());
            CHECK(str(c.key()) == fromModel(s, rit->first));
        }
        CHECK(rit == m.rend());
    }

    void verifyAll()
    {
        if (txns_.empty())
            CHECK(env_.stats().dirtyBytes == 0);
        nosql::Txn r = env_.readTxn();
        for (std::size_t i = 0; i < specs_.size(); ++i)
            verifyTree(r, i, committed_[i]);
    }

    void verifySnapshot()
    {
        for (std::size_t i = 0; i < specs_.size(); ++i)
            verifyTree(*snapshot_, i, snapModel_[i]);
    }

    void check()
    {
        nosql::Txn r = env_.readTxn();
        nosql::checkIntegrity(r);
        CHECK(env_.stats().dirtyBytes == 0);  // no writer: no dirty buffer either
    }

    FuzzedDataProvider& in_;
    std::filesystem::path file_, copy_;
    std::size_t pageSize_ = 4096, maxKey_ = 0;
    nosql::Durability durability_ = nosql::Durability::None;
    bool smallDirty_ = false;
    std::vector<TreeSpec> specs_;
    std::vector<Map> committed_;           // per tree
    std::vector<std::vector<Map>> levels_; // per open write transaction, per tree
    std::vector<nosql::Txn> txns_;         // the writer, then its nested children
    std::optional<nosql::Txn> snapshot_;
    std::vector<Map> snapModel_;
    nosql::Env env_;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    static Paths paths;
    FuzzedDataProvider in(data, size);
    Run(in).go(paths);
    return 0;
}
