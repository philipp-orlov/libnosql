// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/optimistic.hpp"

#include <algorithm>
#include <map>
#include <utility>
#include <vector>

#include "nosql/internal/core.hpp"
#include "nosql/internal/occ_log.hpp"

namespace nosql {

struct OptimisticTxn::Impl
{
    Impl(Env e, Txn snap) : env(std::move(e)), snapshot(std::move(snap)) {}

    Env env;
    /// Pins the snapshot every read comes from, and its reader slot.
    Txn snapshot;
    /// Final value per (sub-database, key); nullopt is an erase.
    std::map<std::pair<std::string, std::string>, std::optional<std::string>> writes;
    /// Hashes of every key read or written, unsorted until commit.
    std::vector<std::uint64_t> touched;
};

OptimisticTxn::OptimisticTxn(Env& env) : impl_(std::make_unique<Impl>(env.share(), env.readTxn()))
{}

OptimisticTxn::~OptimisticTxn() = default;
OptimisticTxn::OptimisticTxn(OptimisticTxn&&) noexcept = default;
OptimisticTxn& OptimisticTxn::operator=(OptimisticTxn&&) noexcept = default;

OptimisticTxn::Impl& OptimisticTxn::active()
{
    if (!impl_)
        throw Error(ErrorCode::BadTransaction, "optimistic transaction already finished");
    return *impl_;
}

std::optional<std::string> OptimisticTxn::get(Slice db, Slice key)
{
    Impl& t = active();
    const std::string name = db.string();
    const std::string k = key.string();
    t.touched.push_back(internal::occKey(name, k));
    if (auto it = t.writes.find({name, k}); it != t.writes.end())
        return it->second;
    if (!name.empty() && !t.snapshot.hasDb(name))
        return std::nullopt;
    const Db d = t.snapshot.db(name);
    const std::optional<Slice> v = d.get(key);
    if (!v)
        return std::nullopt;
    return v->string();
}

void OptimisticTxn::put(Slice db, Slice key, Slice value)
{
    Impl& t = active();
    std::string name = db.string();
    std::string k = key.string();
    t.touched.push_back(internal::occKey(name, k));
    t.writes[{std::move(name), std::move(k)}] = value.string();
}

void OptimisticTxn::erase(Slice db, Slice key)
{
    Impl& t = active();
    std::string name = db.string();
    std::string k = key.string();
    t.touched.push_back(internal::occKey(name, k));
    t.writes[{std::move(name), std::move(k)}] = std::nullopt;
}

void OptimisticTxn::commit()
{
    Impl& t = active();
    std::vector<std::uint64_t> keys = std::move(t.touched);
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

    internal::EnvImpl& e = internal::openEnv(t.env.impl_);
    // Validation and replay happen under the writer slot, so nothing commits
    // between the check and the publish. The body ran without it.
    Txn w = t.env.writeTxn();
    const bool conflict = e.occLog.conflicts(t.snapshot.id(), keys);
    if (!conflict && !t.writes.empty()) {
        for (const auto& [where, value] : t.writes) {
            const std::string& name = where.first;
            if (value) {
                Db d = name.empty() ? w.mainDb() : w.db(name, DbFlags::Create);
                d.put(where.second, *value);
            } else if (name.empty() || w.hasDb(name)) {
                Db d = w.db(name);
                d.erase(where.second);
            }
        }
        // Publish records the keys this commit wrote, not "unknown".
        e.occNext = &keys;
        try {
            w.commit();
        } catch (...) {
            e.occNext = nullptr;
            impl_.reset();
            throw;
        }
    }
    impl_.reset();  // release the snapshot and reader slot before reporting
    if (conflict)
        throw Error(ErrorCode::Conflict,
                    "a commit after this transaction's snapshot touched a key it used");
}

void OptimisticTxn::abort()
{
    impl_.reset();
}

}  // namespace nosql
