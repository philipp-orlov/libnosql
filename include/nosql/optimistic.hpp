// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <optional>
#include <string>

#include "nosql/nosql.hpp"

namespace nosql {

/// A write transaction that does its work without holding the writer slot.
///
/// Reads come from a snapshot taken at begin, and writes are buffered. Only
/// commit() takes the writer slot: it validates the transaction against every
/// commit made since its snapshot, and if none touched a key it read or wrote,
/// replays the buffered writes and commits them. Other writers can therefore
/// run their bodies while this one runs its own.
///
/// Validation is on individual keys, so a committed history is serializable
/// for point operations: a read, or a write, conflicts with any later commit
/// that wrote the same (sub-database, key). Range scans are not tracked, so a
/// phantom insert into a scanned range is not detected; use Env::writeTxn for
/// transactions that depend on a range. A plain Env::writeTxn commit names no
/// keys, so it conflicts with every optimistic transaction that began before it.
///
/// Commits still run one at a time, and each one still pays the engine's
/// commit cost. The gain is in the work done between begin and commit.
class OptimisticTxn
{
public:
    explicit OptimisticTxn(Env& env);
    ~OptimisticTxn();  // aborts if not committed
    OptimisticTxn(OptimisticTxn&&) noexcept;
    OptimisticTxn& operator=(OptimisticTxn&&) noexcept;
    OptimisticTxn(const OptimisticTxn&) = delete;
    OptimisticTxn& operator=(const OptimisticTxn&) = delete;

    /// The value of `key` in sub-database `db` (empty `db` is the main one), as
    /// this transaction sees it: its own buffered writes first, then the
    /// snapshot. The read is validated at commit.
    std::optional<std::string> get(Slice db, Slice key);

    /// Buffer a write. A sub-database that does not exist yet is created at commit.
    void put(Slice db, Slice key, Slice value);

    /// Buffer a delete. Deleting an absent key is not an error.
    void erase(Slice db, Slice key);

    /// Validate and apply. Throws Error(ErrorCode::Conflict) if a later commit
    /// touched a key this transaction read or wrote; the transaction is then
    /// finished and nothing was applied. Either way the transaction is finished.
    void commit();

    /// Discard the buffered writes and release the snapshot.
    void abort();

private:
    struct Impl;
    Impl& active();  // throws BadTransaction once finished
    std::unique_ptr<Impl> impl_;
};

/// Runs `body(OptimisticTxn&)` and commits, starting over from the beginning
/// whenever the commit reports a conflict, up to `maxAttempts` times. The body
/// must be safe to run again: it sees a fresh snapshot each time. An exception
/// from the body aborts the attempt and propagates.
template <class F>
void optimistic(Env& env, F&& body, unsigned maxAttempts = 100)
{
    for (unsigned attempt = 1;; ++attempt) {
        OptimisticTxn t(env);
        body(t);
        try {
            t.commit();
            return;
        } catch (const Error& err) {
            if (err.code() != ErrorCode::Conflict || attempt >= maxAttempts)
                throw;
        }
    }
}

}  // namespace nosql
