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
/// commit made since its snapshot, and if none wrote a key it read or wrote,
/// replays the buffered writes and commits them. Other writers can therefore
/// run their bodies while this one runs its own.
///
/// Commits are grouped. Optimistic commits that arrive while one is being
/// applied queue up, and the next of them to run applies the whole queue in
/// one write transaction: one commit, and under Durability::Safe one pair of
/// device flushes, for the lot. Each transaction in a group is still
/// validated on its own and succeeds or fails on its own.
///
/// Validation is on individual keys, so a committed history is serializable
/// for point operations: a read, or a write, conflicts with any later commit
/// that wrote the same (sub-database, key). Range scans are not tracked, so a
/// phantom insert into a scanned range is not detected; use Env::writeTxn for
/// transactions that depend on a range. A plain Env::writeTxn commit names no
/// keys, so it conflicts with every optimistic transaction that began before it.
///
/// A transaction's buffers are recycled per thread, so a steady stream of
/// small transactions allocates nothing once warm (values returned by get()
/// that do not fit a std::string's inline buffer aside; view() avoids those).
class OptimisticTxn
{
public:
    /// `attempt` is how many times this transaction has already lost a
    /// conflict; optimistic() passes it, and a caller retrying by hand should
    /// too. Within a group the leader validates higher attempts first. From
    /// kExclusiveAfter attempts on, the transaction also holds back every other
    /// optimistic commit from its begin until it finishes, so no other
    /// optimistic transaction can win its keys again: a hot key cannot starve
    /// it. Their bodies keep running; only their commits wait. Plain write
    /// transactions are not held back. Such a transaction must not commit a
    /// second optimistic transaction on the same store before it finishes.
    explicit OptimisticTxn(Env& env, unsigned attempt = 0);

    static constexpr unsigned kExclusiveAfter = 8;
    ~OptimisticTxn();  // aborts if not committed
    OptimisticTxn(OptimisticTxn&&) noexcept;
    OptimisticTxn& operator=(OptimisticTxn&&) noexcept;
    OptimisticTxn(const OptimisticTxn&) = delete;
    OptimisticTxn& operator=(const OptimisticTxn&) = delete;

    /// The value of `key` in sub-database `db` (empty `db` is the main one), as
    /// this transaction sees it: its own buffered writes first, then the
    /// snapshot. The read is validated at commit.
    std::optional<std::string> get(Slice db, Slice key);

    /// Like get(), without copying. The slice stays valid until the next put(),
    /// erase(), commit() or abort() on this transaction.
    std::optional<Slice> view(Slice db, Slice key);

    /// Buffer a write. A sub-database that does not exist yet is created at
    /// commit. Keys and values outside the store's limits are refused here,
    /// with the errors Db::put would raise.
    void put(Slice db, Slice key, Slice value);

    /// Buffer a delete. Deleting an absent key is not an error.
    void erase(Slice db, Slice key);

    /// Validate and apply. Throws Error(ErrorCode::Conflict) if a later commit
    /// wrote a key this transaction read or wrote; nothing was applied then.
    /// Any other error applying this transaction's writes is thrown here too,
    /// and does not affect the other transactions committed with it. Either
    /// way the transaction is finished.
    void commit();

    /// Discard the buffered writes and release the snapshot.
    void abort();

    struct Impl;  // public so the group-commit leader can apply other threads' writes

private:
    Impl& active();  // throws BadTransaction once finished
    void finish() noexcept;
    /// Applies the group in env.occBatch as the group-commit leader. Outcomes go
    /// into each member's pending record; it never throws.
    static void applyBatch(internal::EnvImpl& env) noexcept;
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
        OptimisticTxn t(env, attempt - 1);
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
