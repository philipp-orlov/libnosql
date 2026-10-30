// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace nosql::internal {

/// The one-writer-at-a-time slot, which no waiter waits for without bound.
///
/// A plain mutex lets the thread that releases it take it straight back: a
/// writer committing in a loop then holds the store for as long as it likes,
/// and the others (an optimistic group's leader among them) wait without
/// bound. Handing the slot to the longest waiting at every release cures that
/// and costs a wakeup per commit, a third of the throughput of writers
/// taking turns. So a release wakes the longest waiting to race for the slot,
/// and once it has lost kPasses races the next release hands the slot to it.
/// A waiter so waits for at most kPasses + 1 commits beyond those of the
/// writers ahead of it, for a tenth of the throughput or less. Free, taking
/// the slot costs one uncontended mutex, as before; each waiter is a node on
/// its own stack, so nothing allocates.
///
/// Not owned by a thread: any thread may release it.
class WriterSlot
{
public:
    static constexpr unsigned kPasses = 16;

    void lock()
    {
        std::unique_lock<std::mutex> lk(mtx_);
        if (!held_) {
            held_ = true;
            return;
        }
        Waiter w;
        enqueue(w);
        for (;;) {
            w.cv.wait(lk, [&] { return w.granted || !held_; });
            if (take(w))
                return;
        }
    }

    /// False if the slot was not taken within `timeout`.
    template <class Rep, class Period>
    bool try_lock_for(const std::chrono::duration<Rep, Period>& timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> lk(mtx_);
        if (!held_) {
            held_ = true;
            return true;
        }
        Waiter w;
        enqueue(w);
        for (;;) {
            if (!w.cv.wait_until(lk, deadline, [&] { return w.granted || !held_; })) {
                dequeue(w);
                // A release may have woken this one alone, to race for a slot
                // that is still free: the next in line races instead.
                if (!held_ && head_)
                    head_->cv.notify_one();
                return false;
            }
            if (take(w))
                return true;
        }
    }

    void unlock()
    {
        std::lock_guard<std::mutex> lk(mtx_);
        Waiter* w = head_;
        if (!w) {
            held_ = false;
            return;
        }
        if (w->passes >= kPasses) {
            // It has lost enough races: it gets the slot without one.
            dequeue(*w);
            w->granted = true;
        } else {
            held_ = false;
            ++w->passes;
        }
        w->cv.notify_one();  // under the lock: `w` lives until it looks
    }

private:
    struct Waiter
    {
        std::condition_variable cv;
        Waiter* prev = nullptr;
        Waiter* next = nullptr;
        unsigned passes = 0;  // releases it was woken for and did not get
        bool granted = false;
    };

    /// After a wakeup: whether `w` holds the slot now.
    bool take(Waiter& w) noexcept
    {
        if (w.granted)
            return true;  // handed over, already off the queue
        if (held_)
            return false;  // lost the race
        held_ = true;
        dequeue(w);
        return true;
    }

    void enqueue(Waiter& w) noexcept
    {
        w.prev = tail_;
        (tail_ ? tail_->next : head_) = &w;
        tail_ = &w;
    }
    void dequeue(Waiter& w) noexcept
    {
        (w.prev ? w.prev->next : head_) = w.next;
        (w.next ? w.next->prev : tail_) = w.prev;
        w.prev = w.next = nullptr;
    }

    std::mutex mtx_;
    Waiter* head_ = nullptr;
    Waiter* tail_ = nullptr;
    bool held_ = false;
};

}  // namespace nosql::internal
