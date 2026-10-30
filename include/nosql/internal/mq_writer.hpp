// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Group commit for the message queue: many sends, one transaction, one fsync.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "nosql/error.hpp"

namespace nosql::internal {

/// One dedicated writer thread drains an intake queue and commits everything
/// it finds in a single transaction, then releases the producers waiting on
/// it. The store has one writer anyway, so batching costs no concurrency and
/// amortises the per-commit fsync across the whole batch.
class MqWriter
{
public:
    struct Item
    {
        std::string body;
        std::string correlationId;
        bool external = false;
        std::uint64_t sequence = 0;  ///< filled in by the commit callback
        std::exception_ptr err;
        bool done = false;
    };
    /// Writes the batch inside one transaction and commits it. Setting each
    /// item's `sequence` is the callback's job; throwing fails the whole batch.
    using CommitFn = std::function<void(const std::vector<std::shared_ptr<Item>>&)>;

    MqWriter(CommitFn commit, std::size_t maxBatch, std::chrono::microseconds linger,
             std::size_t byteLimit);
    ~MqWriter();

    MqWriter(const MqWriter&) = delete;
    MqWriter& operator=(const MqWriter&) = delete;

    /// Blocks until the batch containing this message is durable. Rethrows
    /// whatever the batch's commit threw.
    std::uint64_t submit(std::string body, std::string correlationId, bool external);

private:
    void run();

    CommitFn commit_;
    std::size_t maxBatch_;
    std::chrono::microseconds linger_;
    std::size_t byteLimit_, pendingBytes_ = 0;

    std::mutex mtx_;
    std::condition_variable work_;      ///< the writer waits here
    std::condition_variable finished_;  ///< producers wait here
    std::deque<std::shared_ptr<Item>> intake_;
    bool stop_ = false;
    std::thread thread_;
};

}  // namespace nosql::internal
