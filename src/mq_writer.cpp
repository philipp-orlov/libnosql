// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/internal/mq_writer.hpp"

#include <algorithm>

namespace nosql::internal {

MqWriter::MqWriter(CommitFn commit, std::size_t maxBatch, std::chrono::microseconds linger,
                   std::size_t byteLimit)
    : commit_(std::move(commit)), maxBatch_(maxBatch ? maxBatch : 1), linger_(linger), byteLimit_(byteLimit)
{
    thread_ = std::thread([this] { run(); });
}

MqWriter::~MqWriter()
{
    {
        std::lock_guard<std::mutex> lk(mtx_);
        stop_ = true;
    }
    work_.notify_all();
    if (thread_.joinable())
        thread_.join();
}

std::uint64_t MqWriter::submit(std::string body, std::string correlationId, bool external)
{
    auto item = std::make_shared<Item>();
    item->body = std::move(body);
    item->correlationId = std::move(correlationId);
    item->external = external;

    {
        std::unique_lock<std::mutex> lk(mtx_);
        if (stop_)
            throw Error(ErrorCode::BadTransaction, "queue writer is shutting down");
        const auto bytes = item->body.size() + item->correlationId.size();
        if (bytes > byteLimit_ || pendingBytes_ > byteLimit_ - bytes)
            throw Error(ErrorCode::Busy, "queue intake byte limit exceeded");
        intake_.push_back(item);
        pendingBytes_ += bytes;
        work_.notify_one();
        finished_.wait(lk, [&] { return item->done; });
    }
    if (item->err)
        std::rethrow_exception(item->err);
    return item->sequence;
}

void MqWriter::run()
{
    std::vector<std::shared_ptr<Item>> batch;
    for (;;) {
        {
            std::unique_lock<std::mutex> lk(mtx_);
            work_.wait(lk, [&] { return stop_ || !intake_.empty(); });
            if (intake_.empty()) {
                if (stop_)
                    return;
                continue;
            }
            // Linger only while the batch is still small and more may be
            // coming; a full batch or a shutdown goes straight out.
            if (intake_.size() < maxBatch_ && !stop_ && linger_.count() > 0)
                work_.wait_for(lk, linger_, [&] { return stop_ || intake_.size() >= maxBatch_; });

            const std::size_t take = std::min(maxBatch_, intake_.size());
            batch.assign(intake_.begin(), intake_.begin() + std::ptrdiff_t(take));
            intake_.erase(intake_.begin(), intake_.begin() + std::ptrdiff_t(take));
        }

        std::exception_ptr err;
        try {
            commit_(batch);
        } catch (...) {
            err = std::current_exception();
        }

        {
            std::lock_guard<std::mutex> lk(mtx_);
            for (auto& item : batch) {
                pendingBytes_ -= item->body.size() + item->correlationId.size();
                item->err = err;
                item->done = true;
            }
        }
        finished_.notify_all();
        batch.clear();
    }
}

}  // namespace nosql::internal
