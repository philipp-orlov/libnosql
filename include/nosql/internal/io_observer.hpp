// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <functional>
#include <utility>

namespace nosql::internal::os {

enum class IoEvent { Read, Write, WriteProgress, Resize, Sync, DataBarrier, MetaBarrier, Replace, Replaced, Clone, Reserve };
using IoCallback = std::function<void(IoEvent, std::size_t)>;
/// This thread's observer. A function rather than an inline thread_local
/// variable: MinGW emits that one's TLS initialiser in every object file
/// that uses it, and the link then fails on duplicates.
inline IoCallback& ioCallback() noexcept
{
    thread_local IoCallback callback;
    return callback;
}

inline void observeIo(IoEvent event, std::size_t bytes = 0)
{
    if (IoCallback& callback = ioCallback())
        callback(event, bytes);
}

class ScopedIoObserver
{
public:
    explicit ScopedIoObserver(IoCallback callback) : previous_(std::move(ioCallback()))
    {
        ioCallback() = std::move(callback);
    }
    ~ScopedIoObserver() { ioCallback() = std::move(previous_); }
    ScopedIoObserver(const ScopedIoObserver&) = delete;
    ScopedIoObserver& operator=(const ScopedIoObserver&) = delete;

private:
    IoCallback previous_;
};

}  // namespace nosql::internal::os