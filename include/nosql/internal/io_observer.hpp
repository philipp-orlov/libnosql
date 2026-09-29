// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <functional>
#include <utility>

namespace nosql::internal::os {

enum class IoEvent { Read, Write, WriteProgress, Resize, Sync, DataBarrier, MetaBarrier, Replace, Replaced, Clone, Reserve };
using IoCallback = std::function<void(IoEvent, std::size_t)>;
inline thread_local IoCallback ioCallback;

inline void observeIo(IoEvent event, std::size_t bytes = 0)
{
    if (ioCallback)
        ioCallback(event, bytes);
}

class ScopedIoObserver
{
public:
    explicit ScopedIoObserver(IoCallback callback) : previous_(std::move(ioCallback))
    {
        ioCallback = std::move(callback);
    }
    ~ScopedIoObserver() { ioCallback = std::move(previous_); }
    ScopedIoObserver(const ScopedIoObserver&) = delete;
    ScopedIoObserver& operator=(const ScopedIoObserver&) = delete;

private:
    IoCallback previous_;
};

}  // namespace nosql::internal::os