// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

// The I/O observer moved to libsys with the code it instruments.
#include "sys/io_observer.hpp"

namespace nosql::internal::os {
using sys::IoCallback;
using sys::ioCallback;
using sys::IoEvent;
using sys::observeIo;
using sys::ScopedIoObserver;
}  // namespace nosql::internal::os
