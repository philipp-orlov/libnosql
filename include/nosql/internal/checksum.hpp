// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

// XXH3-64 lives in libcore's libbase (moved unchanged from this library, with its golden vectors); the old
// names stay.
#include <cstddef>
#include <cstdint>

#include "base/xxh3.hpp"

namespace nosql::internal {

inline std::uint64_t checksum64(const void* data, std::size_t size) noexcept { return base::xxh3_64(data, size); }
inline const char* checksumBackend() noexcept { return base::xxh3Backend(); }
using Checksum64 = base::Xxh3State;

}  // namespace nosql::internal
