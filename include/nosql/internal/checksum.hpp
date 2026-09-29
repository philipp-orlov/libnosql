// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace nosql::internal {

std::uint64_t checksum64(const void* data, std::size_t size) noexcept;
const char* checksumBackend() noexcept;

class Checksum64
{
public:
    Checksum64() noexcept;
    Checksum64(const Checksum64&) = delete;
    Checksum64& operator=(const Checksum64&) = delete;
    void reset() noexcept;
    void update(const void* data, std::size_t size) noexcept;
    std::uint64_t digest() const noexcept;

private:
    std::array<std::uint64_t, 8> accumulators_{};
    std::array<std::byte, 256> buffer_{};
    std::size_t bufferedBytes_ = 0;
    std::size_t stripesInBlock_ = 0;
    std::uint64_t length_ = 0;
};

}  // namespace nosql::internal
