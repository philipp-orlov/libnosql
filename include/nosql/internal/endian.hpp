// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace nosql::internal {

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
inline constexpr bool kLittleEndian = true;
#elif defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
inline constexpr bool kLittleEndian = false;
#elif defined(_WIN32)
inline constexpr bool kLittleEndian = true;
#else
#error "Unsupported native byte order"
#endif

template<class T> struct Little
{
    static_assert(std::is_unsigned_v<T>);
    T storage;
    constexpr Little() = default;
    constexpr Little(T value) noexcept : storage(convert(value)) {}
    constexpr operator T() const noexcept { return convert(storage); }
    constexpr Little& operator=(T value) noexcept { storage = convert(value); return *this; }
    constexpr Little& operator++() noexcept { return *this = T(*this) + 1; }
    constexpr T operator++(int) noexcept { T old = *this; ++*this; return old; }
    constexpr Little& operator--() noexcept { return *this = T(*this) - 1; }
    constexpr Little& operator+=(T value) noexcept { return *this = T(*this) + value; }
    constexpr Little& operator-=(T value) noexcept { return *this = T(*this) - value; }
    constexpr Little& operator|=(T value) noexcept { return *this = T(*this) | value; }
    constexpr Little& operator&=(T value) noexcept { return *this = T(*this) & value; }
    static constexpr T convert(T value) noexcept
    {
        if constexpr (kLittleEndian) return value;
        T result = 0;
        for (unsigned index = 0; index < sizeof(T); ++index) {
            result = T((result << 8) | (value & 255));
            value >>= 8;
        }
        return result;
    }
};
template<class T> T readLittle(const void* source) noexcept
{
    Little<T> encoded;
    std::memcpy(&encoded, source, sizeof encoded);
    return encoded;
}
template<class T> void writeLittle(void* destination, T value) noexcept
{
    Little<T> encoded(value);
    std::memcpy(destination, &encoded, sizeof encoded);
}
}  // namespace nosql::internal