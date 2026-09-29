// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Independent byte-at-a-time XXH3-64 (seed zero) used only as a test oracle.
// Deliberately shares no code with src/internal/checksum.cpp: bytes are decoded
// one at a time, the 128-bit product is built from four 32-bit products, and
// the long path is a plain stripe loop.
#include <cstddef>
#include <cstdint>

namespace {

using u64 = std::uint64_t;

const unsigned char kSecret[192] = {
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
};

constexpr u64 prime32_1 = 2654435761u;
constexpr u64 prime32_2 = 2246822519u;
constexpr u64 prime32_3 = 3266489917u;
constexpr u64 prime64_1 = 11400714785074694791ull;
constexpr u64 prime64_2 = 14029467366897019727ull;
constexpr u64 prime64_3 = 1609587929392839161ull;
constexpr u64 prime64_4 = 9650029242287828579ull;
constexpr u64 prime64_5 = 2870177450012600261ull;
constexpr u64 mx1 = 1609587791953885689ull;
constexpr u64 mx2 = 11507291218515648293ull;

u64 rotate(u64 value, unsigned bits) { return (value << bits) | (value >> (64 - bits)); }

u64 decode(const unsigned char* data, unsigned bytes)
{
    u64 value = 0;
    for (unsigned index = 0; index < bytes; ++index)
        value |= u64(data[index]) << (index * 8);
    return value;
}

u64 swapBytes(u64 value)
{
    unsigned char bytes[8];
    for (unsigned index = 0; index < 8; ++index)
        bytes[7 - index] = (unsigned char)(value >> (index * 8));
    return decode(bytes, 8);
}

u64 secret(unsigned offset, unsigned bytes) { return decode(kSecret + offset, bytes); }

u64 fold(u64 a, u64 b)
{
    const u64 a0 = a & 0xffffffffu, a1 = a >> 32, b0 = b & 0xffffffffu, b1 = b >> 32;
    const u64 p00 = a0 * b0, p01 = a0 * b1, p10 = a1 * b0, p11 = a1 * b1;
    const u64 middle = (p00 >> 32) + (p10 & 0xffffffffu) + p01;
    const u64 high = p11 + (p10 >> 32) + (middle >> 32);
    const u64 low = (middle << 32) | (p00 & 0xffffffffu);
    return low ^ high;
}

u64 avalancheXxh64(u64 h)
{
    h ^= h >> 33; h *= prime64_2; h ^= h >> 29; h *= prime64_3; return h ^ (h >> 32);
}

u64 avalanche(u64 h)
{
    h ^= h >> 37; h *= mx1; return h ^ (h >> 32);
}

u64 rrmxmx(u64 h, u64 length)
{
    h ^= rotate(h, 49) ^ rotate(h, 24);
    h *= mx2;
    h ^= (h >> 35) + length;
    h *= mx2;
    return h ^ (h >> 28);
}

u64 mix16(const unsigned char* p, unsigned secretOffset)
{
    return fold(decode(p, 8) ^ secret(secretOffset, 8), decode(p + 8, 8) ^ secret(secretOffset + 8, 8));
}

}  // namespace

std::uint64_t referenceChecksum(const void* data, std::size_t size)
{
    const auto* bytes = static_cast<const unsigned char*>(data);
    if (size == 0)
        return avalancheXxh64(secret(56, 8) ^ secret(64, 8));
    if (size <= 3) {
        const u64 combined = (u64(bytes[0]) << 16) | (u64(bytes[size >> 1]) << 24) | u64(bytes[size - 1]) | (u64(size) << 8);
        return avalancheXxh64(combined ^ (secret(0, 4) ^ secret(4, 4)));
    }
    if (size <= 8) {
        const u64 combined = decode(bytes + size - 4, 4) + (decode(bytes, 4) << 32);
        return rrmxmx(combined ^ (secret(8, 8) ^ secret(16, 8)), size);
    }
    if (size <= 16) {
        const u64 low = decode(bytes, 8) ^ (secret(24, 8) ^ secret(32, 8));
        const u64 high = decode(bytes + size - 8, 8) ^ (secret(40, 8) ^ secret(48, 8));
        return avalanche(u64(size) + swapBytes(low) + high + fold(low, high));
    }
    if (size <= 128) {
        u64 acc = u64(size) * prime64_1;
        if (size > 32) {
            if (size > 64) {
                if (size > 96) {
                    acc += mix16(bytes + 48, 96);
                    acc += mix16(bytes + size - 64, 112);
                }
                acc += mix16(bytes + 32, 64);
                acc += mix16(bytes + size - 48, 80);
            }
            acc += mix16(bytes + 16, 32);
            acc += mix16(bytes + size - 32, 48);
        }
        acc += mix16(bytes, 0);
        acc += mix16(bytes + size - 16, 16);
        return avalanche(acc);
    }
    if (size <= 240) {
        u64 acc = u64(size) * prime64_1;
        for (unsigned round = 0; round < 8; ++round)
            acc += mix16(bytes + 16 * round, 16 * round);
        acc = avalanche(acc);
        for (unsigned round = 8; round < size / 16; ++round)
            acc += mix16(bytes + 16 * round, 16 * (round - 8) + 3);
        acc += mix16(bytes + size - 16, 136 - 17);
        return avalanche(acc);
    }
    u64 acc[8] = {prime32_3, prime64_1, prime64_2, prime64_3, prime64_4, prime32_2, prime64_5, prime32_1};
    const auto stripe = [&](const unsigned char* p, unsigned secretOffset) {
        for (unsigned lane = 0; lane < 8; ++lane) {
            const u64 word = decode(p + 8 * lane, 8);
            const u64 keyed = word ^ secret(secretOffset + 8 * lane, 8);
            acc[lane ^ 1u] += word;
            acc[lane] += (keyed & 0xffffffffu) * (keyed >> 32);
        }
    };
    const std::size_t stripes = (size - 1) / 64;
    for (std::size_t index = 0; index < stripes; ++index) {
        stripe(bytes + 64 * index, unsigned(8 * (index % 16)));
        if (index % 16 == 15)
            for (unsigned lane = 0; lane < 8; ++lane) {
                u64 value = acc[lane];
                value ^= value >> 47;
                value ^= secret(128 + 8 * lane, 8);
                acc[lane] = value * prime32_1;
            }
    }
    stripe(bytes + size - 64, 192 - 64 - 7);
    u64 result = u64(size) * prime64_1;
    for (unsigned pair = 0; pair < 4; ++pair)
        result += fold(acc[2 * pair] ^ secret(11 + 16 * pair, 8), acc[2 * pair + 1] ^ secret(19 + 16 * pair, 8));
    return avalanche(result);
}
