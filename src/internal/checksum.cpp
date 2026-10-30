// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/internal/checksum.hpp"

#include <algorithm>
#include <cstring>

#include "nosql/internal/endian.hpp"

// Kernel selection. NEON is part of the AArch64 baseline and SSE2 is part of the
// x86-64 baseline, so neither needs compiler flags. AVX2 is compiled as an extra
// function on GCC/Clang and chosen once at run time. Every kernel produces the
// same digest as the portable scalar code, which remains the fallback.
#if defined(NOSQL_CHECKSUM_SCALAR_ONLY)
#define NOSQL_CHECKSUM_NEON 0
#define NOSQL_CHECKSUM_SSE2 0
#elif defined(__aarch64__) && defined(__ARM_NEON) && defined(__BYTE_ORDER__) && \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#include <arm_neon.h>
#define NOSQL_CHECKSUM_NEON 1
#define NOSQL_CHECKSUM_SSE2 0
#elif defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define NOSQL_CHECKSUM_NEON 0
#define NOSQL_CHECKSUM_SSE2 1
#else
#define NOSQL_CHECKSUM_NEON 0
#define NOSQL_CHECKSUM_SSE2 0
#endif
#if NOSQL_CHECKSUM_SSE2 && defined(__GNUC__)
#define NOSQL_CHECKSUM_AVX2 1
#else
#define NOSQL_CHECKSUM_AVX2 0
#endif

namespace nosql::internal {
namespace {

// XXH3, 64-bit variant, seed zero, default secret.
constexpr std::uint32_t kPrime32_1 = 0x9e3779b1u;
constexpr std::uint32_t kPrime32_2 = 0x85ebca77u;
constexpr std::uint32_t kPrime32_3 = 0xc2b2ae3du;
constexpr std::uint64_t kPrime64_1 = 0x9e3779b185ebca87ull;
constexpr std::uint64_t kPrime64_2 = 0xc2b2ae3d27d4eb4full;
constexpr std::uint64_t kPrime64_3 = 0x165667b19e3779f9ull;
constexpr std::uint64_t kPrime64_4 = 0x85ebca77c2b2ae63ull;
constexpr std::uint64_t kPrime64_5 = 0x27d4eb2f165667c5ull;
constexpr std::uint64_t kPrimeMx1 = 0x165667919e3779f9ull;
constexpr std::uint64_t kPrimeMx2 = 0x9fb21c651e98df25ull;

constexpr std::size_t kStripeBytes = 64;
constexpr std::size_t kStripesPerBlock = 16;
constexpr std::size_t kSecretBytes = 192;
constexpr std::size_t kSecretConsumeRate = 8;
constexpr std::size_t kScrambleSecretOffset = kSecretBytes - kStripeBytes;        // 128
constexpr std::size_t kLastStripeSecretOffset = kSecretBytes - kStripeBytes - 7;  // 121
constexpr std::size_t kMergeSecretOffset = 11;
constexpr std::size_t kMidSizeLastSecretOffset = 136 - 17;  // 119
constexpr std::size_t kMidSizeMax = 240;

alignas(64) constexpr std::uint8_t kSecret[kSecretBytes] = {
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

using Accumulators = std::array<std::uint64_t, 8>;

constexpr Accumulators kInitialAccumulators = {kPrime32_3, kPrime64_1, kPrime64_2, kPrime64_3,
                                               kPrime64_4, kPrime32_2, kPrime64_5, kPrime32_1};

constexpr std::uint64_t rotateLeft(std::uint64_t value, unsigned shift) noexcept
{
    return (value << shift) | (value >> (64 - shift));
}

constexpr std::uint64_t byteSwap(std::uint64_t value) noexcept
{
    std::uint64_t result = 0;
    for (unsigned index = 0; index < 8; ++index) {
        result = (result << 8) | (value & 0xff);
        value >>= 8;
    }
    return result;
}

/// Low 64 bits XOR high 64 bits of the 128-bit product.
std::uint64_t multiplyFold(std::uint64_t a, std::uint64_t b) noexcept
{
#if defined(__SIZEOF_INT128__)
    __extension__ typedef unsigned __int128 Wide;
    const Wide product = Wide(a) * b;
    return std::uint64_t(product) ^ std::uint64_t(product >> 64);
#else
    const std::uint64_t mask = 0xffffffffull;
    const std::uint64_t lowLow = (a & mask) * (b & mask);
    const std::uint64_t highLow = (a >> 32) * (b & mask);
    const std::uint64_t lowHigh = (a & mask) * (b >> 32);
    const std::uint64_t highHigh = (a >> 32) * (b >> 32);
    const std::uint64_t cross = (lowLow >> 32) + (highLow & mask) + lowHigh;
    const std::uint64_t high = (highLow >> 32) + (cross >> 32) + highHigh;
    const std::uint64_t low = (cross << 32) | (lowLow & mask);
    return low ^ high;
#endif
}

std::uint64_t secret64(std::size_t offset) noexcept { return readLittle<std::uint64_t>(kSecret + offset); }
std::uint32_t secret32(std::size_t offset) noexcept { return readLittle<std::uint32_t>(kSecret + offset); }

std::uint64_t xxh64Avalanche(std::uint64_t hash) noexcept
{
    hash ^= hash >> 33;
    hash *= kPrime64_2;
    hash ^= hash >> 29;
    hash *= kPrime64_3;
    return hash ^ (hash >> 32);
}

std::uint64_t avalanche(std::uint64_t hash) noexcept
{
    hash ^= hash >> 37;
    hash *= kPrimeMx1;
    return hash ^ (hash >> 32);
}

std::uint64_t rrmxmx(std::uint64_t hash, std::uint64_t length) noexcept
{
    hash ^= rotateLeft(hash, 49) ^ rotateLeft(hash, 24);
    hash *= kPrimeMx2;
    hash ^= (hash >> 35) + length;
    hash *= kPrimeMx2;
    return hash ^ (hash >> 28);
}

std::uint64_t mix16(const std::byte* input, std::size_t secretOffset) noexcept
{
    return multiplyFold(readLittle<std::uint64_t>(input) ^ secret64(secretOffset),
                        readLittle<std::uint64_t>(input + 8) ^ secret64(secretOffset + 8));
}

std::uint64_t hashUpTo16(const std::byte* input, std::size_t size) noexcept
{
    if (size > 8) {
        const std::uint64_t low = readLittle<std::uint64_t>(input) ^ (secret64(24) ^ secret64(32));
        const std::uint64_t high = readLittle<std::uint64_t>(input + size - 8) ^ (secret64(40) ^ secret64(48));
        return avalanche(std::uint64_t(size) + byteSwap(low) + high + multiplyFold(low, high));
    }
    if (size >= 4) {
        const std::uint64_t combined = std::uint64_t(readLittle<std::uint32_t>(input + size - 4)) +
                                       (std::uint64_t(readLittle<std::uint32_t>(input)) << 32);
        return rrmxmx(combined ^ (secret64(8) ^ secret64(16)), size);
    }
    if (size != 0) {
        const auto first = std::to_integer<std::uint32_t>(input[0]);
        const auto middle = std::to_integer<std::uint32_t>(input[size >> 1]);
        const auto last = std::to_integer<std::uint32_t>(input[size - 1]);
        const std::uint32_t combined = (first << 16) | (middle << 24) | last | (std::uint32_t(size) << 8);
        return xxh64Avalanche(std::uint64_t(combined) ^ (secret32(0) ^ secret32(4)));
    }
    return xxh64Avalanche(secret64(56) ^ secret64(64));
}

std::uint64_t hash17To128(const std::byte* input, std::size_t size) noexcept
{
    std::uint64_t acc = std::uint64_t(size) * kPrime64_1;
    if (size > 32) {
        if (size > 64) {
            if (size > 96) {
                acc += mix16(input + 48, 96);
                acc += mix16(input + size - 64, 112);
            }
            acc += mix16(input + 32, 64);
            acc += mix16(input + size - 48, 80);
        }
        acc += mix16(input + 16, 32);
        acc += mix16(input + size - 32, 48);
    }
    acc += mix16(input, 0);
    acc += mix16(input + size - 16, 16);
    return avalanche(acc);
}

std::uint64_t hash129To240(const std::byte* input, std::size_t size) noexcept
{
    std::uint64_t acc = std::uint64_t(size) * kPrime64_1;
    for (std::size_t round = 0; round < 8; ++round)
        acc += mix16(input + 16 * round, 16 * round);
    acc = avalanche(acc);
    for (std::size_t round = 8; round < size / 16; ++round)
        acc += mix16(input + 16 * round, 16 * (round - 8) + 3);
    acc += mix16(input + size - 16, kMidSizeLastSecretOffset);
    return avalanche(acc);
}

// ---- Long-input kernels: one 64-byte stripe per step over eight 64-bit lanes ----

void accumulateScalar(Accumulators& acc, const std::byte* input, const std::uint8_t* secret,
                      std::size_t stripes) noexcept
{
    for (; stripes != 0; --stripes, input += kStripeBytes, secret += kSecretConsumeRate) {
        for (unsigned lane = 0; lane < 8; ++lane) {
            const std::uint64_t data = readLittle<std::uint64_t>(input + 8 * lane);
            const std::uint64_t key = data ^ readLittle<std::uint64_t>(secret + 8 * lane);
            acc[lane ^ 1] += data;
            acc[lane] += (key & 0xffffffffull) * (key >> 32);
        }
    }
}

void scrambleScalar(Accumulators& acc, const std::uint8_t* secret) noexcept
{
    for (unsigned lane = 0; lane < 8; ++lane) {
        std::uint64_t value = acc[lane];
        value ^= value >> 47;
        value ^= readLittle<std::uint64_t>(secret + 8 * lane);
        acc[lane] = value * kPrime32_1;
    }
}

#if NOSQL_CHECKSUM_NEON
inline uint64x2_t accumulatePairNeon(uint64x2_t acc, const std::uint8_t* input, const std::uint8_t* secret) noexcept
{
    const uint64x2_t data = vreinterpretq_u64_u8(vld1q_u8(input));
    const uint64x2_t key = veorq_u64(data, vreinterpretq_u64_u8(vld1q_u8(secret)));
    acc = vaddq_u64(acc, vextq_u64(data, data, 1));
    return vaddq_u64(acc, vmull_u32(vmovn_u64(key), vshrn_n_u64(key, 32)));
}

inline uint64x2_t scramblePairNeon(uint64x2_t acc, const std::uint8_t* secret) noexcept
{
    const uint64x2_t key = vreinterpretq_u64_u8(vld1q_u8(secret));
    const uint64x2_t mixed = veorq_u64(veorq_u64(acc, vshrq_n_u64(acc, 47)), key);
    const uint32x2_t prime = vdup_n_u32(kPrime32_1);
    return vaddq_u64(vmull_u32(vmovn_u64(mixed), prime),
                     vshlq_n_u64(vmull_u32(vshrn_n_u64(mixed, 32), prime), 32));
}

// Cortex-A cores have fewer 64-bit vector multiply pipes than scalar ones, so
// the reference implementation hashes some lanes in scalar code. Six NEON lanes
// plus two scalar lanes measured fastest on Cortex-A725 and X925.
#ifndef NOSQL_CHECKSUM_NEON_LANES
#define NOSQL_CHECKSUM_NEON_LANES 6
#endif
constexpr unsigned kNeonLanes = NOSQL_CHECKSUM_NEON_LANES;
static_assert(kNeonLanes == 8 || kNeonLanes == 6 || kNeonLanes == 4);

void accumulateNeon(Accumulators& acc, const std::byte* input, const std::uint8_t* secret,
                    std::size_t stripes) noexcept
{
    const auto* cursor = reinterpret_cast<const std::uint8_t*>(input);
    uint64x2_t acc0 = vld1q_u64(acc.data());
    uint64x2_t acc1 = vld1q_u64(acc.data() + 2);
    uint64x2_t acc2 = vld1q_u64(acc.data() + 4);
    uint64x2_t acc3 = vld1q_u64(acc.data() + 6);
    std::uint64_t scalar4 = acc[4], scalar5 = acc[5], scalar6 = acc[6], scalar7 = acc[7];
    for (; stripes != 0; --stripes, cursor += kStripeBytes, secret += kSecretConsumeRate) {
        acc0 = accumulatePairNeon(acc0, cursor, secret);
        acc1 = accumulatePairNeon(acc1, cursor + 16, secret + 16);
        if constexpr (kNeonLanes >= 6) {
            acc2 = accumulatePairNeon(acc2, cursor + 32, secret + 32);
        } else {
            const std::uint64_t data4 = readLittle<std::uint64_t>(cursor + 32);
            const std::uint64_t data5 = readLittle<std::uint64_t>(cursor + 40);
            const std::uint64_t key4 = data4 ^ readLittle<std::uint64_t>(secret + 32);
            const std::uint64_t key5 = data5 ^ readLittle<std::uint64_t>(secret + 40);
            scalar5 += data4;
            scalar4 += data5;
            scalar4 += (key4 & 0xffffffffull) * (key4 >> 32);
            scalar5 += (key5 & 0xffffffffull) * (key5 >> 32);
        }
        if constexpr (kNeonLanes == 8) {
            acc3 = accumulatePairNeon(acc3, cursor + 48, secret + 48);
        } else {
            const std::uint64_t data6 = readLittle<std::uint64_t>(cursor + 48);
            const std::uint64_t data7 = readLittle<std::uint64_t>(cursor + 56);
            const std::uint64_t key6 = data6 ^ readLittle<std::uint64_t>(secret + 48);
            const std::uint64_t key7 = data7 ^ readLittle<std::uint64_t>(secret + 56);
            scalar7 += data6;
            scalar6 += data7;
            scalar6 += (key6 & 0xffffffffull) * (key6 >> 32);
            scalar7 += (key7 & 0xffffffffull) * (key7 >> 32);
        }
    }
    vst1q_u64(acc.data(), acc0);
    vst1q_u64(acc.data() + 2, acc1);
    if constexpr (kNeonLanes >= 6) {
        vst1q_u64(acc.data() + 4, acc2);
    } else {
        acc[4] = scalar4;
        acc[5] = scalar5;
    }
    if constexpr (kNeonLanes == 8) {
        vst1q_u64(acc.data() + 6, acc3);
    } else {
        acc[6] = scalar6;
        acc[7] = scalar7;
    }
}

void scrambleNeon(Accumulators& acc, const std::uint8_t* secret) noexcept
{
    for (unsigned pair = 0; pair < 4; ++pair)
        vst1q_u64(acc.data() + 2 * pair, scramblePairNeon(vld1q_u64(acc.data() + 2 * pair), secret + 16 * pair));
}
#endif

#if NOSQL_CHECKSUM_SSE2
inline __m128i accumulatePairSse2(__m128i acc, const std::byte* input, const std::uint8_t* secret) noexcept
{
    const __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(input));
    const __m128i key = _mm_xor_si128(data, _mm_loadu_si128(reinterpret_cast<const __m128i*>(secret)));
    const __m128i keyHigh = _mm_shuffle_epi32(key, _MM_SHUFFLE(0, 3, 0, 1));
    acc = _mm_add_epi64(acc, _mm_mul_epu32(key, keyHigh));
    return _mm_add_epi64(acc, _mm_shuffle_epi32(data, _MM_SHUFFLE(1, 0, 3, 2)));
}

inline __m128i scramblePairSse2(__m128i acc, const std::uint8_t* secret) noexcept
{
    const __m128i key = _mm_loadu_si128(reinterpret_cast<const __m128i*>(secret));
    const __m128i mixed = _mm_xor_si128(_mm_xor_si128(acc, _mm_srli_epi64(acc, 47)), key);
    const __m128i prime = _mm_set1_epi32(static_cast<int>(kPrime32_1));
    const __m128i low = _mm_mul_epu32(mixed, prime);
    const __m128i high = _mm_mul_epu32(_mm_shuffle_epi32(mixed, _MM_SHUFFLE(0, 3, 0, 1)), prime);
    return _mm_add_epi64(low, _mm_slli_epi64(high, 32));
}

void accumulateSse2(Accumulators& acc, const std::byte* input, const std::uint8_t* secret,
                    std::size_t stripes) noexcept
{
    auto* lanes = reinterpret_cast<__m128i*>(acc.data());
    __m128i acc0 = _mm_loadu_si128(lanes);
    __m128i acc1 = _mm_loadu_si128(lanes + 1);
    __m128i acc2 = _mm_loadu_si128(lanes + 2);
    __m128i acc3 = _mm_loadu_si128(lanes + 3);
    for (; stripes != 0; --stripes, input += kStripeBytes, secret += kSecretConsumeRate) {
        acc0 = accumulatePairSse2(acc0, input, secret);
        acc1 = accumulatePairSse2(acc1, input + 16, secret + 16);
        acc2 = accumulatePairSse2(acc2, input + 32, secret + 32);
        acc3 = accumulatePairSse2(acc3, input + 48, secret + 48);
    }
    _mm_storeu_si128(lanes, acc0);
    _mm_storeu_si128(lanes + 1, acc1);
    _mm_storeu_si128(lanes + 2, acc2);
    _mm_storeu_si128(lanes + 3, acc3);
}

void scrambleSse2(Accumulators& acc, const std::uint8_t* secret) noexcept
{
    auto* lanes = reinterpret_cast<__m128i*>(acc.data());
    for (unsigned pair = 0; pair < 4; ++pair)
        _mm_storeu_si128(lanes + pair, scramblePairSse2(_mm_loadu_si128(lanes + pair), secret + 16 * pair));
}
#endif

#if NOSQL_CHECKSUM_AVX2
__attribute__((target("avx2"))) inline __m256i accumulateQuadAvx2(__m256i acc, const std::byte* input,
                                                                  const std::uint8_t* secret) noexcept
{
    const __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input));
    const __m256i key = _mm256_xor_si256(data, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secret)));
    const __m256i keyHigh = _mm256_shuffle_epi32(key, _MM_SHUFFLE(0, 3, 0, 1));
    acc = _mm256_add_epi64(acc, _mm256_mul_epu32(key, keyHigh));
    return _mm256_add_epi64(acc, _mm256_shuffle_epi32(data, _MM_SHUFFLE(1, 0, 3, 2)));
}

__attribute__((target("avx2"))) inline __m256i scrambleQuadAvx2(__m256i acc, const std::uint8_t* secret) noexcept
{
    const __m256i key = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secret));
    const __m256i mixed = _mm256_xor_si256(_mm256_xor_si256(acc, _mm256_srli_epi64(acc, 47)), key);
    const __m256i prime = _mm256_set1_epi32(static_cast<int>(kPrime32_1));
    const __m256i low = _mm256_mul_epu32(mixed, prime);
    const __m256i high = _mm256_mul_epu32(_mm256_shuffle_epi32(mixed, _MM_SHUFFLE(0, 3, 0, 1)), prime);
    return _mm256_add_epi64(low, _mm256_slli_epi64(high, 32));
}

__attribute__((target("avx2"))) void accumulateAvx2(Accumulators& acc, const std::byte* input,
                                                    const std::uint8_t* secret, std::size_t stripes) noexcept
{
    auto* lanes = reinterpret_cast<__m256i*>(acc.data());
    __m256i acc0 = _mm256_loadu_si256(lanes);
    __m256i acc1 = _mm256_loadu_si256(lanes + 1);
    for (; stripes != 0; --stripes, input += kStripeBytes, secret += kSecretConsumeRate) {
        acc0 = accumulateQuadAvx2(acc0, input, secret);
        acc1 = accumulateQuadAvx2(acc1, input + 32, secret + 32);
    }
    _mm256_storeu_si256(lanes, acc0);
    _mm256_storeu_si256(lanes + 1, acc1);
}

__attribute__((target("avx2"))) void scrambleAvx2(Accumulators& acc, const std::uint8_t* secret) noexcept
{
    auto* lanes = reinterpret_cast<__m256i*>(acc.data());
    _mm256_storeu_si256(lanes, scrambleQuadAvx2(_mm256_loadu_si256(lanes), secret));
    _mm256_storeu_si256(lanes + 1, scrambleQuadAvx2(_mm256_loadu_si256(lanes + 1), secret + 32));
}
#endif

struct Kernel
{
    void (*accumulate)(Accumulators&, const std::byte*, const std::uint8_t*, std::size_t) noexcept;
    void (*scramble)(Accumulators&, const std::uint8_t*) noexcept;
    const char* name;
};

constexpr Kernel kScalarKernel{accumulateScalar, scrambleScalar, "XXH3-64/in-tree-C++17/scalar"};
#if NOSQL_CHECKSUM_NEON
constexpr Kernel kNeonKernel{accumulateNeon, scrambleNeon, "XXH3-64/in-tree-C++17/neon"};
#endif
#if NOSQL_CHECKSUM_SSE2
constexpr Kernel kSse2Kernel{accumulateSse2, scrambleSse2, "XXH3-64/in-tree-C++17/sse2"};
#endif
#if NOSQL_CHECKSUM_AVX2
constexpr Kernel kAvx2Kernel{accumulateAvx2, scrambleAvx2, "XXH3-64/in-tree-C++17/avx2"};
#endif

const Kernel& kernel() noexcept
{
#if NOSQL_CHECKSUM_AVX2
    static const Kernel& chosen = (__builtin_cpu_init(), __builtin_cpu_supports("avx2")) ? kAvx2Kernel : kSse2Kernel;
    return chosen;
#elif NOSQL_CHECKSUM_SSE2
    return kSse2Kernel;
#elif NOSQL_CHECKSUM_NEON
    return kNeonKernel;
#else
    return kScalarKernel;
#endif
}

/// Feeds whole stripes, scrambling after every sixteenth. `stripesInBlock` carries
/// the position inside the current block between calls.
void consumeStripes(Accumulators& acc, std::size_t& stripesInBlock, const std::byte* input,
                    std::size_t stripes, const Kernel& k) noexcept
{
    while (stripes != 0) {
        const std::size_t run = std::min(kStripesPerBlock - stripesInBlock, stripes);
        k.accumulate(acc, input, kSecret + kSecretConsumeRate * stripesInBlock, run);
        input += run * kStripeBytes;
        stripes -= run;
        stripesInBlock += run;
        if (stripesInBlock == kStripesPerBlock) {
            k.scramble(acc, kSecret + kScrambleSecretOffset);
            stripesInBlock = 0;
        }
    }
}

std::uint64_t mergeAccumulators(const Accumulators& acc, std::uint64_t length) noexcept
{
    std::uint64_t result = length * kPrime64_1;
    for (unsigned pair = 0; pair < 4; ++pair)
        result += multiplyFold(acc[2 * pair] ^ secret64(kMergeSecretOffset + 16 * pair),
                               acc[2 * pair + 1] ^ secret64(kMergeSecretOffset + 8 + 16 * pair));
    return avalanche(result);
}

std::uint64_t hashLong(const std::byte* input, std::size_t size) noexcept
{
    const Kernel& k = kernel();
    Accumulators acc = kInitialAccumulators;
    std::size_t stripesInBlock = 0;
    consumeStripes(acc, stripesInBlock, input, (size - 1) / kStripeBytes, k);
    k.accumulate(acc, input + size - kStripeBytes, kSecret + kLastStripeSecretOffset, 1);
    return mergeAccumulators(acc, size);
}

}  // namespace

std::uint64_t checksum64(const void* data, std::size_t size) noexcept
{
    const auto* input = static_cast<const std::byte*>(data);
    if (size <= 16)
        return hashUpTo16(input, size);
    if (size <= 128)
        return hash17To128(input, size);
    if (size <= kMidSizeMax)
        return hash129To240(input, size);
    return hashLong(input, size);
}

const char* checksumBackend() noexcept
{
    return kernel().name;
}

Checksum64::Checksum64() noexcept { reset(); }

void Checksum64::reset() noexcept
{
    accumulators_ = kInitialAccumulators;
    buffer_.fill(std::byte{});
    bufferedBytes_ = 0;
    stripesInBlock_ = 0;
    length_ = 0;
}

void Checksum64::update(const void* data, std::size_t size) noexcept
{
    if (size == 0)
        return;
    const auto* cursor = static_cast<const std::byte*>(data);
    length_ += std::uint64_t(size);
    if (bufferedBytes_ + size <= buffer_.size()) {
        std::memcpy(buffer_.data() + bufferedBytes_, cursor, size);
        bufferedBytes_ += size;
        return;
    }
    const Kernel& k = kernel();
    if (bufferedBytes_ != 0) {
        const std::size_t fill = buffer_.size() - bufferedBytes_;
        std::memcpy(buffer_.data() + bufferedBytes_, cursor, fill);
        consumeStripes(accumulators_, stripesInBlock_, buffer_.data(), buffer_.size() / kStripeBytes, k);
        bufferedBytes_ = 0;
        cursor += fill;
        size -= fill;
    }
    if (size > buffer_.size()) {
        // Leave 1..64 bytes unconsumed: the final stripe is hashed differently.
        const std::size_t stripes = (size - 1) / kStripeBytes;
        consumeStripes(accumulators_, stripesInBlock_, cursor, stripes, k);
        cursor += stripes * kStripeBytes;
        size -= stripes * kStripeBytes;
        // Keep the last consumed stripe at the buffer's tail so digest() can
        // rebuild the final 64 bytes when fewer than 64 are buffered.
        std::memcpy(buffer_.data() + buffer_.size() - kStripeBytes, cursor - kStripeBytes, kStripeBytes);
    }
    std::memcpy(buffer_.data(), cursor, size);
    bufferedBytes_ = size;
}

std::uint64_t Checksum64::digest() const noexcept
{
    if (length_ <= kMidSizeMax)
        return checksum64(buffer_.data(), std::size_t(length_));
    const Kernel& k = kernel();
    Accumulators acc = accumulators_;
    std::size_t stripesInBlock = stripesInBlock_;
    if (bufferedBytes_ >= kStripeBytes) {
        consumeStripes(acc, stripesInBlock, buffer_.data(), (bufferedBytes_ - 1) / kStripeBytes, k);
        k.accumulate(acc, buffer_.data() + bufferedBytes_ - kStripeBytes, kSecret + kLastStripeSecretOffset, 1);
    } else {
        std::array<std::byte, kStripeBytes> last;
        const std::size_t carried = kStripeBytes - bufferedBytes_;
        std::memcpy(last.data(), buffer_.data() + buffer_.size() - carried, carried);
        std::memcpy(last.data() + carried, buffer_.data(), bufferedBytes_);
        k.accumulate(acc, last.data(), kSecret + kLastStripeSecretOffset, 1);
    }
    return mergeAccumulators(acc, length_);
}

}  // namespace nosql::internal
