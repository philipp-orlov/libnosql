// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <thread>

#include "nosql/internal/checksum.hpp"
#include "tests/test_util.hpp"

std::uint64_t referenceChecksum(const void*, std::size_t);

namespace {

struct Golden
{
    std::size_t size;
    std::uint64_t digest;
};

// Produced by xxHash 0.8.2 XXH3_64bits over byte[i] = (i * 7 + 3) & 255.
constexpr Golden kPatternGoldens[] = {
    {0, 0x2d06800538d394c2ull},       {1, 0x13e608bc156defedull},     {2, 0x1c9074b93943b86cull},
    {3, 0xa9088dda485b481cull},       {4, 0x6d9253b16c8b1ed3ull},     {5, 0x998620e10e3a4b37ull},
    {8, 0x60539db630471163ull},       {9, 0xfeff668361d723a8ull},     {12, 0x6829454be0cc3199ull},
    {16, 0xb8c859b0f030b585ull},      {17, 0x714a04408e79b80full},    {32, 0x19ff4ee1d6ba1a55ull},
    {33, 0x3e44983ad21679c8ull},      {64, 0x287eb1fa9e4be2c1ull},    {65, 0x829218de4d798646ull},
    {96, 0xf084e7cfbc624743ull},      {97, 0x1daa83271a8e7b7cull},    {128, 0x67425a03650261bfull},
    {129, 0xc664bf3311c6abc4ull},     {144, 0x606a17fa700bb932ull},   {160, 0xd70dea8de694b883ull},
    {240, 0x64556dc6b462a6cfull},     {241, 0x8beadd3a8874fe17ull},   {256, 0x3c38817f6d79c0daull},
    {257, 0x2a300c3495738ea6ull},     {304, 0xe7930efd34514058ull},   {480, 0xfe81693ba4e45fb9ull},
    {504, 0xafea834f8be16c61ull},     {1023, 0xd26986a0b85dcc44ull},  {1024, 0x9b81661c641c72b1ull},
    {1025, 0x806c2072ed713576ull},    {1088, 0x2f8781e01841f0daull},  {1089, 0x2a90437fe231e7e8ull},
    {2048, 0xabe604813ba62ed1ull},    {4088, 0xa2fb11a80f3c1954ull},  {65528, 0x1d0dc2c5d425081dull},
    {1048576, 0x74823cf2cd45b0feull},
};

std::vector<unsigned char> pattern(std::size_t size)
{
    std::vector<unsigned char> bytes(size);
    for (std::size_t index = 0; index < size; ++index)
        bytes[index] = static_cast<unsigned char>((index * 7 + 3) & 255);
    return bytes;
}

void checkAllSplits(const std::string& bytes, std::size_t size)
{
    using namespace nosql::internal;
    const auto expected = referenceChecksum(bytes.data(), size);
    for (std::size_t split = 0; split <= size; ++split) {
        Checksum64 stream;
        stream.update(bytes.data(), split);
        CHECK_EQ(stream.digest(), referenceChecksum(bytes.data(), split));
        stream.update(nullptr, 0);
        stream.update(bytes.data() + split, size - split);
        CHECK_EQ(stream.digest(), expected);
    }
}

}  // namespace

TEST(checksumMatchesXxhashGoldenStrings)
{
    using namespace nosql::internal;
    CHECK_EQ(checksum64(nullptr, 0), 0x2d06800538d394c2ull);
    CHECK_EQ(checksum64("a", 1), 0xe6c632b61e964e1full);
    CHECK_EQ(checksum64("abc", 3), 0x78af5f94892f3950ull);
    CHECK_EQ(checksum64("123456789", 9), 0x72dcb18b67a17dffull);
    CHECK_EQ(checksum64("The quick brown fox jumps over the lazy dog", 43), 0xce7d19a5418fb365ull);
    CHECK_EQ(referenceChecksum("abc", 3), 0x78af5f94892f3950ull);
}

TEST(checksumMatchesXxhashLengthClassVectors)
{
    using namespace nosql::internal;
    const auto bytes = pattern(1u << 20);
    for (const auto& golden : kPatternGoldens) {
        CHECK_EQ(referenceChecksum(bytes.data(), golden.size), golden.digest);
        CHECK_EQ(checksum64(bytes.data(), golden.size), golden.digest);
        Checksum64 stream;
        const auto first = golden.size / 3;
        stream.update(bytes.data(), first);
        stream.update(bytes.data() + first, golden.size - first);
        CHECK_EQ(stream.digest(), golden.digest);
    }
}

TEST(checksumMatchesReferenceAtEveryAlignment)
{
    using namespace nosql::internal;
    const auto bytes = tst::blob((1u << 20) + 64, 20260906);
    for (std::size_t offset = 0; offset < 32; ++offset) {
        for (std::size_t size : {0u, 1u, 2u, 3u, 4u, 5u, 8u, 9u, 12u, 16u, 17u, 31u, 32u, 33u, 64u, 65u,
                                96u, 97u, 128u, 129u, 144u, 160u, 240u, 241u, 256u, 257u, 304u, 480u,
                                504u, 1023u, 1024u, 1025u, 1088u, 1089u, 2048u, 4088u, 65528u, 1u << 20}) {
            const auto expected = referenceChecksum(bytes.data() + offset, size);
            CHECK_EQ(checksum64(bytes.data() + offset, size), expected);
            Checksum64 stream;
            const auto first = size / 3;
            stream.update(bytes.data() + offset, first);
            stream.update(bytes.data() + offset + first, size - first);
            CHECK_EQ(stream.digest(), expected);
            stream.reset();
            for (std::size_t index = 0; index < size; index += 127)
                stream.update(bytes.data() + offset + index, std::min<std::size_t>(127, size - index));
            CHECK_EQ(stream.digest(), expected);
        }
    }
}

TEST(streamingSplitsDigestAndResetAreStable)
{
    using namespace nosql::internal;
    static_assert(sizeof(Checksum64) <= 512);
    const auto bytes = tst::blob(4096, 7);
    for (std::size_t size = 0; size <= 320; ++size)
        checkAllSplits(bytes, size);
    for (std::size_t size : {1023u, 1024u, 1025u, 1088u, 1089u, 2048u, 2049u, 4088u})
        checkAllSplits(bytes, size);
    Checksum64 stream;
    stream.update(bytes.data(), 300);
    stream.reset();
    CHECK_EQ(stream.digest(), checksum64(nullptr, 0));
}

TEST(checksumConcurrentCallsAgree)
{
    const auto data = tst::blob(8192, 41);
    const auto expected = referenceChecksum(data.data(), data.size());
    std::atomic<bool> valid{true};
    std::vector<std::thread> workers;
    for (unsigned index = 0; index < 8; ++index)
        workers.emplace_back([&] {
            for (unsigned repeat = 0; repeat < 1000; ++repeat)
                if (nosql::internal::checksum64(data.data(), data.size()) != expected)
                    valid.store(false);
        });
    for (auto& worker : workers) worker.join();
    CHECK(valid.load());
}

int main()
{
    std::printf("Backend: %s\n", nosql::internal::checksumBackend());
    return tst::runAll("checksum");
}
