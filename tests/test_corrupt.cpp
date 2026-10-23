// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Pages that pass their checksum but lie about their layout: a damaged file
// re-sealed, a buggy or hostile writer. Whatever a page claims, opening the
// store, reading it every way and writing to it must end in an Error, never
// in a read or write outside the page. Run under ASan this is a fuzzer with a
// fixed seed; fuzz/fuzz_pages.cpp is the open-ended one.

#include <cstdio>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/format.hpp"
#include "nosql/internal/os.hpp"
#include "tests/corrupt_util.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

/// Thousands of single-page lies, each opened, read and written through.
TEST(resealedGarbageIsAnErrorNotACrash)
{
    tst::Scratch scratch("corrupt");
    const auto file = scratch.file();
    const auto cut = scratch.file("cut.db");
    const std::size_t pages = tst::corrupt::buildStore(file);
    const std::string image = tst::corrupt::readAll(file);
    std::mt19937_64 rng(1);
    unsigned errors = 0, survived = 0;
    for (int round = 0; round < 1000; ++round) {
        std::string bytes = image;
        const std::size_t page = 2 + rng() % (pages - 2);
        tst::corrupt::mutatePage(bytes, page, rng);
        tst::corrupt::writeAll(cut, bytes);
        (tst::corrupt::exercise(cut) ? survived : errors)++;
    }
    std::printf("    %u pages refused, %u lies nothing read\n", errors, survived);
    // Most lies are caught; some change only bytes nothing reads.
    CHECK(errors > 500u);
    CHECK(survived > 0u);
}

/// What fuzz/fuzz_pages.cpp found, each fixed: a leaf flag read before its
/// node was checked (treeListSubdbs), and an unaligned `upper` that a writer
/// then added a misaligned node below.
TEST(fuzzerFindingsStayFixed)
{
    static const std::string kFound[] = {
        std::string("\x23\xf7\xfa\x30\x41\x00\x20\x47", 8),
        std::string("\x0e\x0e\x0e\x00\x0e\x7a", 6),
        std::string("\x0e\x0e\x0e\x00\x2f", 5),
        std::string("\x0e\x0e\x0e\x00\xcd\x0e\x0e", 7),
        std::string("\x0e\x0e\x0e\x1f\x1f\x00\x1f\x00\x0e\x00\x7a", 11),
    };
    tst::Scratch scratch("corrupt-found");
    const auto file = scratch.file();
    const auto cut = scratch.file("cut.db");
    const std::size_t pages = tst::corrupt::buildStore(file);
    const std::string image = tst::corrupt::readAll(file);
    for (const std::string& input : kFound) {
        std::string bytes = image;
        tst::corrupt::applyFuzzInput(
            bytes, pages, reinterpret_cast<const std::uint8_t*>(input.data()), input.size());
        tst::corrupt::writeAll(cut, bytes);
        (void)tst::corrupt::exercise(cut);
    }
}

int main()
{
    return tst::runAll("corrupt");
}
