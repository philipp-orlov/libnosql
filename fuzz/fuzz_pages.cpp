// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// libFuzzer target: lies inside one page of a real store, re-sealed so they
// pass its checksum, then the store opened, read every way and written to.
// An Error is the right answer; a sanitizer report or a crash is a bug.
//
//   cmake -S . -B build-fuzz -DCMAKE_CXX_COMPILER=clang++ -DNOSQL_BUILD_FUZZERS=ON
//   cmake --build build-fuzz --target fuzz_pages
//   build-fuzz/fuzz/fuzz_pages -max_total_time=600
//
// The input picks the page, then lists (offset, byte) patches into it.

#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "tests/corrupt_util.hpp"

namespace {

struct Base
{
    std::filesystem::path dir, file, cut;
    std::string image;
    std::size_t pages = 0;

    Base()
    {
        dir = std::filesystem::temp_directory_path() / ("nosql-fuzz-" + std::to_string(::getpid()));
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        file = dir / "base.db";
        cut = dir / "cut.db";
        pages = tst::corrupt::buildStore(file);
        image = tst::corrupt::readAll(file);
    }
    ~Base() { std::filesystem::remove_all(dir); }
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    static Base base;
    if (size < 2)
        return 0;
    std::string bytes = base.image;
    tst::corrupt::applyFuzzInput(bytes, base.pages, data, size);
    tst::corrupt::writeAll(base.cut, bytes);
    (void)tst::corrupt::exercise(base.cut);
    return 0;
}
