// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "tests/test_util.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <string>

#include "nosql/nosql.hpp"

namespace tst {

std::vector<TestCase>& registry()
{
    static std::vector<TestCase> r;
    return r;
}

struct Failure
{
    std::string msg;
};

void fail(const char* file, int line, const std::string& msg)
{
    throw Failure{std::string(file) + ":" + std::to_string(line) + ": " + msg};
}

int runAll(const char* suite)
{
    int failed = 0;
    std::printf("== %s (%zu cases) ==\n", suite, registry().size());
    for (const auto& c : registry()) {
        const auto t0 = std::chrono::steady_clock::now();
        std::string err;
        try {
            c.fn();
        } catch (const Failure& f) {
            err = f.msg;
        } catch (const std::exception& e) {
            err = std::string("unexpected exception: ") + e.what();
        } catch (...) {
            err = "unexpected non-standard exception";
        }
        const double ms =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0)
                .count();
        if (err.empty()) {
            std::printf("  \033[32mPASS\033[0m %-34s %8.1f ms\n", c.name, ms);
        } else {
            std::printf("  \033[31mFAIL\033[0m %-34s %8.1f ms\n        %s\n", c.name, ms,
                        err.c_str());
            ++failed;
        }
        std::fflush(stdout);
    }
    if (failed)
        std::printf("== %s: %d FAILED ==\n", suite, failed);
    else
        std::printf("== %s: all passed ==\n", suite);
    return failed ? 1 : 0;
}

}  // namespace tst
