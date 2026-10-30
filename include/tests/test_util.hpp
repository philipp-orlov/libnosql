// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// A deliberately tiny test harness -- no external dependencies.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace tst {

struct TestCase
{
    const char* name;
    std::function<void()> fn;
};
std::vector<TestCase>& registry();
int runAll(const char* suite);

struct Registrar
{
    Registrar(const char* name, std::function<void()> fn) { registry().push_back({name, fn}); }
};

[[noreturn]] void fail(const char* file, int line, const std::string& msg);

/// A store path that cleans itself up.
class Scratch
{
public:
    explicit Scratch(const char* tag)
    {
        static std::atomic<unsigned> counter{0};
        static const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        dir_ = std::filesystem::temp_directory_path() /
               ("nosql-" + std::string(tag) + "-" + std::to_string(stamp) + "-" +
                std::to_string(counter++));
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    ~Scratch()
    {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }
    std::filesystem::path file(const char* name = "store.db") const { return dir_ / name; }

private:
    std::filesystem::path dir_;
};

inline std::string keyOf(std::uint64_t i, const char* prefix = "key")
{
    char buf[64];
    std::snprintf(buf, sizeof buf, "%s:%012llu", prefix, (unsigned long long)i);
    return buf;
}
inline std::string blob(std::size_t n, std::uint64_t seed)
{
    std::string s(n, '\0');
    std::mt19937_64 rng(seed);
    for (std::size_t i = 0; i < n; ++i)
        s[i] = char('a' + (rng() % 26));
    return s;
}

}  // namespace tst

#define TEST(name)                                   \
    static void name();                              \
    static ::tst::Registrar reg_##name(#name, name); \
    static void name()

#define CHECK(cond)                                                  \
    do {                                                             \
        if (!(cond))                                                 \
            ::tst::fail(__FILE__, __LINE__, "CHECK failed: " #cond); \
    } while (0)

#define CHECK_EQ(a, b)                                                                        \
    do {                                                                                      \
        [&](const auto& left, const auto& right) {                                             \
            if (!(left == right))                                                             \
                ::tst::fail(__FILE__, __LINE__,                                               \
                            "CHECK_EQ failed: " #a " != " #b "\n    left  = " + ::tst::show(left) + \
                                "\n    right = " + ::tst::show(right));                       \
        }((a), (b));                                                                          \
    } while (0)

#define CHECK_THROWS(expr, expectedCode)                                                \
    do {                                                                                \
        bool _caught = false;                                                           \
        try {                                                                           \
            (void)(expr);                                                               \
        } catch (const ::nosql::Error& _err) {                                          \
            _caught = (_err.code() == (expectedCode));                                  \
            if (!_caught)                                                               \
                ::tst::fail(__FILE__, __LINE__,                                         \
                            std::string("wrong error from " #expr ": ") + _err.what()); \
        }                                                                               \
        if (!_caught)                                                                   \
            ::tst::fail(__FILE__, __LINE__, #expr " did not throw " #expectedCode);     \
    } while (0)

namespace tst {
template <class T>
std::string show(const T& v)
{
    if constexpr (std::is_convertible_v<T, std::string>)
        return "\"" + std::string(v) + "\"";
    else if constexpr (std::is_enum_v<T>)
        return std::to_string(static_cast<long long>(v));
    else if constexpr (std::is_arithmetic_v<T>)
        return std::to_string(v);
    else
        return "<value>";
}
inline std::string show(const std::string& v)
{
    return "\"" + v + "\"";
}
}  // namespace tst
