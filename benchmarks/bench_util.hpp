// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Shared plumbing for the benchmark executables: timing, repetition
// statistics, allocation counting, process memory, and a scratch directory.
//
// A translation unit that wants allocation counts defines
// BENCH_DEFINE_ALLOC_HOOKS before including this header; that replaces the
// global operator new/delete with counting versions. Exactly one TU per
// executable may do so.
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <new>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <psapi.h>
#else
#include <sys/resource.h>
#include <unistd.h>
#if defined(__GLIBC__)
#include <malloc.h>
#endif
#endif

namespace bench {

// ------------------------------------------------------------ allocation ---

struct AllocCounters
{
    std::atomic<std::uint64_t> count{0};
    std::atomic<std::uint64_t> bytes{0};
    std::atomic<std::uint64_t> frees{0};
};

inline AllocCounters& allocCounters()
{
    static AllocCounters c;
    return c;
}

struct AllocSnapshot
{
    std::uint64_t count = 0;
    std::uint64_t bytes = 0;
    std::uint64_t frees = 0;
};

inline AllocSnapshot allocSnapshot()
{
    AllocSnapshot s;
    s.count = allocCounters().count.load(std::memory_order_relaxed);
    s.bytes = allocCounters().bytes.load(std::memory_order_relaxed);
    s.frees = allocCounters().frees.load(std::memory_order_relaxed);
    return s;
}

// --------------------------------------------------------------- process ---

/// Bytes the C allocator holds from the OS (arena + mmapped chunks), or 0 when
/// the platform will not say. Growth here with flat live bytes is
/// fragmentation.
inline long long heapBytes()
{
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 33)
    const struct mallinfo2 info = mallinfo2();
    return static_cast<long long>(info.arena) + static_cast<long long>(info.hblkhd);
#else
    return 0;
#endif
#else
    return 0;
#endif
}

/// Resident set size right now, in bytes.
inline long long residentBytes()
{
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS pmc{};
    if (::GetProcessMemoryInfo(::GetCurrentProcess(), &pmc, sizeof pmc))
        return static_cast<long long>(pmc.WorkingSetSize);
    return 0;
#else
    FILE* f = std::fopen("/proc/self/statm", "r");
    if (!f)
        return 0;
    long long pages = 0, resident = 0;
    const int n = std::fscanf(f, "%lld %lld", &pages, &resident);
    std::fclose(f);
    if (n != 2)
        return 0;
    return resident * static_cast<long long>(::sysconf(_SC_PAGESIZE));
#endif
}

/// Page faults so far (minor + major on POSIX, all on Windows).
inline long long pageFaults()
{
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS pmc{};
    if (::GetProcessMemoryInfo(::GetCurrentProcess(), &pmc, sizeof pmc))
        return static_cast<long long>(pmc.PageFaultCount);
    return 0;
#else
    struct rusage ru{};
    if (::getrusage(RUSAGE_SELF, &ru) != 0)
        return 0;
    return static_cast<long long>(ru.ru_minflt) + static_cast<long long>(ru.ru_majflt);
#endif
}

/// Peak resident set size, in bytes.
inline long long peakResidentBytes()
{
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS pmc{};
    if (::GetProcessMemoryInfo(::GetCurrentProcess(), &pmc, sizeof pmc))
        return static_cast<long long>(pmc.PeakWorkingSetSize);
    return 0;
#else
    struct rusage ru{};
    if (::getrusage(RUSAGE_SELF, &ru) != 0)
        return 0;
#if defined(__APPLE__)
    return static_cast<long long>(ru.ru_maxrss);
#else
    return static_cast<long long>(ru.ru_maxrss) * 1024;
#endif
#endif
}

// ---------------------------------------------------------------- timing ---

using Clock = std::chrono::steady_clock;

struct Timer
{
    Clock::time_point t0 = Clock::now();
    double ns() const
    {
        return std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
    }
    double ms() const { return ns() / 1e6; }
};

/// One workload's measurements across repetitions.
struct Sample
{
    std::string name;
    std::vector<double> ns;      ///< wall time of each repetition
    std::uint64_t ops = 0;       ///< operations per repetition
    std::uint64_t bytes = 0;     ///< payload bytes per repetition (0 = n/a)
    std::uint64_t allocs = 0;    ///< operator new calls during the last repetition
    std::uint64_t allocBytes = 0;
    long long heapDelta = 0;     ///< allocator footprint change over the last repetition
    long long faults = 0;        ///< page faults during the last repetition
    std::string note;

    double medianNs() const
    {
        if (ns.empty())
            return 0;
        std::vector<double> v = ns;
        std::sort(v.begin(), v.end());
        return v[v.size() / 2];
    }
    double minNs() const { return ns.empty() ? 0 : *std::min_element(ns.begin(), ns.end()); }
    double nsPerOp() const { return ops ? medianNs() / double(ops) : 0; }
    double opsPerSec() const { return medianNs() > 0 ? double(ops) / (medianNs() / 1e9) : 0; }
    double mibPerSec() const
    {
        return bytes && medianNs() > 0 ? double(bytes) / (1024.0 * 1024.0) / (medianNs() / 1e9) : 0;
    }
};

class Report
{
public:
    explicit Report(std::string suite) : suite_(std::move(suite)) {}

    void jsonPath(std::string path) { json_ = std::move(path); }
    void csvPath(std::string path) { csv_ = std::move(path); }

    /// Run `fn` `reps` times, timing each and counting allocations in the
    /// last one. `before` runs unmeasured ahead of every repetition.
    template <class Fn, class Before>
    Sample& run(const std::string& name, std::uint64_t ops, std::uint64_t bytes, int reps, Before&& before,
                Fn&& fn)
    {
        Sample s;
        s.name = name;
        s.ops = ops;
        s.bytes = bytes;
        for (int r = 0; r < reps; ++r) {
            before();
            const AllocSnapshot a0 = allocSnapshot();
            const long long heap0 = heapBytes();
            const long long faults0 = pageFaults();
            Timer t;
            fn();
            s.ns.push_back(t.ns());
            const AllocSnapshot a1 = allocSnapshot();
            s.allocs = a1.count - a0.count;
            s.allocBytes = a1.bytes - a0.bytes;
            s.heapDelta = heapBytes() - heap0;
            s.faults = pageFaults() - faults0;
        }
        samples_.push_back(std::move(s));
        print(samples_.back());
        return samples_.back();
    }

    template <class Fn>
    Sample& run(const std::string& name, std::uint64_t ops, std::uint64_t bytes, int reps, Fn&& fn)
    {
        return run(name, ops, bytes, reps, [] {}, std::forward<Fn>(fn));
    }

    void header() const
    {
        std::printf("  %-34s %10s %12s %9s %10s %12s %10s %9s\n", "workload", "ns/op", "op/s",
                    "MiB/s", "allocs", "alloc B", "heap dB", "faults");
    }

    void print(const Sample& s) const
    {
        std::printf("  %-34s %10.1f %12.0f", s.name.c_str(), s.nsPerOp(), s.opsPerSec());
        if (s.bytes)
            std::printf(" %9.1f", s.mibPerSec());
        else
            std::printf(" %9s", "-");
        std::printf(" %10llu %12llu %+10lld %9lld", (unsigned long long)s.allocs,
                    (unsigned long long)s.allocBytes, s.heapDelta, s.faults);
        if (!s.note.empty())
            std::printf("  %s", s.note.c_str());
        std::printf("\n");
        std::fflush(stdout);
    }

    void finish() const
    {
        if (!json_.empty())
            writeJson();
        if (!csv_.empty())
            writeCsv();
    }

private:
    static std::string escape(const std::string& s)
    {
        std::string o;
        for (char c : s) {
            if (c == '"' || c == '\\')
                o += '\\';
            o += c;
        }
        return o;
    }

    void writeJson() const
    {
        FILE* f = std::fopen(json_.c_str(), "w");
        if (!f)
            return;
        std::fprintf(f, "{\"suite\":\"%s\",\"results\":[", escape(suite_).c_str());
        for (std::size_t i = 0; i < samples_.size(); ++i) {
            const Sample& s = samples_[i];
            std::fprintf(f,
                         "%s{\"name\":\"%s\",\"ops\":%llu,\"bytes\":%llu,\"median_ns\":%.0f,"
                         "\"min_ns\":%.0f,\"ns_per_op\":%.2f,\"ops_per_sec\":%.1f,\"allocs\":%llu,"
                         "\"alloc_bytes\":%llu,\"heap_delta\":%lld,\"faults\":%lld,\"reps\":%zu}",
                         i ? "," : "", escape(s.name).c_str(), (unsigned long long)s.ops,
                         (unsigned long long)s.bytes, s.medianNs(), s.minNs(), s.nsPerOp(),
                         s.opsPerSec(), (unsigned long long)s.allocs,
                         (unsigned long long)s.allocBytes, s.heapDelta, s.faults, s.ns.size());
        }
        std::fprintf(f, "]}\n");
        std::fclose(f);
    }

    void writeCsv() const
    {
        FILE* f = std::fopen(csv_.c_str(), "w");
        if (!f)
            return;
        std::fprintf(f, "suite,name,ops,bytes,median_ns,min_ns,ns_per_op,ops_per_sec,allocs,alloc_bytes,heap_delta,faults\n");
        for (const Sample& s : samples_)
            std::fprintf(f, "%s,%s,%llu,%llu,%.0f,%.0f,%.2f,%.1f,%llu,%llu,%lld,%lld\n", suite_.c_str(),
                         s.name.c_str(), (unsigned long long)s.ops, (unsigned long long)s.bytes,
                         s.medianNs(), s.minNs(), s.nsPerOp(), s.opsPerSec(),
                         (unsigned long long)s.allocs, (unsigned long long)s.allocBytes,
                         s.heapDelta, s.faults);
        std::fclose(f);
    }

    std::string suite_;
    std::string json_, csv_;
    std::vector<Sample> samples_;
};

// --------------------------------------------------------------- scratch ---

class Scratch
{
public:
    explicit Scratch(const std::string& label)
    {
        std::random_device source;
        for (;;) {
            path_ = std::filesystem::temp_directory_path() /
                    (label + "-" + std::to_string(source()) + "-" + std::to_string(source()));
            if (std::filesystem::create_directory(path_))
                break;
        }
    }
    ~Scratch()
    {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    Scratch(const Scratch&) = delete;
    Scratch& operator=(const Scratch&) = delete;

    std::filesystem::path file(const std::string& name) const { return path_ / name; }
    const std::filesystem::path& dir() const noexcept { return path_; }

    std::uintmax_t sizeOf(const std::string& name) const
    {
        std::error_code ec;
        const auto n = std::filesystem::file_size(path_ / name, ec);
        return ec ? 0 : n;
    }

private:
    std::filesystem::path path_;
};

// ------------------------------------------------------------------ args ---

/// `--name value` and `--flag` parsing, just enough for a benchmark.
class Args
{
public:
    Args(int argc, char** argv)
    {
        for (int i = 1; i < argc; ++i)
            args_.emplace_back(argv[i]);
    }

    bool has(std::string_view flag) const
    {
        for (const std::string& a : args_)
            if (a == flag)
                return true;
        return false;
    }

    std::string get(std::string_view name, std::string fallback) const
    {
        for (std::size_t i = 0; i + 1 < args_.size(); ++i)
            if (args_[i] == name)
                return args_[i + 1];
        return fallback;
    }

    std::uint64_t getU(std::string_view name, std::uint64_t fallback) const
    {
        const std::string v = get(name, "");
        return v.empty() ? fallback : std::strtoull(v.c_str(), nullptr, 10);
    }

private:
    std::vector<std::string> args_;
};

inline std::string humanBytes(long long n)
{
    char buf[64];
    if (n >= (1ll << 30))
        std::snprintf(buf, sizeof buf, "%.2f GiB", double(n) / (1ll << 30));
    else if (n >= (1ll << 20))
        std::snprintf(buf, sizeof buf, "%.2f MiB", double(n) / (1ll << 20));
    else if (n >= 1024)
        std::snprintf(buf, sizeof buf, "%.1f KiB", double(n) / 1024);
    else
        std::snprintf(buf, sizeof buf, "%lld B", n);
    return buf;
}

}  // namespace bench

#ifdef BENCH_DEFINE_ALLOC_HOOKS
// Counting replacements for the plain forms of the global allocation
// functions. Every plain form is replaced together: the standard allows a
// block from the nothrow new to come back through the sized delete, and a
// partial replacement would mismatch.
namespace bench::detail {
inline void* countedAlloc(std::size_t size) noexcept
{
    allocCounters().count.fetch_add(1, std::memory_order_relaxed);
    allocCounters().bytes.fetch_add(size, std::memory_order_relaxed);
    return std::malloc(size ? size : 1);
}
inline void countedFree(void* p) noexcept
{
    if (p)
        allocCounters().frees.fetch_add(1, std::memory_order_relaxed);
    std::free(p);
}
}  // namespace bench::detail

void* operator new(std::size_t size)
{
    if (void* p = bench::detail::countedAlloc(size))
        return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size)
{
    if (void* p = bench::detail::countedAlloc(size))
        return p;
    throw std::bad_alloc();
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept
{
    return bench::detail::countedAlloc(size);
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept
{
    return bench::detail::countedAlloc(size);
}
void operator delete(void* p) noexcept { bench::detail::countedFree(p); }
void operator delete[](void* p) noexcept { bench::detail::countedFree(p); }
void operator delete(void* p, std::size_t) noexcept { bench::detail::countedFree(p); }
void operator delete[](void* p, std::size_t) noexcept { bench::detail::countedFree(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { bench::detail::countedFree(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { bench::detail::countedFree(p); }
#endif
