// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Page-checksum throughput per page size: how long verifying one committed
// page takes on this machine, which bounds every read that validates.
//
//   bench_checksum [--reps R] [--json out.json]

#define BENCH_DEFINE_ALLOC_HOOKS
#include "bench_util.hpp"

#include "nosql/internal/checksum.hpp"

int main(int argc, char** argv)
{
    bench::Args args(argc, argv);
    const int reps = int(args.getU("--reps", 5));

    std::printf("libnosql bench_checksum: backend %s\n\n", nosql::internal::checksumBackend());
    bench::Report report("nosql-checksum");
    if (const std::string j = args.get("--json", ""); !j.empty())
        report.jsonPath(j);
    report.header();

    std::vector<std::byte> buffer(1u << 20);
    std::mt19937_64 rng(7);
    for (std::byte& b : buffer)
        b = std::byte(rng());

    volatile std::uint64_t sink = 0;
    for (const std::size_t size : {16u, 64u, 240u, 512u, 1024u, 4096u, 8192u, 16384u, 65536u, 1048576u}) {
        const std::uint64_t iterations = std::max<std::uint64_t>(1000, (256u << 20) / size);
        char name[64];
        std::snprintf(name, sizeof name, "checksum64 %zu B", size);
        report.run(name, iterations, iterations * size, reps, [&] {
            std::uint64_t acc = 0;
            for (std::uint64_t i = 0; i < iterations; ++i)
                acc ^= nosql::internal::checksum64(buffer.data() + (i & 63) * 16, size);
            sink = acc;
        });
    }

    {
        const std::uint64_t iterations = 1u << 16;
        report.run("streaming 4096 B in 64 B chunks", iterations, iterations * 4096, reps, [&] {
            std::uint64_t acc = 0;
            for (std::uint64_t i = 0; i < iterations; ++i) {
                nosql::internal::Checksum64 h;
                for (std::size_t off = 0; off < 4096; off += 64)
                    h.update(buffer.data() + off, 64);
                acc ^= h.digest();
            }
            sink = acc;
        });
    }
    report.finish();
    return sink == 1 ? 1 : 0;
}
