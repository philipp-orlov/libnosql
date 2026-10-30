// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/internal/io_observer.hpp"
#include "nosql/internal/checksum.hpp"
#include "examples/scratch.hpp"

int main(int argc, char** argv)
{
    const std::size_t entries = argc > 1 ? std::stoull(argv[1]) : 10000;
    const std::size_t batch = argc > 2 ? std::stoull(argv[2]) : 100;
    const unsigned repeats = argc > 3 ? unsigned(std::stoul(argv[3])) : 3;
    if (!entries || !batch || !repeats)
        return 2;
    std::vector<std::string> keys(entries);
    for (std::size_t index = 0; index < entries; ++index) {
        auto text = std::to_string(index);
        keys[index] = std::string(16 - text.size(), '0') + text;
    }
    std::shuffle(keys.begin(), keys.end(), std::mt19937_64(20260906));
    std::cout << "repeat,entries,batch,seed,seconds,p50_us,p95_us,p99_us,data_barriers,meta_barriers,read_bytes,write_bytes,file_bytes,checksum_backend\n";
    for (unsigned repeat = 0; repeat < repeats; ++repeat) {
        Scratch scratch("baseline");
        auto env = nosql::Env::configure().open(scratch.file("store"));
        std::uint64_t data = 0, meta = 0, reads = 0, writes = 0;
        std::vector<double> latency;
        const std::string value(100, 'v');
        const auto start = std::chrono::steady_clock::now();
        {
            nosql::internal::os::ScopedIoObserver observer([&](auto event, std::size_t bytes) {
                using nosql::internal::os::IoEvent;
                data += event == IoEvent::DataBarrier;
                meta += event == IoEvent::MetaBarrier;
                if (event == IoEvent::Read) reads += bytes;
                if (event == IoEvent::Write) writes += bytes;
            });
            for (std::size_t offset = 0; offset < entries; offset += batch) {
                const auto began = std::chrono::steady_clock::now();
                env.write([&](nosql::Txn& txn) {
                    auto db = txn.mainDb();
                    for (std::size_t index = offset; index < std::min(entries, offset + batch); ++index)
                        db.put(keys[index], value);
                });
                latency.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - began).count());
            }
        }
        const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        std::sort(latency.begin(), latency.end());
        const auto percentile = [&](double fraction) { return latency[std::size_t(fraction * (latency.size() - 1))]; };
        std::cout << repeat << ',' << entries << ',' << batch << ",20260906," << seconds << ','
                  << percentile(.50) << ',' << percentile(.95) << ',' << percentile(.99) << ','
                  << data << ',' << meta << ',' << reads << ',' << writes << ',' << env.stats().fileSize
                  << ',' << nosql::internal::checksumBackend() << '\n';
        env.read([](nosql::Txn& txn) { nosql::checkIntegrity(txn); });
    }
}