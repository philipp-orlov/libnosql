// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The five-minute tour: open a store, write, read, iterate.
//
//   ./example_quickstart [path]

#include <cstdio>
#include <string>

#include "nosql/nosql.hpp"

int main(int argc, char** argv)
{
    const std::filesystem::path path = argc > 1 ? argv[1] : "quickstart.db";

    // Everything about the store is configured up front, fluently.
    nosql::Env store = nosql::Env::configure()
                           .pageSize(4096)
                           .maxSize(1ull << 30)  // 1 GiB ceiling
                           .maxDbs(16)
                           .open(path);

    // A write transaction commits when the lambda returns and rolls back if it
    // throws. There is exactly one writer at a time; readers never block.
    store.write([](nosql::Txn& t) {
        auto users = t.db("users", nosql::DbFlags::Create);
        users.put("alice", "admin");
        users.put("bob", "editor");
        users.put("carol", "viewer");

        // Nested transactions are ordinary savepoints.
        t.nested([](nosql::Txn& scratch) { scratch.db("users").put("dave", "intern"); });

        nosql::Txn undone = t.nested();
        undone.db("users").put("mallory", "root");
        undone.abort();  // never happened
    });

    store.read([](nosql::Txn& t) {
        auto users = t.db("users");
        std::printf("%llu users\n", (unsigned long long)users.count());

        // A miss is an empty optional, not an exception.
        if (auto role = users.get("alice"))
            std::printf("alice is %s\n", role->chars());
        std::printf("mallory present: %s\n", users.contains("mallory") ? "yes" : "no");

        // Ranges are half-open and iterate in key order.
        for (auto [name, role] : users.all())
            std::printf("  %-8s %s\n", name.string().c_str(), role.string().c_str());

        std::printf("names in [b, d):\n");
        for (auto [name, role] : users.between("b", "d"))
            std::printf("  %-8s %s\n", name.string().c_str(), role.string().c_str());
    });

    // Explicit transactions work too, and abort if you never commit.
    {
        nosql::Txn t = store.writeTxn();
        auto users = t.db("users");
        users.erase("bob");
        t.commit();
    }

    // Integer keys compare as numbers, not bytes.
    store.write([](nosql::Txn& t) {
        auto events = t.db("events", nosql::DbFlags::Create | nosql::DbFlags::IntegerKey);
        for (std::uint64_t id = 1; id <= 5; ++id)
            events.put(nosql::Slice::ref(id), "event #" + std::to_string(id));
    });

    store.read([](nosql::Txn& t) {
        auto c = t.db("events").cursor();
        for (bool ok = c.last(); ok; ok = c.prev())
            std::printf("event %llu -> %s\n", (unsigned long long)c.key().as<std::uint64_t>(),
                        c.value().string().c_str());
    });

    const nosql::EnvStats s = store.stats();
    std::printf("\n%llu pages in use, %llu free, last txn %llu, file %.1f KiB\n",
                (unsigned long long)s.usedPages, (unsigned long long)s.freePages,
                (unsigned long long)s.lastTxn, s.fileSize / 1024.0);
    return 0;
}
