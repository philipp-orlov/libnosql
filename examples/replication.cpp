// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <iostream>

#include "nosql/replication.hpp"
#include "examples/scratch.hpp"

int main()
{
    try {
        Scratch scratch("replication");
        auto primary = nosql::Env::configure().shipTo(scratch.file("segments"))
            .shipRetain(2).shipSegmentSize(1).open(scratch.file("primary.db"));
        primary.write([](nosql::Txn& txn) { txn.mainDb().put("job", "created"); });
        {
            auto snapshot = primary.readTxn();
            nosql::copySnapshot(snapshot, scratch.file("replica.db"));
        }
        nosql::ShipLog log(scratch.file("segments"));
        primary.write([](nosql::Txn& txn) { txn.mainDb().put("job", "complete"); });
        const auto base = nosql::checkpointOf(scratch.file("replica.db"));
        if (log.extract(base, scratch.file("delta.bundle")) != nosql::ShipStatus::Ok)
            return 1;
        const auto applied = nosql::applyBundle(scratch.file("replica.db"), scratch.file("delta.bundle"));
        if (applied != nosql::checkpointOf(scratch.file("primary.db")))
            return 1;
        std::cout << "Replica advanced to transaction " << applied.txnid << '\n';
        for (int index = 0; index < 5; ++index)
            primary.write([&](nosql::Txn& txn) { txn.mainDb().put("job", std::to_string(index)); });
        if (log.extract(applied, scratch.file("delta.bundle")) != nosql::ShipStatus::NeedBase)
            return 1;
        std::filesystem::remove(scratch.file("replica.db"));
        {
            auto snapshot = primary.readTxn();
            nosql::copySnapshot(snapshot, scratch.file("replica.db"));
        }
        auto replica = nosql::Env::configure().readOnly().open(scratch.file("replica.db"));
        replica.read([](nosql::Txn& txn) { nosql::checkIntegrity(txn); });
        std::cout << "Retention gap recovered with a fresh snapshot\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}