// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/replication.hpp"
#include "nosql/internal/io_observer.hpp"
#include "nosql/internal/atomic_file.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
using internal::os::IoEvent;
using internal::os::ScopedIoObserver;

TEST(failedDataBarrierPreservesPreviousCheckpoint)
{
    tst::Scratch scratch("fault-commit");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "old"); });
    const auto before = checkpointOf(scratch.file());
    {
        ScopedIoObserver fault([](IoEvent event, std::size_t) {
            if (event == IoEvent::DataBarrier)
                throw Error(ErrorCode::IoError, "injected data barrier failure");
        });
        CHECK_THROWS(env.write([](Txn& txn) { txn.mainDb().put("key", "new"); }), ErrorCode::IoError);
    }
    env.close();
    CHECK_EQ(checkpointOf(scratch.file()), before);
    env = Env::configure().open(scratch.file());
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("old")); });
}

TEST(replacementFailuresLeaveEitherCompleteCheckpoint)
{
    for (const auto failure : {IoEvent::Write, IoEvent::WriteProgress, IoEvent::Resize,
                               IoEvent::Sync, IoEvent::Replace, IoEvent::Replaced}) {
        tst::Scratch scratch("fault-apply");
        Env primary = Env::configure().shipTo(scratch.file("ship")).open(scratch.file());
        primary.write([](Txn& txn) { txn.mainDb().put("key", "old"); });
        {
            auto reader = primary.readTxn();
            copySnapshot(reader, scratch.file("replica"));
        }
        const auto base = checkpointOf(scratch.file("replica"));
        for (unsigned index = 0; index < 12; ++index)
            primary.write([](Txn& txn) { txn.mainDb().put("key", "new"); });
        CHECK(ShipLog(scratch.file("ship")).extract(base, scratch.file("bundle")) == ShipStatus::Ok);
        {
            ScopedIoObserver fault([failure](IoEvent event, std::size_t) {
                if (event == failure)
                    throw Error(ErrorCode::IoError, "injected apply failure");
            });
            CHECK_THROWS(applyBundle(scratch.file("replica"), scratch.file("bundle")), ErrorCode::IoError);
        }
        Env replica = Env::configure().readOnly().open(scratch.file("replica"));
        replica.read([&](Txn& txn) {
            checkIntegrity(txn);
            CHECK_EQ(txn.mainDb().at("key").string(), std::string(failure == IoEvent::Replaced ? "new" : "old"));
        });
        replica.close();
        CHECK_EQ(applyBundle(scratch.file("replica"), scratch.file("bundle")), checkpointOf(scratch.file()));
    }
}

TEST(copyFallbackAndCloneKeepSourceIndependent)
{
    for (bool clone : {false, true}) {
        tst::Scratch scratch("clone");
        {
            auto env = Env::configure().open(scratch.file());
            env.write([](Txn& txn) { txn.mainDb().put("key", "old"); });
        }
        const auto source = internal::os::openFile(scratch.file(), true, false, false);
        try {
            internal::AtomicFile replacement(scratch.file());
            replacement.copyFrom(source, clone);
            const std::uint64_t junk = 0;
            internal::os::writeAt(replacement.fd(), 0, &junk, sizeof junk);
        } catch (...) {
            internal::os::closeFile(source);
            throw;
        }
        internal::os::closeFile(source);
        auto env = Env::configure().open(scratch.file());
        env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("old")); });
    }
}

TEST(interruptedBulkBuildNeverPublishesDestination)
{
    tst::Scratch scratch("bulk-fault");
    {
        Env env = Env::configure().open(scratch.file());
        env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    }
    {
        ScopedIoObserver observer([](IoEvent event, std::size_t) {
            if (event == IoEvent::WriteProgress)
                throw Error(ErrorCode::IoError, "injected partial bulk write");
        });
        CHECK_THROWS(compact(scratch.file(), scratch.file("copy")), ErrorCode::IoError);
    }
    CHECK(!std::filesystem::exists(scratch.file("copy")));
    compact(scratch.file(), scratch.file("copy"));
}

TEST(writeFailureCanLeaveAnActualPrefix)
{
    tst::Scratch scratch("short-write");
    const auto file = internal::os::openFile(scratch.file(), false, true, true);
    try {
        std::string bytes(128u << 10, 'x');
        {
            ScopedIoObserver observer([](IoEvent event, std::size_t) {
                if (event == IoEvent::WriteProgress)
                    throw Error(ErrorCode::IoError, "injected short write");
            });
            CHECK_THROWS(internal::os::writeAt(file, 0, bytes.data(), bytes.size()), ErrorCode::IoError);
        }
        CHECK(internal::os::fileSize(file) > 0);
        CHECK(internal::os::fileSize(file) < bytes.size());
    } catch (...) {
        internal::os::closeFile(file);
        throw;
    }
    internal::os::closeFile(file);
}

int main() { return tst::runAll("fault"); }