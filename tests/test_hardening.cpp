// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Hardening of the file layer: creation mode, descriptors, symlinks, and (in later
// tests) filesystem policy, preallocation and failure handling.

#include <fcntl.h>
#include <linux/falloc.h>
#include <sys/stat.h>
#include <unistd.h>

#include "nosql/internal/io_observer.hpp"
#include "nosql/internal/os.hpp"
#ifndef NOSQL_TEST_NO_BLOB
#include "nosql/blob_storage.hpp"
#endif
#include "nosql/nosql.hpp"
#include "tests/test_util.hpp"

using namespace nosql;
using internal::os::IoEvent;
using internal::os::ScopedIoObserver;

namespace {
mode_t modeOf(const std::filesystem::path& path)
{
    struct stat st;
    if (::stat(path.c_str(), &st) != 0)
        return 0;
    return st.st_mode & 07777;
}
}  // namespace

TEST(newStoreIsOwnerOnlyByDefault)
{
    const mode_t previous = ::umask(0);
    tst::Scratch scratch("mode-default");
    {
        Env env = Env::configure().open(scratch.file());
        env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    }
    ::umask(previous);
    CHECK_EQ(unsigned(modeOf(scratch.file())), 0600u);
}

TEST(fileModeOptionAppliesExactlyWhateverTheUmask)
{
    const mode_t previous = ::umask(077);
    tst::Scratch scratch("mode-option");
    {
        Env env = Env::configure().fileMode(0640).open(scratch.file());
        env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    }
    ::umask(previous);
    CHECK_EQ(unsigned(modeOf(scratch.file())), 0640u);
}

TEST(existingStoreKeepsItsMode)
{
    tst::Scratch scratch("mode-existing");
    {
        Env env = Env::configure().open(scratch.file());
        env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    }
    CHECK(::chmod(scratch.file().c_str(), 0644) == 0);
    {
        Env env = Env::configure().open(scratch.file());
        env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("value")); });
    }
    CHECK_EQ(unsigned(modeOf(scratch.file())), 0644u);
}

TEST(symlinkedStorePathIsRefused)
{
    tst::Scratch scratch("symlink");
    {
        Env env = Env::configure().open(scratch.file("real.db"));
        env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    }
    std::filesystem::create_symlink(scratch.file("real.db"), scratch.file("link.db"));
    CHECK_THROWS(Env::configure().open(scratch.file("link.db")), ErrorCode::InvalidArgument);
    CHECK_THROWS(Env::configure().readOnly().open(scratch.file("link.db")), ErrorCode::InvalidArgument);
}

TEST(storeDescriptorDoesNotLeakIntoChildren)
{
    tst::Scratch scratch("cloexec");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    const std::string wanted = std::filesystem::canonical(scratch.file()).string();
    bool found = false;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code ec;
        const auto target = std::filesystem::read_symlink(entry.path(), ec);
        if (ec || target != wanted)
            continue;
        found = true;
        const int fd = std::stoi(entry.path().filename().string());
        CHECK((::fcntl(fd, F_GETFD) & FD_CLOEXEC) != 0);
    }
    CHECK(found);
}

TEST(networkFilesystemTypesAreRecognised)
{
    for (unsigned long type : {0x6969ul, 0x517Bul, 0xFF534D42ul, 0x65735546ul, 0x01021997ul})
        CHECK(internal::os::isNetworkFilesystemType(type));
    // ext4, xfs, btrfs, tmpfs, overlayfs
    for (unsigned long type : {0xEF53ul, 0x58465342ul, 0x9123683Eul, 0x01021994ul, 0x794C7630ul})
        CHECK(!internal::os::isNetworkFilesystemType(type));

    tst::Scratch scratch("localfs");
    Env env = Env::configure().open(scratch.file());  // the temp directory must be accepted
    env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
}

TEST(growthReservesDiskBlocks)
{
    tst::Scratch scratch("prealloc");
    // A filesystem without fallocate leaves the file sparse; nothing to check there.
    const auto probe = scratch.file("probe");
    {
        const int fd = ::open(probe.c_str(), O_RDWR | O_CREAT, 0600);
        CHECK(fd >= 0);
        const bool supported = ::fallocate(fd, 0, 0, 1 << 20) == 0;
        ::close(fd);
        if (!supported)
            return;
    }
    Env env = Env::configure().initialSize(4u << 20).open(scratch.file());
    struct stat st;
    CHECK(::stat(scratch.file().c_str(), &st) == 0);
    CHECK(st.st_size >= (4 << 20));
    CHECK(std::uint64_t(st.st_blocks) * 512 >= std::uint64_t(st.st_size));

    // The sparse behaviour is still available.
    Env sparse = Env::configure().preallocate(false).initialSize(4u << 20).open(scratch.file("sparse.db"));
    CHECK(::stat(scratch.file("sparse.db").c_str(), &st) == 0);
    CHECK(std::uint64_t(st.st_blocks) * 512 < std::uint64_t(st.st_size) / 2);
}

TEST(diskFullAtGrowthIsAnErrorAndTheStoreSurvives)
{
    tst::Scratch scratch("enospc");
    Env env = Env::configure().initialSize(64u << 10).growthStep(64u << 10).open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("small", "value"); });
    {
        ScopedIoObserver fault([](IoEvent event, std::size_t) {
            if (event == IoEvent::Reserve)
                throw Error(ErrorCode::IoError, "injected: no space left on device");
        });
        const std::string big(2u << 20, 'x');
        CHECK_THROWS(env.write([&](Txn& txn) { txn.mainDb().put("big", big); }), ErrorCode::IoError);
    }
    env.read([](Txn& txn) {
        CHECK_EQ(txn.mainDb().at("small").string(), std::string("value"));
        CHECK(!txn.mainDb().contains("big"));
    });
    const std::string big(2u << 20, 'y');
    env.write([&](Txn& txn) { txn.mainDb().put("big", big); });
    env.read([&](Txn& txn) { CHECK_EQ(txn.mainDb().at("big").string(), big); });
}

TEST(fileTruncatedBehindTheProcessIsAnErrorNotSigbus)
{
    tst::Scratch scratch("truncate");
    Env env = Env::configure().initialSize(1u << 20).open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    CHECK(::truncate(scratch.file().c_str(), 4096) == 0);
    CHECK_THROWS(env.readTxn(), ErrorCode::IoError);
    CHECK_THROWS(env.writeTxn(), ErrorCode::IoError);
}

TEST(failedSyncPoisonsTheEnvironmentUntilReopened)
{
    tst::Scratch scratch("sync-failure");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    {
        ScopedIoObserver fault([](IoEvent event, std::size_t) {
            if (event == IoEvent::Sync)
                throw Error(ErrorCode::IoError, "injected fsync failure");
        });
        CHECK_THROWS(env.sync(true), ErrorCode::IoError);
    }
    // The retry would succeed and could report data durable that the kernel dropped.
    CHECK_THROWS(env.writeTxn(), ErrorCode::BadTransaction);
    CHECK_THROWS(env.readTxn(), ErrorCode::BadTransaction);
    env.close();
    env = Env::configure().open(scratch.file());
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("value")); });
}

TEST(failedCommitBarrierPoisonsTheEnvironmentUntilReopened)
{
    tst::Scratch scratch("commit-failure");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "old"); });
    {
        ScopedIoObserver fault([](IoEvent event, std::size_t) {
            if (event == IoEvent::DataBarrier)
                throw Error(ErrorCode::IoError, "injected data barrier failure");
        });
        CHECK_THROWS(env.write([](Txn& txn) { txn.mainDb().put("key", "new"); }), ErrorCode::IoError);
    }
    CHECK_THROWS(env.write([](Txn& txn) { txn.mainDb().put("key", "again"); }), ErrorCode::BadTransaction);
    env.close();
    env = Env::configure().open(scratch.file());
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("old")); });
}

#ifndef NOSQL_TEST_NO_BLOB
TEST(failedArchiveSyncStopsFurtherAppends)
{
    tst::Scratch scratch("blob-sync-failure");
    Env env = Env::configure().open(scratch.file());
    auto archive = BlobStorage::open(env, "images", BlobStorage::Options().syncOnAppend());
    archive.put("a", "a.bin", std::string(1000, 'a'));
    {
        ScopedIoObserver fault([](IoEvent event, std::size_t) {
            if (event == IoEvent::Sync)
                throw Error(ErrorCode::IoError, "injected fsync failure");
        });
        CHECK_THROWS(archive.put("b", "b.bin", std::string(1000, 'b')), ErrorCode::IoError);
    }
    CHECK_THROWS(archive.put("c", "c.bin", std::string(1000, 'c')), ErrorCode::BadTransaction);
    CHECK_THROWS(archive.sync(), ErrorCode::BadTransaction);
}
#endif

#ifdef NOSQL_NO_REPLICATION
TEST(shipToIsRefusedWhenReplicationIsCompiledOut)
{
    tst::Scratch scratch("no-replication");
    CHECK_THROWS(Env::configure().shipTo(scratch.file("ship")).open(scratch.file()),
                 ErrorCode::Unsupported);
    Env env = Env::configure().open(scratch.file());  // plain use is unaffected
    env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("value")); });
}
#endif

TEST(reachingMaxSizeIsARefusalNotAFault)
{
    tst::Scratch scratch("maxsize");
    const std::uint64_t cap = 1u << 20;
    Env env = Env::configure().maxSize(cap).open(scratch.file());
    const std::string payload(700, 'x');
    bool full = false;
    int stored = 0;
    for (int i = 0; i < 100000 && !full; ++i) {
        try {
            env.write([&](Txn& txn) { txn.mainDb().put("k" + std::to_string(i), payload); });
            ++stored;
        } catch (const Error& e) {
            CHECK(e.code() == ErrorCode::MapFull);
            full = true;
        }
    }
    CHECK(full);
    CHECK(stored > 0);
    CHECK(std::filesystem::file_size(scratch.file()) <= cap);

    // The store is not poisoned: it reads, and it takes a write that fits.
    env.read([&](Txn& txn) { CHECK_EQ(txn.mainDb().at("k0").string(), payload); });
    env.write([&](Txn& txn) { txn.mainDb().erase("k0"); });
}

TEST(sharedHandlesReadConcurrentlyAndKeepTheStoreOpen)
{
    tst::Scratch scratch("share");
    Env writer = Env::configure().open(scratch.file());
    writer.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    Env reader = writer.share();
    CHECK_EQ(reader.path(), writer.path());
    reader.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("value")); });

    // A commit through one handle is visible through the other.
    writer.write([](Txn& txn) { txn.mainDb().put("key", "newer"); });
    reader.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("newer")); });

    // The store lives as long as any handle does.
    writer.close();
    reader.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("newer")); });
    Env closed;
    CHECK_THROWS(closed.share(), ErrorCode::InvalidArgument);
}

int main() { return tst::runAll("hardening"); }
