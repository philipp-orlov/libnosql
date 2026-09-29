// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <fstream>
#include <thread>

#include "nosql/replication.hpp"
#include "nosql/internal/format.hpp"
#include "tests/test_util.hpp"

using namespace nosql;

TEST(littleEndianFieldsHaveGoldenBytes)
{
    internal::Little<std::uint64_t> word(0x0807060504030201ull);
    const auto* bytes = reinterpret_cast<const unsigned char*>(&word);
    for (unsigned index = 0; index < 8; ++index)
        CHECK_EQ(bytes[index], index + 1);
    CHECK_EQ(internal::readLittle<std::uint64_t>(bytes), 0x0807060504030201ull);
    CHECK_EQ(sizeof(internal::Page), 16u);
    CHECK_EQ(internal::kFormatVersion, 6u);
    CHECK_EQ(internal::kPageChecksumBytes, 8u);
    CHECK_EQ(sizeof(internal::Meta), 480u);
    CHECK_EQ(offsetof(internal::Meta, catalogTree), 432u);
}

TEST(readsRejectCorruptedLeafAndOverflowPayload)
{
    for (std::size_t size : {10u, 10000u}) {
        tst::Scratch scratch("checksum");
        std::uint64_t offset = 0;
        {
            Env env = Env::configure().open(scratch.file());
            env.write([&](Txn& txn) { txn.mainDb().put("key", std::string(size, 'a')); });
        }
        std::fstream file(scratch.file(), std::ios::in | std::ios::out | std::ios::binary);
        internal::Meta meta{};
        file.seekg(4096 + internal::kPageHdr);
        file.read(reinterpret_cast<char*>(&meta), sizeof meta);
        std::vector<std::uint64_t> storage(4096 / 8);
        file.seekg(std::streamoff(std::uint64_t(meta.mainTree.root) * 4096));
        file.read(reinterpret_cast<char*>(storage.data()), 4096);
        const auto* page = reinterpret_cast<const internal::Page*>(storage.data());
        const auto* node = internal::lnode(page, 0);
        if (node->flags & internal::N_BIGDATA)
            offset = internal::bigPgno(node) * 4096 + internal::kPageHdr + 3;
        else
            offset = std::uint64_t(meta.mainTree.root) * 4096 + internal::slots(page)[0] + sizeof(*node) + node->ksize;
        file.seekp(std::streamoff(offset));
        file.put('b');
        file.close();
        Env env = Env::configure().readOnly().open(scratch.file());
        env.read([](Txn& txn) { CHECK_THROWS(txn.mainDb().get("key"), ErrorCode::Corrupted); });
    }
}

TEST(readChecksumCacheIsOptInAndPersistsUntilInvalidated)
{
    for (bool cached : {false, true}) {
        for (std::size_t size : {10u, 10000u}) {
            tst::Scratch scratch("checksum-cache");
            auto options = Env::configure();
            if (cached)
                options.cacheReadChecksums();
            Env env = options.open(scratch.file());
            env.write([&](Txn& txn) { txn.mainDb().put("key", std::string(size, 'a')); });
            std::fstream file(scratch.file(), std::ios::in | std::ios::out | std::ios::binary);
            internal::Meta meta{};
            file.seekg(4096 + internal::kPageHdr);
            file.read(reinterpret_cast<char*>(&meta), sizeof meta);
            std::vector<std::uint64_t> storage(4096 / 8);
            file.seekg(std::streamoff(std::uint64_t(meta.mainTree.root) * 4096));
            file.read(reinterpret_cast<char*>(storage.data()), 4096);
            const auto* page = reinterpret_cast<const internal::Page*>(storage.data());
            const auto* node = internal::lnode(page, 0);
            const auto offset = (node->flags & internal::N_BIGDATA)
                ? internal::bigPgno(node) * 4096 + internal::kPageHdr
                : std::uint64_t(meta.mainTree.root) * 4096 + internal::slots(page)[0] +
                      sizeof(*node) + node->ksize;
            const auto replace = [&](char value) {
                file.seekp(std::streamoff(offset));
                file.put(value);
                file.flush();
                CHECK(bool(file));
            };
            env.read([&](Txn& txn) {
                CHECK_EQ(txn.mainDb().at("key").string(), std::string(size, 'a'));
                replace('b');
                if (cached)
                    CHECK_EQ(txn.mainDb().at("key").data()[0], std::byte('b'));
                else
                    CHECK_THROWS(txn.mainDb().get("key"), ErrorCode::Corrupted);
                CHECK_THROWS(checkIntegrity(txn), ErrorCode::Corrupted);
            });
            // The check is remembered across transactions until it is dropped.
            env.read([&](Txn& txn) {
                if (cached) {
                    CHECK_EQ(txn.mainDb().at("key").data()[0], std::byte('b'));
                    env.invalidateReadCache();
                }
                CHECK_THROWS(txn.mainDb().get("key"), ErrorCode::Corrupted);
                replace('a');
                CHECK_EQ(txn.mainDb().at("key").string(), std::string(size, 'a'));
            });
            env.write([&](Txn& txn) {
                CHECK_EQ(txn.mainDb().at("key").string(), std::string(size, 'a'));
                replace('b');
                CHECK_THROWS(txn.mainDb().get("key"), ErrorCode::Corrupted);
                replace('a');
            });
        }
    }
}

namespace {

internal::Meta readMeta(const std::filesystem::path& path)
{
    std::ifstream file(path, std::ios::binary);
    internal::Meta a{}, b{};
    file.seekg(internal::kPageHdr);
    file.read(reinterpret_cast<char*>(&a), sizeof a);
    file.seekg(4096 + internal::kPageHdr);
    file.read(reinterpret_cast<char*>(&b), sizeof b);
    return a.txnid >= b.txnid ? a : b;
}

void flipByte(const std::filesystem::path& path, std::uint64_t offset)
{
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    file.seekg(std::streamoff(offset));
    const int c = file.get();
    file.seekp(std::streamoff(offset));
    file.put(char(c ^ 0x5a));
    file.flush();
}

}  // namespace

TEST(readChecksumCacheForgetsPagesTheWriterRewrites)
{
    tst::Scratch scratch("checksum-cache-writer");
    Env env = Env::configure().cacheReadChecksums().open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "one"); });
    const auto first = readMeta(scratch.file()).mainTree.root;
    // Verifying the leaf remembers its page number as checked.
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("one")); });

    // Each commit retires the previous leaf; within a few commits the free
    // list hands the first leaf's page number back out for new bytes.
    std::uint64_t reused = internal::kInvalidPage;
    for (int round = 0; round < 8 && reused == internal::kInvalidPage; ++round) {
        env.write([&](Txn& txn) { txn.mainDb().put("key", "round " + std::to_string(round)); });
        if (readMeta(scratch.file()).mainTree.root == first)
            reused = first;
    }
    if (reused == internal::kInvalidPage)
        return;  // the allocator chose otherwise; nothing to test here
    // The rewritten page is unverified again, so damage to it is caught even
    // though its page number was checked before.
    flipByte(scratch.file(), reused * 4096 + internal::kPageHdr + 40);
    env.read([](Txn& txn) { CHECK_THROWS(txn.mainDb().get("key"), ErrorCode::Corrupted); });
}

TEST(readChecksumCacheExpiresAfterTheRevalidationInterval)
{
    tst::Scratch scratch("checksum-cache-ttl");
    Env env = Env::configure()
                  .cacheReadChecksums()
                  .revalidateAfter(std::chrono::milliseconds(20))
                  .open(scratch.file());
    env.write([](Txn& txn) { txn.mainDb().put("key", "value"); });
    const auto root = readMeta(scratch.file()).mainTree.root;
    env.read([](Txn& txn) { CHECK_EQ(txn.mainDb().at("key").string(), std::string("value")); });
    flipByte(scratch.file(), root * 4096 + internal::kPageHdr + 40);
    // Still trusted right away ...
    env.read([](Txn& txn) { (void)txn.mainDb().get("key"); });
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    // ... and verified again once the interval has passed.
    env.read([](Txn& txn) { CHECK_THROWS(txn.mainDb().get("key"), ErrorCode::Corrupted); });
}

TEST(catalogIsIndependentOfMainKeyspace)
{
    tst::Scratch scratch("catalog");
    Env env = Env::configure().open(scratch.file());
    env.write([](Txn& txn) {
        txn.mainDb().put("same", "user");
        txn.db("same", DbFlags::Create).put("key", "named");
    });
    env.write([](Txn& txn) { txn.mainDb().clear(); });
    env.read([](Txn& txn) {
        CHECK_EQ(txn.mainDb().count(), 0u);
        CHECK_EQ(txn.listDbs().size(), 1u);
        CHECK_EQ(txn.db("same").at("key").string(), std::string("named"));
        checkIntegrity(txn);
    });
}

TEST(bulkBuildsMultipleLevelsAndOverflowRuns)
{
    tst::Scratch scratch("bulk-levels");
    {
        Env env = Env::configure().pageSize(512).sync(Durability::None).open(scratch.file());
        env.write([](Txn& txn) {
            auto db = txn.db("large", DbFlags::Create);
            for (unsigned index = 0; index < 5000; ++index)
                db.put(tst::keyOf(index), std::string(index % 13 == 0 ? 1300 : 80, 'v'));
            txn.db("empty", DbFlags::Create);
        });
    }
    compact(scratch.file(), scratch.file("copy"));
    Env env = Env::configure().open(scratch.file("copy"));
    env.read([](Txn& txn) {
        CHECK_EQ(txn.db("large").count(), 5000u);
        CHECK(txn.db("large").stats().depth >= 3);
        CHECK_EQ(txn.db("large").at(tst::keyOf(13)).size(), 1300u);
        CHECK_EQ(txn.db("empty").count(), 0u);
        checkIntegrity(txn);
    });
    env.write([](Txn& txn) { txn.db("large").erase(tst::keyOf(13)); });
    env.read([](Txn& txn) { checkIntegrity(txn); });
}

TEST(pageAndRunTrailersUseFullXxh3Digest)
{
    for (unsigned count : {1u, 3u}) {
        std::vector<std::uint64_t> storage(count * 4096 / 8);
        auto* page = reinterpret_cast<internal::Page*>(storage.data());
        page->pgno = 2;
        page->flags = count == 1 ? internal::P_LEAF : internal::P_OVERFLOW;
        if (count > 1) internal::setOvPages(page, count);
        internal::sealPage(page, count * 4096);
        const auto* bytes = reinterpret_cast<const std::byte*>(page);
        CHECK_EQ(internal::readLittle<std::uint64_t>(bytes + count * 4096 - 8),
                 internal::checksum64(bytes, count * 4096 - 8));
        internal::validatePage(page, 4096, count);
        storage.back() ^= std::uint64_t(1) << 63;
        CHECK_THROWS(internal::validatePage(page, 4096, count), ErrorCode::Corrupted);
    }
}

TEST(unsupportedFormatsAreRejected)
{
    for (unsigned version : {0u, internal::kFormatVersion + 1}) {
        tst::Scratch scratch("unsupported-format");
        { Env env = Env::configure().open(scratch.file()); }
        {
            std::fstream file(scratch.file(), std::ios::in | std::ios::out | std::ios::binary);
            for (unsigned slot = 0; slot < 2; ++slot) {
                internal::Meta meta{};
                file.seekg(slot * 4096 + internal::kPageHdr);
                file.read(reinterpret_cast<char*>(&meta), sizeof meta);
                meta.version = version;
                meta.checksum = internal::metaChecksum(meta);
                file.seekp(slot * 4096 + internal::kPageHdr);
                file.write(reinterpret_cast<const char*>(&meta), sizeof meta);
            }
        }
        CHECK_THROWS(Env::configure().open(scratch.file()), ErrorCode::Incompatible);
        CHECK_THROWS(Env::configure().readOnly().open(scratch.file()), ErrorCode::Incompatible);
        CHECK_THROWS(compact(scratch.file(), scratch.file("copy")), ErrorCode::Incompatible);
        CHECK(!std::filesystem::exists(scratch.file("copy")));
    }
}

int main() { return tst::runAll("format"); }