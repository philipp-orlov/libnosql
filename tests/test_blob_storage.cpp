// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <string>
#include <vector>

#include "tests/test_util.hpp"
#include "nosql/blob_storage.hpp"
#include "nosql/internal/checksum.hpp"

using namespace nosql;

namespace {

std::string blob(std::size_t n, unsigned seed)
{
    std::string s(n, '\0');
    for (std::size_t i = 0; i < n; ++i)
        s[i] = char('a' + ((seed * 7919 + i * 31) % 26));
    return s;
}

std::string nameOf(unsigned i)
{
    char buf[32];
    std::snprintf(buf, sizeof buf, "%08u.bin", i);
    return buf;
}

/// True when the file is a tar archive GNU tar would accept: every member's
/// header checksum verifies and the chain lands exactly on the zero marker.
bool wellFormedTar(const std::filesystem::path& p)
{
    std::FILE* f = std::fopen(p.string().c_str(), "rb");
    if (!f)
        return false;
    std::fseek(f, 0, SEEK_END);
    const long end = std::ftell(f);
    long off = 0;
    bool ok = false;
    while (off + 512 <= end) {
        unsigned char h[512];
        std::fseek(f, off, SEEK_SET);
        if (std::fread(h, 1, 512, f) != 512)
            break;
        bool zero = true;
        for (int i = 0; i < 512 && zero; ++i)
            zero = h[i] == 0;
        if (zero) {
            ok = true;  // reached the end-of-archive marker
            break;
        }
        unsigned sum = 0;
        for (int i = 0; i < 512; ++i)
            sum += (i >= 148 && i < 156) ? unsigned(' ') : unsigned(h[i]);
        unsigned want = 0;
        for (int i = 148; i < 156; ++i) {
            if (h[i] == 0 || h[i] == ' ')
                break;
            want = (want << 3) | unsigned(h[i] - '0');
        }
        if (sum != want)
            break;
        unsigned long long size = 0;
        for (int i = 124; i < 136; ++i) {
            if (h[i] == 0 || h[i] == ' ')
                break;
            size = (size << 3) | (unsigned long long)(h[i] - '0');
        }
        off += 512 + long((size + 511) / 512 * 512);
    }
    std::fclose(f);
    return ok;
}

TEST(putAndReadBackZeroCopy)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");

    const std::string payload = blob(9000, 1);
    tar.put("key1", "one.bin", payload);

    Blob b = tar.at("key1");
    CHECK(b.valid());
    CHECK_EQ(b.name(), std::string_view("one.bin"));
    CHECK_EQ(b.size(), std::uint64_t(payload.size()));
    CHECK_EQ(b.data().string(), payload);
    CHECK(b.verify());
    // Payload starts one 512-byte header past a block boundary.
    CHECK(b.hasChecksum());
    CHECK(b.storedChecksum() != 0);
    CHECK_EQ(b.offset() % 512, std::uint64_t(0));
    CHECK(!tar.find("missing").valid());
}

TEST(blobVerificationUsesAll64ChecksumBits)
{
    tst::Scratch scratch("blob-checksum");
    Env env = Env::configure().open(scratch.file());
    auto blobs = BlobStorage::open(env, "images");
    const std::string body = "123456789";
    blobs.put("key", "member", body);
    CHECK_EQ(blobs.at("key").storedChecksum(), internal::checksum64(body.data(), body.size()));
    env.write([](Txn& txn) {
        auto db = txn.db("images");
        auto value = db.at("key").string();
        value[31] = char(static_cast<unsigned char>(value[31]) ^ 0x80u);
        db.put("key", value);
    });
    CHECK(!blobs.at("key").verify());
    CHECK(blobs.at("key").hasChecksum());
}

TEST(archiveNameDerivesFromTheSubDatabase)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    {
        auto named = BlobStorage::open(e, "images");
        CHECK_EQ(named.archivePath().filename().string(), std::string("images.tar"));
        CHECK_EQ(named.archivePath().parent_path(), s.file().parent_path());
    }
    {
        auto main = BlobStorage::open(e);
        CHECK_EQ(main.archivePath().filename().string(), s.file().stem().string() + ".tar");
    }
}

TEST(archiveDirectoryAndNameCanBeOverridden)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    const std::filesystem::path side = s.file().parent_path() / "blobs";
    auto tar = BlobStorage::open(
        e, "images", BlobStorage::Options().directory(side).fileName("pool.tar"));
    CHECK_EQ(tar.archivePath(), side / "pool.tar");
    tar.put("k", "a.bin", "hello");
    CHECK_EQ(tar.at("k").data().string(), std::string("hello"));
    CHECK(std::filesystem::exists(side / "pool.tar"));
}

TEST(manyBlobsRoundTrip)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");

    constexpr unsigned kN = 400;
    std::vector<std::string> payloads;
    {
        auto w = tar.beginWrite();
        for (unsigned i = 0; i < kN; ++i) {
            payloads.push_back(blob(64 + (i * 37) % 5000, i));
            w.add(tst::keyOf(int(i)), nameOf(i), payloads.back());
        }
        w.commit();
    }
    CHECK_EQ(tar.count(), std::uint64_t(kN));
    for (unsigned i = 0; i < kN; ++i) {
        Blob b = tar.at(tst::keyOf(int(i)));
        const std::string want = nameOf(i);
        CHECK_EQ(b.data().string(), payloads[i]);
        CHECK_EQ(b.name(), std::string_view(want));
        CHECK(b.verify());
    }
}

TEST(archiveStaysReadableByStandardTools)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    for (unsigned i = 0; i < 10; ++i)
        tar.put(tst::keyOf(int(i)), nameOf(i), blob(1000 + i, i));
    tar.finalize();
    CHECK(wellFormedTar(tar.archivePath()));

    // ...and finalize() pads to tar(1)'s 10 KiB record size.
    const auto sz = std::filesystem::file_size(tar.archivePath());
    CHECK_EQ(sz % 10240, std::uintmax_t(0));
}

TEST(listWalksTheArchiveWithoutTheIndex)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    for (unsigned i = 0; i < 5; ++i)
        tar.put(tst::keyOf(int(i)), nameOf(i), blob(700, i));

    const std::vector<TarEntry> entries = tar.list();
    CHECK_EQ(entries.size(), std::size_t(5));
    for (unsigned i = 0; i < 5; ++i) {
        CHECK_EQ(entries[i].name, nameOf(i));
        CHECK_EQ(entries[i].size, std::uint64_t(700));
    }
}

TEST(indexRebuildsFromTheArchive)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    for (unsigned i = 0; i < 20; ++i)
        tar.put(nameOf(i), nameOf(i), blob(300 + i, i));

    const std::uint64_t n = tar.rebuildIndex();
    CHECK_EQ(n, std::uint64_t(20));
    CHECK_EQ(tar.count(), std::uint64_t(20));
    for (unsigned i = 0; i < 20; ++i) {
        Blob b = tar.at(nameOf(i));
        CHECK_EQ(b.data().string(), blob(300 + i, i));
        // A header-only rebuild cannot recover checksums.
        CHECK(!b.hasChecksum());
        CHECK_EQ(b.storedChecksum(), std::uint64_t(0));
        CHECK(b.verify());
    }
}

TEST(abandonedWriterLeavesNothingHalfVisible)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    tar.put("kept", "kept.bin", "durable");

    {
        auto w = tar.beginWrite();
        w.add("dropped", "dropped.bin", "never indexed");
        // no commit: the bytes reach the archive, the index never learns
    }
    CHECK(!tar.find("dropped").valid());
    CHECK_EQ(tar.at("kept").data().string(), std::string("durable"));

    // The orphaned member is still in the archive, and catchUp() adopts it.
    const std::uint64_t added = tar.catchUp();
    CHECK_EQ(added, std::uint64_t(1));
    CHECK_EQ(tar.at("dropped.bin").data().string(), std::string("never indexed"));
}

TEST(reopenSeesEverythingAndAppendsAfterIt)
{
    tst::Scratch s("tar");
    std::string first, second;
    {
        Env e = Env::configure().open(s.file());
        auto tar = BlobStorage::open(e, "images");
        first = blob(4096, 3);
        tar.put("a", "a.bin", first);
        tar.finalize();
    }
    {
        Env e = Env::configure().open(s.file());
        auto tar = BlobStorage::open(e, "images");
        CHECK_EQ(tar.at("a").data().string(), first);
        second = blob(8192, 4);
        tar.put("b", "b.bin", second);
        CHECK_EQ(tar.at("a").data().string(), first);
        CHECK_EQ(tar.at("b").data().string(), second);
        CHECK_EQ(tar.list().size(), std::size_t(2));
        tar.finalize();
        CHECK(wellFormedTar(tar.archivePath()));
    }
}

TEST(eraseDropsTheIndexEntryOnly)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    tar.put("gone", "gone.bin", "bytes");
    CHECK(tar.contains("gone"));
    CHECK(tar.erase("gone"));
    CHECK(!tar.contains("gone"));
    CHECK_EQ(tar.list().size(), std::size_t(1));  // still in the archive
}

TEST(largeBlobsSpanManyBlocks)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    const std::string big = blob(2u << 20, 9);  // 2 MiB
    tar.put("big", "big.bin", big);
    Blob b = tar.at("big");
    CHECK_EQ(b.size(), std::uint64_t(big.size()));
    CHECK_EQ(b.data().string(), big);
    CHECK(b.verify());
    tar.finalize();
    CHECK(wellFormedTar(tar.archivePath()));
}

TEST(emptyAndOddSizedMembersKeepAlignment)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    tar.put("empty", "empty.bin", Slice("", 0));
    tar.put("one", "one.bin", "x");
    tar.put("exact", "exact.bin", blob(512, 2));
    tar.put("odd", "odd.bin", blob(513, 2));

    CHECK_EQ(tar.at("empty").size(), std::uint64_t(0));
    CHECK_EQ(tar.at("one").data().string(), std::string("x"));
    CHECK_EQ(tar.at("exact").data().string(), blob(512, 2));
    CHECK_EQ(tar.at("odd").data().string(), blob(513, 2));
    for (const TarEntry& en : tar.list())
        CHECK_EQ(en.offset % 512, std::uint64_t(0));
    tar.finalize();
    CHECK(wellFormedTar(tar.archivePath()));
}

TEST(readsSurviveArchiveGrowth)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    const std::string early = blob(5000, 11);
    tar.put("early", "early.bin", early);

    Blob held = tar.at("early");  // pins the mapping it was read through
    for (unsigned i = 0; i < 200; ++i)
        tar.put(tst::keyOf(int(i)), nameOf(i), blob(3000, i));

    // The old mapping stayed alive under the growth, so this is still valid.
    CHECK_EQ(held.data().string(), early);
    CHECK_EQ(tar.at("early").data().string(), early);
}

TEST(rejectsUnusableMemberNames)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    CHECK_THROWS(tar.put("k", "", "v"), ErrorCode::InvalidArgument);
    CHECK_THROWS(tar.put("k", std::string(101, 'x'), "v"), ErrorCode::InvalidArgument);
}

TEST(membersCarryTheCurrentTimestamp)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    const auto before = std::uint64_t(std::time(nullptr));
    tar.put("k", "stamped.bin", "payload");

    // Read the mtime straight out of the header tar(1) would show.
    std::FILE* f = std::fopen(tar.archivePath().string().c_str(), "rb");
    CHECK(f != nullptr);
    char field[12] = {};
    CHECK_EQ(std::fseek(f, 136, SEEK_SET), 0);
    CHECK_EQ(std::fread(field, 1, sizeof field, f), std::size_t(sizeof field));
    std::fclose(f);

    std::uint64_t mtime = 0;
    for (char c : field) {
        if (c == '\0' || c == ' ')
            break;
        mtime = (mtime << 3) | std::uint64_t(c - '0');
    }
    const auto after = std::uint64_t(std::time(nullptr));
    CHECK(mtime >= before);
    CHECK(mtime <= after);
}


TEST(freshStorageReadsAsEmpty)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    CHECK_EQ(tar.count(), std::uint64_t(0));
    CHECK(!tar.find("nothing").valid());
    CHECK(!tar.contains("nothing"));
    CHECK_EQ(tar.indexedUpTo(), std::uint64_t(0));
    std::vector<Slice> keys = {Slice("a"), Slice("b")};
    const std::vector<Blob> got = tar.findMany(keys);
    CHECK_EQ(got.size(), std::size_t(2));
    CHECK(!got[0].valid() && !got[1].valid());
    tar.warm();  // nothing to map; must not throw
}

TEST(findManyKeepsInputOrderAndMarksMisses)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    constexpr unsigned kN = 300;
    {
        auto w = tar.beginWrite();
        for (unsigned i = 0; i < kN; ++i)
            w.add(tst::keyOf(i), nameOf(i), blob(100 + i, i));
        w.commit();
    }
    // A shuffled batch with two misses mixed in.
    std::vector<std::string> owned;
    for (unsigned i : {217u, 3u, 150u, 299u, 0u, 42u})
        owned.push_back(tst::keyOf(i));
    owned.insert(owned.begin() + 2, "missing-a");
    owned.push_back("missing-b");
    std::vector<Slice> keys(owned.begin(), owned.end());

    std::vector<Blob> out(keys.size());
    const std::size_t found = e.read([&](Txn& t) { return tar.findMany(t, keys.data(), keys.size(), out.data()); });
    CHECK_EQ(found, std::size_t(6));
    CHECK_EQ(out[0].data().string(), blob(100 + 217, 217));
    CHECK_EQ(out[1].data().string(), blob(100 + 3, 3));
    CHECK(!out[2].valid());
    CHECK_EQ(out[3].data().string(), blob(100 + 150, 150));
    CHECK_EQ(out[4].data().string(), blob(100 + 299, 299));
    CHECK_EQ(out[5].data().string(), blob(100 + 0, 0));
    CHECK_EQ(out[6].data().string(), blob(100 + 42, 42));
    CHECK(!out[7].valid());
    CHECK_EQ(out[6].name(), std::string_view(nameOf(42)));
    // Blobs outlive the snapshot they came from.
    CHECK(out[0].verify());

    // The convenience form agrees, and a single key degenerates cleanly.
    const std::vector<Blob> again = tar.findMany(keys);
    for (std::size_t i = 0; i < keys.size(); ++i)
        CHECK_EQ(again[i].valid(), out[i].valid());
    const std::vector<Blob> one = tar.findMany({Slice(owned[5])});
    CHECK_EQ(one[0].data().string(), blob(100, 0));
}

TEST(bufferedWriterCoalescesSmallAndBypassesLarge)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    // A small buffer so the test crosses its boundary many times and exercises
    // every path: queued, flushed-because-full, and too-large-to-queue (which
    // starts at a thirty-second of the buffer: 8 KiB here).
    auto tar = BlobStorage::open(e, "images", BlobStorage::Options().writeBuffer(256 * 1024));

    std::vector<std::string> payloads;
    {
        auto w = tar.beginWrite();
        for (unsigned i = 0; i < 200; ++i) {
            std::size_t n;
            switch (i % 5) {
            case 0: n = 0; break;                  // empty member
            case 1: n = 1 + (i * 37) % 700; break;  // sub-block
            case 2: n = 4096; break;               // a few blocks
            case 3: n = 9000 + i; break;           // just over the direct threshold
            default: n = 400000 + i; break;        // far bigger than the buffer: direct
            }
            payloads.push_back(blob(n, i));
            w.add(tst::keyOf(i), nameOf(i), payloads.back());
        }
        // Nothing is visible before commit, however much has already landed.
        CHECK(!tar.find(tst::keyOf(0)).valid());
        w.commit();
    }
    CHECK_EQ(tar.count(), std::uint64_t(200));
    for (unsigned i = 0; i < 200; ++i) {
        Blob b = tar.at(tst::keyOf(i));
        CHECK_EQ(b.size(), std::uint64_t(payloads[i].size()));
        CHECK_EQ(b.data().string(), payloads[i]);
        CHECK_EQ(b.offset() % 512, std::uint64_t(0));
        CHECK(b.verify());
    }
    // The bytes on disk are one ordinary tar, in append order.
    const std::vector<TarEntry> entries = tar.list();
    CHECK_EQ(entries.size(), std::size_t(200));
    for (unsigned i = 0; i < 200; ++i)
        CHECK_EQ(entries[i].name, nameOf(i));
    tar.finalize();
    CHECK(wellFormedTar(tar.archivePath()));
}

TEST(writeBufferZeroWritesThrough)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images", BlobStorage::Options().writeBuffer(0));
    auto w = tar.beginWrite();
    w.add("k", "k.bin", blob(3000, 1));
    // Unbuffered: the member is physically there before commit.
    CHECK_EQ(tar.list().size(), std::size_t(1));
    w.commit();
    CHECK_EQ(tar.at("k").data().string(), blob(3000, 1));
}

TEST(abandonedBufferedWriterStillReachesTheArchive)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");  // default 4 MiB buffer: nothing flushes on its own
    {
        auto w = tar.beginWrite();
        for (unsigned i = 0; i < 50; ++i)
            w.add(tst::keyOf(i), nameOf(i), blob(2000, i));
        // dropped without commit
    }
    CHECK_EQ(tar.count(), std::uint64_t(0));
    CHECK_EQ(tar.list().size(), std::size_t(50));  // the destructor flushed
    CHECK_EQ(tar.catchUp(), std::uint64_t(50));
    CHECK_EQ(tar.at(nameOf(7)).data().string(), blob(2000, 7));
}

TEST(writerOverwrittenByMoveAssignmentFlushesToo)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    BlobStorage::Writer w = tar.beginWrite();
    w.add("k", "k.bin", blob(3000, 1));
    w = BlobStorage::Writer();  // abandon by overwriting, not by scope exit
    CHECK_EQ(tar.list().size(), std::size_t(1));
    CHECK_EQ(tar.catchUp(), std::uint64_t(1));
    CHECK_EQ(tar.at("k.bin").data().string(), blob(3000, 1));
}

TEST(writerJoinsACallerTransaction)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");

    // Blob index and a row of metadata land under one commit.
    {
        Txn t = e.writeTxn();
        auto w = tar.beginWrite(t);
        w.add("sample-1", "sample-1.bin", blob(5000, 1));
        w.add("sample-2", "sample-2.bin", blob(6000, 2));
        w.commit();  // records the index in t, does not commit t
        CHECK(!tar.find("sample-1").valid());
        t.db("meta", DbFlags::Create).put("sample-1", "label=cat");
        t.commit();
    }
    CHECK_EQ(tar.at("sample-1").data().string(), blob(5000, 1));
    CHECK_EQ(tar.at("sample-2").data().string(), blob(6000, 2));
    CHECK_EQ(tar.indexedUpTo(), tar.archiveSize());
    e.read([&](Txn& t) { CHECK_EQ(t.db("meta").at("sample-1").string(), std::string("label=cat")); });

    // Aborting the caller's transaction drops the index entries but not the
    // bytes -- the same state an abandoned Writer leaves.
    {
        Txn t = e.writeTxn();
        auto w = tar.beginWrite(t);
        w.add("sample-3", "sample-3.bin", blob(700, 3));
        w.commit();
        t.abort();
    }
    CHECK(!tar.find("sample-3").valid());
    CHECK_EQ(tar.list().size(), std::size_t(3));
    CHECK_EQ(tar.catchUp(), std::uint64_t(1));
    CHECK_EQ(tar.at("sample-3.bin").data().string(), blob(700, 3));

    // A read transaction is refused up front.
    Txn r = e.readTxn();
    CHECK_THROWS(tar.beginWrite(r), ErrorCode::InvalidArgument);
}

TEST(prefetchAndAccessHintsChangeNothingVisible)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    std::vector<std::string> payloads;
    {
        auto tar = BlobStorage::open(e, "images", BlobStorage::Options().accessPattern(BlobStorage::Access::Random));
        auto w = tar.beginWrite();
        for (unsigned i = 0; i < 64; ++i) {
            payloads.push_back(blob(3000 + i * 100, i));
            w.add(tst::keyOf(i), nameOf(i), payloads.back());
        }
        w.commit();

        std::vector<std::string> owned;
        for (unsigned i : {5u, 6u, 7u, 40u, 1u})
            owned.push_back(tst::keyOf(i));
        owned.push_back("absent");
        std::vector<Slice> keys(owned.begin(), owned.end());
        const std::size_t found = e.read([&](Txn& t) { return tar.prefetch(t, keys.data(), keys.size()); });
        CHECK_EQ(found, std::size_t(5));
        const std::vector<Blob> blobs = tar.findMany(keys);
        tar.prefetch(blobs.data(), blobs.size());
        tar.warm();
        for (unsigned i = 0; i < 5; ++i)
            CHECK(blobs[i].verify());
        CHECK_EQ(blobs[3].data().string(), payloads[40]);
    }
    for (auto access : {BlobStorage::Access::Normal, BlobStorage::Access::Sequential}) {
        auto tar = BlobStorage::open(e, "images", BlobStorage::Options().accessPattern(access));
        tar.warm();
        for (unsigned i = 0; i < 64; ++i)
            CHECK_EQ(tar.at(tst::keyOf(i)).data().string(), payloads[i]);
    }
}

TEST(forEachKeyWalksInOrderAndByPrefix)
{
    tst::Scratch s("tar");
    Env e = Env::configure().open(s.file());
    auto tar = BlobStorage::open(e, "images");
    {
        auto w = tar.beginWrite();
        for (unsigned i = 0; i < 20; ++i)
            w.add(tst::keyOf(i, i % 2 ? "odd" : "even"), nameOf(i), "x");
        w.commit();
    }
    std::vector<std::string> all, odd;
    e.read([&](Txn& t) {
        CHECK_EQ(tar.forEachKey(t, Slice(), [&](Slice k) { all.push_back(k.string()); return true; }),
                 std::uint64_t(20));
        CHECK_EQ(tar.forEachKey(t, "odd:", [&](Slice k) { odd.push_back(k.string()); return true; }),
                 std::uint64_t(10));
        // Stopping early counts what was visited.
        CHECK_EQ(tar.forEachKey(t, Slice(), [&](Slice) { return false; }), std::uint64_t(1));
    });
    CHECK(std::is_sorted(all.begin(), all.end()));
    CHECK_EQ(all.size(), std::size_t(20));
    for (const std::string& k : odd)
        CHECK(k.rfind("odd:", 0) == 0);
}

TEST(readOnlyOpenersShareAStoreNobodyWrites)
{
    tst::Scratch s("tar");
    {
        Env e = Env::configure().open(s.file());
        auto tar = BlobStorage::open(e, "images");
        for (unsigned i = 0; i < 10; ++i)
            tar.put(tst::keyOf(i), nameOf(i), blob(1000 + i, i));
        tar.finalize();
    }
    // Two independent read-only handles -- what two training workers hold --
    // open the same file at once, and neither writes a byte.
    Env a = Env::configure().readOnly().open(s.file());
    Env b = Env::configure().readOnly().open(s.file());
    auto ta = BlobStorage::open(a, "images", BlobStorage::Options().readOnly());
    auto tb = BlobStorage::open(b, "images", BlobStorage::Options().readOnly());
    const auto before = std::filesystem::file_size(ta.archivePath());
    CHECK_EQ(ta.count(), std::uint64_t(10));
    CHECK_EQ(tb.count(), std::uint64_t(10));
    for (unsigned i = 0; i < 10; ++i) {
        CHECK_EQ(ta.at(tst::keyOf(i)).data().string(), blob(1000 + i, i));
        CHECK_EQ(tb.at(tst::keyOf(i)).data().string(), blob(1000 + i, i));
    }
    CHECK_EQ(ta.indexedUpTo(), ta.archiveSize());
    CHECK_THROWS(ta.put("k", "k.bin", "v"), ErrorCode::ReadOnly);
    CHECK_EQ(std::filesystem::file_size(ta.archivePath()), before);
}

TEST(readOnlyOpenerOfAStoreWithoutAnIndexSeesNothing)
{
    tst::Scratch s("tar");
    {
        Env e = Env::configure().open(s.file());
        e.write([](Txn& t) { t.db("other", DbFlags::Create).put("k", "v"); });
        // Create an archive by hand: no BlobStorage ever opened this store.
        std::FILE* f = std::fopen((s.file().parent_path() / "images.tar").string().c_str(), "wb");
        CHECK(f != nullptr);
        std::fclose(f);
    }
    Env e = Env::configure().readOnly().open(s.file());
    auto tar = BlobStorage::open(e, "images", BlobStorage::Options().readOnly());
    CHECK_EQ(tar.count(), std::uint64_t(0));
    CHECK(!tar.find("k").valid());
    CHECK(!tar.contains("k"));
    CHECK_EQ(tar.indexedUpTo(), std::uint64_t(0));
    e.read([&](Txn& t) { CHECK_EQ(tar.forEachKey(t, Slice(), [](Slice) { return true; }), std::uint64_t(0)); });
}

}  // namespace

int main()
{
    return tst::runAll("tar");
}
