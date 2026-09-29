// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Checkpoint inspection, base images, and bundle apply.
//
// Everything here works on files, not on open environments: a replica is a
// store that nothing has mounted, and the whole point of the design is that
// applying a delta needs no knowledge of the tree, only of page numbers.
#include "nosql/replication.hpp"

#include <cstring>
#include <utility>
#include <vector>

#include "nosql/internal/replication_format.hpp"
#include "nosql/internal/atomic_file.hpp"
#include "nosql/internal/core.hpp"
#include "nosql/internal/checksum.hpp"
#include "nosql/internal/os.hpp"

namespace nosql {

using internal::BundleHeader;
using internal::checksum64;
using internal::Checksum64;
using internal::kBundleMagic;
using internal::kMaxPageSize;
using internal::kMinPageSize;
using internal::kPageHdr;
using internal::kReplicationVersion;
using internal::Meta;
using internal::metaSlotFor;
using internal::metaValid;
using internal::PageNo;
using internal::PageRecord;
namespace os = internal::os;

namespace {

std::string describe(const Checkpoint& c)
{
    return "txn " + std::to_string(c.txnid) + "/" + std::to_string(c.metaChecksum);
}

/// Newest meta page that validates. Reads the file rather than a mapping and
/// takes no lock: a concurrent commit can only make us see the older of the
/// two slots, which is a consistent snapshot by construction, and a torn slot
/// fails its own checksum.
Meta readNewestMeta(os::FileHandle fd, const std::filesystem::path& what)
{
    const std::uint64_t bytes = os::fileSize(fd);
    Meta best{};
    bool have = false;
    for (std::uint64_t ps = kMinPageSize; ps <= kMaxPageSize; ps *= 2) {
        for (unsigned slot = 0; slot < 2; ++slot) {
            const std::uint64_t off = std::uint64_t(slot) * ps + kPageHdr;
            if (off + sizeof(Meta) > bytes)
                continue;
            Meta m{};
            os::readAt(fd, off, &m, sizeof m);
            if (m.pageSize != ps || !metaValid(m, ps))
                continue;
            if (!have || m.txnid > best.txnid) {
                best = m;
                have = true;
            }
        }
    }
    if (!have)
        throw Error(ErrorCode::Corrupted, "no usable meta page in " + what.string());
    return best;
}

/// Header, trailing meta, and the file offset of every page image, validated
/// end to end. Deliberately a separate pass from the writing: a bundle that
/// fails any check must not have touched the store.
struct BundleScan
{
    BundleHeader header{};
    internal::BundleIdentity identity{};
    Meta target{};
};

BundleHeader readHeader(os::FileHandle fd, const std::filesystem::path& what, std::uint64_t bytes)
{
    if (bytes < sizeof(BundleHeader) + sizeof(internal::BundleIdentity) + sizeof(Meta))
        throw Error(ErrorCode::Corrupted, "truncated bundle: " + what.string());
    BundleHeader h{};
    os::readAt(fd, 0, &h, sizeof h);
    if (std::memcmp(h.magic, kBundleMagic, sizeof kBundleMagic) != 0)
        throw Error(ErrorCode::Corrupted, "not a replication bundle: " + what.string());
    if (h.version != kReplicationVersion)
        throw Error(ErrorCode::Incompatible,
                    "bundle format version " + std::to_string(h.version) + ", expected " +
                        std::to_string(kReplicationVersion));
    if (!internal::legalPageSize(h.pageSize))
        throw Error(ErrorCode::Corrupted, "bad page size in bundle: " + what.string());
    if (h.flags != 0)
        throw Error(ErrorCode::Unsupported, "bundle uses unknown features: " + what.string());
    return h;
}

BundleScan scanBundle(os::FileHandle fd, const std::filesystem::path& what)
{
    const std::uint64_t bytes = os::fileSize(fd);
    BundleScan s;
    s.header = readHeader(fd, what, bytes);
    os::readAt(fd, sizeof(BundleHeader), &s.identity, sizeof s.identity);
    const std::uint64_t payloadEnd = bytes - sizeof(Meta);
    const std::uint64_t payloadStart = sizeof(BundleHeader) + sizeof(internal::BundleIdentity);
    const std::uint64_t recordBytes = sizeof(PageRecord) + s.header.pageSize;
    if (s.header.pageCount != (payloadEnd - payloadStart) / recordBytes ||
        (payloadEnd - payloadStart) % recordBytes != 0 ||
        s.header.targetTxnid <= s.header.baseTxnid)
        throw Error(ErrorCode::Corrupted, "invalid bundle size or transaction range");

    os::readAt(fd, payloadEnd, &s.target, sizeof s.target);
    if (!metaValid(s.target, s.header.pageSize) || s.target.txnid != s.header.targetTxnid ||
        s.target.storeId != s.identity.storeId || s.target.commitId == internal::Identity{})
        throw Error(ErrorCode::Corrupted, "bundle target meta failed validation: " + what.string());
    if (s.target.fileSize % s.header.pageSize != 0 || s.target.lastPgno < 1 ||
        s.target.lastPgno >= s.target.fileSize / s.header.pageSize ||
        s.target.lastPgno > internal::kMaxPgno)
        throw Error(ErrorCode::Corrupted, "invalid bundle target geometry");

    std::vector<std::byte> image(s.header.pageSize);
    BundleHeader checkedHeader = s.header;
    checkedHeader.payloadChecksum = 0;
    Checksum64 checksum;
    checksum.update(&checkedHeader, sizeof checkedHeader);
    checksum.update(&s.identity, sizeof s.identity);
    std::uint64_t off = sizeof(BundleHeader) + sizeof(internal::BundleIdentity);
    PageNo previous = 1;
    for (std::uint64_t i = 0; i < s.header.pageCount; ++i) {
        PageRecord rec{};
        if (off + sizeof rec > payloadEnd)
            throw Error(ErrorCode::Corrupted, "truncated bundle: " + what.string());
        os::readAt(fd, off, &rec, sizeof rec);
        if (rec.byteLength != s.header.pageSize || rec.reserved != 0)
            throw Error(ErrorCode::Corrupted, "bad page record length in " + what.string());
        if (off + sizeof rec + rec.byteLength > payloadEnd)
            throw Error(ErrorCode::Corrupted, "truncated bundle: " + what.string());
        // Pages 0 and 1 are the meta slots; a payload record for one would
        // publish a checkpoint before its pages were durable.
        if (rec.pgno < 2 || rec.pgno > s.target.lastPgno ||
            rec.pgno <= previous)
            throw Error(ErrorCode::Corrupted,
                        "bundle page " + std::to_string(rec.pgno) + " out of range");
        os::readAt(fd, off + sizeof rec, image.data(), image.size());
        if (checksum64(image.data(), image.size()) != rec.checksum)
            throw Error(ErrorCode::Corrupted,
                        "bundle page " + std::to_string(rec.pgno) + " failed its checksum");
        checksum.update(&rec, sizeof rec);
        checksum.update(image.data(), image.size());
        previous = rec.pgno;
        off += sizeof rec + rec.byteLength;
    }
    if (off != payloadEnd)
        throw Error(ErrorCode::Corrupted, "trailing bytes in bundle: " + what.string());
    checksum.update(&s.target, sizeof s.target);
    if (checksum.digest() != s.header.payloadChecksum)
        throw Error(ErrorCode::Corrupted, "bundle payload failed its checksum: " + what.string());
    return s;
}

}  // namespace

// ------------------------------------------------------------- checkpoints ---

Checkpoint checkpointOf(const std::filesystem::path& store)
{
    const os::File file(os::openFile(store, true, false, false, /*lock=*/false));
    const Meta m = readNewestMeta(file, store);
    return Checkpoint{m.txnid, m.checksum, m.storeId, m.commitId};
}

void copySnapshot(Txn& txn, const std::filesystem::path& dst)
{
    internal::TxnImpl* t = txn.impl_.get();
    if (t == nullptr || t->finished)
        throw Error(ErrorCode::BadTransaction, "copySnapshot needs a live transaction");
    if (!t->readOnly)
        throw Error(ErrorCode::BadTransaction,
                    "copySnapshot needs a read transaction: a write transaction's meta is not "
                    "committed and has no checkpoint");

    const Meta& m = t->meta;
    const std::size_t ps = t->env->pageSize;
    const std::byte* base = t->region->m.base;
    // Two meta pages always exist even before the first commit allocates
    // anything above them.
    const std::uint64_t pages = std::max<std::uint64_t>(m.lastPgno + 1, 2);

    os::File file(os::openFile(dst, false, true, /*exclusiveCreate=*/true));
    try {
        // The file is grown to exactly what the snapshot recorded, not to
        // what its pages need: the meta is copied verbatim so its checksum
        // still matches, and fileSize is one of the fields it covers.
        os::resizeFile(file, m.fileSize);

        constexpr std::uint64_t kChunk = 4u << 20;
        for (std::uint64_t off = 0, end = pages * ps; off < end;) {
            const std::uint64_t n = std::min(kChunk, end - off);
            os::writeAt(file, off, base + off, std::size_t(n));
            off += n;
        }
        // Both slots get this snapshot's meta. The other slot may hold a
        // newer one on the source whose pages we did not copy, and formatStore
        // already establishes that two identical slots are a valid store.
        for (unsigned slot = 0; slot < 2; ++slot)
            os::writeAt(file, std::uint64_t(slot) * ps + kPageHdr, &m, sizeof m);
        os::syncFile(file, true);
    } catch (...) {
        file.close();
        std::error_code ignored;
        std::filesystem::remove(dst, ignored);
        throw;
    }
}

// ----------------------------------------------------------------- bundles ---

BundleInfo inspectBundle(const std::filesystem::path& bundle)
{
    const os::File file(os::openFile(bundle, true, false, false, /*lock=*/false));
    const std::uint64_t bytes = os::fileSize(file);
    const BundleHeader h = readHeader(file, bundle, bytes);
    Meta target{};
    os::readAt(file, bytes - sizeof(Meta), &target, sizeof target);

    BundleInfo info;
    info.pageSize = h.pageSize;
    info.base = Checkpoint{h.baseTxnid, h.baseMetaChecksum};
    internal::BundleIdentity identity{};
    os::readAt(file, sizeof(BundleHeader), &identity, sizeof identity);
    info.base.storeId = identity.storeId;
    info.base.commitId = identity.baseCommitId;
    info.targetTxnid = h.targetTxnid;
    info.pageCount = h.pageCount;
    info.fileSize = target.fileSize;
    return info;
}

Checkpoint applyBundle(const std::filesystem::path& store, const std::filesystem::path& bundle)
{
    const os::File bundleFile(os::openFile(bundle, true, false, false));
    const BundleScan scan = scanBundle(bundleFile, bundle);

    // Exclusive: this is also what enforces "the store must not be open".
    const os::File storeFile(os::openFile(store, false, false, false, true, true));
    const Meta cur = readNewestMeta(storeFile, store);
    const Checkpoint here{cur.txnid, cur.checksum, cur.storeId, cur.commitId};

    if (cur.pageSize != scan.header.pageSize)
        throw Error(ErrorCode::Incompatible,
                    "bundle page size " + std::to_string(scan.header.pageSize) +
                        " does not match store page size " + std::to_string(cur.pageSize));

    const Checkpoint target{scan.target.txnid, scan.target.checksum,
                            scan.target.storeId, scan.target.commitId};
    if (here == target)
        return here;  // already applied; re-running a bundle is a no-op
    if (here != Checkpoint{scan.header.baseTxnid, scan.header.baseMetaChecksum,
                           scan.identity.storeId, scan.identity.baseCommitId})
        throw Error(ErrorCode::Incompatible, "bundle applies onto " +
                                                 describe({scan.header.baseTxnid,
                                                           scan.header.baseMetaChecksum}) +
                                                 " but " + store.string() + " is at " +
                                                 describe(here));

    const std::size_t ps = scan.header.pageSize;
    internal::AtomicFile replacement(store);
    replacement.copyFrom(storeFile);
    os::resizeFile(replacement.fd(), scan.target.fileSize);

    std::vector<std::byte> image(ps);
    BundleHeader checked = scan.header;
    checked.payloadChecksum = 0;
    Checksum64 checksum;
    checksum.update(&checked, sizeof checked);
    checksum.update(&scan.identity, sizeof scan.identity);
    std::uint64_t offset = sizeof(BundleHeader) + sizeof(internal::BundleIdentity);
    PageNo previous = 1;
    for (std::uint64_t index = 0; index < scan.header.pageCount; ++index) {
        PageRecord record{};
        os::readAt(bundleFile, offset, &record, sizeof record);
        os::readAt(bundleFile, offset + sizeof record, image.data(), image.size());
        if (record.pgno <= previous || record.pgno > scan.target.lastPgno || record.byteLength != ps ||
            record.reserved != 0 || checksum64(image.data(), image.size()) != record.checksum)
            throw Error(ErrorCode::Corrupted, "bundle changed during apply");
        checksum.update(&record, sizeof record);
        checksum.update(image.data(), image.size());
        os::writeAt(replacement.fd(), std::uint64_t(record.pgno) * ps, image.data(), image.size());
        previous = record.pgno;
        offset += sizeof record + ps;
    }
    checksum.update(&scan.target, sizeof scan.target);
    if (checksum.digest() != scan.header.payloadChecksum)
        throw Error(ErrorCode::Corrupted, "bundle changed during apply");
    for (unsigned slot = 0; slot < 2; ++slot)
        os::writeAt(replacement.fd(), std::uint64_t(slot) * ps + kPageHdr,
                    &scan.target, sizeof scan.target);
    replacement.publish();
    return target;
}

}  // namespace nosql
