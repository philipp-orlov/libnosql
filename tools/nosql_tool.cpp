// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// nosql -- command line inspector for a libnosql store.
//
//   nosql stats   [-d NAME] <store>
//   nosql check             <store>
//   nosql list              <store>
//   nosql dump    [options] <store>
//   nosql compact <src> <dst>
//   nosql checkpoint        <store>
//   nosql ship    <segment-dir> <replica> <bundle>
//   nosql apply   <replica> <bundle>
//
// Every read-only command opens the store read-only and never creates it, so
// pointing this at the wrong path reports an error instead of leaving an empty
// file behind.

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "nosql/nosql.hpp"
#include "nosql/replication.hpp"

namespace {

using namespace nosql;

constexpr const char* kUsage =
    "nosql -- libnosql store inspector\n"
    "\n"
    "usage:\n"
    "  nosql stats   [-d NAME] <store>     geometry, and per-tree page accounting\n"
    "  nosql check              <store>    full structural integrity walk\n"
    "  nosql list               <store>    names of the sub-databases\n"
    "  nosql dump    [options]  <store>    print key/value pairs\n"
    "  nosql compact <src> <dst>           rebuild densely into a new file\n"
    "\n"
    "replication:\n"
    "  nosql checkpoint <store>            print the snapshot a replica must match\n"
    "  nosql ship <segment-dir> <replica> <bundle>\n"
    "                                      write the delta that advances <replica>\n"
    "  nosql apply <replica> <bundle>      apply a delta to a store nothing has open\n"
    "\n"
    "dump options:\n"
    "  -d NAME      sub-database to read (default: the unnamed main tree)\n"
    "  -p PREFIX    only keys starting with PREFIX\n"
    "  -n COUNT     stop after COUNT entries\n"
    "  -k           keys only\n"
    "  --hex        always hex-encode\n"
    "  --text       never hex-encode (escape non-printable bytes)\n"
    "\n"
    "exit status: 0 ok, 1 bad usage, 2 store error or failed integrity check\n"
    "             ship: 3 up to date, 4 fresh base required\n";

constexpr int kOk = 0;
constexpr int kUsageError = 1;
constexpr int kStoreError = 2;
/// `ship` uses its own status so a scheduled job can tell "nothing to do" and
/// "send a base image" apart from a failure without parsing stderr.
constexpr int kUpToDate = 3;
constexpr int kNeedBase = 4;

// ------------------------------------------------------------------------
// Byte rendering
// ------------------------------------------------------------------------

/// Renders arbitrary key/value bytes so that binary data cannot corrupt the
/// terminal, while leaving ordinary text readable.
class ByteWriter
{
public:
    enum class Mode
    {
        Auto,  ///< escape if mostly printable, hex-encode otherwise
        Text,
        Hex
    };

    explicit ByteWriter(Mode mode) noexcept : resolved_(mode) {}

    /// The first slice decides the encoding for every later one, so a column
    /// never switches representation halfway down a dump.
    void print(Slice s)
    {
        if (resolved_ == Mode::Auto)
            resolved_ = isMostlyPrintable(s) ? Mode::Text : Mode::Hex;
        if (resolved_ == Mode::Hex)
            printHex(s);
        else
            printEscaped(s);
    }

private:
    static bool isMostlyPrintable(Slice s) noexcept
    {
        if (s.empty())
            return true;
        std::size_t printable = 0;
        for (std::size_t i = 0; i < s.size(); ++i) {
            const auto c = static_cast<unsigned char>(s.chars()[i]);
            if (c >= 0x20 && c < 0x7f)
                ++printable;
        }
        return printable * 4 >= s.size() * 3;  // at least three quarters
    }

    static void printHex(Slice s)
    {
        for (std::size_t i = 0; i < s.size(); ++i)
            std::printf("%02x", static_cast<unsigned char>(s.chars()[i]));
    }

    static void printEscaped(Slice s)
    {
        for (std::size_t i = 0; i < s.size(); ++i) {
            const auto c = static_cast<unsigned char>(s.chars()[i]);
            if (c >= 0x20 && c < 0x7f && c != '\\')
                std::putchar(static_cast<int>(c));
            else if (c == '\\')
                std::fputs("\\\\", stdout);
            else
                std::printf("\\x%02x", c);
        }
    }

    Mode resolved_;
};

std::string humanBytes(std::uint64_t n)
{
    constexpr const char* kUnits[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    double v = static_cast<double>(n);
    int unit = 0;
    while (v >= 1024.0 && unit < 5) {
        v /= 1024.0;
        ++unit;
    }
    char buf[64];
    std::snprintf(buf, sizeof buf, unit == 0 ? "%.0f %s" : "%.1f %s", v, kUnits[unit]);
    return buf;
}

// ------------------------------------------------------------------------
// Store access
// ------------------------------------------------------------------------

/// Read-only open: never create, so a typo in the path is an error rather
/// than a brand new empty store.
Env openForReading(const std::string& path)
{
    return Env::configure().readOnly(true).createIfMissing(false).open(path);
}

/// Main tree first, then every named sub-database, in tree order.
std::vector<std::string> treeNames(Txn& t)
{
    std::vector<std::string> names;
    names.emplace_back();  // the unnamed main tree
    for (std::string& n : t.listDbs())
        names.push_back(std::move(n));
    return names;
}

void printTreeRow(Txn& t, const std::string& name)
{
    const TreeStats s = t.db(name).stats();
    const std::uint64_t pages = s.branchPages + s.leafPages + s.overflowPages;
    const std::uint64_t bytes = pages * s.pageSize;
    const double perEntry = s.entries ? double(bytes) / double(s.entries) : 0.0;
    std::printf("  %-20s %12" PRIu64 " %5u %9" PRIu64 " %9" PRIu64 " %9" PRIu64 " %10s %8.1f\n",
                name.empty() ? "(main)" : name.c_str(), s.entries, s.depth, s.branchPages,
                s.leafPages, s.overflowPages, humanBytes(bytes).c_str(), perEntry);
}

// ------------------------------------------------------------------------
// Commands
// ------------------------------------------------------------------------

int commandStats(const std::string& path, const std::string& only)
{
    Env store = openForReading(path);
    const EnvStats s = store.stats();

    std::printf("store       %s\n", store.path().string().c_str());
    std::printf("page size   %u B\n", s.pageSize);
    std::printf("file        %s\n", humanBytes(s.fileSize).c_str());
    std::printf("mapped      %s\n", humanBytes(s.mapSize).c_str());
    std::printf("used        %" PRIu64 " pages (%s)\n", s.usedPages,
                humanBytes(s.usedPages * s.pageSize).c_str());
    std::printf("free list   %" PRIu64 " pages (%s)\n", s.freePages,
                humanBytes(s.freePages * s.pageSize).c_str());
    std::printf("buffers     %s\n", humanBytes(s.bufferBytes).c_str());
    std::printf("dirty       %s\n", humanBytes(s.dirtyBytes).c_str());
    std::printf("captured    %" PRIu64 ", failures %" PRIu64 "\n", s.capturedTxnid, s.captureFailures);
    std::printf("last txn    %" PRIu64 "\n", s.lastTxn);
    std::printf("readers     %u", s.readers);
    if (s.oldestReader)
        std::printf(" (oldest snapshot %" PRIu64 ")", s.oldestReader);
    std::printf("\n\n");

    std::printf("  %-20s %12s %5s %9s %9s %9s %10s %8s\n", "tree", "entries", "depth", "branch",
                "leaf", "overflow", "bytes", "B/entry");
    store.read([&](Txn& t) {
        if (!only.empty()) {
            if (!t.hasDb(only))
                throw Error(ErrorCode::NotFound, "no such sub-database: " + only);
            printTreeRow(t, only);
            return;
        }
        for (const std::string& n : treeNames(t))
            printTreeRow(t, n);
    });
    return kOk;
}

int commandCheck(const std::string& path)
{
    Env store = openForReading(path);
    store.read([](Txn& t) { checkIntegrity(t); });
    std::printf("%s: integrity ok\n", path.c_str());
    return kOk;
}

int commandList(const std::string& path)
{
    Env store = openForReading(path);
    store.read([](Txn& t) {
        const std::vector<std::string> names = t.listDbs();
        if (names.empty()) {
            std::printf("(no named sub-databases)\n");
            return;
        }
        for (const std::string& n : names)
            std::printf("%s\n", n.c_str());
    });
    return kOk;
}

struct DumpOptions
{
    std::string name;
    std::string prefix;
    bool hasPrefix = false;
    bool keysOnly = false;
    std::uint64_t limit = 0;  ///< 0 means no limit
    ByteWriter::Mode mode = ByteWriter::Mode::Auto;
};

int commandDump(const std::string& path, const DumpOptions& opts)
{
    Env store = openForReading(path);
    ByteWriter keyWriter(opts.mode);
    ByteWriter valueWriter(opts.mode);
    std::uint64_t shown = 0;

    store.read([&](Txn& t) {
        if (!opts.name.empty() && !t.hasDb(opts.name))
            throw Error(ErrorCode::NotFound, "no such sub-database: " + opts.name);

        // A cursor rather than Db::prefix() so the limit can stop the walk
        // early without materialising anything.
        class Db d = t.db(opts.name);
        Cursor c = d.cursor();
        const Slice pfx(opts.prefix);
        bool ok = opts.hasPrefix ? c.seek(pfx) : c.first();
        for (; ok; ok = c.next()) {
            if (opts.hasPrefix && !c.key().startsWith(pfx))
                break;
            keyWriter.print(c.key());
            if (!opts.keysOnly) {
                std::fputs(" = ", stdout);
                valueWriter.print(c.value());
            }
            std::putchar('\n');
            if (opts.limit && ++shown >= opts.limit)
                break;
        }
    });
    return kOk;
}

int commandCompact(const std::string& src, const std::string& dst)
{
    compact(src, dst);
    const Env after = Env::configure().readOnly(true).createIfMissing(false).open(dst);
    std::printf("compacted %s -> %s (%s)\n", src.c_str(), dst.c_str(),
                humanBytes(after.stats().fileSize).c_str());
    return kOk;
}

int commandCheckpoint(const std::string& store)
{
    const Checkpoint c = checkpointOf(store);
    std::printf("txnid    %" PRIu64 "\nchecksum %" PRIu64 "\n", c.txnid, c.metaChecksum);
    return kOk;
}

int commandShip(const std::string& segments, const std::string& replica, const std::string& bundle)
{
    const Checkpoint at = checkpointOf(replica);
    const ShipLog log(segments);
    switch (log.extract(at, bundle)) {
        case ShipStatus::Ok: break;
        case ShipStatus::UpToDate:
            std::printf("up to date at txn %" PRIu64 "\n", at.txnid);
            return kUpToDate;
        case ShipStatus::NeedBase: {
            const ShipLog::Range r = log.available();
            std::fprintf(stderr,
                         "nosql: replica is at txn %" PRIu64
                         " but segments only go back to %" PRIu64 "; send a base image\n",
                         at.txnid, r.oldest);
            return kNeedBase;
        }
    }
    const BundleInfo info = inspectBundle(bundle);
    std::printf("%s: txn %" PRIu64 " -> %" PRIu64 ", %" PRIu64 " pages (%s)\n", bundle.c_str(),
                info.base.txnid, info.targetTxnid, info.pageCount,
                humanBytes(info.pageCount * info.pageSize).c_str());
    return kOk;
}

int commandApply(const std::string& replica, const std::string& bundle)
{
    const Checkpoint before = checkpointOf(replica);
    const Checkpoint after = applyBundle(replica, bundle);
    std::printf("%s: txn %" PRIu64 " -> %" PRIu64 "\n", replica.c_str(), before.txnid,
                after.txnid);
    return kOk;
}

// ------------------------------------------------------------------------
// Argument handling
// ------------------------------------------------------------------------

/// Walks argv, pulling option values off as they are recognised and leaving
/// the positional arguments behind.
class ArgReader
{
public:
    ArgReader(int argc, char** argv) : argc_(argc), argv_(argv) {}

    bool done() const noexcept { return index_ >= argc_; }
    std::string_view peek() const { return argv_[index_]; }
    std::string_view take() { return argv_[index_++]; }

    /// Consumes the value belonging to option `flag`, which is already taken.
    std::string takeValue(std::string_view flag)
    {
        if (done())
            throw Error(ErrorCode::InvalidArgument, std::string(flag) + " needs a value");
        return std::string(take());
    }

private:
    int argc_;
    char** argv_;
    int index_ = 0;
};

bool parseUnsigned(std::string_view text, std::uint64_t& out)
{
    if (text.empty())
        return false;
    std::uint64_t v = 0;
    for (const char c : text) {
        if (c < '0' || c > '9')
            return false;
        v = v * 10 + std::uint64_t(c - '0');
    }
    out = v;
    return true;
}

}  // namespace

int main(int argc, char** argv)
{
    if (argc < 2) {
        std::fputs(kUsage, stderr);
        return kUsageError;
    }

    ArgReader args(argc - 1, argv + 1);
    const std::string command(args.take());
    if (command == "-h" || command == "--help" || command == "help") {
        std::fputs(kUsage, stdout);
        return kOk;
    }

    try {
        if (command == "compact" || command == "ship" || command == "apply") {
            std::vector<std::string> positional;
            while (!args.done())
                positional.emplace_back(args.take());
            const std::size_t want = command == "ship" ? 3u : 2u;
            if (positional.size() != want)
                throw Error(ErrorCode::InvalidArgument,
                            command + " needs exactly " + std::to_string(want) + " paths");
            if (command == "compact")
                return commandCompact(positional[0], positional[1]);
            if (command == "apply")
                return commandApply(positional[0], positional[1]);
            return commandShip(positional[0], positional[1], positional[2]);
        }

        DumpOptions opts;
        std::string path;
        while (!args.done()) {
            const std::string_view arg = args.take();
            if (arg == "-d")
                opts.name = args.takeValue(arg);
            else if (arg == "-p") {
                opts.prefix = args.takeValue(arg);
                opts.hasPrefix = true;
            } else if (arg == "-n") {
                const std::string value = args.takeValue(arg);
                if (!parseUnsigned(value, opts.limit))
                    throw Error(ErrorCode::InvalidArgument, "-n needs a non-negative count");
            } else if (arg == "-k")
                opts.keysOnly = true;
            else if (arg == "--hex")
                opts.mode = ByteWriter::Mode::Hex;
            else if (arg == "--text")
                opts.mode = ByteWriter::Mode::Text;
            else if (!arg.empty() && arg[0] == '-')
                throw Error(ErrorCode::InvalidArgument, "unknown option: " + std::string(arg));
            else if (path.empty())
                path = arg;
            else
                throw Error(ErrorCode::InvalidArgument, "unexpected argument: " + std::string(arg));
        }

        if (path.empty())
            throw Error(ErrorCode::InvalidArgument, "no store path given");

        if (command == "stats")
            return commandStats(path, opts.name);
        if (command == "check")
            return commandCheck(path);
        if (command == "list")
            return commandList(path);
        if (command == "dump")
            return commandDump(path, opts);
        if (command == "checkpoint")
            return commandCheckpoint(path);

        std::fprintf(stderr, "nosql: unknown command '%s'\n\n", command.c_str());
        std::fputs(kUsage, stderr);
        return kUsageError;
    } catch (const Error& e) {
        std::fprintf(stderr, "nosql: %s\n", e.what());
        return e.code() == ErrorCode::InvalidArgument ? kUsageError : kStoreError;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "nosql: %s\n", e.what());
        return kStoreError;
    }
}
