// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/internal/os.hpp"

#include <cstdlib>
#include <limits>
#include <new>
#include <string>

#include "base/status.hpp"
#include "nosql/error.hpp"
#include "sys/vm.hpp"

#ifdef _WIN32
#include <malloc.h>
#endif

namespace nosql::internal::os {
namespace {

ErrorCode toErrorCode(base::Code code) noexcept
{
    switch (code) {
        case base::Code::NotFound: return ErrorCode::NotFound;
        case base::Code::InvalidArgument: return ErrorCode::InvalidArgument;
        case base::Code::Unavailable: return ErrorCode::Busy;
        case base::Code::ResourceExhausted: return ErrorCode::OutOfMemory;
        default: return ErrorCode::IoError;
    }
}

// Runs a libsys call; a base::Error becomes the nosql::Error the store has always thrown.
template <class F>
decltype(auto) guarded(F&& f)
{
    try {
        return f();
    } catch (const base::Error& e) {
        throw Error(toErrorCode(e.code()), std::string(e.status().message()));
    }
}

struct alignas(64) PageAllocation
{
    std::size_t bytes;
    bool mapped;
};
constexpr std::size_t kMappedAllocationMin = 64u << 10;

}  // namespace

std::size_t systemPageSize() noexcept { return sys::systemPageSize(); }

FileHandle openFile(const std::filesystem::path& path, bool readOnly, bool create, bool exclusiveCreate, bool lock,
                    bool replaceable, unsigned mode)
{
    return guarded([&] { return sys::openFile(path, readOnly, create, exclusiveCreate, lock, replaceable, mode); });
}
void closeFile(FileHandle h) noexcept { sys::closeFile(h); }
bool cloneFile(FileHandle a, FileHandle b) { return guarded([&] { return sys::cloneFile(a, b); }); }
void replaceFile(const std::filesystem::path& s, const std::filesystem::path& t, bool replace)
{
    guarded([&] { sys::replaceFile(s, t, replace); });
}
std::uint64_t fileSize(FileHandle h) { return guarded([&] { return sys::fileSize(h); }); }
void resizeFile(FileHandle h, std::uint64_t n) { guarded([&] { sys::resizeFile(h, n); }); }
void syncFile(FileHandle h, bool metadata) { guarded([&] { sys::syncFile(h, metadata); }); }
void reserveSpace(FileHandle h, std::uint64_t o, std::uint64_t n) { guarded([&] { sys::reserveSpace(h, o, n); }); }
bool isNetworkFilesystemType(unsigned long t) noexcept { return sys::isNetworkFilesystemType(t); }
bool isNetworkFilesystem(FileHandle h) { return sys::isNetworkFilesystem(h); }
void readAt(FileHandle h, std::uint64_t o, void* d, std::size_t n) { guarded([&] { sys::readAt(h, o, d, n); }); }
void writeAt(FileHandle h, std::uint64_t o, const void* s, std::size_t n) { guarded([&] { sys::writeAt(h, o, s, n); }); }
void writeAtV(FileHandle h, std::uint64_t o, const Span* p, unsigned n) { guarded([&] { sys::writeAtV(h, o, p, n); }); }
Mapping mapFile(FileHandle h, std::size_t size, bool w) { return guarded([&] { return sys::mapFile(h, size, w); }); }
Mapping mapFileReserved(FileHandle h, std::size_t size, std::size_t reserve, bool w)
{
    return guarded([&] { return sys::mapFileReserved(h, size, reserve, w); });
}
bool extendMapping(FileHandle h, Mapping& m, std::size_t size, bool w)
{
    return guarded([&] { return sys::extendMapping(h, m, size, w); });
}
void unmap(Mapping& m) noexcept { sys::unmap(m); }
void flushRange(FileHandle h, const Mapping& m, std::size_t o, std::size_t n) { guarded([&] { sys::flushRange(h, m, o, n); }); }
void flushData(FileHandle h, const Mapping& m, const std::vector<std::pair<std::uint64_t, std::uint64_t>>& runs, std::size_t page)
{
    guarded([&] { sys::flushData(h, m, runs, page); });
}
void advise(const Mapping& m, std::size_t o, std::size_t n, Advice a) noexcept { sys::advise(m, o, n, a); }

void* allocPage(std::size_t size)
{
    if (size > std::numeric_limits<std::size_t>::max() - sizeof(PageAllocation) - 63)
        throw Error(ErrorCode::OutOfMemory, "page buffer allocation size overflow");
    const std::size_t bytes = (size + sizeof(PageAllocation) + 63) & ~std::size_t(63);
    const bool mapped = size >= kMappedAllocationMin;
    void* memory;
    if (mapped) {
        memory = sys::tryVmMap(bytes).base;
    } else {
#ifdef _WIN32
        memory = ::_aligned_malloc(bytes, 64);
#else
        memory = std::aligned_alloc(64, bytes);
#endif
    }
    if (!memory)
        throw Error(ErrorCode::OutOfMemory, "page buffer allocation failed");
    auto* allocation = ::new (memory) PageAllocation{bytes, mapped};
    return allocation + 1;
}

void freePage(void* p) noexcept
{
    if (!p)
        return;
    auto* allocation = static_cast<PageAllocation*>(p) - 1;
    if (allocation->mapped)
        sys::vmUnmap(allocation, allocation->bytes);
    else
#ifdef _WIN32
        ::_aligned_free(allocation);
#else
        std::free(allocation);
#endif
}

void* mapMemory(std::size_t bytes)
{
    const sys::VmRegion r = sys::tryVmMap(bytes);
    if (!r.base)
        throw Error(ErrorCode::OutOfMemory, "page buffer slab allocation failed");
    return r.base;
}
void unmapMemory(void* p, std::size_t bytes) noexcept { sys::vmUnmap(p, bytes); }
void discardMemory(void* p, std::size_t bytes) noexcept { sys::vmDiscard(p, bytes); }

}  // namespace nosql::internal::os
