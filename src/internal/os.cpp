// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "nosql/internal/os.hpp"
#include "nosql/internal/io_observer.hpp"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <new>
#include <string>

#include "nosql/error.hpp"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#include <malloc.h>  // _aligned_malloc / _aligned_free
#else
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#ifdef __linux__
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/statfs.h>
#endif
#include <unistd.h>

#include <cerrno>
#include <cstring>
#endif

namespace nosql::internal::os {
namespace {

struct alignas(64) PageAllocation
{
    std::size_t bytes;
    bool mapped;
};

constexpr std::size_t kMappedAllocationMin = 64u << 10;

#ifdef _WIN32
[[noreturn]] void fail(ErrorCode code, const char* what)
{
    const DWORD e = ::GetLastError();
    throw Error(code, std::string(what) + ": win32 error " + std::to_string(e));
}
#else
[[noreturn]] void fail(ErrorCode code, const char* what)
{
    const int e = errno;
    throw Error(code, std::string(what) + ": " + std::strerror(e));
}
#endif

}  // namespace

std::size_t systemPageSize() noexcept
{
#ifdef _WIN32
    SYSTEM_INFO si;
    ::GetSystemInfo(&si);
    return si.dwPageSize ? si.dwPageSize : 4096;
#else
    const long v = ::sysconf(_SC_PAGESIZE);
    return v > 0 ? std::size_t(v) : 4096;
#endif
}

void* allocPage(std::size_t size)
{
    if (size > std::numeric_limits<std::size_t>::max() - sizeof(PageAllocation) - 63)
        throw Error(ErrorCode::OutOfMemory, "page buffer allocation size overflow");
    const std::size_t bytes = (size + sizeof(PageAllocation) + 63) & ~std::size_t(63);
    const bool mapped = size >= kMappedAllocationMin;
    void* memory;
#ifdef _WIN32
    memory = mapped ? ::VirtualAlloc(nullptr, bytes, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)
                    : ::_aligned_malloc(bytes, 64);
#else
    memory = mapped ? ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)
                    : std::aligned_alloc(64, bytes);
    if (memory == MAP_FAILED)
        memory = nullptr;
#endif
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
#ifdef _WIN32
    if (allocation->mapped)
        ::VirtualFree(allocation, 0, MEM_RELEASE);
    else
        ::_aligned_free(allocation);
#else
    if (allocation->mapped)
        ::munmap(allocation, allocation->bytes);
    else
        std::free(allocation);
#endif
}

#ifdef _WIN32

FileHandle openFile(const std::filesystem::path& path, bool readOnly, bool create,
                    bool exclusiveCreate, bool lock, bool replaceable, unsigned /*mode*/)
{
    const DWORD access = readOnly ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE);
    const DWORD share = FILE_SHARE_READ | FILE_SHARE_WRITE | (replaceable ? FILE_SHARE_DELETE : 0);
    DWORD disp;
    if (readOnly) {
        disp = OPEN_EXISTING;
    } else if (exclusiveCreate) {
        disp = CREATE_NEW;
    } else {
        disp = create ? OPEN_ALWAYS : OPEN_EXISTING;
    }
    HANDLE h =
        ::CreateFileW(path.c_str(), access, share, nullptr, disp, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        const DWORD e = ::GetLastError();
        if (e == ERROR_FILE_NOT_FOUND || e == ERROR_PATH_NOT_FOUND)
            throw Error(ErrorCode::NotFound, "cannot open " + path.string());
        if (e == ERROR_FILE_EXISTS)
            throw Error(ErrorCode::InvalidArgument, "already exists: " + path.string());
        fail(ErrorCode::IoError, "CreateFileW");
    }
    if (!lock)
        return h;
    // Whole-file advisory lock: exclusive for writers, shared for readers.
    // This is what replaces a .lck side file -- one process at a time.
    OVERLAPPED ov{};
    const DWORD flags = LOCKFILE_FAIL_IMMEDIATELY | (readOnly ? 0 : LOCKFILE_EXCLUSIVE_LOCK);
    if (!::LockFileEx(h, flags, 0, MAXDWORD, MAXDWORD, &ov)) {
        ::CloseHandle(h);
        throw Error(ErrorCode::Busy, "store is already open in another process: " + path.string());
    }
    return h;
}

void closeFile(FileHandle h) noexcept
{
    if (h && h != INVALID_HANDLE_VALUE) {
        OVERLAPPED ov{};
        ::UnlockFileEx(h, 0, MAXDWORD, MAXDWORD, &ov);
        ::CloseHandle(h);
    }
}

std::uint64_t fileSize(FileHandle h)
{
    LARGE_INTEGER li;
    if (!::GetFileSizeEx(h, &li))
        fail(ErrorCode::IoError, "GetFileSizeEx");
    return std::uint64_t(li.QuadPart);
}

void resizeFile(FileHandle h, std::uint64_t bytes)
{
    observeIo(IoEvent::Resize, std::size_t(bytes));
    LARGE_INTEGER li;
    li.QuadPart = LONGLONG(bytes);
    if (!::SetFilePointerEx(h, li, nullptr, FILE_BEGIN))
        fail(ErrorCode::IoError, "SetFilePointerEx");
    if (!::SetEndOfFile(h))
        fail(ErrorCode::IoError, "SetEndOfFile");
}

void syncFile(FileHandle h, bool)
{
    observeIo(IoEvent::Sync);
    if (!::FlushFileBuffers(h))
        fail(ErrorCode::IoError, "FlushFileBuffers");
}

void reserveSpace(FileHandle, std::uint64_t, std::uint64_t)
{
    observeIo(IoEvent::Reserve);
}

bool isNetworkFilesystem(FileHandle)
{
    return false;
}

bool isNetworkFilesystemType(unsigned long) noexcept
{
    return false;
}

void replaceFile(const std::filesystem::path& source, const std::filesystem::path& target, bool replace)
{
    observeIo(IoEvent::Replace);
    if (!::MoveFileExW(source.c_str(), target.c_str(),
                      (replace ? MOVEFILE_REPLACE_EXISTING : 0) | MOVEFILE_WRITE_THROUGH))
        fail(ErrorCode::IoError, "MoveFileExW");
    observeIo(IoEvent::Replaced);
}

void readAt(FileHandle h, std::uint64_t offset, void* dst, std::size_t length)
{
    observeIo(IoEvent::Read, length);
    auto* p = static_cast<std::byte*>(dst);
    while (length) {
        OVERLAPPED ov{};
        ov.Offset = DWORD(offset & 0xffffffffu);
        ov.OffsetHigh = DWORD(offset >> 32);
        const DWORD want = DWORD(length > 0x10000000u ? 0x10000000u : length);
        DWORD got = 0;
        if (!::ReadFile(h, p, want, &got, &ov))
            fail(ErrorCode::IoError, "ReadFile");
        if (got == 0)
            throw Error(ErrorCode::IoError, "ReadFile: unexpected end of file");
        p += got;
        offset += got;
        length -= got;
    }
}

void writeAt(FileHandle h, std::uint64_t offset, const void* src, std::size_t length)
{
    observeIo(IoEvent::Write, length);
    const auto* p = static_cast<const std::byte*>(src);
    while (length) {
        OVERLAPPED ov{};
        ov.Offset = DWORD(offset & 0xffffffffu);
        ov.OffsetHigh = DWORD(offset >> 32);
        const DWORD want = DWORD(std::min<std::size_t>(length, ioCallback ? 65536u : 0x10000000u));
        DWORD put = 0;
        if (!::WriteFile(h, p, want, &put, &ov))
            fail(ErrorCode::IoError, "WriteFile");
        if (put == 0)
            throw Error(ErrorCode::IoError, "WriteFile made no progress");
        observeIo(IoEvent::WriteProgress, put);
        p += put;
        offset += put;
        length -= put;
    }
}

void writeAtV(FileHandle h, std::uint64_t offset, const Span* parts, unsigned count)
{
    // No positional gather write on Win32 for buffered handles; the segments
    // are few and the payload one is large, so sequential writes cost the same.
    for (unsigned i = 0; i < count; ++i) {
        if (!parts[i].size)
            continue;
        writeAt(h, offset, parts[i].data, parts[i].size);
        offset += parts[i].size;
    }
}

Mapping mapFile(FileHandle h, std::size_t size, bool writable)
{
    Mapping m;
    const DWORD prot = writable ? PAGE_READWRITE : PAGE_READONLY;
    HANDLE sec = ::CreateFileMappingW(h, nullptr, prot, DWORD(std::uint64_t(size) >> 32),
                                      DWORD(size & 0xffffffffu), nullptr);
    if (!sec)
        fail(ErrorCode::IoError, "CreateFileMappingW");
    void* base = ::MapViewOfFile(sec, writable ? FILE_MAP_WRITE : FILE_MAP_READ, 0, 0, size);
    if (!base) {
        ::CloseHandle(sec);
        fail(ErrorCode::IoError, "MapViewOfFile");
    }
    m.base = static_cast<std::byte*>(base);
    m.size = size;
    m.section = sec;
    return m;
}

void unmap(Mapping& m) noexcept
{
    if (m.base)
        ::UnmapViewOfFile(m.base);
    if (m.section)
        ::CloseHandle(m.section);
    m = Mapping{};
}

void flushRange(FileHandle h, const Mapping& m, std::size_t offset, std::size_t length)
{
    observeIo(IoEvent::MetaBarrier, length);
    if (!length)
        return;
    if (!::FlushViewOfFile(m.base + offset, length))
        fail(ErrorCode::IoError, "FlushViewOfFile");
    if (!::FlushFileBuffers(h))
        fail(ErrorCode::IoError, "FlushFileBuffers");
}

void advise(const Mapping& m, std::size_t offset, std::size_t length, Advice a) noexcept
{
    // PrefetchVirtualMemory is Windows 8+; resolve it at run time so an older
    // SDK or system still builds and runs, just without the hint.
    if (a != Advice::WillNeed || !m.base || !length || offset >= m.size)
        return;
    struct RangeEntry
    {
        PVOID VirtualAddress;
        SIZE_T NumberOfBytes;
    };
    using Fn = BOOL(WINAPI*)(HANDLE, ULONG_PTR, RangeEntry*, ULONG);
    static const Fn fn = reinterpret_cast<Fn>(reinterpret_cast<void*>(
        ::GetProcAddress(::GetModuleHandleW(L"kernel32.dll"), "PrefetchVirtualMemory")));
    if (!fn)
        return;
    if (offset + length > m.size)
        length = m.size - offset;
    RangeEntry e{m.base + offset, length};
    fn(::GetCurrentProcess(), 1, &e, 0);
}

#else  // ---------------------------------------------------------- POSIX ---

FileHandle openFile(const std::filesystem::path& path, bool readOnly, bool create,
                    bool exclusiveCreate, bool lock, bool, unsigned mode)
{
    int flags = (readOnly ? O_RDONLY : O_RDWR) | O_CLOEXEC | O_NOFOLLOW;
    if (!readOnly && create)
        flags |= O_CREAT;
    if (!readOnly && exclusiveCreate)
        flags |= O_CREAT | O_EXCL;
    const int fd = ::open(path.c_str(), flags, mode_t(mode));
    if (fd < 0) {
        if (errno == ENOENT)
            throw Error(ErrorCode::NotFound, "cannot open " + path.string());
        if (errno == EEXIST)
            throw Error(ErrorCode::InvalidArgument, "already exists: " + path.string());
        if (errno == ELOOP)
            throw Error(ErrorCode::InvalidArgument, "refusing to follow a symbolic link: " + path.string());
        fail(ErrorCode::IoError, "open");
    }
    struct stat st;
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        fail(ErrorCode::IoError, "fstat");
    }
    if (st.st_uid != ::geteuid() && ::geteuid() != 0) {
        ::close(fd);
        throw Error(ErrorCode::InvalidArgument,
                    "not owned by the effective user: " + path.string());
    }
    // A file created just now gets the requested mode whatever the umask.
    if (!readOnly && create && st.st_size == 0 && (st.st_mode & 07777) != mode_t(mode))
        (void)::fchmod(fd, mode_t(mode));
    if (!lock)
        return fd;
    // Advisory whole-file lock in place of a .lck side file.
    if (::flock(fd, (readOnly ? LOCK_SH : LOCK_EX) | LOCK_NB) != 0) {
        ::close(fd);
        throw Error(ErrorCode::Busy, "store is already open in another process: " + path.string());
    }
    return fd;
}

void closeFile(FileHandle fd) noexcept
{
    if (fd >= 0) {
        ::flock(fd, LOCK_UN);
        ::close(fd);
    }
}

std::uint64_t fileSize(FileHandle fd)
{
    struct stat st;
    if (::fstat(fd, &st) != 0)
        fail(ErrorCode::IoError, "fstat");
    return std::uint64_t(st.st_size);
}

void resizeFile(FileHandle fd, std::uint64_t bytes)
{
    observeIo(IoEvent::Resize, std::size_t(bytes));
    if (::ftruncate(fd, off_t(bytes)) != 0)
        fail(ErrorCode::IoError, "ftruncate");
}

void syncFile(FileHandle fd, bool metadata)
{
    observeIo(IoEvent::Sync);
    const int rc = metadata ? ::fsync(fd) : ::fdatasync(fd);
    if (rc != 0)
        fail(ErrorCode::IoError, "fsync");
}

void reserveSpace(FileHandle fd, std::uint64_t offset, std::uint64_t length)
{
    observeIo(IoEvent::Reserve, std::size_t(length));
#ifdef __linux__
    if (!length)
        return;
    // fallocate rather than posix_fallocate: glibc's fallback writes every
    // block one by one on a filesystem without support, which is far too slow.
    if (::fallocate(fd, 0, off_t(offset), off_t(length)) != 0) {
        if (errno == EOPNOTSUPP || errno == ENOSYS || errno == EINVAL || errno == ENODEV)
            return;
        fail(ErrorCode::IoError, "fallocate");
    }
#else
    (void)fd;
    (void)offset;
    (void)length;
#endif
}

bool isNetworkFilesystemType(unsigned long type) noexcept
{
    switch (type) {
        case 0x6969:      // NFS
        case 0x517B:      // SMB
        case 0xFF534D42:  // CIFS
        case 0xFE534D42:  // SMB2
        case 0x65735546:  // FUSE
        case 0x5346414F:  // AFS
        case 0x00C36400:  // Ceph
        case 0x0BD00BD0:  // Lustre
        case 0x01021997:  // 9p
        case 0x73757245:  // Coda
        case 0x564C:      // NCP
        case 0x47504653:  // GPFS
        case 0x7461636F:  // OCFS2
        case 0x01161970:  // GFS2
        case 0x786F4256:  // VirtualBox shared folders
            return true;
        default: return false;
    }
}

bool isNetworkFilesystem(FileHandle fd)
{
#ifdef __linux__
    struct statfs fs;
    return ::fstatfs(fd, &fs) == 0 && isNetworkFilesystemType(static_cast<unsigned long>(fs.f_type));
#else
    (void)fd;
    return false;
#endif
}

void replaceFile(const std::filesystem::path& source, const std::filesystem::path& target, bool replace)
{
    observeIo(IoEvent::Replace);
    if (replace) {
        std::filesystem::rename(source, target);
    } else {
        std::filesystem::create_hard_link(source, target);
        std::filesystem::remove(source);
    }
    const auto parent = target.parent_path().empty() ? std::filesystem::path(".") : target.parent_path();
    observeIo(IoEvent::Replaced);
    const int directory = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0)
        fail(ErrorCode::IoError, "open parent directory");
    const int result = ::fsync(directory);
    const int saved = errno;
    ::close(directory);
    if (result != 0) {
        errno = saved;
        fail(ErrorCode::IoError, "fsync parent directory");
    }
}

void readAt(FileHandle fd, std::uint64_t offset, void* dst, std::size_t length)
{
    observeIo(IoEvent::Read, length);
    auto* p = static_cast<std::byte*>(dst);
    while (length) {
        const ssize_t n = ::pread(fd, p, length, off_t(offset));
        if (n < 0) {
            if (errno == EINTR)
                continue;
            fail(ErrorCode::IoError, "pread");
        }
        if (n == 0)
            throw Error(ErrorCode::IoError, "pread: unexpected end of file");
        p += n;
        offset += std::uint64_t(n);
        length -= std::size_t(n);
    }
}

void writeAt(FileHandle fd, std::uint64_t offset, const void* src, std::size_t length)
{
    observeIo(IoEvent::Write, length);
    const auto* p = static_cast<const std::byte*>(src);
    while (length) {
        const ssize_t n = ::pwrite(fd, p, ioCallback ? std::min<std::size_t>(length, 65536) : length, off_t(offset));
        if (n < 0) {
            if (errno == EINTR)
                continue;
            fail(ErrorCode::IoError, "pwrite");
        }
        if (n == 0)
            throw Error(ErrorCode::IoError, "pwrite made no progress");
        observeIo(IoEvent::WriteProgress, std::size_t(n));
        p += n;
        offset += std::uint64_t(n);
        length -= std::size_t(n);
    }
}

void writeAtV(FileHandle fd, std::uint64_t offset, const Span* parts, unsigned count)
{
    for (unsigned index = 0; index < count; ++index)
        observeIo(IoEvent::Write, parts[index].size);
    // Gathered in groups small enough for a stack array and well under IOV_MAX;
    // a commit's longest contiguous run may hold hundreds of pages.
    constexpr unsigned kMaxIov = 64;
    iovec iov[kMaxIov];
    unsigned taken = 0;
    while (taken < count) {
        unsigned n = 0;
        std::size_t total = 0;
        for (; taken < count && n < kMaxIov; ++taken) {
            if (!parts[taken].size)
                continue;
            iov[n].iov_base = const_cast<void*>(parts[taken].data);
            iov[n].iov_len = parts[taken].size;
            total += parts[taken].size;
            ++n;
        }
        if (!n)
            continue;

        while (total) {
            const ssize_t w = ::pwritev(fd, iov, int(n), off_t(offset));
            if (w < 0) {
                if (errno == EINTR)
                    continue;
                fail(ErrorCode::IoError, "pwritev");
            }
            if (w == 0)
                throw Error(ErrorCode::IoError, "pwritev made no progress");
            observeIo(IoEvent::WriteProgress, std::size_t(w));
            offset += std::uint64_t(w);
            total -= std::size_t(w);
            // Drop whole segments the kernel consumed, then trim the partial one.
            std::size_t done = std::size_t(w);
            unsigned first = 0;
            while (first < n && done >= iov[first].iov_len) {
                done -= iov[first].iov_len;
                ++first;
            }
            if (first) {
                for (unsigned i = first; i < n; ++i)
                    iov[i - first] = iov[i];
                n -= first;
            }
            if (n && done) {
                iov[0].iov_base = static_cast<std::byte*>(iov[0].iov_base) + done;
                iov[0].iov_len -= done;
            }
        }
    }
}

Mapping mapFile(FileHandle fd, std::size_t size, bool writable)
{
    const int prot = writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
    void* base = ::mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED)
        fail(ErrorCode::IoError, "mmap");
    Mapping m;
    m.base = static_cast<std::byte*>(base);
    m.size = size;
    return m;
}

void unmap(Mapping& m) noexcept
{
    if (m.base)
        ::munmap(m.base, m.size);
    m = Mapping{};
}

void flushRange(FileHandle fd, const Mapping& m, std::size_t offset, std::size_t length)
{
    observeIo(IoEvent::MetaBarrier, length);
    if (!length)
        return;
    // msync needs a system-page-aligned start; widen the window outwards.
    const std::size_t sp = systemPageSize();
    const std::size_t begin = offset & ~(sp - 1);
    std::size_t end = offset + length;
    end = (end + sp - 1) & ~(sp - 1);
    if (end > m.size)
        end = m.size;
    if (begin >= end)
        return;
    if (::msync(m.base + begin, end - begin, MS_SYNC) != 0) {
        // Some filesystems reject MS_SYNC on a range; fall back to a file sync.
        if (errno != EINVAL)
            fail(ErrorCode::IoError, "msync");
        syncFile(fd, false);
    }
}

void advise(const Mapping& m, std::size_t offset, std::size_t length, Advice a) noexcept
{
    if (!m.base || !length || offset >= m.size)
        return;
    // madvise wants a page-aligned start; widen the window outwards like msync.
    const std::size_t sp = systemPageSize();
    const std::size_t begin = offset & ~(sp - 1);
    std::size_t end = offset + length;
    if (end > m.size)
        end = m.size;
    int adv = MADV_NORMAL;
    switch (a) {
    case Advice::Normal: adv = MADV_NORMAL; break;
    case Advice::Random: adv = MADV_RANDOM; break;
    case Advice::Sequential: adv = MADV_SEQUENTIAL; break;
    case Advice::WillNeed: adv = MADV_WILLNEED; break;
    }
    (void)::madvise(m.base + begin, end - begin, adv);
}

#endif

bool cloneFile(FileHandle source, FileHandle destination)
{
    observeIo(IoEvent::Clone);
#ifdef __linux__
    if (::ioctl(destination, FICLONE, source) == 0)
        return true;
    if (errno != EOPNOTSUPP && errno != ENOTTY && errno != EXDEV && errno != EINVAL && errno != ENOSYS)
        fail(ErrorCode::IoError, "FICLONE");
#else
    (void)source;
    (void)destination;
#endif
    return false;
}

void flushData(FileHandle fd, const Mapping& mapping,
               const std::vector<std::pair<std::uint64_t, std::uint64_t>>& pageRuns,
               std::size_t pageSize)
{
    observeIo(IoEvent::DataBarrier);
#ifdef _WIN32
    for (const auto& [first, last] : pageRuns)
        if (!::FlushViewOfFile(mapping.base + first * pageSize, (last - first + 1) * pageSize))
            fail(ErrorCode::IoError, "FlushViewOfFile");
#else
    (void)mapping;
    (void)pageRuns;
    (void)pageSize;
#endif
    syncFile(fd, false);
}

}  // namespace nosql::internal::os
