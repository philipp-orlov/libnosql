// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <utility>
#include <vector>

namespace nosql::internal::os {

#ifdef _WIN32
using FileHandle = void*;  // HANDLE. A successful CreateFileW never yields null,
                           // and failures throw, so null is a safe "unset".
inline constexpr FileHandle kInvalidFile = nullptr;
#else
using FileHandle = int;
inline constexpr FileHandle kInvalidFile = -1;
#endif

/// A live mapping of the whole file. Recreated (never resized in place) when
/// the file grows, so readers holding an older mapping stay valid.
struct Mapping
{
    std::byte* base = nullptr;
    std::size_t size = 0;
#ifdef _WIN32
    void* section = nullptr;  // file mapping HANDLE
#endif
};

std::size_t systemPageSize() noexcept;

/// Opens (and, unless readOnly, exclusively locks) the store file.
/// Pass `lock = false` for side-car files that have a single writer by
/// construction and must stay readable while that writer holds them open.
/// A file this call creates gets `mode` (POSIX, exactly, whatever the umask);
/// the descriptor is close-on-exec, a symlink at `path` is not followed, and a
/// file owned by another user is refused (root excepted).
/// Throws nosql::Error on failure.
FileHandle openFile(const std::filesystem::path& path, bool readOnly, bool create,
                    bool exclusiveCreate, bool lock = true, bool replaceable = false,
                    unsigned mode = 0600);
void closeFile(FileHandle) noexcept;

/// Owns a FileHandle and closes it on the way out. Move-only.
class File
{
public:
    File() = default;
    explicit File(FileHandle handle) noexcept : fd_(handle) {}
    File(File&& other) noexcept : fd_(std::exchange(other.fd_, kInvalidFile)) {}
    File& operator=(File&& other) noexcept
    {
        std::swap(fd_, other.fd_);
        return *this;
    }
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    ~File() { closeFile(fd_); }

    FileHandle get() const noexcept { return fd_; }
    operator FileHandle() const noexcept { return fd_; }
    bool open() const noexcept { return fd_ != kInvalidFile; }
    /// Closes now, or hands the handle back without closing it.
    void close() noexcept { closeFile(std::exchange(fd_, kInvalidFile)); }
    FileHandle release() noexcept { return std::exchange(fd_, kInvalidFile); }

private:
    FileHandle fd_ = kInvalidFile;
};
bool cloneFile(FileHandle source, FileHandle destination);
void replaceFile(const std::filesystem::path& source, const std::filesystem::path& target,
                 bool replace = true);

std::uint64_t fileSize(FileHandle);
void resizeFile(FileHandle, std::uint64_t bytes);
void syncFile(FileHandle, bool metadata);

/// Makes the disk blocks for [offset, offset+length) exist now, so a full
/// disk is reported here as ErrorCode::IoError instead of surfacing later as a
/// fault when a hole in the sparse file is touched. A filesystem without
/// preallocation is not an error: the call does nothing there. No-op on Windows.
void reserveSpace(FileHandle, std::uint64_t offset, std::uint64_t length);

/// Filesystem `f_type` values (Linux statfs) of network and user-space
/// filesystems, where flock is not dependable across hosts and a remote
/// truncation or I/O error arrives as SIGBUS through the memory map.
bool isNetworkFilesystemType(unsigned long type) noexcept;
/// Whether the file lives on such a filesystem (false where undetectable).
bool isNetworkFilesystem(FileHandle);

/// Positional whole-buffer I/O; both loop until the request is satisfied and
/// throw rather than returning a short count.
void readAt(FileHandle, std::uint64_t offset, void* dst, std::size_t length);
void writeAt(FileHandle, std::uint64_t offset, const void* src, std::size_t length);
/// Gathered append: writes `parts` back to back starting at `offset`. One
/// syscall where the platform has writev, which keeps a blob append from
/// copying its payload just to prepend a 512-byte header.
struct Span
{
    const void* data = nullptr;
    std::size_t size = 0;
};
void writeAtV(FileHandle, std::uint64_t offset, const Span* parts, unsigned count);

Mapping mapFile(FileHandle, std::size_t size, bool writable);
void unmap(Mapping&) noexcept;
/// Flush [offset, offset+length) of a writable mapping through to storage.
/// Blocks until the range is durable on the device (msync(MS_SYNC), or
/// FlushViewOfFile+FlushFileBuffers on Windows) -- a synchronisation barrier
/// callers rely on to order one flush before the next, not just a request to
/// hurry the writeback along. See docs/design.md, "Durability and the memory
/// map".
void flushRange(FileHandle, const Mapping&, std::size_t offset, std::size_t length);
void flushData(FileHandle, const Mapping&,
               const std::vector<std::pair<std::uint64_t, std::uint64_t>>& pageRuns,
               std::size_t pageSize);

/// Paging hints for a read-only mapping. Purely advisory: nothing here changes
/// what a read returns, only how eagerly the OS pulls pages in, so a platform
/// without the facility may ignore the call. Windows honours WillNeed only.
enum class Advice
{
    Normal,      ///< the OS's default read-ahead heuristics
    Random,      ///< no read-ahead: faults bring in only the pages touched
    Sequential,  ///< aggressive read-ahead, and pages behind the cursor may go early
    WillNeed,    ///< start reading [offset, offset+length) now, without blocking
};
void advise(const Mapping&, std::size_t offset, std::size_t length, Advice) noexcept;

void* allocPage(std::size_t size);
void freePage(void*) noexcept;

}  // namespace nosql::internal::os
