// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

// libnosql's OS layer. The implementation moved to develop-core's libsys (sys/file.hpp, sys/vm.hpp); the functions
// here call it and re-throw its base::Error as nosql::Error, so the store's catch sites and error codes are
// unchanged. Types are shared with libsys (one WriteRecorder, one Mapping).

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <utility>
#include <vector>

#include "sys/file.hpp"
#include "nosql/internal/io_observer.hpp"

namespace nosql::internal::os {

using sys::Advice;
using sys::File;
using sys::FileHandle;
using sys::kInvalidFile;
using sys::Mapping;
using sys::recordMappedWrite;
using sys::Span;
using sys::WriteRecorder;
using sys::writeRecorder;

std::size_t systemPageSize() noexcept;

FileHandle openFile(const std::filesystem::path& path, bool readOnly, bool create, bool exclusiveCreate, bool lock = true,
                    bool replaceable = false, unsigned mode = 0600);
void closeFile(FileHandle) noexcept;
bool cloneFile(FileHandle source, FileHandle destination);
void replaceFile(const std::filesystem::path& source, const std::filesystem::path& target, bool replace = true);
std::uint64_t fileSize(FileHandle);
void resizeFile(FileHandle, std::uint64_t bytes);
void syncFile(FileHandle, bool metadata);
void reserveSpace(FileHandle, std::uint64_t offset, std::uint64_t length);
bool isNetworkFilesystemType(unsigned long type) noexcept;
bool isNetworkFilesystem(FileHandle);
void readAt(FileHandle, std::uint64_t offset, void* dst, std::size_t length);
void writeAt(FileHandle, std::uint64_t offset, const void* src, std::size_t length);
void writeAtV(FileHandle, std::uint64_t offset, const Span* parts, unsigned count);
Mapping mapFile(FileHandle, std::size_t size, bool writable);
Mapping mapFileReserved(FileHandle, std::size_t size, std::size_t reserve, bool writable);
bool extendMapping(FileHandle, Mapping&, std::size_t size, bool writable);
void unmap(Mapping&) noexcept;
void flushRange(FileHandle, const Mapping&, std::size_t offset, std::size_t length);
void flushData(FileHandle, const Mapping&, const std::vector<std::pair<std::uint64_t, std::uint64_t>>& pageRuns,
               std::size_t pageSize);
void advise(const Mapping&, std::size_t offset, std::size_t length, Advice) noexcept;

void* allocPage(std::size_t size);
void freePage(void*) noexcept;
void* mapMemory(std::size_t bytes);
void unmapMemory(void* p, std::size_t bytes) noexcept;
void discardMemory(void* p, std::size_t bytes) noexcept;

}  // namespace nosql::internal::os
