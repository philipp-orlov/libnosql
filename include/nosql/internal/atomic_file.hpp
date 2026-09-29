// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <filesystem>
#include <vector>

#include "nosql/internal/format.hpp"
#include "nosql/internal/os.hpp"

namespace nosql::internal {

class AtomicFile
{
public:
    explicit AtomicFile(const std::filesystem::path& target) : target_(target)
    {
        const Identity identity = newIdentity();
        temporary_ = target;
        temporary_ += ".tmp-" + std::to_string(identity[0]) + "-" + std::to_string(identity[1]);
        fd_ = os::openFile(temporary_, false, true, true, true, true);
    }

    AtomicFile(const AtomicFile&) = delete;
    AtomicFile& operator=(const AtomicFile&) = delete;

    ~AtomicFile()
    {
        os::closeFile(fd_);
        std::error_code ignored;
        std::filesystem::remove(temporary_, ignored);
    }

    os::FileHandle fd() const noexcept { return fd_; }
    const std::filesystem::path& path() const noexcept { return temporary_; }

    void close()
    {
        os::closeFile(fd_);
        fd_ = os::kInvalidFile;
    }

    void copyFrom(os::FileHandle source, bool allowClone = true)
    {
        const std::uint64_t bytes = os::fileSize(source);
        if (allowClone && os::cloneFile(source, fd_)) {
            std::filesystem::permissions(temporary_, std::filesystem::status(target_).permissions());
            return;
        }
        os::resizeFile(fd_, bytes);
        std::vector<std::byte> buffer(1u << 20);
        for (std::uint64_t offset = 0; offset < bytes;) {
            const auto length = std::size_t(std::min<std::uint64_t>(buffer.size(), bytes - offset));
            os::readAt(source, offset, buffer.data(), length);
            os::writeAt(fd_, offset, buffer.data(), length);
            offset += length;
        }
        std::filesystem::permissions(temporary_, std::filesystem::status(target_).permissions());
    }

    void publish(bool replace = true)
    {
        if (fd_ == os::kInvalidFile)
            fd_ = os::openFile(temporary_, false, false, false, true, true);
        os::syncFile(fd_, true);
        os::replaceFile(temporary_, target_, replace);
    }

private:
    std::filesystem::path target_, temporary_;
    os::FileHandle fd_ = os::kInvalidFile;
};

}  // namespace nosql::internal