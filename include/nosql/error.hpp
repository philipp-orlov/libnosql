// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stdexcept>
#include <string>

namespace nosql {

enum class ErrorCode
{
    Ok = 0,
    NotFound,   ///< key or sub-database absent
    KeyExists,  ///< insertUnique hit an existing key
    Corrupted,  ///< on-disk structure failed validation
    InvalidArgument,
    Unsupported,
    KeyTooLarge,
    ValueTooLarge,
    MapFull,         ///< store hit its configured upper size bound
    TooManyDbs,      ///< maxDbs exhausted
    Incompatible,    ///< sub-database reopened with conflicting flags
    BadTransaction,  ///< operation on a finished/misused transaction
    ReadOnly,
    Busy,  ///< store already locked by another process
    IoError,
    OutOfMemory,
};

const char* toString(ErrorCode) noexcept;

/// Every failure path in libnosql throws this.
class Error : public std::runtime_error
{
public:
    Error(ErrorCode code, const std::string& what) : std::runtime_error(what), code_(code) {}
    explicit Error(ErrorCode code) : std::runtime_error(toString(code)), code_(code) {}

    ErrorCode code() const noexcept { return code_; }

private:
    ErrorCode code_;
};

}  // namespace nosql
