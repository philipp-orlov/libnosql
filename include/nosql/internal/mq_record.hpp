// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// Queue record integers are little-endian; message bodies remain opaque.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include "nosql/error.hpp"
#include "nosql/slice.hpp"
#include "nosql/internal/endian.hpp"

namespace nosql::internal {

inline constexpr std::uint8_t kMqExternalBody = 1u << 0;

/// enqueuedAt | expiresAt | deliveryCount | bodyLen | cidLen | flags | pad
inline constexpr std::size_t kMqRecordHdr = 28;
/// leaseExpiresAt | consumerToken, then the message record verbatim -- keeping
/// it verbatim is what makes nack and lease expiry a move rather than a
/// re-encode, and what makes a crash mid-lease lose nothing.
inline constexpr std::size_t kMqLeaseHdr = 16;
/// reason | pad | deadAt, then the message record verbatim.
inline constexpr std::size_t kMqDeadHdr = 16;

struct MqRecord
{
    std::uint64_t enqueuedAtNanos = 0;
    std::uint64_t expiresAtNanos = 0;  ///< 0 = never
    std::uint32_t deliveryCount = 0;
    std::uint8_t flags = 0;
    std::string_view correlationId;
    Slice body;  ///< the blob key when kMqExternalBody is set
};

template <class T>
void mqAppend(std::string& out, T v)
{
    static_assert(std::is_trivially_copyable_v<T>);
    const Little<T> encoded(v);
    out.append(reinterpret_cast<const char*>(&encoded), sizeof encoded);
}

template <class T>
T mqRead(const std::byte* p)
{
    return readLittle<T>(p);
}

inline std::string mqEncode(const MqRecord& r)
{
    if (r.correlationId.size() > 0xffff)
        throw Error(ErrorCode::InvalidArgument, "correlation id exceeds 65535 bytes");
    if (r.body.size() > 0xffffffffull)
        throw Error(ErrorCode::ValueTooLarge, "message body exceeds 4 GiB");

    std::string out;
    out.reserve(kMqRecordHdr + r.correlationId.size() + r.body.size());
    mqAppend(out, r.enqueuedAtNanos);
    mqAppend(out, r.expiresAtNanos);
    mqAppend(out, r.deliveryCount);
    mqAppend(out, std::uint32_t(r.body.size()));
    mqAppend(out, std::uint16_t(r.correlationId.size()));
    mqAppend(out, r.flags);
    mqAppend(out, std::uint8_t(0));
    out.append(r.correlationId);
    out.append(r.body.chars(), r.body.size());
    return out;
}

/// Views point into `s`; nothing is copied.
inline MqRecord mqDecode(Slice s)
{
    if (s.size() < kMqRecordHdr)
        throw Error(ErrorCode::Corrupted, "message record shorter than its header");
    const std::byte* p = s.data();
    MqRecord r;
    r.enqueuedAtNanos = mqRead<std::uint64_t>(p);
    r.expiresAtNanos = mqRead<std::uint64_t>(p + 8);
    r.deliveryCount = mqRead<std::uint32_t>(p + 16);
    const auto bodyLen = mqRead<std::uint32_t>(p + 20);
    const auto cidLen = mqRead<std::uint16_t>(p + 24);
    r.flags = mqRead<std::uint8_t>(p + 26);
    if (kMqRecordHdr + cidLen + std::uint64_t(bodyLen) != s.size())
        throw Error(ErrorCode::Corrupted, "message record length mismatch");
    r.correlationId = std::string_view(s.chars() + kMqRecordHdr, cidLen);
    r.body = Slice(p + kMqRecordHdr + cidLen, bodyLen);
    return r;
}

inline std::string mqWrapLease(std::uint64_t expiresAtNanos, std::uint64_t token, Slice record)
{
    std::string out;
    out.reserve(kMqLeaseHdr + record.size());
    mqAppend(out, expiresAtNanos);
    mqAppend(out, token);
    out.append(record.chars(), record.size());
    return out;
}

inline std::string mqWrapDead(std::uint8_t reason, std::uint64_t atNanos, Slice record)
{
    std::string out;
    out.reserve(kMqDeadHdr + record.size());
    mqAppend(out, reason);
    for (int i = 0; i < 7; ++i)
        mqAppend(out, std::uint8_t(0));
    mqAppend(out, atNanos);
    out.append(record.chars(), record.size());
    return out;
}

inline Slice mqUnwrap(Slice wrapped, std::size_t hdr)
{
    if (wrapped.size() < hdr)
        throw Error(ErrorCode::Corrupted, "wrapped message record is truncated");
    return wrapped.subspan(hdr);
}

/// The two fields of a lease header; `lease` must already have passed mqUnwrap.
inline std::uint64_t mqLeaseDeadline(Slice lease)
{
    return mqRead<std::uint64_t>(lease.data());
}
inline std::uint64_t mqLeaseToken(Slice lease)
{
    return mqRead<std::uint64_t>(lease.data() + 8);
}

/// Sequence numbers are the keys of every queue tree.
inline Little<std::uint64_t> mqSeqKey(std::uint64_t sequence) noexcept
{
    return Little<std::uint64_t>(sequence);
}

}  // namespace nosql::internal
