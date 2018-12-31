/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <limits>
#include <type_traits>
#include <seastar/core/future.hh>
#include "gc_clock.hh"
#include "timestamp.hh"
#include "sstables/types.hh"
#include "sstables/exceptions.hh"
#include "clustering_bounds_comparator.hh"
#include "sstables/mc/types.hh"

namespace sstables {

class random_access_reader;

// Utilities for reading integral values in variable-length format
// See vint-serialization.hh for more details
future<uint64_t> read_unsigned_vint(random_access_reader& in);
future<int64_t> read_signed_vint(random_access_reader& in);

template <typename T>
typename std::enable_if_t<!std::is_integral_v<T>>
read_vint(random_access_reader& in, T& t) = delete;

template <typename T>
inline future<> read_vint(random_access_reader& in, T& value) {
    static_assert(std::is_integral_v<T>, "Non-integral values can't be read using read_vint");
    if constexpr(std::is_unsigned_v<T>) {
        return read_unsigned_vint(in).then([&value] (uint64_t res) {
            value = res;
        });
    } else {
        return read_signed_vint(in).then([&value] (int64_t res) {
            value = res;
        });
    }
}

inline api::timestamp_type parse_timestamp(const serialization_header& header,
                                           uint64_t delta) {
    return static_cast<api::timestamp_type>(header.get_min_timestamp() + delta);
}

inline gc_clock::duration parse_ttl(const serialization_header& header,
                                    uint64_t delta, bool wide_local_deletion_time) {
    int64_t min_ttl = header.get_min_ttl(wide_local_deletion_time);
    int64_t ttl;

    if (!wide_local_deletion_time) {
        if (delta > std::numeric_limits<uint32_t>::max()) {
            throw malformed_sstable_exception(format("Too big delta ttl: {}", delta));
        }
        ttl = static_cast<int32_t>(min_ttl) + static_cast<int32_t>(delta);
    } else {
        ttl = min_ttl + static_cast<int64_t>(delta);
        if (ttl < min_ttl) {
            throw malformed_sstable_exception(format("Too big delta ttl: {}. min_ttl={}", delta, min_ttl));
        }
    }
    if (ttl > max_ttl.count() && ! is_expired_liveness_ttl(ttl)) {
        throw malformed_sstable_exception(format("Too big ttl: {}", ttl));
    }
    return gc_clock::duration(ttl);
}

inline gc_clock::time_point parse_expiry(const serialization_header& header,
                                   uint64_t delta, bool wide_local_deletion_time) {
    int64_t min_expiry = header.get_min_local_deletion_time(wide_local_deletion_time);
    int64_t expiry;

    if (!wide_local_deletion_time) {
        if (delta > std::numeric_limits<uint32_t>::max()) {
            throw malformed_sstable_exception(format("Too big delta local_deletion_time: {}", delta));
        }
        expiry = static_cast<int32_t>(min_expiry) + static_cast<int32_t>(delta);
    } else {
        expiry = min_expiry + static_cast<int64_t>(delta);
        if (expiry < min_expiry) {
            throw malformed_sstable_exception(format("Too big delta local_deletion_time: {}. min_local_deletion_time={}", delta, min_expiry));
        }
    }
    return gc_clock::time_point(gc_clock::duration(expiry));
}

};   // namespace sstables

inline std::ostream& operator<<(std::ostream& out, sstables::bound_kind_m kind) {
    switch (kind) {
    case sstables::bound_kind_m::excl_end:
        out << "excl_end";
        break;
    case sstables::bound_kind_m::incl_start:
        out << "incl_start";
        break;
    case sstables::bound_kind_m::excl_end_incl_start:
        out << "excl_end_incl_start";
        break;
    case sstables::bound_kind_m::static_clustering:
        out << "static_clustering";
        break;
    case sstables::bound_kind_m::clustering:
        out << "clustering";
        break;
    case sstables::bound_kind_m::incl_end_excl_start:
        out << "incl_end_excl_start";
        break;
    case sstables::bound_kind_m::incl_end:
        out << "incl_end";
        break;
    case sstables::bound_kind_m::excl_start:
        out << "excl_start";
        break;
    default:
        out << static_cast<std::underlying_type_t<sstables::bound_kind_m>>(kind);
    }
    return out;
}
