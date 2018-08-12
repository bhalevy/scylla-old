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

namespace sstables {

class sstables_stats {
    static thread_local struct stats {
        uint64_t partition_writes = 0;
        uint64_t static_row_writes = 0;
        uint64_t row_writes = 0;
        uint64_t tombstone_writes = 0;
        uint64_t range_tombstone_writes = 0;
    } _shard_stats;

public:
    static const stats& shard_stats() { return _shard_stats; }

    static inline void submit_partition_write() {
        ++_shard_stats.partition_writes;
    }

    static inline void submit_static_row_write() {
        ++_shard_stats.static_row_writes;
    }

    static inline void submit_row_write() {
        ++_shard_stats.row_writes;
    }

    static inline void submit_tombstone_write() {
        ++_shard_stats.tombstone_writes;
    }

    static inline void submit_range_tombstone_write() {
        ++_shard_stats.range_tombstone_writes;
    }
};

}
