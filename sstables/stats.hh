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
        uint64_t created = 0; // Number of sstables opened with open_flags::create
        uint64_t opened_w = 0; // Number of sstables opened for write
        uint64_t opened_r = 0; // Number of sstables opened for read
        uint64_t closed_w = 0; // Number of sstables closed after being opened for write
        uint64_t closed_r = 0; // Number of sstables closed after being opened for read
    } _shard_stats;

public:
    static const stats& shard_stats() { return _shard_stats; }

    static inline void submit_open(open_flags _flags) {
        auto flags = static_cast<unsigned int>(_flags);
        if (flags & O_CREAT) {
            _shard_stats.created++;
        }
        if ((flags & O_WRONLY) || (flags & O_RDWR)) {
            _shard_stats.opened_w++;
        } else {
            _shard_stats.opened_r++;
        }
    };

    static inline void submit_close(open_flags _flags) {
        auto flags = static_cast<unsigned int>(_flags);
        if ((flags & O_WRONLY) || (flags & O_RDWR)) {
            _shard_stats.closed_w++;
        } else {
            _shard_stats.closed_r++;
        }
    };
};

}
