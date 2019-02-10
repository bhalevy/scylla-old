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

#include <seastar/core/print.hh>
#include "db/query_context.hh"
#include "db/system_keyspace.hh"
#include "db/large_data_handler.hh"
#include "sstables/sstables.hh"

namespace db {

// A global no-operation large_data_handler
thread_local nop_large_data_handler default_large_data_handler;

future<> large_data_handler::maybe_update_large_partitions(const sstables::sstable& sst, const sstables::key& key, uint64_t partition_size) const {
    if (partition_size > _partition_threshold_bytes) {
        ++_stats.partitions_bigger_than_threshold;

        const schema& s = *sst.get_schema();
        return update_large_partitions(s, sst.get_filename(), key, partition_size);
    }
    return make_ready_future<>();
}

future<> large_data_handler::maybe_delete_large_partitions_entry(const sstables::sstable& sst) const {
    try {
        if (sst.data_size() > _partition_threshold_bytes) {
            const schema& s = *sst.get_schema();
            return delete_large_partitions_entry(s, sst.get_filename());
        }
    } catch (...) {
        // no-op
    }

    return make_ready_future<>();
}

logging::logger cql_table_large_data_handler::large_data_logger("large_data");

future<> cql_table_large_data_handler::update_large_partitions(const schema& s, const sstring& sstable_name, const sstables::key& key, uint64_t partition_size) const {
    static const sstring req = format("INSERT INTO system.{} (keyspace_name, table_name, sstable_name, partition_size, partition_key, compaction_time) VALUES (?, ?, ?, ?, ?, ?) USING TTL 2592000",
            db::system_keyspace::LARGE_PARTITIONS);
    auto ks_name = s.ks_name();
    auto cf_name = s.cf_name();
    std::ostringstream oss;
    oss << key.to_partition_key(s).with_schema(s);
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(db_clock::now().time_since_epoch()).count();
    auto key_str = oss.str();
    return db::execute_cql(req, ks_name, cf_name, sstable_name, int64_t(partition_size), key_str, timestamp)
    .then_wrapped([ks_name, cf_name, key_str, partition_size](auto&& f) {
        try {
            f.get();
            large_data_logger.warn("Writing large partition {}/{}:{} ({} bytes)", ks_name, cf_name, key_str, partition_size);
        } catch (...) {
            large_data_logger.warn("Failed to update {}: {}", db::system_keyspace::LARGE_PARTITIONS, std::current_exception());
        }
    });
}

future<> cql_table_large_data_handler::delete_large_partitions_entry(const schema& s, const sstring& sstable_name) const {
    static const sstring req = format("DELETE FROM system.{} WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?", db::system_keyspace::LARGE_PARTITIONS);
    return db::execute_cql(req, s.ks_name(), s.cf_name(), sstable_name).discard_result().handle_exception([](std::exception_ptr ep) {
            large_data_logger.warn("Failed to drop entries from {}: {}", db::system_keyspace::LARGE_PARTITIONS, ep);
        });
}

void cql_table_large_data_handler::log_large_row(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) const {
    const schema &s = *sst.get_schema();
    if (clustering_key) {
        large_data_logger.warn("Writing large row {}/{}: {} {} ({} bytes)", s.ks_name(), s.cf_name(),
                partition_key.to_partition_key(s).with_schema(s), clustering_key->with_schema(s), row_size);
    } else {
        large_data_logger.warn("Writing large static row {}/{}: {} ({} bytes)", s.ks_name(), s.cf_name(),
                partition_key.to_partition_key(s).with_schema(s), row_size);
    }
}
}
