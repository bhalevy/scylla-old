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

#include "sstables/sstables.hh"
#include "sstables/shared_index_lists.hh"
#include "tests/sstable_test.hh"

class sstable_assertions final {
    sstables::shared_sstable _sst;
public:
    sstable_assertions(schema_ptr schema, const sstring& path, int generation = 1)
        : _sst(sstables::make_sstable(std::move(schema),
                    path,
                    generation,
                    sstables::sstable_version_types::mc,
                    sstables::sstable_format_types::big,
                    gc_clock::now(),
                    default_io_error_handler_gen(),
                    1))
    { }
    void read_toc() {
        _sst->read_toc().get();
    }
    void read_summary() {
        _sst->read_summary(default_priority_class()).get();
    }
    void read_filter() {
        _sst->read_filter(default_priority_class()).get();
    }
    void read_statistics() {
        _sst->read_statistics(default_priority_class()).get();
    }
    void load() {
        _sst->load().get();
    }
    future<sstables::index_list> read_index() {
        load();
        return sstables::test(_sst).read_indexes();
    }
    flat_mutation_reader read_rows_flat() {
        return _sst->read_rows_flat(_sst->_schema);
    }

    const sstables::stats_metadata& get_stats_metadata() const {
        return _sst->get_stats_metadata();
    }

    flat_mutation_reader read_range_rows_flat(
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc = default_priority_class(),
            reader_resource_tracker resource_tracker = no_resource_tracking(),
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes,
            sstables::read_monitor& monitor = sstables::default_read_monitor()) {
        return _sst->read_range_rows_flat(_sst->_schema,
                                          range,
                                          slice,
                                          pc,
                                          std::move(resource_tracker),
                                          fwd,
                                          fwd_mr,
                                          monitor);
    }

    void assert_toc(const std::set<component_type>& expected_components);
};
