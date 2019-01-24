/*
 * Copyright (C) 2016 ScyllaDB
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


#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "tests/cql_test_env.hh"
#include "tests/result_set_assertions.hh"

#include "database.hh"
#include "partition_slice_builder.hh"
#include "frozen_mutation.hh"
#include "mutation_source_test.hh"
#include "schema_registry.hh"
#include "service/migration_manager.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "tmpdir.hh"

SEASTAR_TEST_CASE(test_querying_with_limits) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
            auto& db = e.local_db();
            auto s = db.find_schema("ks", "cf");
            dht::partition_range_vector pranges;
            for (uint32_t i = 1; i <= 3; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(format("key{:d}", i)));
                mutation m(s, pkey);
                m.partition().apply(tombstone(api::timestamp_type(1), gc_clock::now()));
                db.apply(s, freeze(m)).get();
            }
            for (uint32_t i = 3; i <= 8; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(format("key{:d}", i)));
                mutation m(s, pkey);
                m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", data_value(bytes("v1")), 1);
                db.apply(s, freeze(m)).get();
                pranges.emplace_back(dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*s, std::move(pkey))));
            }

            auto max_size = std::numeric_limits<size_t>::max();
            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), 3);
                auto result = db.query(s, cmd, query::result_options::only_result(), pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(3);
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(),
                        query::max_rows, gc_clock::now(), std::nullopt, 5);
                auto result = db.query(s, cmd, query::result_options::only_result(), pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(5);
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(),
                        query::max_rows, gc_clock::now(), std::nullopt, 3);
                auto result = db.query(s, cmd, query::result_options::only_result(), pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(3);
            }
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source) {
    do_with_cql_env([] (cql_test_env& e) {
        run_mutation_source_tests([&] (schema_ptr s, const std::vector<mutation>& partitions) -> mutation_source {
            try {
                e.local_db().find_column_family(s->ks_name(), s->cf_name());
                service::get_local_migration_manager().announce_column_family_drop(s->ks_name(), s->cf_name(), true).get();
            } catch (const no_such_column_family&) {
                // expected
            }
            service::get_local_migration_manager().announce_new_column_family(s, true).get();
            column_family& cf = e.local_db().find_column_family(s);
            for (auto&& m : partitions) {
                e.local_db().apply(cf.schema(), freeze(m)).get();
            }
            cf.flush().get();
            cf.get_row_cache().invalidate([] {}).get();
            return mutation_source([&] (schema_ptr s,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr) {
                return cf.make_reader(s, range, slice, pc, std::move(trace_state), fwd, fwd_mr);
            });
        });
        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_distributed_loader_with_incomplete_sstables) {
  do_with(tmpdir(), [] (tmpdir& data_dir) {
    using sst = sstables::sstable;

    db::config db_cfg;

    db_cfg.data_file_directories({data_dir.path}, db::config::config_source::CommandLine);

    // Create incomplete sstables in test data directory
    sstring ks = "system";
    sstring cf = "local-7ad54392bcdd35a684174e047860b377";
    sstring sst_dir = data_dir.path + "/" + ks + "/" + cf;
    sstring temp_sst_dir;
    sstring temp_file_name;

    auto touch_dir = [] (const sstring& dir_name) -> future<> {
        return recursive_touch_directory(dir_name).then([dir_name = std::move(dir_name)] {
            return file_exists(dir_name).then([] (bool dir_exists) {
                BOOST_REQUIRE(dir_exists == true);
            });
        });
    };

    auto touch_file = [] (const sstring& file_name) -> future<> {
        return open_file_dma(file_name, open_flags::create).then([] (file f) mutable {
            return f.close();
        }).then([file_name = std::move(file_name)] {
            return file_exists(file_name).then([] (bool file_exists) {
                BOOST_REQUIRE(file_exists == true);
            });
        });
    };

    temp_sst_dir = sst::temp_sst_dir(sst_dir, 2);
    touch_dir(temp_sst_dir).get();

    temp_sst_dir = sst::temp_sst_dir(sst_dir, 3);
    touch_dir(temp_sst_dir).get();
    temp_file_name = sst::filename(temp_sst_dir, ks, cf, sst::version_types::mc, 3, sst::format_types::big, component_type::TemporaryTOC);
    touch_file(temp_file_name).get0();

    temp_file_name = sst::filename(sst_dir, ks, cf, sst::version_types::mc, 4, sst::format_types::big, component_type::TemporaryTOC);
    touch_file(temp_file_name).get0();
    temp_file_name = sst::filename(sst_dir, ks, cf, sst::version_types::mc, 4, sst::format_types::big, component_type::Data);
    touch_file(temp_file_name).get0();

    return do_with_cql_env([sst_dir = std::move(sst_dir), ks = std::move(ks), cf = std::move(cf)] (cql_test_env& e) {
        auto temp_dir_exists = file_exists(sst::temp_sst_dir(sst_dir, 2)).get0();
        BOOST_REQUIRE(temp_dir_exists == false);
        temp_dir_exists = file_exists(sst::temp_sst_dir(sst_dir, 3)).get0();
        BOOST_REQUIRE(temp_dir_exists == false);

        auto temp_file_name = sst::filename(sst_dir, ks, cf, sst::version_types::mc, 4, sst::format_types::big, component_type::TemporaryTOC);
        auto temp_file_exists = file_exists(temp_file_name).get0();
        BOOST_REQUIRE(temp_file_exists == false);
        temp_file_name = sst::filename(sst_dir, ks, cf, sst::version_types::mc, 4, sst::format_types::big, component_type::Data);
        temp_file_exists = file_exists(temp_file_name).get0();
        BOOST_REQUIRE(temp_file_exists == false);

        return make_ready_future<>();
    }, db_cfg);
  }).get();
}
