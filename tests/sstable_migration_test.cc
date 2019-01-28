/*
 * Copyright (C) 2019 ScyllaDB
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

#include <boost/test/unit_test.hpp>

#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>
#include <seastar/util/defer.hh>

#include "tests/flat_mutation_reader_assertions.hh"
#include "tests/sstable_assertions.hh"
#include "sstables/types.hh"
#include "types.hh"

using namespace sstables;

SEASTAR_THREAD_TEST_CASE(test_migration_with_compact_storage_and_no_clustering_key) {
    // Created with Cassandra v3.11.3 using:
    //
    // cqlsh> CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor':1};
    // cqlsh> CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c2 text, c1 text) WITH comment='test cf' AND compression = {} AND read_repair_chance=0.000000 AND COMPACT STORAGE;
    // cqlsh> INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
    static thread_local const schema_ptr schema =
        schema_builder("ks", "cf")
            .with(schema_builder::compact_storage::yes)
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("c2", utf8_type, column_kind::static_column)
            .with_column("c1", utf8_type, column_kind::static_column)
            .set_compressor_params(compression_parameters::no_compression())
            .build();

    static thread_local const sstring sstable_path =
        "tests/sstables/migration/3_0_mc/with_compact_storage/no_clustering_key";

    auto abj = defer([] { await_background_jobs().get(); });
    sstable_assertions sst(schema, sstable_path);
    sst.load();

    auto to_key = [] (const char* key) {
        auto bytes = utf8_type->decompose(key);
        auto pk = partition_key::from_single_value(*schema, bytes);
        return dht::global_partitioner().decorate_key(*schema, pk);
    };

    auto c1_cdef = schema->get_column_definition(to_bytes("c1"));
    BOOST_REQUIRE(c1_cdef);
    auto c2_cdef = schema->get_column_definition(to_bytes("c2"));
    BOOST_REQUIRE(c2_cdef);

    auto to_expected = [] (const column_definition* cdef1, const char* val1, const column_definition* cdef2, const char* val2) {
        return std::vector<flat_reader_assertions::expected_column>{
            {cdef1, utf8_type->decompose(val1)},
            {cdef2, utf8_type->decompose(val2)},
        };
    };

    // Sequential read
    {
        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key("a"))
            .produces_static_row(to_expected(c1_cdef, "abc", c2_cdef, "cde"))
            .produces_partition_end()
            .produces_end_of_stream();
    }
}

SEASTAR_THREAD_TEST_CASE(test_migration_with_compact_storage_and_clustering_key) {
    // Created with Cassandra v3.10 using:
    //
    // cqlsh> CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor':1};
    // cqlsh> CREATE COLUMNFAMILY ks.cf (key varchar, c1 text, PRIMARY KEY (key, c1)) WITH compression = {} AND COMPACT STORAGE;
    // cqlsh> INSERT INTO ks.cf (key, c1) VALUES ('a', 'abc');
    static thread_local const schema_ptr schema =
        schema_builder("ks", "cf")
            .with(schema_builder::compact_storage::yes)
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("c1", utf8_type, column_kind::clustering_key)
            .set_compressor_params(compression_parameters::no_compression())
            .build();

    static thread_local const sstring sstable_path =
        "tests/sstables/migration/3_0_mc/with_compact_storage/with_clustering_key";

    auto abj = defer([] { await_background_jobs().get(); });
    sstable_assertions sst(schema, sstable_path);
    sst.load();

    auto to_key = [] (const char* key) {
        auto bytes = utf8_type->decompose(key);
        auto pk = partition_key::from_single_value(*schema, bytes);
        return dht::global_partitioner().decorate_key(*schema, pk);
    };

    auto to_ck = [] (const char* ck) {
        return clustering_key::from_single_value(*schema,
                                                 utf8_type->decompose(ck));
    };

    auto c1_cdef = schema->get_column_definition(to_bytes("c1"));
    BOOST_REQUIRE(c1_cdef);

    auto to_expected = [] (const column_definition* cdef1, const char* val1) {
        return std::vector<flat_reader_assertions::expected_column>{
            {cdef1, utf8_type->decompose(val1)},
        };
    };


    // Sequential read
    {
        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key("a"))
            .produces_row(to_ck("abc"), to_expected(c1_cdef, ""))
            .produces_partition_end()
            .produces_end_of_stream();
    }
}

SEASTAR_THREAD_TEST_CASE(test_migration_with_compact_storage_and_regular_column) {
    // Created with Cassandra v3.10 using:
    //
    // cqlsh> CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor':1};
    // cqlsh> CREATE COLUMNFAMILY ks.cf (key varchar, c1 text, c2 text, PRIMARY KEY (key, c1)) WITH compression = {} AND COMPACT STORAGE;
    // cqlsh> INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
    static thread_local const schema_ptr schema =
        schema_builder("ks", "cf")
            .with(schema_builder::compact_storage::yes)
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("c1", utf8_type, column_kind::clustering_key)
            .with_column("c2", utf8_type)
            .set_compressor_params(compression_parameters::no_compression())
            .build();

    static thread_local const sstring sstable_path =
        "tests/sstables/migration/3_0_mc/with_compact_storage/with_regular_column";

    auto abj = defer([] { await_background_jobs().get(); });
    sstable_assertions sst(schema, sstable_path);
    sst.load();

    auto to_key = [] (const char* key) {
        auto bytes = utf8_type->decompose(key);
        auto pk = partition_key::from_single_value(*schema, bytes);
        return dht::global_partitioner().decorate_key(*schema, pk);
    };

    auto to_ck = [] (const char* ck) {
        return clustering_key::from_single_value(*schema,
                                                 utf8_type->decompose(ck));
    };

    auto c1_cdef = schema->get_column_definition(to_bytes("c1"));
    BOOST_REQUIRE(c1_cdef);
    auto c2_cdef = schema->get_column_definition(to_bytes("c2"));
    BOOST_REQUIRE(c2_cdef);

    auto to_expected = [] (const column_definition* cdef2, const char* val2) {
        return std::vector<flat_reader_assertions::expected_column>{
            {cdef2, utf8_type->decompose(val2)},
        };
    };

    // Sequential read
    {
        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key("a"))
            .produces_row(to_ck("abc"), to_expected(c2_cdef, "cde"))
            .produces_partition_end()
            .produces_end_of_stream();
    }
}
