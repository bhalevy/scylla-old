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

#include <boost/test/unit_test.hpp>

#include "tests/sstable_assertions.hh"

void sstable_assertions::assert_toc(const std::set<component_type>& expected_components) {
    for (auto& expected : expected_components) {
        if(_sst->_recognized_components.count(expected) == 0) {
            BOOST_FAIL(format("Expected component of TOC missing: {}\n ... in: {}",
                              expected,
                              std::set<component_type>(
                                  cbegin(_sst->_recognized_components),
                                  cend(_sst->_recognized_components))));
        }
    }
    for (auto& present : _sst->_recognized_components) {
        if (expected_components.count(present) == 0) {
            BOOST_FAIL(format("Unexpected component of TOC: {}\n ... when expecting: {}",
                              present,
                              expected_components));
        }
    }
}

