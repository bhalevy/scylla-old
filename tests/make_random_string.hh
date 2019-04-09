/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <seastar/testing/random_utils.hh>

#include "bytes.hh"

using namespace seastar;

inline
sstring make_random_string(size_t size = 1024) {
    sstring str(sstring::initialized_later(), size);
    boost::generate(str, [] { return testing::random.get_int<sstring::value_type>('a', 'z'); });
    return std::move(str);
}

inline
bytes make_random_bytes(size_t size = 128 * 1024) {
    bytes b(bytes::initialized_later(), size);
    boost::generate(b, [] { return testing::random.get_int<bytes::value_type>(); });
    return std::move(b);
}
