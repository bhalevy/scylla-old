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


#pragma once

#include "query-request.hh"
#include <optional>
#include <variant>

// Wraps ring_position or ring_position_view so either is compatible with old-style C++: default
// constructor, stateless comparators, yada yada.
// The motivations for supporting both types are to make containers self-sufficient by not relying
// on callers to keep ring position alive, allow lookup on containers that don't support different
// key types, and also avoiding unnecessary copies.
class compatible_ring_position {
    const schema* _schema = nullptr;
    // Optional to supply a default constructor, no more.
    std::optional<std::variant<dht::ring_position, dht::ring_position_view>> _rp_data;
public:
    constexpr compatible_ring_position() = default;
    explicit compatible_ring_position(const schema& s, dht::ring_position rp)
        : _schema(&s), _rp_data(std::move(rp)) {
    }
    explicit compatible_ring_position(const schema& s, dht::ring_position_view rpv)
        : _schema(&s), _rp_data(rpv) {
    }
    dht::ring_position_view position() const {
        struct rp_data_visitor {
            dht::ring_position_view operator()(const dht::ring_position& rp) {
                return rp;
            }
            dht::ring_position_view operator()(const dht::ring_position_view& rpv) {
                return rpv;
            }
        };
        return std::visit(rp_data_visitor{}, *_rp_data);
    }
    friend int tri_compare(const compatible_ring_position& x, const compatible_ring_position& y) {
        return dht::ring_position_tri_compare(*x._schema, x.position(), y.position());
    }
    friend bool operator<(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) < 0;
    }
    friend bool operator<=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) <= 0;
    }
    friend bool operator>(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) > 0;
    }
    friend bool operator>=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) >= 0;
    }
    friend bool operator==(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) == 0;
    }
    friend bool operator!=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) != 0;
    }
};

