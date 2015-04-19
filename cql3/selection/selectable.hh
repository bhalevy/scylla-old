/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_SELECTION_SELECTABLE_HH
#define CQL3_SELECTION_SELECTABLE_HH

#include "schema.hh"
#include "core/shared_ptr.hh"
#include "cql3/selection/selector.hh"
#include "cql3/functions/function_name.hh"

namespace cql3 {

namespace selection {

#if 0
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.commons.lang3.text.StrBuilder;
#endif

class selectable {
public:
    virtual ~selectable() {}
    virtual ::shared_ptr<selector::factory> new_selector_factory(database& db, schema_ptr schema, std::vector<const column_definition*>& defs) = 0;
protected:
    static size_t add_and_get_index(const column_definition& def, std::vector<const column_definition*>& defs) {
        auto i = std::find(defs.begin(), defs.end(), &def);
        if (i != defs.end()) {
            return std::distance(defs.begin(), i);
        }
        defs.push_back(&def);
        return defs.size() - 1;
    }
public:
    class raw {
    public:
        virtual ~raw() {}

        virtual ::shared_ptr<selectable> prepare(schema_ptr s) = 0;

        /**
         * Returns true if any processing is performed on the selected column.
         **/
        virtual bool processes_selection() const = 0;
    };

    class writetime_or_ttl;

    class with_function;

    class with_field_selection;
};

class selectable::with_function : public selectable {
    functions::function_name _function_name;
    std::vector<shared_ptr<selectable>> _args;
public:
    with_function(functions::function_name fname, std::vector<shared_ptr<selectable>> args)
        : _function_name(std::move(fname)), _args(std::move(args)) {
    }

#if 0
    @Override
    public String toString()
    {
        return new StrBuilder().append(functionName)
                               .append("(")
                               .appendWithSeparators(args, ", ")
                               .append(")")
                               .toString();
    }
#endif

    virtual shared_ptr<selector::factory> new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) override;
    class raw : public selectable::raw {
        functions::function_name _function_name;
        std::vector<shared_ptr<selectable::raw>> _args;
    public:
        raw(functions::function_name function_name, std::vector<shared_ptr<selectable::raw>> args)
                : _function_name(std::move(function_name)), _args(std::move(args)) {
        }
        virtual shared_ptr<selectable> prepare(schema_ptr s) override;
        virtual bool processes_selection() const override;
    };
};

}

}

#endif
