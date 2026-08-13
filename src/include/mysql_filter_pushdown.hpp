//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

class MySQLFilterPushdown {
public:
	static bool CanPushExpressionDown(ClientContext &context, const LogicalGet &get, Expression &expr);

	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<string> &names);
};

} // namespace duckdb
