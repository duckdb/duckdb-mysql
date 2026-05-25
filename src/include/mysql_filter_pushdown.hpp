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

namespace duckdb {

class MySQLFilterPushdown {
public:
	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<string> &names);

	static string TransformFilter(const string &column_name, const TableFilter &filter);

private:
	static string TransformExpression(const string &column_name, const Expression &expr);
	static string TransformComparison(ExpressionType type);
	static string CreateExpression(const string &column_name, const vector<unique_ptr<Expression>> &filters,
	                               const string &op);
};

} // namespace duckdb
