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

#include "dbconnector/attached.hpp"
#include "dbconnector/table_scan/filter_pushdown.hpp"

namespace duckdb {

class MySQLFilterPushdown {
	dbconnector::attached::AttachedCatalog attached_catalog;

public:
	explicit MySQLFilterPushdown(dbconnector::attached::AttachedCatalog attached_catalog_p)
	    : attached_catalog(std::move(attached_catalog_p)) {
	}

	static bool CanPushExpressionDown(ClientContext &context, const LogicalGet &get, Expression &expr);

	string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                        const vector<string> &names);

private:
	dbconnector::table_scan::FilterPushdown::Config CreatePushdownConfig();
};

} // namespace duckdb
