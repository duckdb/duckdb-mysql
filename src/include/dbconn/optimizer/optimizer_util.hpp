#pragma once

#include <string>

#include "duckdb/common/types.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/column_binding.hpp"

#include "dbconn/bind_data.hpp"

namespace dbconnector {
namespace optimizer {

struct TracedBindingColumn {
	std::string col_name;
	duckdb::LogicalType col_type;

	bool Found() {
		return !col_name.empty();
	}
};

class OptimizerUtil {

public:
	static bool FindExtensionGet(const std::string &table_scan_name, duckdb::LogicalOperator &start,
	                             duckdb::LogicalGet *&get_out, dbconnector::BindData *&bind_out);

	static TracedBindingColumn TraceBindingToColumn(duckdb::ColumnBinding binding, duckdb::LogicalOperator &child,
	                                                duckdb::LogicalGet &get);
};

} // namespace optimizer
} // namespace dbconnector
