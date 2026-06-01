#include "dbconn/optimizer/optimizer_util.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace dbconnector {
namespace optimizer {

using namespace duckdb;

bool OptimizerUtil::FindExtensionGet(const std::string &table_scan_name, LogicalOperator &start, LogicalGet *&get_out,
                                     dbconnector::BindData *&bind_out) {
	reference<LogicalOperator> current = start;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (current.get().children.empty()) {
			return false;
		}
		current = *current.get().children[0];
	}
	if (current.get().type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = current.get().Cast<LogicalGet>();
	if (get.function.name != table_scan_name) {
		return false;
	}
	get_out = &get;
	bind_out = &get.bind_data->Cast<dbconnector::BindData>();
	return true;
}

TracedBindingColumn OptimizerUtil::TraceBindingToColumn(ColumnBinding binding, LogicalOperator &child,
                                                        LogicalGet &get) {
	TracedBindingColumn res;
	reference<LogicalOperator> current = child;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = current.get().Cast<LogicalProjection>();
		if (binding.table_index != proj.table_index) {
			break;
		}
		if (binding.column_index >= proj.expressions.size()) {
			return res;
		}
		auto &proj_expr = *proj.expressions[binding.column_index];
		if (proj_expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return res;
		}
		auto &inner_ref = proj_expr.Cast<BoundColumnRefExpression>();
		if (inner_ref.depth > 0) {
			return res;
		}
		binding = inner_ref.binding;
		current = *current.get().children[0];
	}

	if (binding.table_index != get.table_index) {
		return res;
	}
	auto &column_ids = get.GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return res;
	}
	auto &col_index = column_ids[binding.column_index];
	if (col_index.IsRowIdColumn()) {
		return res;
	}
	auto actual_col_idx = col_index.GetPrimaryIndex();
	if (actual_col_idx >= get.names.size()) {
		return res;
	}
	res.col_name = get.names[actual_col_idx];
	res.col_type = get.returned_types[actual_col_idx];
	return res;
}

} // namespace optimizer
} // namespace dbconnector
