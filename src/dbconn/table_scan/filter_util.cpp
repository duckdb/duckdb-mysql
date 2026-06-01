#include "dbconn/table_scan/filter_util.hpp"

#include "duckdb/planner/filter/expression_filter.hpp"

namespace dbconnector {
namespace table_scan {

const duckdb::Expression &FilterUtil::GetExpression(const duckdb::TableFilter &filter,
                                                    const std::string &call_context) {
	auto &expr_filter = duckdb::ExpressionFilter::GetExpressionFilter(filter, call_context.c_str());
	return *expr_filter.expr;
}

bool FilterUtil::IsInternalFilter(const duckdb::TableFilter &filter) {
	auto &expr = GetExpression(filter, "FilterPushdown::IsInternalFilter");
	return expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION &&
	       expr.ToString().find("__internal_tablefilter_") == 0;
}

std::string FilterUtil::ToString(const duckdb::TableFilter &filter) {
	auto &expr = GetExpression(filter, "FilterPushdown::ToString");
	return expr.ToString();
}

} // namespace table_scan
} // namespace dbconnector
