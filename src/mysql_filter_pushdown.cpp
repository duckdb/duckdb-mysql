#include "mysql_filter_pushdown.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"

#include "dbconnector/query/query_writer.hpp"
#include "dbconnector/table_scan/filter_pushdown.hpp"
#include "dbconnector/table_scan/filter_util.hpp"

#include "mysql_utils.hpp"

namespace duckdb {

static string WriteDistinctFrom(ExpressionType distinct_type, const string &column_name,
                                const string &constant_string) {
	string res = StringUtil::Format("%s %s %s", column_name, "<=>", constant_string);
	switch (distinct_type) {
	// "a IS DISTINCT b" is trasformed to "NOT (a <=> b)"
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "NOT (" + res + ")";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return res;
	default:
		throw InvalidInputException("Unsupported DISTINCT FROM comparion type: %s", EnumUtil::ToString(distinct_type));
	}
}

static dbconnector::table_scan::FilterConstantRange GetConstantRange(const Value &constant) {
	using dbconnector::table_scan::FilterConstantRange;

	if (constant.IsNull()) {
		return FilterConstantRange::FINITE;
	}
	switch (constant.type().id()) {
	case LogicalTypeId::FLOAT: {
		auto value = FloatValue::Get(constant);
		if (Value::FloatIsFinite(value)) {
			return FilterConstantRange::FINITE;
		}
		// DuckDB sorts nan above infinity
		return value < 0 ? FilterConstantRange::BELOW_ALL_VALUES : FilterConstantRange::ABOVE_ALL_VALUES;
	}
	case LogicalTypeId::DOUBLE: {
		auto value = DoubleValue::Get(constant);
		if (Value::DoubleIsFinite(value)) {
			return FilterConstantRange::FINITE;
		}
		return value < 0 ? FilterConstantRange::BELOW_ALL_VALUES : FilterConstantRange::ABOVE_ALL_VALUES;
	}
	case LogicalTypeId::DATE: {
		auto value = DateValue::Get(constant);
		if (Value::IsFinite(value)) {
			return FilterConstantRange::FINITE;
		}
		return value == date_t::ninfinity() ? FilterConstantRange::BELOW_ALL_VALUES
		                                    : FilterConstantRange::ABOVE_ALL_VALUES;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto value = TimestampValue::Get(constant);
		if (Value::IsFinite(value)) {
			return FilterConstantRange::FINITE;
		}
		return value == timestamp_t::ninfinity() ? FilterConstantRange::BELOW_ALL_VALUES
		                                         : FilterConstantRange::ABOVE_ALL_VALUES;
	}
	default:
		return FilterConstantRange::FINITE;
	}
}

static dbconnector::table_scan::FilterPushdown::Config CreateMySQLConfig() {
	using namespace dbconnector;

	return table_scan::FilterPushdown::CreateConfig('`', '\'', query::QuoteEscapeStyle::BACKSLASH, "x'", "'",
	                                                "utf8mb4_bin", WriteDistinctFrom, GetConstantRange);
}

bool MySQLFilterPushdown::CanPushExpressionDown(ClientContext &, const LogicalGet &, Expression &expr) {
	using dbconnector::table_scan::FilterPushdown;

	auto config = CreateMySQLConfig();
	string filter = FilterPushdown::TransformFilterExpression(config, "dummy", expr);
	return !filter.empty();
}

string MySQLFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                             const vector<string> &names) {
	using namespace dbconnector;

	if (!filters || !filters->HasFilters()) {
		// no filters
		return string();
	}
	string result;
	for (auto &entry : *filters) {
		column_t col_id = column_ids[entry.GetIndex()];
		auto column_name = names[col_id];
		auto &filter = entry.Filter();
		auto config = CreateMySQLConfig();
		auto new_filter = table_scan::FilterPushdown::TransformFilter(config, column_name, filter, col_id);
		if (new_filter.empty()) {
			if (table_scan::FilterUtil::IsInternalFilter(filter)) {
				continue;
			}
			throw NotImplementedException(
			    "Unsupported filter pushdown, use 'mysql_enable_filter_pushdown=FALSE' to disable pushdowns."
			    " Problematic filter: \"%s\"",
			    table_scan::FilterUtil::ToString(filter));
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += new_filter;
	}
	return result;
}

} // namespace duckdb
