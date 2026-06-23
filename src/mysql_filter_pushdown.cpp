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

#include "dbconnector/table_scan/filter_util.hpp"

#include "mysql_utils.hpp"

namespace duckdb {

// This is a MySQL-specific variant of dbconnector::table_scan::FilterPushdown.
// The main difference is the handling of string constants: MySQL string comparisons are
// collation-dependent (case-insensitive with the default collation) while DuckDB compares
// bytes. We compare against binary strings (CAST(... AS BINARY)) to match DuckDB's semantics.

static string WriteFilterConstant(const Value &constant) {
	using namespace dbconnector::query;
	auto config = QueryWriter::CreateConfig('`', QuoteEscapeStyle::BACKSLASH, "x'", "");
	auto constant_string = QueryWriter::WriteConstant(config, constant);
	if (constant.type().id() == LogicalTypeId::VARCHAR) {
		// force a binary (byte-wise) comparison to match DuckDB's string comparison semantics
		return "CAST(" + constant_string + " AS BINARY)";
	}
	return constant_string;
}

//! Where a constant sorts relative to every value a MySQL column can contain. MySQL has no
//! infinite dates / timestamps and no inf / nan doubles, so DuckDB's non-finite constants
//! compare above (infinity, nan) or below (-infinity) every value in the column.
enum class MySQLConstantRange { FINITE, ABOVE_ALL_VALUES, BELOW_ALL_VALUES };

static MySQLConstantRange GetConstantRange(const Value &constant) {
	if (constant.IsNull()) {
		return MySQLConstantRange::FINITE;
	}
	switch (constant.type().id()) {
	case LogicalTypeId::FLOAT: {
		auto value = FloatValue::Get(constant);
		if (Value::FloatIsFinite(value)) {
			return MySQLConstantRange::FINITE;
		}
		// DuckDB sorts nan above infinity
		return value < 0 ? MySQLConstantRange::BELOW_ALL_VALUES : MySQLConstantRange::ABOVE_ALL_VALUES;
	}
	case LogicalTypeId::DOUBLE: {
		auto value = DoubleValue::Get(constant);
		if (Value::DoubleIsFinite(value)) {
			return MySQLConstantRange::FINITE;
		}
		return value < 0 ? MySQLConstantRange::BELOW_ALL_VALUES : MySQLConstantRange::ABOVE_ALL_VALUES;
	}
	case LogicalTypeId::DATE: {
		auto value = DateValue::Get(constant);
		if (Value::IsFinite(value)) {
			return MySQLConstantRange::FINITE;
		}
		return value == date_t::ninfinity() ? MySQLConstantRange::BELOW_ALL_VALUES
		                                    : MySQLConstantRange::ABOVE_ALL_VALUES;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto value = TimestampValue::Get(constant);
		if (Value::IsFinite(value)) {
			return MySQLConstantRange::FINITE;
		}
		return value == timestamp_t::ninfinity() ? MySQLConstantRange::BELOW_ALL_VALUES
		                                         : MySQLConstantRange::ABOVE_ALL_VALUES;
	}
	default:
		return MySQLConstantRange::FINITE;
	}
}

//! Serialize a comparison against a constant that compares above / below every value the column
//! can contain - the comparison is always true (for non-NULL values) or always false
static string WriteNonFiniteComparison(const string &column_name, ExpressionType comparison_type,
                                       MySQLConstantRange range) {
	bool always_true;
	switch (comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		always_true = range == MySQLConstantRange::ABOVE_ALL_VALUES;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		always_true = range == MySQLConstantRange::BELOW_ALL_VALUES;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		always_true = true;
		break;
	case ExpressionType::COMPARE_EQUAL:
		always_true = false;
		break;
	default:
		return string();
	}
	// note: a NULL value compares as NULL and is filtered out either way, matching IS NOT NULL
	return always_true ? column_name + " IS NOT NULL" : "FALSE";
}

static string TransformComparisonOperator(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		// unsupported comparison type
		return string();
	}
}

static bool IsDirectReference(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	default:
		return false;
	}
}

static string TransformFilterExpression(const string &column_name, const Expression &expr);

static string CreateConjunctionExpression(const string &column_name, const vector<unique_ptr<Expression>> &filters,
                                          const string &op) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto new_filter = TransformFilterExpression(column_name, *filter);
		if (new_filter.empty()) {
			continue;
		}
		filter_entries.push_back(std::move(new_filter));
	}
	if (filter_entries.empty()) {
		return string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

static string TransformFilterExpression(const string &column_name, const Expression &expr) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		const Value *constant = nullptr;
		if (IsDirectReference(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().GetValue();
		} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && IsDirectReference(right)) {
			constant = &left.Cast<BoundConstantExpression>().GetValue();
			comparison_type = FlipComparisonExpression(comparison_type);
		} else {
			return string();
		}
		auto constant_range = GetConstantRange(*constant);
		if (constant_range != MySQLConstantRange::FINITE) {
			// the constant cannot be represented in MySQL - but the column can never contain
			// a non-finite value either, so the comparison has a known outcome
			return WriteNonFiniteComparison(column_name, comparison_type, constant_range);
		}
		auto operator_string = TransformComparisonOperator(comparison_type);
		if (operator_string.empty()) {
			return string();
		}
		auto constant_string = WriteFilterConstant(*constant);
		return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return CreateConjunctionExpression(column_name, conjunction.GetChildren(), "AND");
		case ExpressionType::CONJUNCTION_OR:
			return CreateConjunctionExpression(column_name, conjunction.GetChildren(), "OR");
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			if (op.GetChildren().size() == 1 && IsDirectReference(*op.GetChildren()[0])) {
				return column_name + " IS NULL";
			}
			return string();
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			if (op.GetChildren().size() == 1 && IsDirectReference(*op.GetChildren()[0])) {
				return column_name + " IS NOT NULL";
			}
			return string();
		case ExpressionType::COMPARE_IN: {
			if (op.GetChildren().empty() || !IsDirectReference(*op.GetChildren()[0])) {
				return string();
			}
			string in_list;
			for (idx_t i = 1; i < op.GetChildren().size(); i++) {
				if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return string();
				}
				auto &constant = op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue();
				if (GetConstantRange(constant) != MySQLConstantRange::FINITE) {
					// the column can never contain a non-finite value - drop the element
					continue;
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				in_list += WriteFilterConstant(constant);
			}
			if (in_list.empty()) {
				// all elements were non-finite - the filter matches no rows
				return "FALSE";
			}
			return column_name + " IN (" + in_list + ")";
		}
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformFilterExpression(column_name, *data.child_filter_expr) : string();
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformFilterExpression(column_name, *data.child_filter_expr) : string();
		}
		if (func.Function().GetName() == DynamicFilterScalarFun::NAME) {
			return string();
		}
		return string();
	}
	default:
		return string();
	}
}

string MySQLFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                             const vector<string> &names) {
	if (!filters || !filters->HasFilters()) {
		// no filters
		return string();
	}
	string result;
	for (auto &entry : *filters) {
		column_t col_id = column_ids[entry.GetIndex()];
		auto column_name = MySQLUtils::WriteIdentifier(names[col_id]);
		auto &filter = entry.Filter();
		auto &filter_expr = dbconnector::table_scan::FilterUtil::GetExpression(filter, "MySQLFilterPushdown");
		auto new_filter = TransformFilterExpression(column_name, filter_expr);
		if (new_filter.empty()) {
			if (dbconnector::table_scan::FilterUtil::IsInternalFilter(filter)) {
				continue;
			}
			throw NotImplementedException(
			    "Unsupported filter pushdown, use 'mysql_enable_filter_pushdown=FALSE' to disable pushdowns."
			    " Problematic filter: \"%s\"",
			    dbconnector::table_scan::FilterUtil::ToString(filter));
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += new_filter;
	}
	return result;
}

} // namespace duckdb
