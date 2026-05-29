#include "dbconn/table_scan/filter_pushdown.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"

#include "dbconn/query/query_writer.hpp"

#include "dbconn/table_scan/table_scan_exception.hpp"

namespace dbconnector {
namespace table_scan {

bool FilterPushdown::IsDirectReference(const duckdb::Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case duckdb::ExpressionClass::BOUND_REF:
	case duckdb::ExpressionClass::BOUND_COLUMN_REF:
		return true;
	default:
		return false;
	}
}

std::string FilterPushdown::CreateExpression(const std::string &column_name,
                                             const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters,
                                             const std::string &op) {
	duckdb::vector<std::string> filter_entries;
	for (auto &filter : filters) {
		auto new_filter = TransformExpression(column_name, *filter);
		if (new_filter.empty()) {
			continue;
		}
		filter_entries.push_back(std::move(new_filter));
	}
	if (filter_entries.empty()) {
		return std::string();
	}
	return "(" + duckdb::StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

std::string FilterPushdown::TransformComparison(duckdb::ExpressionType type) {
	switch (type) {
	case duckdb::ExpressionType::COMPARE_EQUAL:
		return "=";
	case duckdb::ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case duckdb::ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw TableScanException("Unsupported expression type: '" + duckdb::EnumUtil::ToString(type) + "'");
	}
}

std::string FilterPushdown::TransformExpression(const std::string &column_name, const duckdb::Expression &expr) {
	if (duckdb::BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<duckdb::BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = duckdb::BoundComparisonExpression::Left(comparison);
		auto &right = duckdb::BoundComparisonExpression::Right(comparison);
		const duckdb::Value *constant = nullptr;
		if (IsDirectReference(left) && right.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<duckdb::BoundConstantExpression>().value;
		} else if (left.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT && IsDirectReference(right)) {
			constant = &left.Cast<duckdb::BoundConstantExpression>().value;
			comparison_type = FlipComparisonExpression(comparison_type);
		} else {
			return std::string();
		}
		auto constant_string = query::QueryWriter::WriteConstant(*constant);
		auto operator_string = TransformComparison(comparison_type);
		return duckdb::StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
	}

	switch (expr.GetExpressionClass()) {
	case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<duckdb::BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case duckdb::ExpressionType::CONJUNCTION_AND:
			return CreateExpression(column_name, conjunction.children, "AND");
		case duckdb::ExpressionType::CONJUNCTION_OR:
			return CreateExpression(column_name, conjunction.children, "OR");
		default:
			return std::string();
		}
	}
	case duckdb::ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<duckdb::BoundOperatorExpression>();
		switch (op.GetExpressionType()) {
		case duckdb::ExpressionType::OPERATOR_IS_NULL:
			if (op.children.size() == 1 && IsDirectReference(*op.children[0])) {
				return column_name + " IS NULL";
			}
			return std::string();
		case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL:
			if (op.children.size() == 1 && IsDirectReference(*op.children[0])) {
				return column_name + " IS NOT NULL";
			}
			return std::string();
		case duckdb::ExpressionType::COMPARE_IN: {
			if (op.children.empty() || !IsDirectReference(*op.children[0])) {
				return std::string();
			}
			std::string in_list;
			for (idx_t i = 1; i < op.children.size(); i++) {
				if (op.children[i]->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
					return std::string();
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				in_list +=
				    query::QueryWriter::WriteConstant(op.children[i]->Cast<duckdb::BoundConstantExpression>().value);
			}
			return column_name + " IN (" + in_list + ")";
		}
		default:
			return std::string();
		}
	}
	case duckdb::ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<duckdb::BoundFunctionExpression>();
		if (func.function.GetName() == duckdb::OptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<duckdb::OptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr) : std::string();
		}
		if (func.function.GetName() == duckdb::SelectivityOptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<duckdb::SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr) : std::string();
		}
		if (func.function.GetName() == duckdb::DynamicFilterScalarFun::NAME) {
			return std::string();
		}
		return std::string();
	}
	default:
		return std::string();
	}
}

std::string FilterPushdown::TransformFilter(const FilterPushdownConfig &config, const std::string &column_name,
                                            const duckdb::TableFilter &filter) {
	std::string column_name_quoted = query::QueryWriter::WriteIdentifier(column_name, config.identifier_quote);
	auto &expr = GetExpression(filter, "FilterPushdown::TransformFilter");
	return TransformExpression(column_name_quoted, expr);
}

bool FilterPushdown::IsInternalFilter(const duckdb::TableFilter &filter) {
	auto &expr = GetExpression(filter, "FilterPushdown::IsInternalFilter");
	return expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION &&
	       expr.ToString().find("__internal_tablefilter_") == 0;
}

std::string FilterPushdown::ToString(const duckdb::TableFilter &filter) {
	auto &expr = GetExpression(filter, "FilterPushdown::ToString");
	return expr.ToString();
}

const duckdb::Expression &FilterPushdown::GetExpression(const duckdb::TableFilter &filter, const std::string &context) {
	auto &expr_filter = duckdb::ExpressionFilter::GetExpressionFilter(filter, context.c_str());
	return *expr_filter.expr;
}

} // namespace table_scan
} // namespace dbconnector
