#include "mysql_filter_pushdown.hpp"
#include "mysql_utils.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

static bool IsDirectReference(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	default:
		return false;
	}
}

string MySQLFilterPushdown::CreateExpression(const string &column_name, const vector<unique_ptr<Expression>> &filters,
                                             const string &op) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto new_filter = TransformExpression(column_name, *filter);
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

string MySQLFilterPushdown::TransformComparison(ExpressionType type) {
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
		throw NotImplementedException("Unsupported expression type");
	}
}

string MySQLFilterPushdown::TransformExpression(const string &column_name, const Expression &expr) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		const Value *constant = nullptr;
		if (IsDirectReference(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().value;
		} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && IsDirectReference(right)) {
			constant = &left.Cast<BoundConstantExpression>().value;
			comparison_type = FlipComparisonExpression(comparison_type);
		} else {
			return string();
		}
		auto constant_string = MySQLUtils::TransformConstant(*constant);
		auto operator_string = TransformComparison(comparison_type);
		return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return CreateExpression(column_name, conjunction.children, "AND");
		case ExpressionType::CONJUNCTION_OR:
			return CreateExpression(column_name, conjunction.children, "OR");
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			if (op.children.size() == 1 && IsDirectReference(*op.children[0])) {
				return column_name + " IS NULL";
			}
			return string();
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			if (op.children.size() == 1 && IsDirectReference(*op.children[0])) {
				return column_name + " IS NOT NULL";
			}
			return string();
		case ExpressionType::COMPARE_IN: {
			if (op.children.empty() || !IsDirectReference(*op.children[0])) {
				return string();
			}
			string in_list;
			for (idx_t i = 1; i < op.children.size(); i++) {
				if (op.children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return string();
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				in_list += MySQLUtils::TransformConstant(op.children[i]->Cast<BoundConstantExpression>().value);
			}
			return column_name + " IN (" + in_list + ")";
		}
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.GetName() == OptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr) : string();
		}
		if (func.function.GetName() == SelectivityOptionalFilterScalarFun::NAME && func.bind_info) {
			auto &data = func.bind_info->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr) : string();
		}
		if (func.function.GetName() == DynamicFilterScalarFun::NAME) {
			return string();
		}
		return string();
	}
	default:
		return string();
	}
}

string MySQLFilterPushdown::TransformFilter(const string &column_name, const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "MySQLFilterPushdown::TransformFilter");
	string filter_string = TransformExpression(column_name, *expr_filter.expr);
	if (filter_string.empty()) {
		string expr_str = expr_filter.expr->ToString();
		// TODO: FIXME with non-string checks
		if (expr_str.find("__internal_") == std::string::npos) {
			throw NotImplementedException(
			    "Unsupported filter pushdown, use 'mysql_enable_filter_pushdown=FALSE' to disable pushdowns."
			    " Problematic filter: \"%s\"",
			    expr_str);
		}
	}
	return filter_string;
}

string MySQLFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                             const vector<string> &names) {
	if (!filters || !filters->HasFilters()) {
		// no filters
		return string();
	}
	string result;
	for (auto &entry : *filters) {
		auto column_name = MySQLUtils::WriteIdentifier(names[column_ids[entry.GetIndex()]]);
		auto &filter = entry.Filter();
		auto new_filter = TransformFilter(column_name, filter);
		if (!result.empty()) {
			result += " AND ";
		}
		result += new_filter;
	}
	return result;
}

} // namespace duckdb
