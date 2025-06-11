#include "mysql_filter_pushdown.hpp"
#include "mysql_utils.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

namespace duckdb {

string MySQLFilterPushdown::CreateExpression(string &column_name, vector<unique_ptr<TableFilter>> &filters, string op) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto new_filter = TransformFilter(column_name, *filter);
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

static string TransformBlobToMySQL(const string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	string result = "x'";
	for (idx_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}
	result += "'";
	return result;
}

string MySQLFilterPushdown::TransformConstant(const Value &val) {
	if (val.type().IsNumeric()) {
		return val.ToSQLString();
	}
	if (val.type().id() == LogicalTypeId::BLOB) {
		return TransformBlobToMySQL(StringValue::Get(val));
	}
	if (val.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
		return val.DefaultCastAs(LogicalType::TIMESTAMP).DefaultCastAs(LogicalType::VARCHAR).ToSQLString();
	}
	return val.DefaultCastAs(LogicalType::VARCHAR).ToSQLString();
}

string MySQLFilterPushdown::TransformFilter(string &column_name, TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		return column_name + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return column_name + " IS NOT NULL";
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = filter.Cast<ConjunctionAndFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "AND");
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_filter = filter.Cast<ConjunctionOrFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "OR");
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto constant_string = TransformConstant(constant_filter.constant);
		auto operator_string = TransformComparison(constant_filter.comparison_type);
		return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return TransformFilter(column_name, *optional_filter.child_filter);
	}
	case TableFilterType::DYNAMIC_FILTER: {
		return string();
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		string in_list;
		for (auto &val : in_filter.values) {
			if (!in_list.empty()) {
				in_list += ", ";
			}
			in_list += TransformConstant(val);
		}
		return column_name + " IN (" + in_list + ")";
	}
	default:
		throw InternalException("Unsupported table filter type");
	}
}

string MySQLFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                             const vector<string> &names) {
	if (!filters || filters->filters.empty()) {
		// no filters
		return string();
	}
	string result;
	for (auto &entry : filters->filters) {
		auto column_name = MySQLUtils::WriteIdentifier(names[column_ids[entry.first]]);
		auto &filter = *entry.second;
		auto new_filter = TransformFilter(column_name, filter);
		if (new_filter.empty()) {
			continue;
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += new_filter;
	}
	return result;
}

} // namespace duckdb
