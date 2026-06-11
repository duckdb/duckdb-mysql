#include "mysql_sql_writer.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/common/extra_type_info.hpp"

#include "mysql_utils.hpp"

namespace duckdb {

MySQLSQLWriter::MySQLSQLWriter(ClientContext &context, const MySQLVersion &version)
    : context(context), binary_collation(version.GetBinaryCollation()) {
}

string MySQLSQLWriter::MySQLToString(ClientContext &context, const MySQLVersion &version, const QueryNode &node) {
	MySQLSQLWriter writer(context, version);
	return writer.WriteQueryNode(node);
}

//===--------------------------------------------------------------------===//
// Identifiers / Constants / Types
//===--------------------------------------------------------------------===//
string MySQLSQLWriter::WriteIdentifier(const string &identifier) {
	return MySQLUtils::WriteIdentifier(identifier);
}

string MySQLSQLWriter::WriteConstant(const Value &value) {
	if (value.IsNull()) {
		return "NULL";
	}
	auto type_id = value.type().id();
	switch (type_id) {
	case LogicalTypeId::BOOLEAN:
		return BooleanValue::Get(value) ? "TRUE" : "FALSE";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return value.ToString();
	case LogicalTypeId::VARCHAR:
		if (binary_collation.empty()) {
			// SupportsPushdown blocks string literals when the server has no binary collation
			throw InternalException("MySQLSQLWriter: no binary collation available for this server version");
		}
		// WriteLiteral escapes both quotes and backslashes
		// (MySQL treats backslash as an escape character within string literals)
		// The explicit binary collation makes any comparison involving the literal byte-wise,
		// matching DuckDB's string comparison semantics (MySQL's default collation is
		// case-insensitive). The collation is a NO PAD collation, so trailing spaces are
		// significant as well. Comparisons against non-string types are unaffected - MySQL
		// still coerces the string to the other operand's type (date, number, ...).
		return "(" + MySQLUtils::WriteLiteral(StringValue::Get(value)) + " COLLATE " + binary_collation + ")";
	case LogicalTypeId::BLOB:
		return MySQLUtils::TransformConstant(value);
	case LogicalTypeId::DATE:
		return "DATE " + MySQLUtils::WriteLiteral(value.ToString());
	case LogicalTypeId::TIME:
		return "TIME " + MySQLUtils::WriteLiteral(value.ToString());
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP " + MySQLUtils::WriteLiteral(value.ToString());
	default:
		throw InternalException("MySQLSQLWriter: unsupported constant type %s - should have been blocked by "
		                        "SupportsPushdown",
		                        value.type().ToString());
	}
}

string MySQLSQLWriter::WriteCastType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return "SIGNED";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return "UNSIGNED";
	case LogicalTypeId::VARCHAR:
		return "CHAR";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP:
		return "DATETIME";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::DECIMAL:
		return StringUtil::Format("DECIMAL(%d,%d)", DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::UNBOUND: {
		auto try_bind = UnboundType::TryDefaultBind(type);
		if (try_bind.id() == LogicalTypeId::UNBOUND) {
			throw InternalException("MySQLSQLWriter: unsupported unbound cast type - should have been blocked by "
			                        "SupportsPushdown");
		}
		return WriteCastType(try_bind);
	}
	default:
		throw InternalException("MySQLSQLWriter: unsupported cast type %s - should have been blocked by "
		                        "SupportsPushdown",
		                        type.ToString());
	}
}

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
string MySQLSQLWriter::WriteExpressionList(const vector<unique_ptr<ParsedExpression>> &expressions,
                                           const string &separator) {
	string result;
	for (auto &expr : expressions) {
		if (!result.empty()) {
			result += separator;
		}
		result += WriteExpression(*expr);
	}
	return result;
}

string MySQLSQLWriter::WriteColumnRef(const ColumnRefExpression &column_ref) {
	string result;
	for (auto &column_name : column_ref.ColumnNames()) {
		if (!result.empty()) {
			result += ".";
		}
		result += WriteIdentifier(column_name.GetIdentifierName());
	}
	return result;
}

string MySQLSQLWriter::WriteArgumentList(const vector<FunctionArgument> &arguments) {
	string result;
	for (auto &argument : arguments) {
		if (!result.empty()) {
			result += ", ";
		}
		result += WriteExpression(argument.GetExpression());
	}
	return result;
}

string MySQLSQLWriter::WriteFunctionCall(const string &function_name, const vector<FunctionArgument> &children,
                                         bool distinct) {
	auto name = StringUtil::Lower(function_name);
	if ((name == "count_star" || name == "count") && children.empty()) {
		return "count(*)";
	}
	if (name == "concat") {
		// DuckDB's concat skips NULL inputs - the MySQL equivalent is concat_ws with an empty separator
		string result = "concat_ws(''";
		for (auto &child : children) {
			result += ", " + WriteExpression(child.GetExpression());
		}
		result += ")";
		return result;
	}
	if (name == "avg" && children.size() == 1) {
		// MySQL's avg returns DECIMAL(x, 4) for integer inputs - cast to DOUBLE to match DuckDB
		return "avg(" + string(distinct ? "DISTINCT " : "") + "CAST(" + WriteExpression(children[0].GetExpression()) +
		       " AS DOUBLE))";
	}
	if ((name == "greatest" || name == "least") && children.size() >= 2) {
		// MySQL returns NULL when ANY argument is NULL while DuckDB skips NULLs.
		// Replace every argument with a COALESCE over all arguments starting at itself:
		// each rotation yields the argument itself when it is non-NULL and some other
		// non-NULL argument otherwise, so the result is NULL only if all arguments are NULL
		string result = name + "(";
		vector<string> args;
		for (auto &child : children) {
			args.push_back(WriteExpression(child.GetExpression()));
		}
		for (idx_t i = 0; i < args.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += "COALESCE(";
			for (idx_t k = 0; k < args.size(); k++) {
				if (k > 0) {
					result += ", ";
				}
				result += args[(i + k) % args.size()];
			}
			result += ")";
		}
		result += ")";
		return result;
	}
	if (name == "microsecond" && children.size() == 1) {
		// DuckDB's microsecond includes the seconds component, MySQL's does not
		auto child = WriteExpression(children[0].GetExpression());
		return "(microsecond(" + child + ") + second(" + child + ") * 1000000)";
	}
	if ((name == "dayofweek" || name == "weekday") && children.size() == 1) {
		// DuckDB numbers days Sunday=0 .. Saturday=6; MySQL's DAYOFWEEK is Sunday=1 .. Saturday=7
		return "(dayofweek(" + WriteExpression(children[0].GetExpression()) + ") - 1)";
	}
	if (name == "isodow" && children.size() == 1) {
		// DuckDB's ISO day-of-week is Monday=1 .. Sunday=7; MySQL's WEEKDAY is Monday=0 .. Sunday=6
		return "(weekday(" + WriteExpression(children[0].GetExpression()) + ") + 1)";
	}
	if (name == "week" && children.size() == 1) {
		// DuckDB's week is the ISO week; MySQL's default week mode is not - mode 3 matches
		return "week(" + WriteExpression(children[0].GetExpression()) + ", 3)";
	}
	if (name == "trunc" && children.size() == 1) {
		// MySQL spells truncation towards zero as truncate(x, d)
		return "truncate(" + WriteExpression(children[0].GetExpression()) + ", 0)";
	}
	if (name == "log" && children.size() == 1) {
		// DuckDB's log(x) is log base 10; MySQL's log(x) is the natural logarithm
		// (the two-argument log(b, x) has identical semantics in both systems)
		name = "log10";
	} else if (name == "length" || name == "len") {
		// MySQL's length() counts bytes; char_length() counts characters like DuckDB
		name = "char_length";
	} else if (name == "stddev") {
		// DuckDB's stddev is the sample standard deviation; MySQL's is the population variant
		name = "stddev_samp";
	} else if (name == "variance") {
		// DuckDB's variance is the sample variance; MySQL's is the population variant
		name = "var_samp";
	}
	string result = name + "(";
	if (distinct) {
		result += "DISTINCT ";
	}
	result += WriteArgumentList(children);
	result += ")";
	return result;
}

string MySQLSQLWriter::WriteFunction(const FunctionExpression &func) {
	auto &children = func.GetArguments();
	auto name = StringUtil::Lower(func.FunctionName().GetIdentifierName());
	if (name == "~~" || name == "!~~") {
		// [NOT] LIKE - DuckDB's LIKE has no default escape character, while MySQL's default
		// escape character is backslash; disable it with an empty ESCAPE clause
		D_ASSERT(children.size() == 2);
		return "(" + WriteExpression(children[0].GetExpression()) + (name == "~~" ? " LIKE " : " NOT LIKE ") +
		       WriteExpression(children[1].GetExpression()) + " ESCAPE '')";
	}
	static const case_insensitive_set_t OPERATOR_NAMES = {"+", "-", "*", "/", "%", "||"};
	if (func.IsOperator() || OPERATOR_NAMES.count(name) > 0) {
		if (name == "||") {
			// DuckDB's || propagates NULL inputs - identical to MySQL's concat
			// (note: MySQL's || is a logical OR unless PIPES_AS_CONCAT is enabled)
			D_ASSERT(children.size() == 2);
			return "concat(" + WriteExpression(children[0].GetExpression()) + ", " +
			       WriteExpression(children[1].GetExpression()) + ")";
		}
		if (children.size() == 1) {
			return "(" + name + WriteExpression(children[0].GetExpression()) + ")";
		}
		if (children.size() == 2) {
			return "(" + WriteExpression(children[0].GetExpression()) + " " + name + " " +
			       WriteExpression(children[1].GetExpression()) + ")";
		}
		throw InternalException("MySQLSQLWriter: unsupported operator %s - should have been blocked by "
		                        "SupportsPushdown",
		                        name);
	}
	return WriteFunctionCall(func.FunctionName().GetIdentifierName(), children, func.Distinct());
}

static string MySQLWriteWindowBoundary(WindowBoundary boundary, const string &boundary_expr) {
	switch (boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		return "UNBOUNDED PRECEDING";
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		return "UNBOUNDED FOLLOWING";
	case WindowBoundary::CURRENT_ROW_ROWS:
	case WindowBoundary::CURRENT_ROW_RANGE:
		return "CURRENT ROW";
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		return boundary_expr + " PRECEDING";
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		return boundary_expr + " FOLLOWING";
	default:
		throw InternalException("MySQLSQLWriter: unsupported window boundary - should have been blocked by "
		                        "SupportsPushdown");
	}
}

string MySQLSQLWriter::WriteWindow(const WindowExpression &window) {
	string result =
	    WriteFunctionCall(window.FunctionName().GetIdentifierName(), window.GetArguments(), window.Distinct());
	result += " OVER (";
	string sep;
	if (!window.Partitions().empty()) {
		result += "PARTITION BY " + WriteExpressionList(window.Partitions());
		sep = " ";
	}
	if (!window.OrderBy().empty()) {
		result += sep + "ORDER BY " + WriteOrderList(window.OrderBy(), nullptr);
		sep = " ";
	}
	auto window_start = window.WindowStart();
	auto window_end = window.WindowEnd();
	if (!(window_start == WindowBoundary::UNBOUNDED_PRECEDING && window_end == WindowBoundary::CURRENT_ROW_RANGE)) {
		// non-default frame
		bool is_rows =
		    window_start == WindowBoundary::CURRENT_ROW_ROWS || window_start == WindowBoundary::EXPR_PRECEDING_ROWS ||
		    window_start == WindowBoundary::EXPR_FOLLOWING_ROWS || window_end == WindowBoundary::CURRENT_ROW_ROWS ||
		    window_end == WindowBoundary::EXPR_PRECEDING_ROWS || window_end == WindowBoundary::EXPR_FOLLOWING_ROWS;
		string start_expr = window.StartExpr() ? WriteExpression(*window.StartExpr()) : string();
		string end_expr = window.EndExpr() ? WriteExpression(*window.EndExpr()) : string();
		result += sep + (is_rows ? "ROWS" : "RANGE");
		result += " BETWEEN " + MySQLWriteWindowBoundary(window_start, start_expr);
		result += " AND " + MySQLWriteWindowBoundary(window_end, end_expr);
	}
	result += ")";
	return result;
}

string MySQLSQLWriter::WriteOperator(const OperatorExpression &op) {
	auto &children = op.GetChildren();
	switch (op.GetExpressionType()) {
	case ExpressionType::OPERATOR_IS_NULL:
		return "(" + WriteExpression(*children[0]) + " IS NULL)";
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return "(" + WriteExpression(*children[0]) + " IS NOT NULL)";
	case ExpressionType::OPERATOR_NOT:
		return "(NOT " + WriteExpressionList(children) + ")";
	case ExpressionType::OPERATOR_COALESCE:
		return "COALESCE(" + WriteExpressionList(children) + ")";
	case ExpressionType::OPERATOR_NULLIF:
		return "NULLIF(" + WriteExpressionList(children) + ")";
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN: {
		string result = "(" + WriteExpression(*children[0]);
		result += op.GetExpressionType() == ExpressionType::COMPARE_IN ? " IN (" : " NOT IN (";
		for (idx_t i = 1; i < children.size(); i++) {
			if (i > 1) {
				result += ", ";
			}
			result += WriteExpression(*children[i]);
		}
		result += "))";
		return result;
	}
	default:
		throw InternalException("MySQLSQLWriter: unsupported operator type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

string MySQLSQLWriter::WriteCase(const CaseExpression &case_expr) {
	string result = "CASE";
	for (auto &check : case_expr.CaseChecks()) {
		result += " WHEN " + WriteExpression(*check.when_expr);
		result += " THEN " + WriteExpression(*check.then_expr);
	}
	result += " ELSE " + WriteExpression(case_expr.Else());
	result += " END";
	return result;
}

//! Serialize a comparison between two (already serialized) operands
static string MySQLWriteComparison(ExpressionType type, const string &left, const string &right) {
	string comparison_operator;
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		comparison_operator = "=";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		comparison_operator = "<>";
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		comparison_operator = "<";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		comparison_operator = ">";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		comparison_operator = "<=";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		comparison_operator = ">=";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		// IS NOT DISTINCT FROM maps to MySQL's NULL-safe equality operator <=>
		return "(" + left + " <=> " + right + ")";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		// <=> never returns NULL, so negating it is safe
		return "(NOT (" + left + " <=> " + right + "))";
	default:
		throw InternalException("MySQLSQLWriter: unsupported comparison type - should have been blocked by "
		                        "SupportsPushdown");
	}
	return "(" + left + " " + comparison_operator + " " + right + ")";
}

string MySQLSQLWriter::WriteSubquery(const SubqueryExpression &subquery) {
	switch (subquery.GetSubqueryType()) {
	case SubqueryType::EXISTS:
		return "EXISTS(" + WriteQueryNode(*subquery.Subquery()->node) + ")";
	case SubqueryType::NOT_EXISTS:
		return "NOT EXISTS(" + WriteQueryNode(*subquery.Subquery()->node) + ")";
	case SubqueryType::SCALAR:
		return "(" + WriteQueryNode(*subquery.Subquery()->node) + ")";
	case SubqueryType::ANY:
		return MySQLWriteComparison(subquery.GetComparisonType(), WriteExpression(*subquery.GetChild()),
		                            "ANY(" + WriteQueryNode(*subquery.Subquery()->node) + ")");
	default:
		throw InternalException("MySQLSQLWriter: unsupported subquery type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

string MySQLSQLWriter::WriteExpression(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
		return WriteConstant(expr.Cast<ConstantExpression>().GetValue());
	case ExpressionClass::COLUMN_REF:
		return WriteColumnRef(expr.Cast<ColumnRefExpression>());
	case ExpressionClass::STAR: {
		auto &star = expr.Cast<StarExpression>();
		if (!star.RelationName().empty()) {
			// qualified star (e.g. t.*)
			return WriteIdentifier(star.RelationName().GetIdentifierName()) + ".*";
		}
		return "*";
	}
	case ExpressionClass::FUNCTION:
		return WriteFunction(expr.Cast<FunctionExpression>());
	case ExpressionClass::WINDOW:
		return WriteWindow(expr.Cast<WindowExpression>());
	case ExpressionClass::COMPARISON: {
		auto &comparison = expr.Cast<ComparisonExpression>();
		return MySQLWriteComparison(expr.GetExpressionType(), WriteExpression(comparison.Left()),
		                            WriteExpression(comparison.Right()));
	}
	case ExpressionClass::CONJUNCTION: {
		auto &conjunction = expr.Cast<ConjunctionExpression>();
		auto op = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
		return "(" + WriteExpressionList(conjunction.GetChildren(), op) + ")";
	}
	case ExpressionClass::OPERATOR:
		return WriteOperator(expr.Cast<OperatorExpression>());
	case ExpressionClass::CASE:
		return WriteCase(expr.Cast<CaseExpression>());
	case ExpressionClass::CAST: {
		auto &cast_expr = expr.Cast<CastExpression>();
		if (cast_expr.Child().GetExpressionClass() == ExpressionClass::CONSTANT) {
			// casts of literals are evaluated locally and serialized as the resulting value -
			// MySQL's CAST diverges from DuckDB's for malformed input (NULL / partial values
			// instead of errors), numeric strings ('1e2', '5.7') and time zone offsets
			// (SupportsPushdown verifies that the cast succeeds)
			auto &value = cast_expr.Child().Cast<ConstantExpression>().GetValue();
			auto target_type = cast_expr.TargetType();
			if (target_type.id() == LogicalTypeId::UNBOUND) {
				target_type = UnboundType::TryDefaultBind(target_type);
			}
			Value cast_result;
			string error;
			if (!value.DefaultTryCastAs(target_type, cast_result, &error)) {
				throw InternalException("MySQLSQLWriter: cast of literal failed - should have been blocked by "
				                        "SupportsPushdown");
			}
			return WriteConstant(cast_result);
		}
		return "CAST(" + WriteExpression(cast_expr.Child()) + " AS " + WriteCastType(cast_expr.TargetType()) + ")";
	}
	case ExpressionClass::BETWEEN: {
		auto &between = expr.Cast<BetweenExpression>();
		return "(" + WriteExpression(between.Input()) + " BETWEEN " + WriteExpression(between.LowerBound()) + " AND " +
		       WriteExpression(between.UpperBound()) + ")";
	}
	case ExpressionClass::SUBQUERY:
		return WriteSubquery(expr.Cast<SubqueryExpression>());
	default:
		throw InternalException("MySQLSQLWriter: unsupported expression class - should have been blocked by "
		                        "SupportsPushdown");
	}
}

//===--------------------------------------------------------------------===//
// Table references
//===--------------------------------------------------------------------===//
string MySQLSQLWriter::WriteTableRef(const TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		string result;
		if (!base.schema_name.empty()) {
			result += WriteIdentifier(base.schema_name.GetIdentifierName()) + ".";
		}
		result += WriteIdentifier(base.table_name.GetIdentifierName());
		if (!base.alias.empty()) {
			result += " AS " + WriteIdentifier(base.alias.GetIdentifierName());
		}
		return result;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		string result = "(" + WriteTableRef(*join.left) + " ";
		if (join.ref_type == JoinRefType::CROSS) {
			result += "CROSS JOIN ";
		} else if (join.ref_type == JoinRefType::REGULAR || join.ref_type == JoinRefType::NATURAL) {
			if (join.ref_type == JoinRefType::NATURAL) {
				result += "NATURAL ";
			}
			switch (join.type) {
			case JoinType::INNER:
				result += "INNER JOIN ";
				break;
			case JoinType::LEFT:
				result += "LEFT JOIN ";
				break;
			case JoinType::RIGHT:
				result += "RIGHT JOIN ";
				break;
			default:
				throw InternalException("MySQLSQLWriter: unsupported join type - should have been blocked by "
				                        "SupportsPushdown");
			}
		} else {
			throw InternalException("MySQLSQLWriter: unsupported join ref type - should have been blocked by "
			                        "SupportsPushdown");
		}
		result += WriteTableRef(*join.right);
		if (join.condition) {
			result += " ON " + WriteExpression(*join.condition);
		} else if (!join.using_columns.empty()) {
			result += " USING (";
			for (idx_t i = 0; i < join.using_columns.size(); i++) {
				if (i > 0) {
					result += ", ";
				}
				result += WriteIdentifier(join.using_columns[i].GetIdentifierName());
			}
			result += ")";
		}
		result += ")";
		return result;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		// MySQL requires an alias for every derived table - generate one if there is none
		string alias = subquery.alias.GetIdentifierName();
		if (alias.empty()) {
			alias = "mysql_unnamed_subquery" + to_string(++unnamed_subquery_index);
		}
		return "(" + WriteQueryNode(*subquery.subquery->node) + ") AS " + WriteIdentifier(alias);
	}
	default:
		throw InternalException("MySQLSQLWriter: unsupported table ref type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

//===--------------------------------------------------------------------===//
// Result modifiers
//===--------------------------------------------------------------------===//
string MySQLSQLWriter::WriteOrderList(const vector<OrderByNode> &orders,
                                      optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list) {
	auto &config = DBConfig::GetConfig(context);
	string result;
	for (auto &order : orders) {
		auto order_type = config.ResolveOrder(context, order.type);
		auto null_order = config.ResolveNullOrder(context, order_type, order.null_order);
		auto direction = order_type == OrderType::DESCENDING ? " DESC" : " ASC";

		// figure out the expression whose NULL ordering we need to make explicit:
		// MySQL sorts NULLs first when ascending, DuckDB defaults to NULLS LAST
		string null_key;
		auto &expr = *order.expression;
		if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &val = expr.Cast<ConstantExpression>().GetValue();
			if (val.type().IsIntegral() && !val.IsNull() && select_list) {
				// positional reference (e.g. ORDER BY 1) - resolve it against the select list
				// SupportsPushdown verifies that this resolution is possible
				Value bigint_value;
				string error;
				if (val.DefaultTryCastAs(LogicalType::BIGINT, bigint_value, &error) && !bigint_value.IsNull()) {
					auto index = BigIntValue::Get(bigint_value);
					if (index >= 1 && idx_t(index) <= select_list->size()) {
						auto &target = *(*select_list)[idx_t(index) - 1];
						// reference the output column by its alias where possible - expressions over
						// output columns satisfy MySQL's ONLY_FULL_GROUP_BY checks where raw expression
						// copies may not
						null_key = target.GetAlias().empty() ? WriteExpression(target)
						                                     : WriteIdentifier(target.GetAlias().GetIdentifierName());
					}
				}
			}
			// other constants do not influence the ordering - no NULL ordering key required
		} else {
			null_key = WriteExpression(expr);
		}
		if (!result.empty()) {
			result += ", ";
		}
		if (!null_key.empty()) {
			// NULLS LAST -> "expr IS NULL" ascending (false sorts before true)
			// NULLS FIRST -> "expr IS NULL" descending
			result += "(" + null_key + " IS NULL)";
			result += null_order == OrderByNullType::NULLS_FIRST ? " DESC" : " ASC";
			result += ", ";
		}
		result += WriteExpression(expr);
		result += direction;
	}
	return result;
}

string MySQLSQLWriter::WriteResultModifiers(const QueryNode &node,
                                            optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list) {
	string result;
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_modifier = modifier->Cast<OrderModifier>();
			result += " ORDER BY " + WriteOrderList(order_modifier.orders, select_list);
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_modifier = modifier->Cast<LimitModifier>();
			if (limit_modifier.limit) {
				result += " LIMIT " + WriteExpression(*limit_modifier.limit);
			}
			if (limit_modifier.offset) {
				result += " OFFSET " + WriteExpression(*limit_modifier.offset);
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER:
			// handled in WriteSelectNode
			break;
		default:
			throw InternalException("MySQLSQLWriter: unsupported result modifier - should have been blocked by "
			                        "SupportsPushdown");
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Query nodes
//===--------------------------------------------------------------------===//
string MySQLSQLWriter::WriteCTEMap(const CommonTableExpressionMap &cte_map) {
	if (cte_map.map.empty()) {
		return string();
	}
	bool has_recursive = false;
	for (auto &kv : cte_map.map) {
		if (kv.second->query_node && kv.second->query_node->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			has_recursive = true;
			break;
		}
	}
	string result = "WITH ";
	if (has_recursive) {
		result += "RECURSIVE ";
	}
	bool first_cte = true;
	for (auto &kv : cte_map.map) {
		if (!first_cte) {
			result += ", ";
		}
		auto &cte = *kv.second;
		result += WriteIdentifier(kv.first.GetIdentifierName());
		if (!cte.aliases.empty()) {
			result += " (";
			for (idx_t k = 0; k < cte.aliases.size(); k++) {
				if (k > 0) {
					result += ", ";
				}
				result += WriteIdentifier(cte.aliases[k].GetIdentifierName());
			}
			result += ")";
		}
		result += " AS (";
		result += WriteQueryNode(*cte.query_node);
		result += ")";
		first_cte = false;
	}
	result += " ";
	return result;
}

string MySQLSQLWriter::WriteSelectNode(const SelectNode &node) {
	string result = WriteCTEMap(node.cte_map);
	result += "SELECT ";
	for (auto &modifier : node.modifiers) {
		if (modifier->type == ResultModifierType::DISTINCT_MODIFIER) {
			result += "DISTINCT ";
			break;
		}
	}
	for (idx_t i = 0; i < node.select_list.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &select_expr = *node.select_list[i];
		result += WriteExpression(select_expr);
		if (!select_expr.GetAlias().empty()) {
			result += " AS " + WriteIdentifier(select_expr.GetAlias().GetIdentifierName());
		} else if (select_expr.GetExpressionClass() != ExpressionClass::STAR &&
		           select_expr.GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			// alias the column with DuckDB's name for the expression so that the result column
			// names match what DuckDB itself would produce (MySQL names result columns after the
			// serialized - and remapped - SQL text otherwise)
			auto duckdb_name = select_expr.ToString();
			if (duckdb_name.size() <= 64) { // MySQL's identifier length limit
				result += " AS " + WriteIdentifier(duckdb_name);
			}
		}
	}
	if (node.from_table && node.from_table->type != TableReferenceType::EMPTY_FROM) {
		result += " FROM " + WriteTableRef(*node.from_table);
	} else if (node.where_clause || node.having || !node.groups.grouping_sets.empty()) {
		// MariaDB requires a FROM clause when WHERE / GROUP BY / HAVING is present
		result += " FROM DUAL";
	}
	if (node.where_clause) {
		result += " WHERE " + WriteExpression(*node.where_clause);
	}
	if (!node.groups.grouping_sets.empty()) {
		if (node.groups.grouping_sets.size() > 1) {
			throw InternalException("MySQLSQLWriter: GROUPING SETS should have been blocked by SupportsPushdown");
		}
		result += " GROUP BY ";
		bool first = true;
		for (auto &group_index : node.groups.grouping_sets[0]) {
			if (!first) {
				result += ", ";
			}
			result += WriteExpression(*node.groups.group_expressions[group_index]);
			first = false;
		}
	}
	if (node.having) {
		result += " HAVING " + WriteExpression(*node.having);
	}
	if (node.qualify || node.sample) {
		throw InternalException("MySQLSQLWriter: QUALIFY / SAMPLE should have been blocked by SupportsPushdown");
	}
	result += WriteResultModifiers(node, &node.select_list);
	return result;
}

string MySQLSQLWriter::WriteSetOperationNode(const SetOperationNode &node) {
	string result = WriteCTEMap(node.cte_map);
	for (idx_t i = 0; i < node.children.size(); i++) {
		if (i > 0) {
			switch (node.setop_type) {
			case SetOperationType::UNION:
				result += node.setop_all ? " UNION ALL " : " UNION ";
				break;
			case SetOperationType::EXCEPT:
				result += node.setop_all ? " EXCEPT ALL " : " EXCEPT ";
				break;
			case SetOperationType::INTERSECT:
				result += node.setop_all ? " INTERSECT ALL " : " INTERSECT ";
				break;
			default:
				throw InternalException("MySQLSQLWriter: unsupported set operation - should have been blocked by "
				                        "SupportsPushdown");
			}
		}
		result += "(" + WriteQueryNode(*node.children[i]) + ")";
	}
	result += WriteResultModifiers(node, nullptr);
	return result;
}

string MySQLSQLWriter::WriteRecursiveCTENode(const RecursiveCTENode &node) {
	string result = WriteCTEMap(node.cte_map);
	result += "(" + WriteQueryNode(*node.left) + ")";
	result += node.union_all ? " UNION ALL " : " UNION ";
	result += "(" + WriteQueryNode(*node.right) + ")";
	result += WriteResultModifiers(node, nullptr);
	return result;
}

string MySQLSQLWriter::WriteQueryNode(const QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		return WriteSelectNode(node.Cast<SelectNode>());
	case QueryNodeType::SET_OPERATION_NODE:
		return WriteSetOperationNode(node.Cast<SetOperationNode>());
	case QueryNodeType::RECURSIVE_CTE_NODE:
		return WriteRecursiveCTENode(node.Cast<RecursiveCTENode>());
	default:
		throw InternalException("MySQLSQLWriter: unsupported query node type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

} // namespace duckdb
