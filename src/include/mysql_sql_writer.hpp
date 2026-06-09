//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_sql_writer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {
class CaseExpression;
class FunctionArgument;
class CastExpression;
class ClientContext;
class ColumnRefExpression;
class FunctionExpression;
class OperatorExpression;
class RecursiveCTENode;
class SelectNode;
class SetOperationNode;
class SubqueryExpression;
class WindowExpression;
class CommonTableExpressionMap;

//! Serializes a parsed query tree into MySQL-compatible SQL.
//! This mirrors the structure of the DuckDB ToString() methods, with MySQL-specific
//! differences applied during serialization:
//! * identifiers are quoted with backticks
//! * string literals escape backslashes (MySQL treats backslash as an escape character)
//! * cast types are remapped (e.g. INTEGER -> SIGNED, VARCHAR -> CHAR)
//! * functions are remapped where semantics are identical under a different name
//!   (e.g. count_star() -> count(*), log -> log10, length -> char_length)
//! * ORDER BY entries get an explicit NULL ordering key (MySQL sorts NULLs first when
//!   ascending, DuckDB defaults to NULLS LAST)
//! * unnamed derived tables get a generated alias (required by MySQL)
//! Constructs that cannot be serialized are rejected up-front by MySQLCatalog::SupportsPushdown -
//! encountering them here is an internal error.
struct MySQLVersion;

class MySQLSQLWriter {
public:
	MySQLSQLWriter(ClientContext &context, const MySQLVersion &version);

	//! Convert a query node to a MySQL-compatible SQL string
	static string MySQLToString(ClientContext &context, const MySQLVersion &version, const QueryNode &node);

private:
	string WriteQueryNode(const QueryNode &node);
	string WriteSelectNode(const SelectNode &node);
	string WriteSetOperationNode(const SetOperationNode &node);
	string WriteRecursiveCTENode(const RecursiveCTENode &node);
	string WriteCTEMap(const CommonTableExpressionMap &cte_map);
	string WriteResultModifiers(const QueryNode &node,
	                            optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list);
	string WriteOrderList(const vector<OrderByNode> &orders,
	                      optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list);

	string WriteTableRef(const TableRef &ref);

	string WriteExpression(const ParsedExpression &expr);
	string WriteExpressionList(const vector<unique_ptr<ParsedExpression>> &expressions, const string &separator = ", ");
	string WriteArgumentList(const vector<FunctionArgument> &arguments);
	string WriteFunction(const FunctionExpression &func);
	string WriteFunctionCall(const string &function_name, const vector<FunctionArgument> &children, bool distinct);
	string WriteWindow(const WindowExpression &window);
	string WriteOperator(const OperatorExpression &op);
	string WriteCase(const CaseExpression &case_expr);
	string WriteSubquery(const SubqueryExpression &subquery);
	string WriteColumnRef(const ColumnRefExpression &column_ref);
	string WriteConstant(const Value &value);
	string WriteCastType(const LogicalType &type);
	string WriteIdentifier(const string &identifier);

private:
	ClientContext &context;
	//! The NO PAD binary collation attached to string literals (derived from the server version)
	string binary_collation;
	//! Counter for generating aliases for unnamed derived tables (MySQL requires an alias)
	idx_t unnamed_subquery_index = 0;
};

} // namespace duckdb
