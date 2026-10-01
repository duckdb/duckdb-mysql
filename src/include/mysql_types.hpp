//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "mysql.h"

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"

namespace duckdb {

struct MySQLTypeData {
	string type_name;
	string column_type;
	int64_t precision;
	int64_t scale;
};

enum class MySQLTypeAnnotation { STANDARD, CAST_TO_VARCHAR, NUMERIC_AS_DOUBLE, CTID, JSONB, FIXED_LENGTH_CHAR };

struct MySQLType {
	idx_t oid = 0;
	MySQLTypeAnnotation info = MySQLTypeAnnotation::STANDARD;
	vector<MySQLType> children;
};

struct MySQLTypeConfig {
	bool bit1_as_boolean = false;
	bool tinyint1_as_boolean = false;
	bool time_as_time = false;
	bool incomplete_dates_as_nulls = false;

	MySQLTypeConfig();
	MySQLTypeConfig(ClientContext &context);
};

class MySQLTypes {
public:
	static LogicalType ToMySQLType(const MySQLTypeConfig &type_config, const LogicalType &input);
	static LogicalType TypeToLogicalType(const MySQLTypeConfig &type_config, const MySQLTypeData &input);
	static LogicalType FieldToLogicalType(const MySQLTypeConfig &type_config, MYSQL_FIELD *field);
	static string TypeToString(const LogicalType &input);
	static LogicalType ToLogicalType(optional_ptr<ParsedExpression> type_expr_ptr);
	static LogicalType ToLogicalType(const TypeExpression &type_expr);
};

} // namespace duckdb
