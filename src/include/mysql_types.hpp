//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "mysql.h"

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

	MySQLTypeConfig();
	MySQLTypeConfig(ClientContext &context);
};

class MySQLTypes {
public:
	static LogicalType ToMySQLType(const MySQLTypeConfig &type_config, const LogicalType &input);
	static LogicalType TypeToLogicalType(const MySQLTypeConfig &type_config, const MySQLTypeData &input);
	static LogicalType FieldToLogicalType(const MySQLTypeConfig &type_config, MYSQL_FIELD *field);
	static string TypeToString(const LogicalType &input);
};

} // namespace duckdb
