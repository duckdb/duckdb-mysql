//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_field.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "mysql_types.hpp"

namespace duckdb {

struct MySQLField {
	string name;
	enum_field_types mysql_type;
	LogicalType duckdb_type;

	vector<char> bind_buffer;
	vector<char> varlen_buffer;
	unsigned long bind_length = 0;
	bool bind_is_null = 0;
	bool bind_error = 0;

	MySQLField(MYSQL_FIELD *mf, LogicalType duckdb_type_p, const MySQLTypeConfig &type_config);

	void ResetBind();

	MYSQL_BIND CreateBindStruct();

	static vector<MySQLField> ReadFields(const std::string &query, MYSQL_STMT *stmt,
	                                     const MySQLTypeConfig &type_config);
};

} // namespace duckdb
