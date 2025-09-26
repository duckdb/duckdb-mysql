//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_parameter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "mysql.h"

namespace duckdb {

struct MySQLParameter {
	Value value;
	enum_field_types buffer_type = MYSQL_TYPE_INVALID;
	bool is_unsigned = false;

	vector<char> bind_buffer;
	unsigned long bind_length = 0;

	MySQLParameter(const string &query, Value value_p);

	MYSQL_BIND CreateBind();
};

} // namespace duckdb
