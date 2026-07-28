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
	enum_field_types buffer_type = MYSQL_TYPE_NULL;
	bool is_unsigned = false;

	vector<char> bind_buffer;
	unsigned long bind_length = 0;

	MySQLParameter(const string &query, Value value_p);

	MYSQL_BIND CreateBind();
};

class MySQLParameterHandles {
	inline static mutex lock;
	inline static std::set<int64_t> registry;

public:
	static int64_t Add(unique_ptr<vector<Value>> params);

	static unique_ptr<vector<Value>> Remove(int64_t params_id);
};

} // namespace duckdb
