//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "mysql_types.hpp"
#include "mysql_utils.hpp"

namespace duckdb {
class MySQLConnection;
struct OwnedMySQLConnection;

struct MySQLField {
	string name;
	LogicalType type;
};

class MySQLResult {
public:
	MySQLResult(MYSQL_RES *res_p, vector<MySQLField> fields_p, bool streaming_p, MySQLConnection &con);
	MySQLResult(idx_t affected_rows);
	~MySQLResult();

public:
	string GetString(idx_t col) {
		D_ASSERT(res);
		return string(GetNonNullValue(col), lengths[col]);
	}
	string_t GetStringT(idx_t col) {
		D_ASSERT(res);
		return string_t(GetNonNullValue(col), lengths[col]);
	}
	int32_t GetInt32(idx_t col) {
		return atoi(GetNonNullValue(col));
	}
	int64_t GetInt64(idx_t col) {
		return atoll(GetNonNullValue(col));
	}
	bool GetBool(idx_t col) {
		return strcmp(GetNonNullValue(col), "t");
	}
	bool IsNull(idx_t col) {
		return !GetValueInternal(col);
	}
	bool Next() {
		if (!res) {
			throw InternalException("MySQLResult::Next called without result");
		}
		mysql_row = mysql_fetch_row(res);
		lengths = mysql_fetch_lengths(res);
		return mysql_row;
	}
	idx_t AffectedRows() {
		if (affected_rows == idx_t(-1)) {
			throw InternalException("MySQLResult::AffectedRows called for result "
			                        "that didn't affect any rows");
		}
		return affected_rows;
	}
	idx_t ColumnCount() {
		return field_count;
	}
	const vector<MySQLField> &Fields() {
		return fields;
	}

private:
	MYSQL_RES *res = nullptr;
	idx_t affected_rows = idx_t(-1);
	MYSQL_ROW mysql_row = nullptr;
	unsigned long *lengths = nullptr;
	idx_t field_count = 0;
	vector<MySQLField> fields;
	bool streaming = false;
	string dsn;
	shared_ptr<OwnedMySQLConnection> connection;

	bool TryCancelQuery();

	char *GetNonNullValue(idx_t col) {
		auto val = GetValueInternal(col);
		if (!val) {
			throw InternalException("MySQLResult::GetNonNullValue called for a NULL value");
		}
		return val;
	}

	char *GetValueInternal(idx_t col) {
		if (!mysql_row) {
			throw InternalException("MySQLResult::GetValueInternal called without row");
		}
		if (col >= field_count) {
			throw InternalException("MySQLResult::GetValueInternal row out of range of field count");
		}
		return mysql_row[col];
	}
};

} // namespace duckdb
