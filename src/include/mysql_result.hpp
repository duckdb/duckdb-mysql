//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "mysql_field.hpp"
#include "mysql_statement.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"

namespace duckdb {
class MySQLConnection;
struct OwnedMySQLConnection;

using MySQLResultPtr = duckdb::unique_ptr<MYSQL_RES, void (*)(MYSQL_RES *)>;

inline void MySQLResultDelete(MYSQL_RES *res) {
	mysql_free_result(res);
}

class MySQLResult {
public:
	MySQLResult(const std::string &query_p, MySQLStatementPtr stmt_p, MySQLTypeConfig type_config_p,
	            idx_t affected_rows_p, vector<MySQLField> fields_p = vector<MySQLField>());

	string GetString(idx_t col);
	int32_t GetInt32(idx_t col);
	int64_t GetInt64(idx_t col);
	bool IsNull(idx_t col);

	DataChunk &NextChunk();
	bool Next();
	idx_t AffectedRows();
	const vector<MySQLField> &Fields();

private:
	string query;
	MySQLStatementPtr stmt;
	MySQLTypeConfig type_config;
	idx_t affected_rows = static_cast<idx_t>(-1);

	vector<MySQLField> fields;

	DataChunk data_chunk;
	idx_t row_idx = static_cast<idx_t>(-1);

	bool FetchNext();
	void HandleTruncatedData();
	void WriteToChunk(idx_t row);
	void CheckColumnIdx(idx_t col);
	void CheckNotNull(idx_t col);
	void CheckType(idx_t col, LogicalTypeId type_id);
};

} // namespace duckdb
