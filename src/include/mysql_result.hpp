//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"

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

enum struct MySQLResultStatementLifetime { OWNED, BORROWED };

class MySQLResult {
public:
	MySQLResult(optional_ptr<ClientContext> context_p, const std::string &query_p, MySQLStatementPtr stmt_p,
	            MySQLTypeConfig type_config_p, const string &connection_string_p, unsigned long connection_id_p,
	            MySQLResultStreaming streaming_p, idx_t affected_rows_p,
	            vector<MySQLField> fields_p = vector<MySQLField>(),
	            MySQLResultStatementLifetime stmt_lifetime_p = MySQLResultStatementLifetime::OWNED);

	~MySQLResult();

	string GetString(idx_t col);
	int32_t GetInt32(idx_t col);
	int64_t GetInt64(idx_t col);
	bool IsNull(idx_t col);

	//! Reads all remaining rows from the server into a buffer-managed collection, after that the connection
	//! can be used for other queries while this result is being scanned
	void Materialize();

	DataChunk &NextChunk();
	bool Next();
	bool Exhausted();
	idx_t AffectedRows();
	int64_t AffectedRowsSigned();
	const vector<MySQLField> &Fields();

private:
	optional_ptr<ClientContext> context;
	string query;
	MySQLStatementPtr stmt;
	MySQLTypeConfig type_config;
	string connection_string;
	unsigned long connection_id;
	MySQLResultStreaming streaming;
	idx_t affected_rows = static_cast<idx_t>(-1);

	vector<MySQLField> fields;

	MySQLResultStatementLifetime stmt_lifetime = MySQLResultStatementLifetime::OWNED;

	DataChunk data_chunk;
	idx_t row_idx = static_cast<idx_t>(-1);
	bool exhausted = false;

	unique_ptr<ColumnDataCollection> materialized;
	ColumnDataScanState materialized_scan_state;
	idx_t materialized_rows_scanned = 0;

	void FetchChunk();
	bool FetchNext();
	void HandleTruncatedData();
	void WriteToChunk(idx_t row);
	void CheckColumnIdx(idx_t col);
	void CheckNotNull(idx_t col);
	void CheckType(idx_t col, LogicalTypeId type_id);
	bool TryCancelQuery();
};

} // namespace duckdb
