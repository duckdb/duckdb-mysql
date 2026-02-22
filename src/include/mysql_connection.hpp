//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/mutex.hpp"

#include "mysql_result.hpp"
#include "mysql_statement.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"

namespace duckdb {
class MySQLBinaryWriter;
class MySQLTextWriter;
struct MySQLBinaryReader;
class MySQLSchemaEntry;
class MySQLTableEntry;
class MySQLStatement;
class MySQLResult;
struct IndexInfo;

struct OwnedMySQLConnection {

	explicit OwnedMySQLConnection(MYSQL *conn = nullptr) : connection(conn) {
	}

	~OwnedMySQLConnection() {
		if (!connection) {
			return;
		}
		mysql_close(connection);
		connection = nullptr;
	}

	unsigned long GetID() {
		if (!connection) {
			return 0;
		}
		return mysql_thread_id(connection);
	}

	MYSQL *connection;
};

class MySQLConnection {
public:
	explicit MySQLConnection(shared_ptr<OwnedMySQLConnection> connection, MySQLTypeConfig type_config_p,
	                         const string &connection_string);
	~MySQLConnection();
	// disable copy constructors
	MySQLConnection(const MySQLConnection &other) = delete;
	MySQLConnection &operator=(const MySQLConnection &) = delete;
	//! enable move constructors
	MySQLConnection(MySQLConnection &&other) noexcept;
	MySQLConnection &operator=(MySQLConnection &&) noexcept;

public:
	static MySQLConnection Open(MySQLTypeConfig type_config, const string &connection_string,
	                            const string &attach_path);
	void Execute(const string &query);
	void Execute(const string &query, const vector<Value> &params);
	unique_ptr<MySQLResult> Query(const string &query, MySQLResultStreaming streaming);
	unique_ptr<MySQLResult> Query(const string &query, const vector<Value> &params, MySQLResultStreaming streaming);
	unique_ptr<MySQLResult> Query(MySQLStatement &stmt, const vector<Value> &params, MySQLResultStreaming streaming);
	unique_ptr<MySQLStatement> Prepare(const string &query);

	vector<IndexInfo> GetIndexInfo(const string &table_name);

	bool IsOpen();
	void Close();

	shared_ptr<OwnedMySQLConnection> GetConnection() {
		return connection;
	}

	MYSQL *GetConn() {
		if (!connection || !connection->connection) {
			throw InternalException("MySQLConnection::GetConn - no connection available");
		}
		return connection->connection;
	}

	static void DebugSetPrintQueries(bool print);
	static bool DebugPrintQueries();

	void SetTypeConfig(MySQLTypeConfig new_config) {
		type_config = std::move(new_config);
	}

private:
	unique_ptr<MySQLResult> QueryInternal(const string &query, const vector<Value> &params,
	                                      MySQLResultStreaming streaming, MySQLConnectorInterface con_interface);
	idx_t MySQLExecute(MYSQL_STMT *stmt, const string &query, const vector<Value> &params, bool streaming,
	                   bool prepared = false);

	mutex query_lock;
	shared_ptr<OwnedMySQLConnection> connection;
	MySQLTypeConfig type_config;
	string connection_string;
};

} // namespace duckdb
