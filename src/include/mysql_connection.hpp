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

	MYSQL *connection;
};

class MySQLConnection {
public:
	explicit MySQLConnection(shared_ptr<OwnedMySQLConnection> connection, const std::string &dsn_p,
	                         MySQLTypeConfig type_config_p);
	~MySQLConnection();
	// disable copy constructors
	MySQLConnection(const MySQLConnection &other) = delete;
	MySQLConnection &operator=(const MySQLConnection &) = delete;
	//! enable move constructors
	MySQLConnection(MySQLConnection &&other) noexcept;
	MySQLConnection &operator=(MySQLConnection &&) noexcept;

public:
	static MySQLConnection Open(MySQLTypeConfig type_config, const string &connection_string);
	void Execute(const string &query, MySQLConnectorInterface con_interface = MySQLConnectorInterface::BASIC);
	unique_ptr<MySQLResult> Query(const string &query, MySQLResultStreaming streaming);

	vector<IndexInfo> GetIndexInfo(const string &table_name);

	bool IsOpen();
	void Close();

	shared_ptr<OwnedMySQLConnection> GetConnection() {
		return connection;
	}
	string GetDSN() {
		return dsn;
	}

	MYSQL *GetConn() {
		if (!connection || !connection->connection) {
			throw InternalException("MySQLConnection::GetConn - no connection available");
		}
		return connection->connection;
	}

	static void DebugSetPrintQueries(bool print);
	static bool DebugPrintQueries();

private:
	unique_ptr<MySQLResult> QueryInternal(const string &query, MySQLResultStreaming streaming,
	                                      MySQLConnectorInterface con_interface);
	idx_t MySQLExecute(MYSQL_STMT *stmt, const string &query, bool streaming);

	mutex query_lock;
	shared_ptr<OwnedMySQLConnection> connection;
	string dsn;
	MySQLTypeConfig type_config;
};

} // namespace duckdb
