//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "dbconnector/pool.hpp"
#include "duckdb/main/client_context.hpp"

#include "mysql_connection.hpp"
#include "network_calibration.hpp"

namespace duckdb {

using MySQLPooledConnection = dbconnector::pool::PooledConnection<MySQLConnection>;

class MySQLConnectionPool : public dbconnector::pool::ConnectionPool<MySQLConnection> {
public:
	MySQLConnectionPool(ClientContext &context, string connection_string, string attach_path);
	~MySQLConnectionPool() override;

	MySQLTypeConfig GetTypeConfig() const;
	void SetTypeConfig(MySQLTypeConfig config);

	static void ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter);

protected:
	std::unique_ptr<MySQLConnection> CreateNewConnection() override;
	bool CheckConnectionHealthy(MySQLConnection &conn) override;
	void ResetConnection(MySQLConnection &conn) override;

private:
	static dbconnector::pool::ConnectionPoolConfig CreateConfig(ClientContext &ctx);

	const string connection_string;
	const string attach_path;

	mutable mutex config_lock;
	MySQLTypeConfig type_config;
};

class MySQLConfigurePoolFunction : public TableFunction {
public:
	MySQLConfigurePoolFunction();
};

} // namespace duckdb
