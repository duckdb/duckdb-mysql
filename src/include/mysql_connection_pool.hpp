//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "dbconnector/pool.hpp"

#include "mysql_connection.hpp"
#include "network_calibration.hpp"

namespace duckdb {

enum class MySQLPoolAcquireMode : uint8_t { FORCE, WAIT, TRY };

using MySQLPooledConnection = dbconnector::pool::PooledConnection<MySQLConnection>;

class MySQLConnectionPool : public dbconnector::pool::ConnectionPool<MySQLConnection> {
public:
	MySQLConnectionPool(ClientContext &context, string connection_string, string attach_path);
	~MySQLConnectionPool() override;

	void UpdateTypeConfig(MySQLTypeConfig new_config);

	NetworkCalibration GetNetworkCalibration() const;
	void EnsureCalibrated(MySQLConnection &conn);
	void SetNetworkCompression(bool enabled, double ratio = NetworkCalibration::DEFAULT_COMPRESSION_RATIO);
	static idx_t DefaultPoolSize() noexcept;
	MySQLPooledConnection Acquire(MySQLPoolAcquireMode acquire_mode, const std::string &time_zone = std::string());
	static MySQLPoolAcquireMode GetAcquireMode(ClientContext &context);

protected:
	std::unique_ptr<MySQLConnection> CreateNewConnection() override;
	bool CheckConnectionHealthy(MySQLConnection &conn) override;
	void ResetConnection(MySQLConnection &conn) override;

private:
	void CalibrateNetwork(MySQLConnection &conn);
	static dbconnector::pool::ConnectionPoolConfig CreateConfig(ClientContext &ctx);

	const string connection_string;
	const string attach_path;
	MySQLTypeConfig type_config;
	//! Lock order: pool_lock (base) before calibration_lock. Never acquire pool_lock while holding calibration_lock.
	mutable mutex calibration_lock;
	NetworkCalibration network_calibration;
};

} // namespace duckdb
