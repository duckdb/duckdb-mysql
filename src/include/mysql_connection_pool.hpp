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

class MySQLConnectionPool : public dbconnector::pool::ConnectionPool<MySQLConnection> {
public:
	static idx_t DefaultPoolSize() noexcept {
		unsigned int hw = std::thread::hardware_concurrency();
		idx_t detected = (hw == 0) ? 4u : static_cast<idx_t>(hw);
		return detected < 8u ? detected : 8u;
	}

	MySQLConnectionPool(string connection_string, string attach_path, MySQLTypeConfig type_config,
	                    idx_t max_connections = DefaultPoolSize(), idx_t timeout_ms = DEFAULT_POOL_TIMEOUT_MS);
	~MySQLConnectionPool() override;

	void UpdateTypeConfig(MySQLTypeConfig new_config);

	NetworkCalibration GetNetworkCalibration() const;
	void EnsureCalibrated(MySQLConnection &conn);
	void SetNetworkCompression(bool enabled, double ratio = NetworkCalibration::DEFAULT_COMPRESSION_RATIO);

protected:
	std::unique_ptr<MySQLConnection> CreateNewConnection() override;
	bool CheckConnectionHealthy(MySQLConnection &conn) override;
	void ResetConnection(MySQLConnection &conn) override;

private:
	void CalibrateNetwork(MySQLConnection &conn);

	const string connection_string;
	const string attach_path;
	MySQLTypeConfig type_config;
	//! Lock order: pool_lock (base) before calibration_lock. Never acquire pool_lock while holding calibration_lock.
	mutable mutex calibration_lock;
	NetworkCalibration network_calibration;
};

} // namespace duckdb
