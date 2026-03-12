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

namespace duckdb {

struct NetworkCalibration {
	static constexpr double DEFAULT_LATENCY_MS = 1.0;
	static constexpr double DEFAULT_BANDWIDTH_MBPS = 100.0;
	static constexpr double MBPS_TO_BYTES_PER_SEC = 125000.0;
	static constexpr double DEFAULT_COMPRESSION_RATIO = 0.7;

	double latency_ms = DEFAULT_LATENCY_MS;
	double bandwidth_mbps = DEFAULT_BANDWIDTH_MBPS;
	bool is_calibrated = false;
	bool calibration_failed = false;
	bool has_network_compression = false;
	double network_compression_ratio = DEFAULT_COMPRESSION_RATIO;

	double ByteTransferTime(idx_t bytes) const {
		double effective_bw = std::max(bandwidth_mbps, 1.0);
		double transfer_seconds = static_cast<double>(bytes) / (effective_bw * MBPS_TO_BYTES_PER_SEC);
		return (latency_ms / 1000.0) + transfer_seconds;
	}
};

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
