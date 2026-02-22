#include "mysql_connection_pool.hpp"

#include <chrono>
#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MySQLConnectionPool
//===--------------------------------------------------------------------===//
MySQLConnectionPool::MySQLConnectionPool(string connection_string_p, string attach_path_p,
                                         MySQLTypeConfig type_config_p, idx_t max_connections_p, idx_t timeout_ms_p)
    : GenericConnectionPool<MySQLConnection>(max_connections_p, timeout_ms_p, true),
      connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      type_config(std::move(type_config_p)) {
}

MySQLConnectionPool::~MySQLConnectionPool() = default;

unique_ptr<MySQLConnection> MySQLConnectionPool::CreateNewConnection() {
	MySQLTypeConfig config_snapshot;
	bool should_calibrate = false;
	{
		lock_guard<mutex> lock(calibration_lock);
		config_snapshot = type_config;
		should_calibrate = !network_calibration.is_calibrated && !network_calibration.calibration_failed;
	}

	auto conn = MySQLConnection::Open(config_snapshot, connection_string, attach_path);
	auto result = make_uniq<MySQLConnection>(std::move(conn));

	if (should_calibrate) {
		CalibrateNetwork(*result);
	}

	return result;
}

bool MySQLConnectionPool::CheckConnectionHealthy(MySQLConnection &conn) {
	if (!conn.IsOpen()) {
		return false;
	}

	MYSQL *mysql_conn = conn.GetConn();
	if (mysql_ping(mysql_conn) != 0) {
		unsigned int err = mysql_errno(mysql_conn);
		(void)err;
		return false;
	}
	return true;
}

void MySQLConnectionPool::ResetConnection(MySQLConnection &conn) {
	MYSQL *mysql_conn = conn.GetConn();

#if defined(MARIADB_VERSION_ID) || (defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 50703)
	if (mysql_reset_connection(mysql_conn) != 0) {
		throw IOException("Failed to reset MySQL connection: %s", mysql_error(mysql_conn));
	}
#else
	if (mysql_change_user(mysql_conn, nullptr, nullptr, nullptr) != 0) {
		throw IOException("Failed to reset MySQL connection: %s", mysql_error(mysql_conn));
	}
#endif

	if (mysql_query(mysql_conn, "SET autocommit=1") != 0) {
		throw IOException("Failed to set autocommit after connection reset: %s", mysql_error(mysql_conn));
	}

	if (mysql_set_character_set(mysql_conn, "utf8mb4") != 0) {
		throw IOException("Failed to set character set after connection reset: %s", mysql_error(mysql_conn));
	}

	const char *charset = mysql_character_set_name(mysql_conn);
	if (strcmp(charset, "utf8mb4") != 0) {
		throw IOException("Character set verification failed: expected utf8mb4, got %s", charset);
	}
}

//===--------------------------------------------------------------------===//
// MySQL-Specific Methods
//===--------------------------------------------------------------------===//
void MySQLConnectionPool::UpdateTypeConfig(MySQLTypeConfig new_config) {
	ForEachIdleConnection([&new_config](MySQLConnection &conn) { conn.SetTypeConfig(new_config); });
	lock_guard<mutex> lock(calibration_lock);
	type_config = std::move(new_config);
}

NetworkCalibration MySQLConnectionPool::GetNetworkCalibration() const {
	lock_guard<mutex> lock(calibration_lock);
	return network_calibration;
}

void MySQLConnectionPool::EnsureCalibrated(MySQLConnection &conn) {
	bool needs_calibration = false;
	{
		lock_guard<mutex> lock(calibration_lock);
		needs_calibration = !network_calibration.is_calibrated && !network_calibration.calibration_failed;
	}
	if (needs_calibration) {
		CalibrateNetwork(conn);
	}
}

void MySQLConnectionPool::SetNetworkCompression(bool enabled, double ratio) {
	lock_guard<mutex> lock(calibration_lock);
	network_calibration.has_network_compression = enabled;
	network_calibration.network_compression_ratio = ratio;
}

void MySQLConnectionPool::CalibrateNetwork(MySQLConnection &conn) {
	static constexpr int NUM_SAMPLES = 3;
	double total_latency_ms = 0.0;

	MYSQL *mysql_conn = conn.GetConn();
	for (int i = 0; i < NUM_SAMPLES; i++) {
		auto start = std::chrono::steady_clock::now();
		if (mysql_query(mysql_conn, "SELECT 1") != 0) {
			lock_guard<mutex> lock(calibration_lock);
			network_calibration.calibration_failed = true;
			Printer::Print("Warning: MySQL network calibration failed (latency probe error), using default values\n");
			return;
		}
		MYSQL_RES *result = mysql_store_result(mysql_conn);
		if (result) {
			mysql_free_result(result);
		}
		auto end = std::chrono::steady_clock::now();
		double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
		total_latency_ms += elapsed_ms;
	}

	double avg_latency_ms = total_latency_ms / NUM_SAMPLES;

	static constexpr idx_t BANDWIDTH_PAYLOAD_BYTES = 65536;
	double bandwidth_mbps = NetworkCalibration::DEFAULT_BANDWIDTH_MBPS;

	auto bw_start = std::chrono::steady_clock::now();
	if (mysql_query(mysql_conn, "SELECT REPEAT('x', 65536)") == 0) {
		MYSQL_RES *bw_result = mysql_store_result(mysql_conn);
		if (bw_result) {
			mysql_fetch_row(bw_result);
			mysql_free_result(bw_result);
		}
		auto bw_end = std::chrono::steady_clock::now();
		double bw_elapsed_s = std::chrono::duration<double>(bw_end - bw_start).count();
		double transfer_s = bw_elapsed_s - (avg_latency_ms / 1000.0);
		if (transfer_s > 0.0001) {
			bandwidth_mbps =
			    (static_cast<double>(BANDWIDTH_PAYLOAD_BYTES) / NetworkCalibration::MBPS_TO_BYTES_PER_SEC) / transfer_s;
			bandwidth_mbps = std::max(bandwidth_mbps, 1.0);
		}
	}

	{
		lock_guard<mutex> lock(calibration_lock);
		network_calibration.latency_ms = avg_latency_ms;
		network_calibration.bandwidth_mbps = bandwidth_mbps;
		network_calibration.is_calibrated = true;
	}
}

} // namespace duckdb
