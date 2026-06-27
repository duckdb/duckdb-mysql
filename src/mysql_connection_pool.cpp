#include "mysql_connection_pool.hpp"

#include <chrono>
#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MySQLConnectionPool
//===--------------------------------------------------------------------===//
MySQLConnectionPool::MySQLConnectionPool(ClientContext &context, string connection_string_p, string attach_path_p)
    : dbconnector::pool::ConnectionPool<MySQLConnection>(CreateConfig(context)),
      connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      type_config(MySQLTypeConfig(context)) {
}

MySQLConnectionPool::~MySQLConnectionPool() = default;

std::unique_ptr<MySQLConnection> MySQLConnectionPool::CreateNewConnection() {
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
	return conn.IsConnectionHealthy();
}

void MySQLConnectionPool::ResetConnection(MySQLConnection &conn) {
	conn.Reset();
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

	for (int i = 0; i < NUM_SAMPLES; i++) {
		auto start = std::chrono::steady_clock::now();
		try {
			conn.Query("SELECT 1", MySQLResultStreaming::FORCE_MATERIALIZATION);
		} catch (...) {
			lock_guard<mutex> lock(calibration_lock);
			network_calibration.calibration_failed = true;
			Printer::Print("Warning: MySQL network calibration failed (latency probe error), using default values\n");
			return;
		}
		auto end = std::chrono::steady_clock::now();
		double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
		total_latency_ms += elapsed_ms;
	}

	double avg_latency_ms = total_latency_ms / NUM_SAMPLES;

	static constexpr idx_t BANDWIDTH_PAYLOAD_BYTES = 65536;
	double bandwidth_mbps = NetworkCalibration::DEFAULT_BANDWIDTH_MBPS;

	auto bw_start = std::chrono::steady_clock::now();
	conn.Query("SELECT REPEAT('x', 65536)", MySQLResultStreaming::FORCE_MATERIALIZATION);
	auto bw_end = std::chrono::steady_clock::now();
	double bw_elapsed_s = std::chrono::duration<double>(bw_end - bw_start).count();
	double transfer_s = bw_elapsed_s - (avg_latency_ms / 1000.0);
	if (transfer_s > 0.0001) {
		bandwidth_mbps =
		    (static_cast<double>(BANDWIDTH_PAYLOAD_BYTES) / NetworkCalibration::MBPS_TO_BYTES_PER_SEC) / transfer_s;
		bandwidth_mbps = std::max(bandwidth_mbps, 1.0);
	}

	{
		lock_guard<mutex> lock(calibration_lock);
		network_calibration.latency_ms = avg_latency_ms;
		network_calibration.bandwidth_mbps = bandwidth_mbps;
		network_calibration.is_calibrated = true;
	}
}

static idx_t ReadUBigIntOption(ClientContext &ctx, const std::string &name, idx_t default_val) {
	Value val;
	if (ctx.TryGetCurrentSetting(name, val)) {
		return UBigIntValue::Get(val);
	}
	return default_val;
}

static bool ReadBooleanOption(ClientContext &ctx, const std::string &name, bool default_val) {
	Value val;
	if (ctx.TryGetCurrentSetting(name, val)) {
		return BooleanValue::Get(val);
	}
	return default_val;
}

dbconnector::pool::ConnectionPoolConfig MySQLConnectionPool::CreateConfig(ClientContext &ctx) {
	dbconnector::pool::ConnectionPoolConfig config;
	config.max_connections = ReadUBigIntOption(ctx, "mysql_pool_size", config.max_connections);
	config.wait_timeout_millis = ReadUBigIntOption(ctx, "mysql_pool_wait_timeout_millis", config.wait_timeout_millis);
	config.tl_cache_enabled = ReadBooleanOption(ctx, "mysql_pool_enable_thread_local_cache", config.tl_cache_enabled);
	config.max_lifetime_millis =
	    ReadUBigIntOption(ctx, "mysql_pool_connection_max_lifetime_millis", config.max_lifetime_millis);
	config.idle_timeout_millis =
	    ReadUBigIntOption(ctx, "mysql_pool_connection_idle_timeout_millis", config.idle_timeout_millis);
	config.start_reaper_thread = ReadBooleanOption(ctx, "mysql_pool_enable_reaper_thread", config.start_reaper_thread);

	return config;
}

MySQLPooledConnection MySQLConnectionPool::Acquire(dbconnector::pool::AcquireMode acquire_mode,
                                                   const std::string &time_zone) {
	MySQLPooledConnection pc;
	switch (acquire_mode) {
	case dbconnector::pool::AcquireMode::FORCE:
		pc = ForceAcquire();
		break;
	case dbconnector::pool::AcquireMode::WAIT:
		pc = WaitAcquire();
		break;
	case dbconnector::pool::AcquireMode::TRY:
		pc = TryAcquire();
		if (!pc) {
			throw IOException("Connection pool exhausted: no connections available (try mode)");
		}
		break;
	}

	if (!time_zone.empty()) {
		try {
			pc->Execute("SET TIME_ZONE = ?", {Value(time_zone)});
		} catch (...) {
			pc.Invalidate();
			throw;
		}
	}

	return pc;
}

dbconnector::pool::AcquireMode MySQLConnectionPool::GetAcquireMode(ClientContext &context) {
	Value mode_val;
	if (context.TryGetCurrentSetting("mysql_pool_acquire_mode", mode_val)) {
		auto mode_str = StringUtil::Lower(mode_val.ToString());
		return dbconnector::pool::AcquireModeHelpers::FromString(mode_str);
	}
	return dbconnector::pool::AcquireMode::FORCE;
}

} // namespace duckdb
