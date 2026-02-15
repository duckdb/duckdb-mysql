#include "mysql_connection_pool.hpp"

#include <chrono>
#include <cstring>

namespace duckdb {

static thread_local ThreadLocalConnectionCache tl_cache;

ThreadLocalConnectionCache::~ThreadLocalConnectionCache() {
	auto pool = owner.lock();
	if (pool && connection) {
		pool->ReturnFromThreadLocalCache(std::move(connection));
	}
	connection.reset();
	owner.reset();
	available = false;
}

void ThreadLocalConnectionCache::Clear() {
	auto pool = owner.lock();
	if (connection && pool) {
		pool->ReturnFromThreadLocalCache(std::move(connection));
	}
	connection = nullptr;
	owner.reset();
	available = false;
}

//===--------------------------------------------------------------------===//
// PooledConnection
//===--------------------------------------------------------------------===//
PooledConnection::PooledConnection() : pool(nullptr), connection(nullptr), valid(false) {
}

PooledConnection::PooledConnection(std::shared_ptr<MySQLConnectionPool> pool_p,
                                   unique_ptr<MySQLConnection> connection_p)
    : pool(std::move(pool_p)), connection(std::move(connection_p)), valid(true) {
}

PooledConnection::~PooledConnection() noexcept {
	ReturnToPool();
}

PooledConnection::PooledConnection(PooledConnection &&other) noexcept
    : pool(std::move(other.pool)), connection(std::move(other.connection)), valid(other.valid) {
	other.valid = false;
}

PooledConnection &PooledConnection::operator=(PooledConnection &&other) noexcept {
	if (this != &other) {
		ReturnToPool();
		pool = std::move(other.pool);
		connection = std::move(other.connection);
		valid = other.valid;
		other.valid = false;
	}
	return *this;
}

MySQLConnection &PooledConnection::GetConnection() {
	if (!connection) {
		throw InternalException("PooledConnection::GetConnection - no connection available");
	}
	return *connection;
}

MySQLConnection *PooledConnection::operator->() {
	return connection.get();
}

PooledConnection::operator bool() const {
	return connection != nullptr && valid;
}

void PooledConnection::Invalidate() {
	valid = false;
}

void PooledConnection::ReturnToPool() noexcept {
	if (!pool || !connection) {
		return;
	}
	try {
		if (valid) {
			pool->Return(std::move(connection));
		} else {
			pool->Discard();
		}
	} catch (...) {
		try {
			pool->Discard();
		} catch (...) {
		}
	}
	pool = nullptr;
}

//===--------------------------------------------------------------------===//
// MySQLConnectionPool
//===--------------------------------------------------------------------===//
MySQLConnectionPool::MySQLConnectionPool(string connection_string_p, string attach_path_p,
                                         MySQLTypeConfig type_config_p, idx_t max_connections_p, idx_t timeout_ms_p)
    : connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      type_config(std::move(type_config_p)), max_connections(max_connections_p), timeout_ms(timeout_ms_p),
      total_connections(0), shutdown(false) {
}

MySQLConnectionPool::~MySQLConnectionPool() {
	Shutdown();
}

void MySQLConnectionPool::Shutdown() {
	{
		lock_guard<mutex> lock(pool_lock);
		if (shutdown) {
			return;
		}
		shutdown = true;
		available.clear();
	}
	pool_cv.notify_all();

	auto pool = tl_cache.owner.lock();
	if (pool.get() == this) {
		tl_cache.connection = nullptr;
		tl_cache.owner.reset();
		tl_cache.available = false;
	}
}

bool MySQLConnectionPool::IsShutdown() const {
	lock_guard<mutex> lock(pool_lock);
	return shutdown;
}

unique_ptr<MySQLConnection> MySQLConnectionPool::TryAcquireFromThreadLocal() {
	if (!thread_local_cache_enabled.load(std::memory_order_relaxed)) {
		return nullptr;
	}

	auto cached_owner = tl_cache.owner.lock();
	if (cached_owner.get() != this || !tl_cache.available || !tl_cache.connection) {
		return nullptr;
	}

	if (!IsConnectionHealthy(*tl_cache.connection)) {
		tl_cache.Clear();
		return nullptr;
	}

	tl_cache.available = false;
	thread_local_cache_hits.fetch_add(1, std::memory_order_relaxed);
	return std::move(tl_cache.connection);
}

bool MySQLConnectionPool::TryReturnToThreadLocal(unique_ptr<MySQLConnection> &conn) {
	if (!thread_local_cache_enabled.load(std::memory_order_relaxed)) {
		return false;
	}

	auto cached_owner = tl_cache.owner.lock();
	if (cached_owner && cached_owner.get() != this) {
		return false;
	}

	if (tl_cache.connection != nullptr) {
		return false;
	}

	{
		lock_guard<mutex> lock(pool_lock);
		if (total_connections >= max_connections && available.empty()) {
			return false;
		}
	}

	tl_cache.connection = std::move(conn);
	tl_cache.owner = shared_from_this();
	tl_cache.available = true;
	return true;
}

void MySQLConnectionPool::ReturnFromThreadLocalCache(unique_ptr<MySQLConnection> conn) {
	if (!conn) {
		return;
	}

	{
		lock_guard<mutex> lock(pool_lock);
		if (shutdown) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		available.push_back(std::move(conn));
	}
	pool_cv.notify_one();
}

PooledConnection MySQLConnectionPool::Acquire() {
	auto tl_conn = TryAcquireFromThreadLocal();
	if (tl_conn) {
		return PooledConnection(shared_from_this(), std::move(tl_conn));
	}

	thread_local_cache_misses.fetch_add(1, std::memory_order_relaxed);

	unique_lock<mutex> lock(pool_lock);

	auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

	while (true) {
		if (shutdown) {
			throw IOException("Connection pool has been shut down");
		}

		while (!available.empty()) {
			auto conn = std::move(available.front());
			available.pop_front();

			lock.unlock();
			bool healthy = IsConnectionHealthy(*conn);
			lock.lock();

			if (healthy) {
				return PooledConnection(shared_from_this(), std::move(conn));
			}
			if (total_connections > 0) {
				total_connections--;
			}
		}

		if (total_connections < max_connections) {
			total_connections++;
			lock.unlock();

			try {
				auto conn = CreateConnection();
				return PooledConnection(shared_from_this(), std::move(conn));
			} catch (...) {
				lock.lock();
				if (total_connections > 0) {
					total_connections--;
				}
				pool_cv.notify_one();
				throw;
			}
		}

		if (pool_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
			throw IOException("Connection pool timeout: all %llu connections in use, waited %llu ms", max_connections,
			                  timeout_ms);
		}
	}
}

void MySQLConnectionPool::Return(unique_ptr<MySQLConnection> conn) {
	if (!conn) {
		Discard();
		return;
	}

	if (!IsConnectionHealthy(*conn)) {
		Discard();
		return;
	}

	try {
		ResetConnectionState(*conn);
	} catch (...) {
		Discard();
		return;
	}

	if (!IsConnectionHealthy(*conn)) {
		Discard();
		return;
	}

	if (TryReturnToThreadLocal(conn)) {
		return;
	}

	{
		lock_guard<mutex> lock(pool_lock);
		if (shutdown) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		available.push_back(std::move(conn));
	}
	pool_cv.notify_one();
}

void MySQLConnectionPool::Discard() {
	{
		lock_guard<mutex> lock(pool_lock);
		if (total_connections > 0) {
			total_connections--;
		}
	}
	pool_cv.notify_one();
}

idx_t MySQLConnectionPool::GetMaxConnections() const {
	return max_connections;
}

idx_t MySQLConnectionPool::GetAvailableConnections() const {
	lock_guard<mutex> lock(pool_lock);
	return available.size();
}

idx_t MySQLConnectionPool::GetTotalConnections() const {
	lock_guard<mutex> lock(pool_lock);
	return total_connections;
}

void MySQLConnectionPool::UpdateTypeConfig(MySQLTypeConfig new_config) {
	lock_guard<mutex> lock(pool_lock);
	type_config = std::move(new_config);
	for (auto &conn : available) {
		conn->SetTypeConfig(type_config);
	}
}

NetworkCalibration MySQLConnectionPool::GetNetworkCalibration() const {
	lock_guard<mutex> lock(pool_lock);
	return network_calibration;
}

void MySQLConnectionPool::EnsureCalibrated(MySQLConnection &conn) {
	bool needs_calibration = false;
	{
		lock_guard<mutex> lock(pool_lock);
		needs_calibration = !network_calibration.is_calibrated;
	}
	if (needs_calibration) {
		CalibrateNetwork(conn);
	}
}

void MySQLConnectionPool::SetNetworkCompression(bool enabled, double ratio) {
	lock_guard<mutex> lock(pool_lock);
	network_calibration.has_network_compression = enabled;
	network_calibration.network_compression_ratio = ratio;
}

idx_t MySQLConnectionPool::GetThreadLocalCacheHits() const {
	return thread_local_cache_hits.load(std::memory_order_relaxed);
}

idx_t MySQLConnectionPool::GetThreadLocalCacheMisses() const {
	return thread_local_cache_misses.load(std::memory_order_relaxed);
}

void MySQLConnectionPool::SetThreadLocalCacheEnabled(bool enabled) {
	thread_local_cache_enabled.store(enabled, std::memory_order_relaxed);
}

bool MySQLConnectionPool::IsThreadLocalCacheEnabled() const {
	return thread_local_cache_enabled.load(std::memory_order_relaxed);
}

void MySQLConnectionPool::CalibrateNetwork(MySQLConnection &conn) {
	static constexpr int NUM_SAMPLES = 3;
	double total_latency_ms = 0.0;

	MYSQL *mysql_conn = conn.GetConn();
	for (int i = 0; i < NUM_SAMPLES; i++) {
		auto start = std::chrono::steady_clock::now();
		if (mysql_query(mysql_conn, "SELECT 1") != 0) {
			lock_guard<mutex> lock(pool_lock);
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
		lock_guard<mutex> lock(pool_lock);
		network_calibration.latency_ms = avg_latency_ms;
		network_calibration.bandwidth_mbps = bandwidth_mbps;
		network_calibration.is_calibrated = true;
	}
}

unique_ptr<MySQLConnection> MySQLConnectionPool::CreateConnection() {
	auto conn = MySQLConnection::Open(type_config, connection_string, attach_path);
	auto result = make_uniq<MySQLConnection>(std::move(conn));

	bool should_calibrate = false;
	{
		lock_guard<mutex> lock(pool_lock);
		if (!network_calibration.is_calibrated) {
			should_calibrate = true;
		}
	}
	if (should_calibrate) {
		CalibrateNetwork(*result);
	}

	return result;
}

bool MySQLConnectionPool::IsConnectionHealthy(MySQLConnection &conn) {
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

void MySQLConnectionPool::ResetConnectionState(MySQLConnection &conn) {
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

} // namespace duckdb
