//===----------------------------------------------------------------------===//
//                         DuckDB
//
// generic_connection_pool.hpp
//
// Generic connection pool template — designed for extraction to a shared repo.
// Backend-specific behavior is provided via 3 virtual hooks:
//   CreateNewConnection(), CheckConnectionHealthy(), ResetConnection()
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>

namespace duckdb {

template <typename ConnectionT>
class GenericConnectionPool;

//===--------------------------------------------------------------------===//
// PooledConnection<T>
//===--------------------------------------------------------------------===//
template <typename ConnectionT>
class PooledConnection {
public:
	PooledConnection();
	PooledConnection(shared_ptr<GenericConnectionPool<ConnectionT>> pool, unique_ptr<ConnectionT> connection);
	~PooledConnection() noexcept;

	// disable copy constructors
	PooledConnection(const PooledConnection &) = delete;
	PooledConnection &operator=(const PooledConnection &) = delete;

	PooledConnection(PooledConnection &&other) noexcept;
	PooledConnection &operator=(PooledConnection &&other) noexcept;

	ConnectionT &GetConnection();
	ConnectionT *operator->();
	explicit operator bool() const;

	void Invalidate();

private:
	void ReturnToPool() noexcept;

	shared_ptr<GenericConnectionPool<ConnectionT>> pool;
	unique_ptr<ConnectionT> connection;
	bool valid = false;
};

//===--------------------------------------------------------------------===//
// ThreadLocalConnectionCache<T>
//===--------------------------------------------------------------------===//
template <typename ConnectionT>
struct ThreadLocalConnectionCache {
	unique_ptr<ConnectionT> connection;
	weak_ptr<GenericConnectionPool<ConnectionT>> owner;
	bool available = false;
	std::thread::id thread_id;

	ThreadLocalConnectionCache() : thread_id(std::this_thread::get_id()) {
	}

	~ThreadLocalConnectionCache();

	void Clear();
};

//===--------------------------------------------------------------------===//
// GenericConnectionPool<T>
//===--------------------------------------------------------------------===//
template <typename ConnectionT>
class GenericConnectionPool : public enable_shared_from_this<GenericConnectionPool<ConnectionT>> {
public:
	static constexpr idx_t DEFAULT_POOL_SIZE = 4;
	static constexpr idx_t DEFAULT_POOL_TIMEOUT_MS = 30000;

	GenericConnectionPool(idx_t max_connections = DEFAULT_POOL_SIZE, idx_t timeout_ms = DEFAULT_POOL_TIMEOUT_MS,
	                      bool thread_local_cache_enabled = false);
	virtual ~GenericConnectionPool();

	PooledConnection<ConnectionT> Acquire();
	PooledConnection<ConnectionT> TryAcquire();
	PooledConnection<ConnectionT> ForceAcquire();

	void Return(unique_ptr<ConnectionT> conn);
	void Discard();
	void Shutdown();

	void SetMaxConnections(idx_t new_max);

	idx_t GetMaxConnections() const;
	idx_t GetAvailableConnections() const;
	idx_t GetTotalConnections() const;
	bool IsShutdown() const;

	idx_t GetThreadLocalCacheHits() const;
	idx_t GetThreadLocalCacheMisses() const;
	void SetThreadLocalCacheEnabled(bool enabled);
	bool IsThreadLocalCacheEnabled() const;

protected:
	virtual unique_ptr<ConnectionT> CreateNewConnection() = 0;
	virtual bool CheckConnectionHealthy(ConnectionT &conn) = 0;
	virtual void ResetConnection(ConnectionT &conn) = 0;

	//! Calls fn(conn) for each idle connection while holding pool_lock.
	//! fn MUST NOT call any pool method that acquires pool_lock.
	template <typename Fn>
	void ForEachIdleConnection(Fn &&fn);

private:
	friend struct ThreadLocalConnectionCache<ConnectionT>;

	static ThreadLocalConnectionCache<ConnectionT> &GetThreadLocalCache() {
		static thread_local ThreadLocalConnectionCache<ConnectionT> cache;
		return cache;
	}

	unique_ptr<ConnectionT> TryAcquireFromThreadLocal();
	bool TryReturnToThreadLocal(unique_ptr<ConnectionT> &conn);
	void ReturnFromThreadLocalCache(unique_ptr<ConnectionT> conn);

	idx_t max_connections;
	idx_t timeout_ms;
	mutable mutex pool_lock;
	std::condition_variable pool_cv;
	std::deque<unique_ptr<ConnectionT>> available;
	idx_t total_connections = 0;
	bool shutdown_flag = false;

	std::atomic<bool> tl_cache_enabled;
	std::atomic<idx_t> tl_cache_hits {0};
	std::atomic<idx_t> tl_cache_misses {0};
};

//===--------------------------------------------------------------------===//
// ThreadLocalConnectionCache<T> Implementation
//===--------------------------------------------------------------------===//
template <typename ConnectionT>
ThreadLocalConnectionCache<ConnectionT>::~ThreadLocalConnectionCache() {
	auto pool = owner.lock();
	if (pool && connection) {
		pool->ReturnFromThreadLocalCache(std::move(connection));
	}
	connection.reset();
	owner.reset();
	available = false;
}

template <typename ConnectionT>
void ThreadLocalConnectionCache<ConnectionT>::Clear() {
	auto pool = owner.lock();
	if (connection && pool) {
		pool->ReturnFromThreadLocalCache(std::move(connection));
	}
	connection = nullptr;
	owner.reset();
	available = false;
}

//===--------------------------------------------------------------------===//
// PooledConnection<T> Implementation
//===--------------------------------------------------------------------===//
template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection() : pool(nullptr), connection(nullptr), valid(false) {
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection(shared_ptr<GenericConnectionPool<ConnectionT>> pool_p,
                                                unique_ptr<ConnectionT> connection_p)
    : pool(std::move(pool_p)), connection(std::move(connection_p)), valid(true) {
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::~PooledConnection() noexcept {
	ReturnToPool();
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection(PooledConnection &&other) noexcept
    : pool(std::move(other.pool)), connection(std::move(other.connection)), valid(other.valid) {
	other.valid = false;
}

template <typename ConnectionT>
PooledConnection<ConnectionT> &PooledConnection<ConnectionT>::operator=(PooledConnection &&other) noexcept {
	if (this != &other) {
		ReturnToPool();
		pool = std::move(other.pool);
		connection = std::move(other.connection);
		valid = other.valid;
		other.valid = false;
	}
	return *this;
}

template <typename ConnectionT>
ConnectionT &PooledConnection<ConnectionT>::GetConnection() {
	if (!connection) {
		throw InternalException("PooledConnection::GetConnection - no connection available");
	}
	return *connection;
}

template <typename ConnectionT>
ConnectionT *PooledConnection<ConnectionT>::operator->() {
	if (!connection) {
		throw InternalException("PooledConnection::operator-> - no connection available");
	}
	return connection.get();
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::operator bool() const {
	return connection != nullptr && valid;
}

template <typename ConnectionT>
void PooledConnection<ConnectionT>::Invalidate() {
	valid = false;
}

template <typename ConnectionT>
void PooledConnection<ConnectionT>::ReturnToPool() noexcept {
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
// GenericConnectionPool<T> Implementation
//===--------------------------------------------------------------------===//
template <typename ConnectionT>
GenericConnectionPool<ConnectionT>::GenericConnectionPool(idx_t max_connections_p, idx_t timeout_ms_p,
                                                          bool thread_local_cache_enabled_p)
    : max_connections(max_connections_p), timeout_ms(timeout_ms_p), total_connections(0), shutdown_flag(false),
      tl_cache_enabled(thread_local_cache_enabled_p) {
}

template <typename ConnectionT>
GenericConnectionPool<ConnectionT>::~GenericConnectionPool() {
	Shutdown();
}

template <typename ConnectionT>
void GenericConnectionPool<ConnectionT>::Shutdown() {
	{
		lock_guard<mutex> lock(pool_lock);
		if (shutdown_flag) {
			return;
		}
		shutdown_flag = true;
		available.clear();
	}
	pool_cv.notify_all();
	// Other threads' TL caches are cleaned up by their own ThreadLocalConnectionCache
	// destructors, which safely check owner.lock() returning nullptr after pool destruction.
}

template <typename ConnectionT>
bool GenericConnectionPool<ConnectionT>::IsShutdown() const {
	lock_guard<mutex> lock(pool_lock);
	return shutdown_flag;
}

template <typename ConnectionT>
unique_ptr<ConnectionT> GenericConnectionPool<ConnectionT>::TryAcquireFromThreadLocal() {
	if (!tl_cache_enabled.load(std::memory_order_relaxed)) {
		return nullptr;
	}

	auto &cache = GetThreadLocalCache();
	auto cached_owner = cache.owner.lock();
	if (!cached_owner || cached_owner.get() != this) {
		if (!cached_owner && cache.connection) {
			cache.Clear();
		}
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return nullptr;
	}

	if (!cache.available || !cache.connection) {
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return nullptr;
	}

	if (!CheckConnectionHealthy(*cache.connection)) {
		cache.Clear();
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return nullptr;
	}

	cache.available = false;
	tl_cache_hits.fetch_add(1, std::memory_order_relaxed);
	return std::move(cache.connection);
}

template <typename ConnectionT>
bool GenericConnectionPool<ConnectionT>::TryReturnToThreadLocal(unique_ptr<ConnectionT> &conn) {
	if (!tl_cache_enabled.load(std::memory_order_relaxed)) {
		return false;
	}

	auto &cache = GetThreadLocalCache();
	auto cached_owner = cache.owner.lock();
	if (cached_owner && cached_owner.get() != this) {
		return false;
	}

	if (cache.connection != nullptr) {
		return false;
	}

	lock_guard<mutex> lock(pool_lock);
	if (shutdown_flag) {
		return false;
	}
	if (total_connections >= max_connections && available.empty()) {
		return false;
	}
	cache.connection = std::move(conn);
	cache.owner = this->shared_from_this();
	cache.available = true;
	return true;
}

template <typename ConnectionT>
void GenericConnectionPool<ConnectionT>::ReturnFromThreadLocalCache(unique_ptr<ConnectionT> conn) {
	if (!conn) {
		return;
	}

	{
		lock_guard<mutex> lock(pool_lock);
		if (shutdown_flag) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		available.push_back(std::move(conn));
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
PooledConnection<ConnectionT> GenericConnectionPool<ConnectionT>::Acquire() {
	auto tl_conn = TryAcquireFromThreadLocal();
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	unique_lock<mutex> lock(pool_lock);

	auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

	while (true) {
		if (shutdown_flag) {
			throw IOException("Connection pool has been shut down");
		}

		while (!available.empty()) {
			auto conn = std::move(available.front());
			available.pop_front();

			lock.unlock();
			bool healthy = CheckConnectionHealthy(*conn);
			lock.lock();

			if (shutdown_flag) {
				if (total_connections > 0) {
					total_connections--;
				}
				throw IOException("Connection pool has been shut down");
			}

			if (healthy) {
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
			}
			if (total_connections > 0) {
				total_connections--;
			}
		}

		if (total_connections < max_connections) {
			total_connections++;
			lock.unlock();

			try {
				auto conn = CreateNewConnection();
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
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
			throw IOException("Connection pool timeout: all %llu connections in use, waited %llu ms",
			                  static_cast<unsigned long long>(max_connections),
			                  static_cast<unsigned long long>(timeout_ms));
		}
	}
}

template <typename ConnectionT>
PooledConnection<ConnectionT> GenericConnectionPool<ConnectionT>::TryAcquire() {
	auto tl_conn = TryAcquireFromThreadLocal();
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	unique_lock<mutex> lock(pool_lock);

	if (shutdown_flag) {
		return PooledConnection<ConnectionT>();
	}

	while (!available.empty()) {
		auto conn = std::move(available.front());
		available.pop_front();

		lock.unlock();
		bool healthy = CheckConnectionHealthy(*conn);
		lock.lock();

		if (shutdown_flag) {
			if (total_connections > 0) {
				total_connections--;
			}
			return PooledConnection<ConnectionT>();
		}

		if (healthy) {
			return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
		}
		if (total_connections > 0) {
			total_connections--;
		}
	}

	if (total_connections < max_connections) {
		total_connections++;
		lock.unlock();

		try {
			auto conn = CreateNewConnection();
			return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
		} catch (...) {
			lock.lock();
			if (total_connections > 0) {
				total_connections--;
			}
			pool_cv.notify_one();
			throw;
		}
	}

	return PooledConnection<ConnectionT>();
}

template <typename ConnectionT>
PooledConnection<ConnectionT> GenericConnectionPool<ConnectionT>::ForceAcquire() {
	auto tl_conn = TryAcquireFromThreadLocal();
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	{
		unique_lock<mutex> lock(pool_lock);

		if (shutdown_flag) {
			throw IOException("Connection pool has been shut down");
		}

		while (!available.empty()) {
			auto conn = std::move(available.front());
			available.pop_front();

			lock.unlock();
			bool healthy = CheckConnectionHealthy(*conn);
			lock.lock();

			if (shutdown_flag) {
				if (total_connections > 0) {
					total_connections--;
				}
				throw IOException("Connection pool has been shut down");
			}

			if (healthy) {
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
			}
			if (total_connections > 0) {
				total_connections--;
			}
		}

		total_connections++;
	}

	try {
		auto conn = CreateNewConnection();
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
	} catch (...) {
		lock_guard<mutex> lock(pool_lock);
		if (total_connections > 0) {
			total_connections--;
		}
		pool_cv.notify_one();
		throw;
	}
}

template <typename ConnectionT>
void GenericConnectionPool<ConnectionT>::Return(unique_ptr<ConnectionT> conn) {
	if (!conn) {
		return;
	}

	if (!CheckConnectionHealthy(*conn)) {
		Discard();
		return;
	}

	try {
		ResetConnection(*conn);
	} catch (...) {
		Discard();
		return;
	}

	if (!CheckConnectionHealthy(*conn)) {
		Discard();
		return;
	}

	if (TryReturnToThreadLocal(conn)) {
		return;
	}

	{
		lock_guard<mutex> lock(pool_lock);
		if (shutdown_flag) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		if (total_connections > max_connections) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		available.push_back(std::move(conn));
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
void GenericConnectionPool<ConnectionT>::Discard() {
	{
		lock_guard<mutex> lock(pool_lock);
		if (total_connections > 0) {
			total_connections--;
		}
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
void GenericConnectionPool<ConnectionT>::SetMaxConnections(idx_t new_max) {
	{
		lock_guard<mutex> lock(pool_lock);
		max_connections = new_max;
	}
	pool_cv.notify_all();
}

template <typename ConnectionT>
idx_t GenericConnectionPool<ConnectionT>::GetMaxConnections() const {
	lock_guard<mutex> lock(pool_lock);
	return max_connections;
}

template <typename ConnectionT>
idx_t GenericConnectionPool<ConnectionT>::GetAvailableConnections() const {
	lock_guard<mutex> lock(pool_lock);
	return available.size();
}

template <typename ConnectionT>
idx_t GenericConnectionPool<ConnectionT>::GetTotalConnections() const {
	lock_guard<mutex> lock(pool_lock);
	return total_connections;
}

template <typename ConnectionT>
idx_t GenericConnectionPool<ConnectionT>::GetThreadLocalCacheHits() const {
	return tl_cache_hits.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
idx_t GenericConnectionPool<ConnectionT>::GetThreadLocalCacheMisses() const {
	return tl_cache_misses.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
void GenericConnectionPool<ConnectionT>::SetThreadLocalCacheEnabled(bool enabled) {
	tl_cache_enabled.store(enabled, std::memory_order_relaxed);
}

template <typename ConnectionT>
bool GenericConnectionPool<ConnectionT>::IsThreadLocalCacheEnabled() const {
	return tl_cache_enabled.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
template <typename Fn>
void GenericConnectionPool<ConnectionT>::ForEachIdleConnection(Fn &&fn) {
	lock_guard<mutex> lock(pool_lock);
	for (auto &conn : available) {
		fn(*conn);
	}
}

} // namespace duckdb
