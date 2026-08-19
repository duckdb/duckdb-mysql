#include "storage/mysql_connection_pool.hpp"

#include <chrono>
#include <cstring>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "dbconnector/functions/configure_pool.hpp"

#include "storage/mysql_catalog.hpp"

namespace duckdb {

using dbconnector::functions::ConfigurePool;

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
	auto tc = GetTypeConfig();
	auto conn = MySQLConnection::Open(std::move(tc), connection_string, attach_path);
	return make_uniq<MySQLConnection>(std::move(conn));
}

bool MySQLConnectionPool::CheckConnectionHealthy(MySQLConnection &conn) {
	string health_check_query = GetHealthCheckQuery();
	return conn.IsConnectionHealthy(health_check_query);
}

void MySQLConnectionPool::ResetConnection(MySQLConnection &conn) {
	conn.Reset();
}

//===--------------------------------------------------------------------===//
// MySQL-Specific Methods
//===--------------------------------------------------------------------===//
MySQLTypeConfig MySQLConnectionPool::GetTypeConfig() const {
	lock_guard<mutex> guard(config_lock);
	return type_config;
}

void MySQLConnectionPool::SetTypeConfig(MySQLTypeConfig config) {
	ForEachIdleConnection([&config](MySQLConnection &conn) { conn.SetTypeConfig(config); });
	lock_guard<mutex> lock(config_lock);
	this->type_config = std::move(config);
}

static idx_t ReadUBigIntOption(ClientContext &ctx, const std::string &name, idx_t default_val) {
	Value val;
	if (ctx.TryGetCurrentSetting(Identifier(name), val)) {
		return UBigIntValue::Get(val);
	}
	return default_val;
}

static bool ReadBooleanOption(ClientContext &ctx, const std::string &name, bool default_val) {
	Value val;
	if (ctx.TryGetCurrentSetting(Identifier(name), val)) {
		return BooleanValue::Get(val);
	}
	return default_val;
}

static string ReadVarcharOption(ClientContext &ctx, const std::string &name, const string &default_val = string()) {
	Value val;
	if (ctx.TryGetCurrentSetting(Identifier(name), val)) {
		return StringValue::Get(val);
	}
	return default_val;
}

dbconnector::pool::ConnectionPoolConfig MySQLConnectionPool::CreateConfig(ClientContext &ctx) {
	dbconnector::pool::ConnectionPoolConfig config;
	string acquire_mode_str = ReadVarcharOption(ctx, "mysql_pool_acquire_mode");
	if (!acquire_mode_str.empty()) {
		config.acquire_mode = dbconnector::pool::AcquireModeHelpers::FromString(acquire_mode_str);
	}
	config.max_connections = ReadUBigIntOption(ctx, "mysql_pool_size", config.max_connections);
	config.wait_timeout_millis = ReadUBigIntOption(ctx, "mysql_pool_wait_timeout_millis", config.wait_timeout_millis);
	config.tl_cache_enabled = ReadBooleanOption(ctx, "mysql_pool_enable_thread_local_cache", config.tl_cache_enabled);
	config.max_lifetime_millis =
	    ReadUBigIntOption(ctx, "mysql_pool_connection_max_lifetime_millis", config.max_lifetime_millis);
	config.idle_timeout_millis =
	    ReadUBigIntOption(ctx, "mysql_pool_connection_idle_timeout_millis", config.idle_timeout_millis);
	config.start_reaper_thread = ReadBooleanOption(ctx, "mysql_pool_enable_reaper_thread", config.start_reaper_thread);
	config.health_check_query = ReadVarcharOption(ctx, "mysql_pool_health_check_query");
	return config;
}

void MySQLConnectionPool::ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter) {
	dbconnector::pool::AcquireMode mode = dbconnector::pool::AcquireModeHelpers::FromString(parameter.ToString());
	if (mode != dbconnector::pool::AcquireMode::FORCE) {
		Value pool_size_val;
		if (context.TryGetCurrentSetting("mysql_pool_size", pool_size_val)) {
			auto pool_size = pool_size_val.GetValue<uint64_t>();
			if (pool_size == 0) {
				std::string mode_str = dbconnector::pool::AcquireModeHelpers::ToString(mode);
				throw InvalidInputException(
				    "mysql_pool_acquire_mode='%s' requires mysql_pool_size > 0 (pooling enabled)", mode_str);
			}
		}
	}
}

static shared_ptr<MySQLConnectionPool> GetConnnectionPoolFromCatalog(Catalog &catalog) {
	if (catalog.GetCatalogType() != "mysql") {
		return nullptr;
	}
	return catalog.Cast<MySQLCatalog>().GetConnectionPoolPtr();
}

MySQLConfigurePoolFunction::MySQLConfigurePoolFunction()
    : TableFunction("mysql_configure_pool", std::vector<LogicalType>(),
                    ConfigurePool::Function<MySQLConnection, GetConnnectionPoolFromCatalog>, ConfigurePool::Bind,
                    ConfigurePool::InitGlobalState, ConfigurePool::InitLocalState) {
	for (auto &en : ConfigurePool::NamedParameters()) {
		named_parameters[en.first] = en.second;
	}
}

} // namespace duckdb
