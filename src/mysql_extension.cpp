#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "mysql_connection_pool.hpp"
#include "mysql_scanner.hpp"
#include "mysql_storage.hpp"
#include "mysql_scanner_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_optimizer.hpp"

using namespace duckdb;

static void SetMySQLDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	MySQLConnection::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void ValidatePoolSize(ClientContext &context, SetScope scope, Value &parameter) {
	auto new_size = parameter.GetValue<uint64_t>();
	if (new_size == 0) {
		Value acquire_mode_val;
		if (context.TryGetCurrentSetting("mysql_pool_acquire_mode", acquire_mode_val)) {
			auto mode = StringUtil::Lower(acquire_mode_val.ToString());
			if (mode != "force") {
				throw InvalidInputException(
				    "mysql_pool_size=0 (pooling disabled) requires mysql_pool_acquire_mode='force'");
			}
		}
	}
}

static void ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter) {
	auto mode = StringUtil::Lower(parameter.ToString());
	if (mode != "force" && mode != "wait" && mode != "try") {
		throw InvalidInputException("mysql_pool_acquire_mode must be 'force', 'wait', or 'try'");
	}
	if (mode != "force") {
		Value pool_size_val;
		if (context.TryGetCurrentSetting("mysql_pool_size", pool_size_val)) {
			auto pool_size = pool_size_val.GetValue<uint64_t>();
			if (pool_size == 0) {
				throw InvalidInputException(
				    "mysql_pool_acquire_mode='%s' requires mysql_pool_size > 0 (pooling enabled)", mode);
			}
		}
	}
}

static void ValidateUnitInterval(ClientContext &context, SetScope scope, Value &parameter) {
	auto val = parameter.GetValue<double>();
	if (val < 0.0 || val > 1.0) {
		throw InvalidInputException("Value must be between 0.0 and 1.0, got %f", val);
	}
}

unique_ptr<BaseSecret> CreateMySQLSecretFunction(ClientContext &, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "mysql", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "host") {
			result->secret_map["host"] = named_param.second.ToString();
		} else if (lower_name == "user") {
			result->secret_map["user"] = named_param.second.ToString();
		} else if (lower_name == "database") {
			result->secret_map["database"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
		} else if (lower_name == "port") {
			result->secret_map["port"] = named_param.second.ToString();
		} else if (lower_name == "socket") {
			result->secret_map["socket"] = named_param.second.ToString();
		} else if (lower_name == "ssl_mode") {
			result->secret_map["ssl_mode"] = named_param.second.ToString();
		} else if (lower_name == "ssl_ca") {
			result->secret_map["ssl_ca"] = named_param.second.ToString();
		} else if (lower_name == "ssl_capath") {
			result->secret_map["ssl_capath"] = named_param.second.ToString();
		} else if (lower_name == "ssl_cert") {
			result->secret_map["ssl_cert"] = named_param.second.ToString();
		} else if (lower_name == "ssl_cipher") {
			result->secret_map["ssl_cipher"] = named_param.second.ToString();
		} else if (lower_name == "ssl_crl") {
			result->secret_map["ssl_crl"] = named_param.second.ToString();
		} else if (lower_name == "ssl_crlpath") {
			result->secret_map["ssl_crlpath"] = named_param.second.ToString();
		} else if (lower_name == "ssl_key") {
			result->secret_map["ssl_key"] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown named parameter passed to CreateMySQLSecretFunction: " + lower_name);
		}
	}

	//! Set redact keys
	result->redact_keys = {"password"};
	return std::move(result);
}

void SetMySQLSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::VARCHAR;
	function.named_parameters["password"] = LogicalType::VARCHAR;
	function.named_parameters["user"] = LogicalType::VARCHAR;
	function.named_parameters["database"] = LogicalType::VARCHAR;
	function.named_parameters["socket"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_mode"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_ca"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_capath"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_cert"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_cipher"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_crl"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_crlpath"] = LogicalType::VARCHAR;
	function.named_parameters["ssl_key"] = LogicalType::VARCHAR;
}

static void LoadInternal(ExtensionLoader &loader) {
	mysql_library_init(0, NULL, NULL);
	MySQLClearCacheFunction clear_cache_func;
	loader.RegisterFunction(clear_cache_func);

	MySQLExecuteFunction execute_function;
	loader.RegisterFunction(execute_function);

	MySQLQueryFunction query_function;
	loader.RegisterFunction(query_function);

	MySQLExplainFederatedFunction explain_federated_function;
	loader.RegisterFunction(explain_federated_function);

	MySQLDebugExecutionPlanFunction debug_plan_function;
	loader.RegisterFunction(debug_plan_function);

	SecretType secret_type;
	secret_type.name = "mysql";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction mysql_secret_function = {"mysql", "config", CreateMySQLSecretFunction};
	SetMySQLSecretParameters(mysql_secret_function);
	loader.RegisterFunction(mysql_secret_function);

	auto &db = loader.GetDatabaseInstance();

	dbconnector::pool::ConnectionPoolConfig default_pool_config;
	auto &config = DBConfig::GetConfig(db);
	StorageExtension::Register(config, "mysql_scanner", make_shared_ptr<MySQLStorageExtension>());

	config.AddExtensionOption("mysql_experimental_filter_pushdown",
	                          "Whether or not to use filter pushdown (currently experimental)", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
	config.AddExtensionOption("mysql_debug_show_queries", "DEBUG SETTING: print all queries sent to MySQL to stdout",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), SetMySQLDebugQueryPrint);
	config.AddExtensionOption("mysql_tinyint1_as_boolean", "Whether or not to convert TINYINT(1) columns to BOOLEAN",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), MySQLClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("mysql_bit1_as_boolean", "Whether or not to convert BIT(1) columns to BOOLEAN",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), MySQLClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("mysql_session_time_zone",
	                          "Value to use as a session time zone for newly opened"
	                          " connections to MySQL server",
	                          LogicalType::VARCHAR, Value(""), MySQLClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("mysql_time_as_time", "Whether or not to convert MySQL's TIME columns to DuckDB's TIME",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false),
	                          MySQLClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("mysql_incomplete_dates_as_nulls",
	                          "Whether to return DATEs with zero month or day as NULLs", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false), MySQLClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("mysql_enable_transactions",
	                          "Whether to run 'START TRANSACTION'/'COMMIT'/'ROLLBACK' on MySQL connections",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), MySQLClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption(
	    "mysql_pool_size",
	    "Maximum number of connections per MySQL catalog (default: min(cpu_count, 4 <= cpu_count * 1.5 <= 32))",
	    LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.max_connections), ValidatePoolSize);
	config.AddExtensionOption("mysql_pool_wait_timeout_millis",
	                          "Timeout in milliseconds when waiting for a connection from the pool (default: 30000)",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.wait_timeout_millis));
	config.AddExtensionOption("mysql_pool_connection_max_lifetime_millis",
	                          "Maximum age in milliseconds of a pooled connection since it was first opened. When "
	                          "exceeded, the connection is closed instead of being returned to the cache (default: 0 - "
	                          "disabled)",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.max_lifetime_millis));
	config.AddExtensionOption("mysql_pool_connection_idle_timeout_millis",
	                          "Maximum time in milliseconds a connection can sit idle in the cache before being closed "
	                          "(default: 60000 - disabled)",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.idle_timeout_millis));
	config.AddExtensionOption("mysql_pool_enable_reaper_thread",
	                          "Whether to run a dedicated thread that periodically scans the pool and removes expired "
	                          "connections (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(default_pool_config.start_reaper_thread));
	config.AddExtensionOption(
	    "mysql_pool_acquire_mode",
	    "How to acquire connections from the pool: 'force' (always connect, ignore pool limit), "
	    "'wait' (block until available), 'try' (fail immediately if unavailable) (default: force)",
	    LogicalType::VARCHAR, Value(dbconnector::pool::AcquireModeHelpers::ToString(default_pool_config.acquire_mode)),
	    ValidatePoolAcquireMode);
	config.AddExtensionOption(
	    "mysql_pool_enable_thread_local_cache",
	    "Enable thread-local connection caching for faster same-thread connection reuse (default: false)",
	    LogicalType::BOOLEAN, Value::BOOLEAN(default_pool_config.tl_cache_enabled));
	config.AddExtensionOption("mysql_compression_aware_costs",
	                          "Apply compression ratios when estimating transfer costs (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("mysql_compression_ratio",
	                          "Compression ratio for transfer cost estimation (default: 0.7)", LogicalType::DOUBLE,
	                          Value::DOUBLE(0.7), ValidateUnitInterval);
	config.AddExtensionOption("mysql_push_threshold_with_index",
	                          "Selectivity threshold for pushing filters with index support (default: 0.5)",
	                          LogicalType::DOUBLE, Value::DOUBLE(0.5), ValidateUnitInterval);
	config.AddExtensionOption("mysql_push_threshold_no_index",
	                          "Selectivity threshold for pushing filters without index support (default: 0.3)",
	                          LogicalType::DOUBLE, Value::DOUBLE(0.3), ValidateUnitInterval);
	config.AddExtensionOption("mysql_hint_injection_enabled",
	                          "Inject MySQL optimizer hints when statistics appear stale (default: false)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("mysql_hint_staleness_threshold",
	                          "Staleness score threshold for injecting optimizer hints (default: 0.5)",
	                          LogicalType::DOUBLE, Value::DOUBLE(0.5), ValidateUnitInterval);
	config.AddExtensionOption("mysql_adaptive_replan_enabled",
	                          "Enable adaptive execution strategy based on actual vs estimated rows (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("mysql_explain_validation_enabled",
	                          "Validate MySQL execution plans with EXPLAIN (default: false)", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false));
	config.AddExtensionOption("mysql_query_timeout_enabled",
	                          "Add MAX_EXECUTION_TIME hint to MySQL queries for safety (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("mysql_query_timeout_min_ms", "Minimum query timeout in milliseconds (default: 5000)",
	                          LogicalType::UBIGINT, Value::UBIGINT(5000));
	config.AddExtensionOption("mysql_query_timeout_max_ms", "Maximum query timeout in milliseconds (default: 300000)",
	                          LogicalType::UBIGINT, Value::UBIGINT(300000));
	config.AddExtensionOption("mysql_sql_buffer_result",
	                          "Add SQL_BUFFER_RESULT for large result sets to release row locks faster (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("mysql_aggregate_pushdown_enabled",
	                          "Push aggregate functions (COUNT, SUM, etc.) to MySQL (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("mysql_order_pushdown_enabled",
	                          "Push ORDER BY and LIMIT clauses to MySQL (default: true)", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
	OptimizerExtension mysql_optimizer;
	mysql_optimizer.optimize_function = MySQLOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(mysql_optimizer));
}

void MysqlScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(mysql_scanner, loader) {
	LoadInternal(loader);
}
}
