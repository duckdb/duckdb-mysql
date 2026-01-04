#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

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

	SecretType secret_type;
	secret_type.name = "mysql";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction mysql_secret_function = {"mysql", "config", CreateMySQLSecretFunction};
	SetMySQLSecretParameters(mysql_secret_function);
	loader.RegisterFunction(mysql_secret_function);

	auto &db = loader.GetDatabaseInstance();

	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["mysql_scanner"] = make_uniq<MySQLStorageExtension>();

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

	OptimizerExtension mysql_optimizer;
	mysql_optimizer.optimize_function = MySQLOptimizer::Optimize;
	config.optimizer_extensions.push_back(std::move(mysql_optimizer));
}

void MysqlScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(mysql_scanner, loader) {
	LoadInternal(loader);
}
}
