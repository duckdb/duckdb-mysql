#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "mysql_scanner.hpp"
#include "mysql_storage.hpp"
#include "mysql_scanner_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"
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

static void LoadInternal(DatabaseInstance &db) {
	mysql_library_init(0, NULL, NULL);
	MySQLClearCacheFunction clear_cache_func;
	ExtensionUtil::RegisterFunction(db, clear_cache_func);

	MySQLExecuteFunction execute_function;
	ExtensionUtil::RegisterFunction(db, execute_function);

	MySQLQueryFunction query_function;
	ExtensionUtil::RegisterFunction(db, query_function);

	SecretType secret_type;
	secret_type.name = "mysql";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(db, secret_type);

	CreateSecretFunction mysql_secret_function = {"mysql", "config", CreateMySQLSecretFunction};
	SetMySQLSecretParameters(mysql_secret_function);
	ExtensionUtil::RegisterFunction(db, mysql_secret_function);

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

	OptimizerExtension mysql_optimizer;
	mysql_optimizer.optimize_function = MySQLOptimizer::Optimize;
	config.optimizer_extensions.push_back(std::move(mysql_optimizer));
}

void MysqlScannerExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

extern "C" {

DUCKDB_EXTENSION_API void mysql_scanner_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *mysql_scanner_version() {
	return DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void mysql_scanner_storage_init(DBConfig &config) {
	mysql_library_init(0, NULL, NULL);
	config.storage_extensions["mysql_scanner"] = make_uniq<MySQLStorageExtension>();
}
}
