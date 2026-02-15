#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "mysql_scanner.hpp"
#include "mysql_types.hpp"
#include "mysql_connection_pool.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {

struct ClearCacheFunctionData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ClearCacheFunctionData>();
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void ClearMySQLCaches(ClientContext &context) {
	MySQLTypeConfig new_config(context);
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "mysql") {
			continue;
		}
		auto &mysql_catalog = catalog.Cast<MySQLCatalog>();
		mysql_catalog.ClearCache();
		mysql_catalog.GetConnectionPool().UpdateTypeConfig(new_config);
	}
}

static void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	ClearMySQLCaches(context);
	data.finished = true;
}

void MySQLClearCacheFunction::ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter) {
	ClearMySQLCaches(context);
}

MySQLClearCacheFunction::MySQLClearCacheFunction()
    : TableFunction("mysql_clear_cache", {}, ClearCacheFunction, ClearCacheBind) {
}
} // namespace duckdb
