#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "mysql_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_transaction.hpp"
#include "mysql_connection.hpp"

namespace duckdb {

struct MySQLExecuteBindData : public TableFunctionData {
	explicit MySQLExecuteBindData(MySQLCatalog &mysql_catalog, string query_p, vector<Value> params_p)
	    : mysql_catalog(mysql_catalog), query(std::move(query_p)), params(std::move(params_p)) {
	}

	bool finished = false;
	MySQLCatalog &mysql_catalog;
	string query;
	vector<Value> params;
};

static duckdb::unique_ptr<FunctionData> MySQLExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in mysql_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "mysql") {
		throw BinderException("Attached database \"%s\" does not refer to a MySQL database", db_name);
	}
	auto &mysql_catalog = catalog.Cast<MySQLCatalog>();
	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to mysql_execute cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}
	return make_uniq<MySQLExecuteBindData>(mysql_catalog, input.inputs[1].GetValue<string>(), std::move(params));
}

static void MySQLExecuteFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<MySQLExecuteBindData>();
	if (data.finished) {
		return;
	}
	auto &transaction = Transaction::Get(context, data.mysql_catalog).Cast<MySQLTransaction>();
	if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
		throw PermissionException("mysql_execute cannot be run in a read-only connection");
	}
	transaction.GetConnection().Execute(data.query, std::move(data.params));
	data.finished = true;
}

MySQLExecuteFunction::MySQLExecuteFunction()
    : TableFunction("mysql_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLExecuteFunc, MySQLExecuteBind) {
	named_parameters["params"] = LogicalType::ANY;
}

} // namespace duckdb
