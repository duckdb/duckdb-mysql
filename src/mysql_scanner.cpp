#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "mysql_scanner.hpp"
#include "mysql_result.hpp"
#include "mysql_connection_pool.hpp"
#include "storage/mysql_transaction.hpp"
#include "duckdb/main/query_result.hpp"
#include "storage/mysql_table_set.hpp"
#include "storage/mysql_catalog.hpp"
#include "mysql_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

struct MySQLLocalState : public LocalTableFunctionState {};

struct MySQLGlobalState : public GlobalTableFunctionState {
	explicit MySQLGlobalState(MySQLCatalog &catalog_p, string query_p, vector<Value> params_p,
	                          vector<MySQLField> fields_p)
	    : catalog(&catalog_p), query(std::move(query_p)), params(std::move(params_p)), fields(std::move(fields_p)) {
	}

	explicit MySQLGlobalState(unique_ptr<MySQLResult> result_p) : result(std::move(result_p)) {
	}

	optional_ptr<MySQLCatalog> catalog;
	string query;
	vector<Value> params;
	vector<MySQLField> fields;
	unique_ptr<MySQLResult> result;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> MySQLBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("MySQLBind");
}

static unique_ptr<GlobalTableFunctionState> MySQLInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLBindData>();
	auto &transaction = MySQLTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();

	auto &mysql_catalog = bind_data.table.catalog.Cast<MySQLCatalog>();

	string select;
	select += "SELECT ";
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			select += "NULL";
		} else {
			auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
			auto col_name = col.GetName();
			select += MySQLUtils::WriteIdentifier(col_name.GetIdentifierName());
		}
	}
	select += " FROM ";
	select += MySQLUtils::WriteIdentifier(bind_data.table.schema.name.GetIdentifierName());
	select += ".";
	select += MySQLUtils::WriteIdentifier(bind_data.table.name.GetIdentifierName());

	string filter_string = MySQLFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);

	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}

	auto query_result = con.Query(select, MySQLResultStreaming::FORCE_MATERIALIZATION);
	auto result = make_uniq<MySQLGlobalState>(std::move(query_result));

	return result;
}

static unique_ptr<LocalTableFunctionState> MySQLInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<MySQLLocalState>();
}

static void MySQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();

	while (true) {
		if (gstate.result->Exhausted()) {
			output.SetChildCardinality(0);
			return;
		}

		DataChunk &res_chunk = gstate.result->NextChunk();
		D_ASSERT(output.ColumnCount() == res_chunk.ColumnCount());
		string error;
		for (idx_t c = 0; c < output.ColumnCount(); c++) {
			Vector &output_vec = output.data[c];
			Vector &res_vec = res_chunk.data[c];
			switch (output_vec.GetType().id()) {
			case LogicalTypeId::BOOLEAN:
			case LogicalTypeId::TINYINT:
			case LogicalTypeId::UTINYINT:
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::USMALLINT:
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::UINTEGER:
			case LogicalTypeId::BIGINT:
			case LogicalTypeId::UBIGINT:
			case LogicalTypeId::FLOAT:
			case LogicalTypeId::DOUBLE:
			case LogicalTypeId::BLOB:
			case LogicalTypeId::DATE:
			case LogicalTypeId::TIME:
			case LogicalTypeId::TIMESTAMP: {
				if (output_vec.GetType().id() == res_vec.GetType().id() ||
				    (output_vec.GetType().id() == LogicalTypeId::BLOB &&
				     res_vec.GetType().id() == LogicalTypeId::VARCHAR)) {
					output_vec.Reinterpret(res_vec);
				} else {
					VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
				}
				break;
			}
			default: {
				VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
				break;
			}
			}
			if (!error.empty()) {
				throw BinderException(error);
			}
		}
		output.SetChildCardinality(res_chunk.size());
		return;
	}
}

static InsertionOrderPreservingMap<string> MySQLScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<MySQLBindData>();
	result["Table"] = bind_data.table.name.GetIdentifierName();
	return result;
}

static void MySQLScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("MySQLScanSerialize");
}

static unique_ptr<FunctionData> MySQLScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("MySQLScanDeserialize");
}

static BindInfo MySQLGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MySQLBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table;
	return info;
}

MySQLScanFunction::MySQLScanFunction()
    : TableFunction("mysql_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLScan,
                    MySQLBind, MySQLInitGlobalState, MySQLInitLocalState) {
	to_string = MySQLScanToString;
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	get_bind_info = MySQLGetBindInfo;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// MySQL Query
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> MySQLQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to mysql_query cannot be NULL");
	}

	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in mysql_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "mysql") {
		throw BinderException("Attached database \"%s\" does not refer to a MySQL database", db_name);
	}
	auto sql = input.inputs[1].GetValue<string>();

	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Query parameters cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}

	try {
		auto &transaction = MySQLTransaction::Get(context, catalog);
		MySQLConnection &conn = transaction.GetConnection();
		auto stmt = conn.Prepare(sql);
		if (stmt->Fields().size() > 0) {
			for (auto &field : stmt->Fields()) {
				names.push_back(field.name);
				return_types.push_back(field.duckdb_type);
			}
		} else {
			return_types.emplace_back(LogicalType::BOOLEAN);
			names.emplace_back("Success");
		}
		// the remote result can contain duplicate column names (e.g. "SELECT a.id, b.id ...") -
		// rename them as table functions require unique column names
		QueryResult::DeduplicateColumns(names);
		return make_uniq<MySQLQueryBindData>(catalog, sql, std::move(params), std::move(stmt->FieldsCopy()));
	} catch (const std::exception &ex) {
		ErrorData error(ex);
		throw BinderException("PREPARE error, query: \"%s\", message: \"%s\"", sql, error.RawMessage());
	}
}

static unique_ptr<GlobalTableFunctionState> MySQLQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLQueryBindData>();
	auto &catalog = bind_data.catalog.Cast<MySQLCatalog>();
	return make_uniq<MySQLGlobalState>(catalog, std::move(bind_data.query), std::move(bind_data.params),
	                                   std::move(bind_data.fields));
}

static void MySQLQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
	if (!gstate.result) {
		auto &transaction = MySQLTransaction::Get(context, *gstate.catalog);
		MySQLConnection &conn = transaction.GetConnection();
		gstate.result = conn.Query(gstate.query, gstate.params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	}
	MySQLScan(context, data, output);
}

MySQLQueryFunction::MySQLQueryFunction()
    : TableFunction("mysql_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLQueryScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
	// TODO: reimplement me
	named_parameters["stream_results"] = LogicalType::BOOLEAN;
}

static void MySQLExecuteScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
	if (!gstate.result) {
		auto &transaction = Transaction::Get(context, *gstate.catalog).Cast<MySQLTransaction>();
		if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
			throw PermissionException("mysql_execute cannot be run in a read-only connection");
		}
	}
	return MySQLQueryScan(context, data, output);
}

MySQLExecuteFunction::MySQLExecuteFunction()
    : TableFunction("mysql_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLExecuteScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
}

} // namespace duckdb
