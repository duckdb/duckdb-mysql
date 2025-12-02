#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "mysql_scanner.hpp"
#include "mysql_result.hpp"
#include "storage/mysql_transaction.hpp"
#include "storage/mysql_table_set.hpp"
#include "mysql_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct MySQLGlobalState;

struct MySQLLocalState : public LocalTableFunctionState {};

struct MySQLGlobalState : public GlobalTableFunctionState {
	explicit MySQLGlobalState(unique_ptr<MySQLResult> result_p) : result(std::move(result_p)) {
	}

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
	auto &bind_data = input.bind_data->Cast<MySQLBindData>();
	// generate the SELECT statement
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
			select += MySQLUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += MySQLUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += MySQLUtils::WriteIdentifier(bind_data.table.name);
	string filter_string = MySQLFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}
	if (!bind_data.limit.empty()) {
		select += bind_data.limit;
	}
	// run the query
	auto &transaction = MySQLTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();
	auto query_result = con.Query(select, bind_data.streaming);
	auto result = make_uniq<MySQLGlobalState>(std::move(query_result));

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> MySQLInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<MySQLLocalState>();
}

static void MySQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
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
		// throw an error in case of TryCast fails
		if (!error.empty()) {
			throw BinderException(error);
		}
	}
	output.SetCardinality(res_chunk.size());
}

static InsertionOrderPreservingMap<string> MySQLScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<MySQLBindData>();
	result["Table"] = bind_data.table.name;
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
	auto &transaction = MySQLTransaction::Get(context, catalog);
	auto sql = input.inputs[1].GetValue<string>();

	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to mysql_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}

	MySQLResultStreaming streaming = MySQLResultStreaming::FORCE_MATERIALIZATION;
	auto streaming_it = input.named_parameters.find("stream_results");
	if (streaming_it != input.named_parameters.end()) {
		Value &bool_val = streaming_it->second;
		if (!bool_val.IsNull()) {
			if (BooleanValue::Get(bool_val)) {
				streaming = MySQLResultStreaming::REQUIRE_STREAMING;
			} else {
				streaming = MySQLResultStreaming::FORCE_MATERIALIZATION;
			}
		} else {
			streaming = MySQLResultStreaming::ALLOW_STREAMING;
		}
	}

	MySQLConnection &conn = transaction.GetConnection();

	if (streaming == MySQLResultStreaming::FORCE_MATERIALIZATION) {
		auto result = conn.Query(sql, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
		for (auto &field : result->Fields()) {
			names.push_back(field.name);
			return_types.push_back(field.duckdb_type);
		}
		return make_uniq<MySQLQueryBindData>(catalog, std::move(result), std::move(sql), streaming);
	}

	auto stmt = transaction.GetConnection().Prepare(sql);
	for (auto &field : stmt->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.duckdb_type);
	}
	return make_uniq<MySQLQueryBindData>(catalog, std::move(stmt), std::move(params), std::move(sql), streaming);
}

static unique_ptr<GlobalTableFunctionState> MySQLQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLQueryBindData>();
	unique_ptr<MySQLResult> mysql_result;
	if (bind_data.result) {
		mysql_result = std::move(bind_data.result);
	}
	return make_uniq<MySQLGlobalState>(std::move(mysql_result));
}

static void MySQLQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
	if (!gstate.result) {
		auto &bind_data = data.bind_data->CastNoConst<MySQLQueryBindData>();
		if (bind_data.user_streaming == MySQLResultStreaming::REQUIRE_STREAMING &&
		    bind_data.streaming != MySQLResultStreaming::ALLOW_STREAMING) {
			throw IOException("Unable to stream results of 'mysql_query' function, ensure that each invocation with "
			                  "'stream_results=TRUE' uses its own private connection");
		}
		auto &transaction = MySQLTransaction::Get(context, bind_data.catalog);
		MySQLStatement &stmt = *bind_data.stmt;
		auto result = transaction.GetConnection().Query(stmt, bind_data.params, bind_data.streaming);
		gstate.result = std::move(result);
	}
	MySQLScan(context, data, output);
}

MySQLQueryFunction::MySQLQueryFunction()
    : TableFunction("mysql_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLQueryScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
	named_parameters["stream_results"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
