#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
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
	DataChunk varchar_chunk;

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

	// generate the varchar chunk
	vector<LogicalType> varchar_types;
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		varchar_types.push_back(LogicalType::VARCHAR);
	}
	result->varchar_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> MySQLInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<MySQLLocalState>();
}

void CastBoolFromMySQL(ClientContext &context, Vector &input, Vector &result, idx_t size) {
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t r = 0; r < size; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		auto str_data = input_data[r].GetData();
		auto str_size = input_data[r].GetSize();
		if (str_size == 0) {
			throw BinderException(
			    "Failed to cast MySQL boolean - expected 1 byte element but got element of size %d\n* SET "
			    "mysql_tinyint1_as_boolean=false to disable loading TINYINT(1) columns as booleans\n* SET "
			    "mysql_bit1_as_boolean=false to disable loading BIT(1) columns as booleans",
			    str_size);
		}
		// booleans are EITHER binary "1" or "0" (BIT(1))
		// OR a number
		// in both cases we can figure out what value it is from the first character:
		// \0 -> zero byte, false
		// - -> negative number, false
		// 0 -> zero number, false
		if (*str_data == '\0' || *str_data == '0' || *str_data == '-') {
			result_data[r] = false;
		} else {
			result_data[r] = true;
		}
	}
}

static void MySQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
	idx_t r;
	gstate.varchar_chunk.Reset();
	for (r = 0; r < STANDARD_VECTOR_SIZE; r++) {
		if (!gstate.result->Next()) {
			// exhausted result
			break;
		}
		for (idx_t c = 0; c < output.ColumnCount(); c++) {
			auto &vec = gstate.varchar_chunk.data[c];
			if (gstate.result->IsNull(c)) {
				FlatVector::SetNull(vec, r, true);
			} else {
				auto string_data = FlatVector::GetData<string_t>(vec);
				string_data[r] = StringVector::AddStringOrBlob(vec, gstate.result->GetStringT(c));
			}
		}
	}
	if (r == 0) {
		// done
		return;
	}
	D_ASSERT(output.ColumnCount() == gstate.varchar_chunk.ColumnCount());
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		switch (output.data[c].GetType().id()) {
		case LogicalTypeId::BLOB:
			// blobs are sent over the wire as-is
			output.data[c].Reinterpret(gstate.varchar_chunk.data[c]);
			break;
		case LogicalTypeId::BOOLEAN:
			// booleans can be sent either as numbers ('0' or '1') or as bits ('\0' or
			// '\1')
			CastBoolFromMySQL(context, gstate.varchar_chunk.data[c], output.data[c], r);
			break;
		case LogicalTypeId::TIMESTAMP_TZ: {
			string error;
			VectorOperations::DefaultTryCast(gstate.varchar_chunk.data[c], output.data[c], r, &error);
			break;
		}
		default: {
			string error;
			VectorOperations::TryCast(context, gstate.varchar_chunk.data[c], output.data[c], r, &error);
			break;
		}
		}
	}
	output.SetCardinality(r);
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
	auto result = transaction.GetConnection().Query(sql, MySQLResultStreaming::FORCE_MATERIALIZATION, context);
	for (auto &field : result->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.type);
	}
	if (return_types.empty()) {
		throw InvalidInputException("Failed to fetch return types for query '%s'", sql);
	}
	return make_uniq<MySQLQueryBindData>(catalog, std::move(result), std::move(sql));
}

static unique_ptr<GlobalTableFunctionState> MySQLQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLQueryBindData>();
	unique_ptr<MySQLResult> mysql_result;
	if (bind_data.result) {
		mysql_result = std::move(bind_data.result);
	} else {
		auto &transaction = MySQLTransaction::Get(context, bind_data.catalog);
		mysql_result =
		    transaction.GetConnection().Query(bind_data.query, MySQLResultStreaming::FORCE_MATERIALIZATION, context);
	}
	auto column_count = mysql_result->ColumnCount();

	auto result = make_uniq<MySQLGlobalState>(std::move(mysql_result));

	// generate the varchar chunk
	vector<LogicalType> varchar_types;
	for (idx_t c = 0; c < column_count; c++) {
		varchar_types.push_back(LogicalType::VARCHAR);
	}
	result->varchar_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
	return std::move(result);
}

MySQLQueryFunction::MySQLQueryFunction()
    : TableFunction("mysql_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
}

} // namespace duckdb
