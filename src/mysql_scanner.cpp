#include "mysql_scanner.hpp"

#include "dbconnector/defer.hpp"

#include "duckdb.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

#include "storage/mysql_connection_pool.hpp"
#include "mysql_filter_pushdown.hpp"
#include "mysql_parameter.hpp"
#include "mysql_result.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_table_set.hpp"
#include "storage/mysql_transaction.hpp"

namespace duckdb {

struct MySQLLocalState : public LocalTableFunctionState {};

enum class MySQLQueryExecState { UNINITIALIZED, EXECUTED, EXHAUSTED };

struct MySQLGlobalState : public GlobalTableFunctionState {
	explicit MySQLGlobalState(MySQLPooledConnection pinned_connection_p)
	    : pinned_connection(std::move(pinned_connection_p)) {
	}

	explicit MySQLGlobalState(string scan_query_p) : scan_query(std::move(scan_query_p)) {
	}

	~MySQLGlobalState() {
		if (!pinned_connection) {
			return;
		}
		try {
			pinned_connection.PinBack();
		} catch (...) {
			// suppress
		}
	}

	MySQLPooledConnection pinned_connection;
	vector<Value> params;
	string scan_query;
	unique_ptr<MySQLResult> result;
	MySQLQueryExecState exec_state = MySQLQueryExecState::UNINITIALIZED;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> MySQLBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	throw InternalException("MySQLBind");
}

static unique_ptr<GlobalTableFunctionState> MySQLInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLBindData>();

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

	return make_uniq<MySQLGlobalState>(std::move(select));
}

static unique_ptr<LocalTableFunctionState> MySQLInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<MySQLLocalState>();
}

static void MySQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();

	if (gstate.exec_state == MySQLQueryExecState::UNINITIALIZED) {
		auto &bdata = data.bind_data->CastNoConst<MySQLBindData>();
		auto &transaction = MySQLTransaction::Get(context, bdata.table.catalog);
		auto &con = transaction.GetConnection();
		gstate.result = con.Query(gstate.scan_query, bdata.optimizer_streaming);
		gstate.exec_state = MySQLQueryExecState::EXECUTED;
	}

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

static MySQLCatalog &GetCatalogByName(ClientContext &context, const std::string &name) {
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(name));
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "mysql") {
		throw BinderException("Attached database \"%s\" does not refer to a MySQL database", name);
	}
	return catalog.Cast<MySQLCatalog>();
}

static vector<Value> ExtractParams(TableFunctionBindInput &input) {
	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		auto params_handle_it = input.named_parameters.find("params_handle");
		if (params_handle_it != input.named_parameters.end()) {
			throw BinderException("Either \"params\" or \"params_handle\" option can be specified, not both");
		}
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Query parameters cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT && struct_val.type().id() != LogicalTypeId::TUPLE) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}

	return params;
}

static int64_t ExtractParamsHandle(TableFunctionBindInput &input) {
	auto params_handle_it = input.named_parameters.find("params_handle");
	if (params_handle_it != input.named_parameters.end()) {
		Value &bigint_val = params_handle_it->second;
		if (bigint_val.IsNull()) {
			throw BinderException("Query parameters handle cannot be NULL");
		}
		int64_t params_handle = BigIntValue::Get(bigint_val);
		auto params_ptr = MySQLParameterHandles::Remove(params_handle);
		if (params_ptr.get() == nullptr) {
			throw BinderException("Parameters not found, ID: %lld", params_handle);
		}
		MySQLParameterHandles::Add(std::move(params_ptr));
		return params_handle;
	}
	return 0;
}

static MySQLResultStreamingUser ExtractUserStreaming(TableFunctionBindInput &input) {
	auto user_streaming = MySQLResultStreamingUser::UNINITIALIZED;
	auto streaming_it = input.named_parameters.find("stream_results");
	if (streaming_it != input.named_parameters.end()) {
		Value &bool_val = streaming_it->second;
		if (!bool_val.IsNull()) {
			if (BooleanValue::Get(bool_val)) {
				user_streaming = MySQLResultStreamingUser::REQUIRE_STREAMING;
			} else {
				user_streaming = MySQLResultStreamingUser::FORCE_MATERIALIZATION;
			}
		}
	}
	return user_streaming;
}

static uint64_t ExtractPinnedConnId(TableFunctionBindInput &input) {
	uint64_t pinned_connection_id = 0;
	auto conn_it = input.named_parameters.find("connection");
	if (conn_it != input.named_parameters.end()) {
		Value &conn_val = conn_it->second;
		if (conn_val.IsNull()) {
			throw BinderException("Specified connection must be not null");
		}
		pinned_connection_id = UBigIntValue::Get(conn_val);
	}
	return pinned_connection_id;
}

static bool ExtractFlag(TableFunctionBindInput &input, const string &name, bool default_val) {
	auto it = input.named_parameters.find(Identifier(name));
	if (it != input.named_parameters.end()) {
		Value &bool_val = it->second;
		if (!bool_val.IsNull()) {
			return BooleanValue::Get(bool_val);
		}
	}
	return default_val;
}

static unique_ptr<FunctionData> MySQLQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to mysql_query cannot be NULL");
	}

	string db_name = input.inputs[0].GetValue<string>();
	MySQLCatalog &catalog = GetCatalogByName(context, db_name);
	auto sql = input.inputs[1].GetValue<string>();
	vector<Value> params = ExtractParams(input);
	int64_t params_handle = ExtractParamsHandle(input);
	MySQLResultStreamingUser user_streaming = ExtractUserStreaming(input);
	uint64_t pinned_connection_id = ExtractPinnedConnId(input);

	bool tran_restrict_dml = false;
	try {
		optional_ptr<MySQLConnection> conn_ptr = nullptr;
		MySQLPooledConnection pinned_connection;
		uint64_t prepare_connection_id = 0;
		if (pinned_connection_id > 0) {
			pinned_connection = catalog.GetConnectionPool().UnpinConnection(pinned_connection_id);
			MySQLConnection &conn = pinned_connection.GetConnection();
			conn_ptr = &conn;
			prepare_connection_id = pinned_connection.Id();
		} else {
			MySQLTransaction &transaction = MySQLTransaction::Get(context, catalog);
			MySQLConnection &conn = transaction.GetConnection();
			conn_ptr = &conn;
			prepare_connection_id = transaction.GetConnectionId();
			tran_restrict_dml = transaction.GetAccessMode() == AccessMode::READ_ONLY;
		}

		auto deferred_pin = dbconnector::Defer([&pinned_connection, &catalog] {
			if (!pinned_connection) {
				return;
			}
			try {
				catalog.GetConnectionPool().PinConnection(std::move(pinned_connection));
			} catch (...) {
				// suppress
			}
		});

		MySQLConnection &conn = *conn_ptr;
		if (!ExtractFlag(input, "prepare", true)) {
			if (params.size() > 0 || params_handle != 0) {
				throw BinderException("query parameters cannot be used with 'prepare=FALSE'");
			}
			return_types.emplace_back(LogicalType::BIGINT);
			names.emplace_back("rowcount");
			return make_uniq<MySQLQueryBindData>(catalog, sql, user_streaming, pinned_connection_id);
		}

		unique_ptr<MySQLStatement> stmt = conn.Prepare(sql);
		if (stmt->Fields().size() > 0) {
			for (auto &field : stmt->Fields()) {
				names.push_back(Identifier(field.name));
				return_types.push_back(field.duckdb_type);
			}
		} else if (tran_restrict_dml) {
			// check is bind-time only, while different transaction can be used exec-time - we are not checking whether
			// it is writable
			throw PermissionException(
			    "statements that do not produce result sets cannot be run in a read-only connection");
		} else {
			return_types.emplace_back(LogicalType::BIGINT);
			names.emplace_back("rowcount");
			if (ExtractFlag(input, "suppress_dml_output", false)) {
				input.table_function.call_return_type = StatementReturnType::NOTHING;
			}
		}

		// the remote result can contain duplicate column names (e.g. "SELECT a.id, b.id ...") -
		// rename them as table functions require unique column names
		QueryResult::DeduplicateColumns(names);

		return make_uniq<MySQLQueryBindData>(catalog, sql, std::move(params), params_handle,
		                                     std::move(stmt->FieldsCopy()), user_streaming, std::move(stmt),
		                                     prepare_connection_id, pinned_connection_id);
	} catch (const std::exception &ex) {
		ErrorData error(ex);
		throw BinderException("PREPARE error, query: \"%s\", message: \"%s\"", sql, error.RawMessage());
	}
}

static unique_ptr<GlobalTableFunctionState> MySQLQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->CastNoConst<MySQLQueryBindData>();
	MySQLPooledConnection pinned_connection;
	if (bdata.pinned_connection_id > 0) {
		pinned_connection = bdata.catalog.GetConnectionPool().UnpinConnection(bdata.pinned_connection_id);
	}
	return make_uniq<MySQLGlobalState>(std::move(pinned_connection));
}

static MySQLResultStreaming ResolveStreaming(MySQLQueryBindData &bdata) {
	if (bdata.prepared_stmt && bdata.optimizer_streaming == MySQLResultStreaming::ALLOW_STREAMING &&
	    bdata.user_streaming != MySQLResultStreamingUser::FORCE_MATERIALIZATION) {
		return MySQLResultStreaming::ALLOW_STREAMING;
	}
	if (bdata.optimizer_streaming != MySQLResultStreaming::ALLOW_STREAMING &&
	    bdata.user_streaming == MySQLResultStreamingUser::REQUIRE_STREAMING) {
		throw BinderException(
		    "Query result streaming requested in the 'mysql_query' function parameter cannot be performed due to "
		    "multiple MySQL scans in the query."
		    "If streaming is required, consider using a separate attached catalog for each 'mysql_query' call - "
		    "will run on a separate connection without transcactional guarantees");
	}
	if (!bdata.prepared_stmt && bdata.user_streaming == MySQLResultStreamingUser::REQUIRE_STREAMING) {
		throw BinderException("Query result streaming cannot be used along with 'prepare=FALSE' option");
	}
	return MySQLResultStreaming::FORCE_MATERIALIZATION;
}

static const vector<Value> &ResolveParams(const MySQLQueryBindData &bdata, MySQLGlobalState &gstate) {
	if (bdata.params_handle == 0) {
		// fixed bind-time params, live until the prepared statement is deallocated
		return bdata.params;
	}

	// execute-time params, consumed on execution, must be param-rebind before every execution
	auto params_ptr = MySQLParameterHandles::Remove(bdata.params_handle);
	if (params_ptr.get() == nullptr) {
		throw BinderException("Parameters not found, ID: %lld", bdata.params_handle);
	}
	gstate.params.clear();
	for (Value &par : *params_ptr) {
		gstate.params.emplace_back(std::move(par));
	}
	params_ptr->clear();
	MySQLParameterHandles::Add(std::move(params_ptr));
	return gstate.params;
}

static void SetRowCount(DataChunk &output, int64_t count) {
	Vector &vec = output.data[0];
	D_ASSERT(vec.GetType() == LogicalType::BIGINT);
	int64_t *data = FlatVector::GetDataMutable<int64_t>(vec);
	data[0] = count;
	output.SetChildCardinality(1);
}

static void MySQLQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bdata = data.bind_data->CastNoConst<MySQLQueryBindData>();
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();

	if (gstate.exec_state == MySQLQueryExecState::EXHAUSTED) {
		output.SetChildCardinality(0);
		return;
	}

	if (gstate.exec_state == MySQLQueryExecState::UNINITIALIZED) {
		MySQLResultStreaming result_streaming = ResolveStreaming(bdata);
		optional_ptr<MySQLConnection> conn_ptr = nullptr;
		uint64_t current_connection_id = 0;
		if (gstate.pinned_connection) {
			MySQLConnection &conn = gstate.pinned_connection.GetConnection();
			conn_ptr = &conn;
			current_connection_id = gstate.pinned_connection.Id();
		} else {
			auto &transaction = MySQLTransaction::Get(context, bdata.catalog);
			MySQLConnection &conn = transaction.GetConnection();
			conn_ptr = &conn;
			current_connection_id = transaction.GetConnectionId();
		}

		MySQLConnection &conn = *conn_ptr;
		const vector<Value> &params = ResolveParams(bdata, gstate);

		if (!bdata.prepared_stmt) {
			conn.Execute(bdata.query);
			SetRowCount(output, -1);
			gstate.exec_state = MySQLQueryExecState::EXHAUSTED;
			return;
		}

		if (current_connection_id == bdata.prepare_connection_id) {
			gstate.result = conn.QueryStmt(*bdata.prepared_stmt, params, result_streaming);
		} else if (bdata.params_handle != 0) {
			throw InvalidInputException(
			    "MySQL connection is no longer available to reuse the prepared statement. Use 'params' option instead "
			    "of 'params_handle' if re-preparing of the statement is acceptable.");
		} else {
			gstate.result = conn.Query(bdata.query, params, result_streaming);
		}
		gstate.exec_state = MySQLQueryExecState::EXECUTED;
	}

	if (bdata.fields.size() == 0) {
		SetRowCount(output, gstate.result->AffectedRowsSigned());
		gstate.exec_state = MySQLQueryExecState::EXHAUSTED;
		return;
	}

	MySQLScan(context, data, output);
}

MySQLQueryFunction::MySQLQueryFunction()
    : TableFunction("mysql_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLQueryScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
	named_parameters["params_handle"] = LogicalType::BIGINT;
	named_parameters["stream_results"] = LogicalType::BOOLEAN;
	named_parameters["connection"] = LogicalType::UBIGINT;
	named_parameters["prepare"] = LogicalType::BOOLEAN;
	named_parameters["suppress_dml_output"] = LogicalType::BOOLEAN;
}

MySQLExecuteFunction::MySQLExecuteFunction()
    : TableFunction("mysql_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLQueryScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
	named_parameters["params_handle"] = LogicalType::BIGINT;
	named_parameters["connection"] = LogicalType::UBIGINT;
	named_parameters["prepare"] = LogicalType::BOOLEAN;
}

static void MySQLPinConnection(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat catalog_name_data;
	args.data[0].ToUnifiedFormat(catalog_name_data);
	if (!catalog_name_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified attached database name must be not null");
	}

	auto catalog_name_strt = UnifiedVectorFormat::GetData<string_t>(catalog_name_data)[0];
	string catalog_name(catalog_name_strt.GetData(), catalog_name_strt.GetSize());

	auto &catalog = GetCatalogByName(state.GetContext(), catalog_name);
	auto &pool = catalog.GetConnectionPool();

	auto conn = pool.ForceAcquire();
	uint64_t conn_id = pool.PinConnection(std::move(conn));

	auto result_data = FlatVector::GetDataMutable<uint64_t>(result);
	result_data[0] = conn_id;
}

MySQLPinConnectionFunction::MySQLPinConnectionFunction()
    : ScalarFunction("mysql_pin_connection", {LogicalType::VARCHAR}, LogicalType::UBIGINT, MySQLPinConnection) {
	SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	SetFallible();
	SetVolatile();
}

static void MySQLClosePinnedConnection(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat catalog_name_data;
	args.data[0].ToUnifiedFormat(catalog_name_data);
	if (!catalog_name_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified catalog name must be not null");
	}
	auto catalog_name_strt = UnifiedVectorFormat::GetData<string_t>(catalog_name_data)[0];
	string catalog_name(catalog_name_strt.GetData(), catalog_name_strt.GetSize());

	UnifiedVectorFormat conn_id_data;
	args.data[1].ToUnifiedFormat(conn_id_data);
	if (!conn_id_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified connection ID must be not null");
	}
	auto conn_id = UnifiedVectorFormat::GetData<uint64_t>(conn_id_data)[0];

	auto &catalog = GetCatalogByName(state.GetContext(), catalog_name);
	{ auto conn = catalog.GetConnectionPool().UnpinConnection(conn_id); }

	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.SetInvalid(0);
}

MySQLClosePinnedConnectionFunction::MySQLClosePinnedConnectionFunction()
    : ScalarFunction("mysql_close_pinned_connection", {LogicalType::VARCHAR, LogicalType::UBIGINT},
                     LogicalType::BOOLEAN, MySQLClosePinnedConnection) {
	SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	SetFallible();
	SetVolatile();
}

MySQLQueryBindData::~MySQLQueryBindData() {
	if (params_handle > 0) {
		MySQLParameterHandles::Remove(params_handle);
	}
}

} // namespace duckdb
