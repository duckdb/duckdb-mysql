//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "mysql_connection.hpp"
#include "mysql_statement.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_connection_pool.hpp"

namespace duckdb {

struct MySQLBindData : public FunctionData {
	explicit MySQLBindData(MySQLTableEntry &table, weak_ptr<ClientContext> ctx)
	    : table_name(table.ParentCatalog().GetName(), table.ParentSchema().name, table.name),
	      columns(table.GetColumns().Copy()), context_ptr(std::move(ctx)) {
	}

	QualifiedName table_name;
	ColumnList columns;

	// required for get_bind_info and only used there
	weak_ptr<ClientContext> context_ptr;

	vector<MySQLType> mysql_types;
	vector<string> names;
	vector<LogicalType> types;
	MySQLResultStreaming optimizer_streaming = MySQLResultStreaming::UNINITIALIZED;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("MySQLBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

struct MySQLQueryBindData : public FunctionData {
	MySQLQueryBindData(MySQLCatalog &catalog, string query_p, vector<Value> params_p, int64_t params_handle_p,
	                   vector<MySQLField> fields_p, MySQLResultStreamingUser user_streaming_p,
	                   unique_ptr<MySQLStatement> prepared_stmt_p, uint64_t prepare_connection_id_p,
	                   uint64_t pinned_connection_id_p)
	    : catalog_name(catalog.GetName()), query(std::move(query_p)), params(std::move(params_p)),
	      params_handle(params_handle_p), fields(std::move(fields_p)), user_streaming(user_streaming_p),
	      prepared_stmt(std::move(prepared_stmt_p)), prepare_connection_id(prepare_connection_id_p),
	      pinned_connection_id(pinned_connection_id_p) {
	}

	MySQLQueryBindData(MySQLCatalog &catalog, string query_p, MySQLResultStreamingUser user_streaming_p,
	                   uint64_t pinned_connection_id_p)
	    : catalog_name(catalog.GetName()), query(std::move(query_p)), user_streaming(user_streaming_p),
	      pinned_connection_id(pinned_connection_id_p) {
	}

	~MySQLQueryBindData();

	Identifier catalog_name;
	string query;
	vector<Value> params;
	int64_t params_handle = 0;
	vector<MySQLField> fields;
	MySQLResultStreamingUser user_streaming = MySQLResultStreamingUser::UNINITIALIZED;
	MySQLResultStreaming optimizer_streaming = MySQLResultStreaming::UNINITIALIZED;

	unique_ptr<MySQLStatement> prepared_stmt;
	uint64_t prepare_connection_id = 0;
	uint64_t pinned_connection_id = 0;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("MySQLBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

class MySQLScanFunction : public TableFunction {
public:
	MySQLScanFunction();
};

class MySQLQueryFunction : public TableFunction {
public:
	MySQLQueryFunction();
};

class MySQLClearCacheFunction : public TableFunction {
public:
	MySQLClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

class MySQLExecuteFunction : public TableFunction {
public:
	MySQLExecuteFunction();
};

class MySQLPinConnectionFunction : public ScalarFunction {
public:
	MySQLPinConnectionFunction();
};

class MySQLClosePinnedConnectionFunction : public ScalarFunction {
public:
	MySQLClosePinnedConnectionFunction();
};

class MySQLCreateParamsFunction : public ScalarFunction {
public:
	MySQLCreateParamsFunction();
};

class MySQLBindParamsFunction : public ScalarFunction {
public:
	MySQLBindParamsFunction();
};

} // namespace duckdb
