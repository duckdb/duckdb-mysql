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
#include "mysql_connection_pool.hpp"
#include "mysql_statement.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {
class MySQLTableEntry;
class MySQLTransaction;

struct MySQLBindData : public FunctionData {
	explicit MySQLBindData(MySQLTableEntry &table) : table(table) {
	}

	MySQLTableEntry &table;
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
	MySQLQueryBindData(MySQLCatalog &catalog, string query_p, vector<Value> params_p, vector<MySQLField> fields_p,
	                   MySQLResultStreamingUser user_streaming_p, unique_ptr<MySQLStatement> prepared_stmt_p,
	                   uint64_t prepare_connection_id_p, uint64_t pinned_connection_id_p)
	    : catalog(catalog), query(std::move(query_p)), params(std::move(params_p)), fields(std::move(fields_p)),
	      user_streaming(user_streaming_p), prepared_stmt(std::move(prepared_stmt_p)),
	      prepare_connection_id(prepare_connection_id_p), pinned_connection_id(pinned_connection_id_p) {
	}

	MySQLCatalog &catalog;
	string query;
	vector<Value> params;
	vector<MySQLField> fields;
	MySQLResultStreamingUser user_streaming = MySQLResultStreamingUser::UNINITIALIZED;
	MySQLResultStreaming optimizer_streaming = MySQLResultStreaming::UNINITIALIZED;

	unique_ptr<MySQLStatement> prepared_stmt;
	uint64_t prepare_connection_id;
	uint64_t pinned_connection_id;

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

} // namespace duckdb
