//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include "mysql_connection.hpp"
#include "mysql_statement.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"

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
	string limit;
	MySQLResultStreaming streaming = MySQLResultStreaming::UNINITIALIZED;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("MySQLBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

struct MySQLQueryBindData : public FunctionData {
	MySQLQueryBindData(Catalog &catalog, unique_ptr<MySQLStatement> stmt_p, vector<Value> params_p, string query_p,
	                   MySQLResultStreaming streaming_p)
	    : catalog(catalog), stmt(std::move(stmt_p)), params(std::move(params_p)), query(std::move(query_p)),
	      user_streaming(streaming_p) {
	}

	MySQLQueryBindData(Catalog &catalog, unique_ptr<MySQLResult> result_p, string query_p,
	                   MySQLResultStreaming streaming_p)
	    : catalog(catalog), result(std::move(result_p)), query(std::move(query_p)), user_streaming(streaming_p) {
	}

	Catalog &catalog;
	unique_ptr<MySQLResult> result;
	unique_ptr<MySQLStatement> stmt;
	vector<Value> params;
	string query;
	MySQLResultStreaming user_streaming = MySQLResultStreaming::UNINITIALIZED;
	MySQLResultStreaming streaming = MySQLResultStreaming::UNINITIALIZED;

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

} // namespace duckdb
