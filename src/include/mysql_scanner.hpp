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
#include "mysql_connection_pool.hpp"
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

	bool use_predicate_analyzer = false;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("MySQLBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

struct MySQLQueryBindData : public FunctionData {
	MySQLQueryBindData(Catalog &catalog, string query_p, vector<Value> params_p, vector<MySQLField> fields_p)
	    : catalog(catalog), query(std::move(query_p)), params(std::move(params_p)), fields(std::move(fields_p)) {
	}

	Catalog &catalog;
	string query;
	vector<Value> params;
	vector<MySQLField> fields;

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

class MySQLExplainFederatedFunction : public TableFunction {
public:
	MySQLExplainFederatedFunction();
};

class MySQLDebugExecutionPlanFunction : public TableFunction {
public:
	MySQLDebugExecutionPlanFunction();
};

} // namespace duckdb
