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
	string limit;
	string order_by_clause;
	string aggregate_select_list;
	string group_by_clause;
	string aggregate_where_clause;
	bool has_aggregate_pushdown = false;
	MySQLResultStreaming streaming = MySQLResultStreaming::UNINITIALIZED;

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
	MySQLQueryBindData(string query_p, Catalog &catalog, MySQLPooledConnection pooled_connection_p,
	                   unique_ptr<MySQLStatement> stmt_p, vector<Value> params_p)
	    : query(std::move(query_p)), catalog(catalog), pooled_connection(std::move(pooled_connection_p)),
	      stmt(std::move(stmt_p)), params(std::move(params_p)) {
	}

	MySQLQueryBindData(string query_p, Catalog &catalog, unique_ptr<MySQLResult> result_p)
	    : query(std::move(query_p)), catalog(catalog), pooled_connection(), result(std::move(result_p)) {
	}

	string query;
	Catalog &catalog;
	MySQLPooledConnection pooled_connection;
	unique_ptr<MySQLResult> result;
	unique_ptr<MySQLStatement> stmt;
	vector<Value> params;

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
