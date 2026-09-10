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
#include "storage/mysql_catalog.hpp"

namespace duckdb {

static QualifiedName CreateQualifiedName(const string &catalog, const string &schema, const string &name) {
	QualifiedName qn;
	qn.catalog = catalog;
	qn.schema = schema;
	qn.name = name;
	return qn;
}

struct MySQLBindData : public FunctionData {
	explicit MySQLBindData(MySQLTableEntry &table, weak_ptr<ClientContext> ctx)
	    : table_name(CreateQualifiedName(table.ParentCatalog().GetName(), table.ParentSchema().name, table.name)),
	      columns(table.GetColumns().Copy()), context_ptr(std::move(ctx)) {
	}

	QualifiedName table_name;
	ColumnList columns;

	// required for get_bind_info and only used there
	weak_ptr<ClientContext> context_ptr;

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
	MySQLQueryBindData(Catalog &catalog, string query_p, vector<Value> params_p, MySQLResultStreaming streaming_p,
	                   unique_ptr<MySQLStatement> prepared_stmt_p, uint64_t prepare_connection_id_p)
	    : catalog_name(catalog.GetName()), query(std::move(query_p)), params(std::move(params_p)),
	      streaming(streaming_p), prepared_stmt(std::move(prepared_stmt_p)),
	      prepare_connection_id(prepare_connection_id_p) {
	}

	string catalog_name;
	string query;
	vector<Value> params;
	MySQLResultStreaming streaming = MySQLResultStreaming::UNINITIALIZED;

	unique_ptr<MySQLStatement> prepared_stmt;
	uint64_t prepare_connection_id = 0;

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
