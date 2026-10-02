//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "dbconnector/attached.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/access_mode.hpp"

#include "storage/mysql_capabilities.hpp"
#include "mysql_connection.hpp"
#include "storage/mysql_connection_pool.hpp"
#include "storage/mysql_schema_set.hpp"

namespace duckdb {
class MySQLSchemaEntry;

class MySQLCatalog : public Catalog {
public:
	explicit MySQLCatalog(AttachedDatabase &db_p, string connection_string, string attach_path, AccessMode access_mode,
	                      vector<string> schemas_to_load, shared_ptr<MySQLConnectionPool> pool_p,
	                      bool ddl_pushdown_enabled);
	~MySQLCatalog();

	Identifier catalog_name;
	string connection_string;
	string attach_path;
	AccessMode access_mode;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "mysql";
	}

	static string GetConnectionString(ClientContext &context, const string &attach_path, string secret_name);

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//! Whether or not this is an in-memory MySQL database
	bool InMemory() override;
	string GetDBPath() override;

	bool Supports(RemoteCapability capability) const override {
		return capabilities.Supports(capability);
	}
	bool SupportsPushdown(const ParsedExpression &expression) override {
		return capabilities.SupportsPushdown(expression);
	}
	bool SupportsPushdown(const TableRef &ref) override {
		return capabilities.SupportsPushdown(ref);
	}
	bool SupportsPushdown(const QueryNode &node) override {
		return capabilities.SupportsPushdown(node);
	}
	bool SupportsPushdown(const SQLStatement &statement) override {
		return ddl_pushdown_enabled && capabilities.SupportsPushdown(statement);
	}

	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement);
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	unique_ptr<TableRef> RemoteExecuteInternal(ClientContext &context, vector<string> statements);

	void ClearCache();

	static void MaterializeMySQLScans(PhysicalOperator &op);
	static bool IsMySQLScan(const string &name);
	static bool IsMySQLQuery(const string &name);

	static dbconnector::attached::AttachedCatalog Lookup(ClientContext &ctx, const Identifier &name);

	MySQLConnectionPool &GetConnectionPool();
	shared_ptr<MySQLConnectionPool> GetConnectionPoolPtr();
	//! The server version, fetched when the database was attached
	const MySQLVersion &GetVersion() const {
		return capabilities.GetVersion();
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	MySQLSchemaSet schemas;
	string default_schema;
	MySQLCapabilities capabilities;
	shared_ptr<MySQLConnectionPool> connection_pool;
	bool ddl_pushdown_enabled;
};

} // namespace duckdb
