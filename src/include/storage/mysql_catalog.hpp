//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "mysql_connection.hpp"
#include "mysql_connection_pool.hpp"
#include "storage/mysql_schema_set.hpp"

namespace duckdb {
class MySQLSchemaEntry;

class MySQLCatalog : public Catalog {
public:
	explicit MySQLCatalog(AttachedDatabase &db_p, string connection_string, string attach_path, AccessMode access_mode,
	                      shared_ptr<MySQLConnectionPool> pool_p);
	~MySQLCatalog();

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
		switch (capability) {
		case RemoteCapability::IS_REMOTE:
		case RemoteCapability::EXECUTE_QUERY_NODE:
		case RemoteCapability::CONNECT:
			return true;
		default:
			return false;
		}
	}
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	bool SupportsPushdown(const ParsedExpression &expression) override;
	bool SupportsPushdown(const TableRef &ref) override;
	bool SupportsPushdown(const QueryNode &node) override;

	void ClearCache();

	static bool IsMySQLScan(const string &name);

	MySQLConnectionPool &GetConnectionPool();
	//! The server version, fetched when the database was attached
	const MySQLVersion &GetVersion() const {
		return version;
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	MySQLSchemaSet schemas;
	string default_schema;
	MySQLVersion version;
	shared_ptr<MySQLConnectionPool> connection_pool;
};

} // namespace duckdb
