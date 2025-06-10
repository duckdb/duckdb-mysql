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
#include "storage/mysql_schema_set.hpp"

namespace duckdb {
class MySQLSchemaEntry;

class MySQLCatalog : public Catalog {
public:
	explicit MySQLCatalog(AttachedDatabase &db_p, string connection_string, string attach_path, AccessMode access_mode);
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

	void ClearCache();

	static void MaterializeMySQLScans(PhysicalOperator &op);
	static bool IsMySQLScan(const string &name);
	static bool IsMySQLQuery(const string &name);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	MySQLSchemaSet schemas;
	string default_schema;
};

} // namespace duckdb
