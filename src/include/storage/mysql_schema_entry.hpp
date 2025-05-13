//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/mysql_table_set.hpp"
#include "storage/mysql_index_set.hpp"

namespace duckdb {
class MySQLTransaction;

class MySQLSchemaEntry : public SchemaCatalogEntry {
public:
	MySQLSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

private:
	void AlterTable(MySQLTransaction &transaction, RenameTableInfo &info);
	void AlterTable(MySQLTransaction &transaction, RenameColumnInfo &info);
	void AlterTable(MySQLTransaction &transaction, AddColumnInfo &info);
	void AlterTable(MySQLTransaction &transaction, RemoveColumnInfo &info);

	void TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name);

	MySQLCatalogSet &GetCatalogSet(CatalogType type);

private:
	MySQLTableSet tables;
	MySQLIndexSet indexes;
};

} // namespace duckdb
