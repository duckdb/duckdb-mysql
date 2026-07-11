//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_index_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

class MySQLIndexEntry : public IndexCatalogEntry {
public:
	MySQLIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info, string table_name);

	string table_name;

public:
	Identifier GetSchemaName() const override;
	Identifier GetTableName() const override;
};

} // namespace duckdb
