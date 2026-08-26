//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/mysql_catalog_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/mysql_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class MySQLSchemaSet : public MySQLCatalogSet {
public:
	explicit MySQLSchemaSet(Catalog &catalog, vector<string> schemas_to_load);

public:
	optional_ptr<CatalogEntry> CreateSchema(MySQLTransaction &transaction, CreateSchemaInfo &info);

protected:
	void LoadEntries(MySQLTransaction &transaction) override;

	vector<string> schemas_to_load;
};

} // namespace duckdb
