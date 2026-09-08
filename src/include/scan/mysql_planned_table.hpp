#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"

#include "storage/mysql_catalog.hpp"
#include "storage/mysql_schema_entry.hpp"
#include "storage/mysql_table_entry.hpp"

namespace duckdb {

class MySQLPlannedTable {
public:
	Identifier catalog_name;
	Identifier schema_name;
	Identifier name;
	ColumnList columns;

	explicit MySQLPlannedTable(MySQLTableEntry &table);

	MySQLTableEntry &LookupTable(ClientContext &ctx);

private:
	MySQLSchemaEntry &LookupSchema(CatalogTransaction &catalog_transaction, MySQLCatalog &catalog);

	MySQLTableEntry &LookupTableInternal(CatalogTransaction &catalog_transaction, MySQLSchemaEntry &schema);
};

} // namespace duckdb
