#include "scan/mysql_planned_table.hpp"

#include "duckdb/main/database_manager.hpp"

namespace duckdb {

MySQLPlannedTable::MySQLPlannedTable(MySQLTableEntry &table) {
	this->catalog_name = table.ParentCatalog().Cast<MySQLCatalog>().catalog_name;
	this->schema_name = table.schema.name;
	this->name = table.name;
	this->columns = table.GetColumns().Copy();
}

MySQLTableEntry &MySQLPlannedTable::LookupTable(ClientContext &ctx) {
	vector<shared_ptr<AttachedDatabase>> databases = DatabaseManager::Get(ctx).GetDatabases(ctx);
	MySQLCatalog &catalog = MySQLCatalog::Lookup(databases, catalog_name);
	CatalogTransaction catalog_transaction(catalog, ctx);
	MySQLSchemaEntry &schema = LookupSchema(catalog_transaction, catalog);
	return LookupTableInternal(catalog_transaction, schema);
}

MySQLSchemaEntry &MySQLPlannedTable::LookupSchema(CatalogTransaction &catalog_transaction, MySQLCatalog &catalog) {
	QualifiedName qualified_name(schema_name);
	EntryLookupInfo lookup(CatalogType::SCHEMA_ENTRY, std::move(qualified_name));
	optional_ptr<SchemaCatalogEntry> schema =
	    catalog.LookupSchema(catalog_transaction, lookup, OnEntryNotFound::RETURN_NULL);
	if (!schema) {
		throw InvalidInputException(
		    "MySQL schema not found in the specified client session, name: %s, attached database name: %s", schema_name,
		    catalog_name);
	}
	return schema->Cast<MySQLSchemaEntry>();
}

MySQLTableEntry &MySQLPlannedTable::LookupTableInternal(CatalogTransaction &catalog_transaction,
                                                        MySQLSchemaEntry &schema) {
	QualifiedName qualified_name(name);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, std::move(qualified_name));
	optional_ptr<CatalogEntry> table = schema.LookupEntry(catalog_transaction, lookup);
	if (!table) {
		throw InvalidInputException("MySQL table not found in the specified client session, name: %s, schema name: "
		                            "%s, attached database name: %s",
		                            name, schema_name, catalog_name);
	}
	return table->Cast<MySQLTableEntry>();
}

} // namespace duckdb
