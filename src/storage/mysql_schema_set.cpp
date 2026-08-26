#include "storage/mysql_schema_set.hpp"
#include "storage/mysql_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

static bool MySQLSchemaIsInternal(const string &name) {
	if (name == "information_schema" || name == "performance_schema" || name == "sys") {
		return true;
	}
	return false;
}

MySQLSchemaSet::MySQLSchemaSet(Catalog &catalog, vector<string> schemas_to_load_p)
    : MySQLCatalogSet(catalog), schemas_to_load(std::move(schemas_to_load_p)) {
}

void MySQLSchemaSet::LoadEntries(MySQLTransaction &transaction) {
	string query = R"(
SELECT schema_name
FROM information_schema.schemata
)";

	if (!schemas_to_load.empty()) {
		query += "WHERE schema_name IN (";
		for (idx_t i = 0; i < schemas_to_load.size(); i++) {
			if (i > 0) {
				query += ", ";
			}
			query += MySQLUtils::WriteLiteral(schemas_to_load[i]);
		}
		query += ")";
	}

	auto result = transaction.Query(query);
	while (result->Next()) {
		CreateSchemaInfo info;
		info.schema = result->GetString(0);
		info.internal = MySQLSchemaIsInternal(info.schema);
		auto schema = make_shared_ptr<MySQLSchemaEntry>(catalog, info);
		CreateEntry(transaction, std::move(schema));
	}
}

optional_ptr<CatalogEntry> MySQLSchemaSet::CreateSchema(MySQLTransaction &transaction, CreateSchemaInfo &info) {
	string create_sql = "CREATE SCHEMA " + MySQLUtils::WriteIdentifier(info.schema);
	transaction.Query(create_sql);
	auto schema_entry = make_shared_ptr<MySQLSchemaEntry>(catalog, info);
	return CreateEntry(transaction, std::move(schema_entry));
}

} // namespace duckdb
