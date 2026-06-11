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

MySQLSchemaSet::MySQLSchemaSet(Catalog &catalog) : MySQLCatalogSet(catalog) {
}

void MySQLSchemaSet::LoadEntries(ClientContext &context) {
	auto query = R"(
SELECT schema_name
FROM information_schema.schemata;
)";

	auto &transaction = MySQLTransaction::Get(context, catalog);
	auto result = transaction.Query(query);
	while (result->Next()) {
		CreateSchemaInfo info;
		info.schema = Identifier(result->GetString(0));
		info.internal = MySQLSchemaIsInternal(info.schema.GetIdentifierName());
		auto schema = make_uniq<MySQLSchemaEntry>(catalog, info);
		CreateEntry(std::move(schema));
	}
}

optional_ptr<CatalogEntry> MySQLSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &transaction = MySQLTransaction::Get(context, catalog);

	string create_sql = "CREATE SCHEMA " + MySQLUtils::WriteIdentifier(info.schema.GetIdentifierName());
	transaction.Query(create_sql);
	auto schema_entry = make_uniq<MySQLSchemaEntry>(catalog, info);
	return CreateEntry(std::move(schema_entry));
}

} // namespace duckdb
