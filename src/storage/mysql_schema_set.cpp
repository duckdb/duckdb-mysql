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

void MySQLSchemaSet::LoadEntries(ClientContext &context) {
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

	auto &transaction = MySQLTransaction::Get(context, catalog);
	auto result = transaction.Query(query);
	while (result->Next()) {
		CreateSchemaInfo info;
		info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), Identifier(result->GetString(0)),
		                                    info.GetQualifiedName().Name()));
		info.internal = MySQLSchemaIsInternal(info.GetQualifiedName().Schema().GetIdentifierName());
		auto schema = make_uniq<MySQLSchemaEntry>(catalog, info);
		CreateEntry(std::move(schema));
	}
}

optional_ptr<CatalogEntry> MySQLSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &transaction = MySQLTransaction::Get(context, catalog);

	string create_sql =
	    "CREATE SCHEMA " + MySQLUtils::WriteIdentifier(info.GetQualifiedName().Schema().GetIdentifierName());
	transaction.Query(create_sql);
	auto schema_entry = make_uniq<MySQLSchemaEntry>(catalog, info);
	return CreateEntry(std::move(schema_entry));
}

} // namespace duckdb
