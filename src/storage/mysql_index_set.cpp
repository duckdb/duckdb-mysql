#include "storage/mysql_index_set.hpp"
#include "storage/mysql_schema_entry.hpp"
#include "storage/mysql_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/mysql_index_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

MySQLIndexSet::MySQLIndexSet(MySQLSchemaEntry &schema) : MySQLInSchemaSet(schema) {
}

void MySQLIndexSet::DropEntry(ClientContext &context, DropInfo &info) {
	auto entry = GetEntry(context, info.name.GetIdentifierName());
	if (!entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Failed to DROP INDEX \"%s\": entry not found", info.name);
	}
	auto &mysql_index = entry->Cast<MySQLIndexEntry>();
	string drop_query = "DROP INDEX ";
	drop_query += MySQLUtils::WriteIdentifier(info.name.GetIdentifierName());
	drop_query += " ON ";
	drop_query += MySQLUtils::WriteIdentifier(mysql_index.table_name);
	auto &transaction = MySQLTransaction::Get(context, catalog);
	transaction.Query(drop_query);

	EraseEntryInternal(info.name.GetIdentifierName());
}

void MySQLIndexSet::LoadEntries(ClientContext &context) {
	auto query = StringUtil::Replace(R"(
SELECT DISTINCT TABLE_NAME, INDEX_NAME
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = 'mysqlscanner';
)",
	                                 "${SCHEMA_NAME}", MySQLUtils::WriteLiteral(schema.name.GetIdentifierName()));

	auto &transaction = MySQLTransaction::Get(context, catalog);
	auto result = transaction.Query(query);
	while (result->Next()) {
		auto table_name = result->GetString(0);
		auto index_name = result->GetString(1);
		CreateIndexInfo info;
		info.schema = schema.name;
		info.table = Identifier(table_name);
		info.index_name = Identifier(index_name);
		auto index_entry = make_uniq<MySQLIndexEntry>(catalog, schema, info, table_name);
		CreateEntry(std::move(index_entry));
	}
}

} // namespace duckdb
