#include "storage/mysql_catalog_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/mysql_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/mysql_schema_entry.hpp"

namespace duckdb {

MySQLCatalogSet::MySQLCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> MySQLCatalogSet::GetEntry(MySQLTransaction &transaction, const string &name) {
	TryLoadEntries(transaction);
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return transaction.ReferenceEntry(entry->second);
}

void MySQLCatalogSet::TryLoadEntries(MySQLTransaction &transaction) {
	lock_guard<mutex> l(load_lock);
	if (is_loaded) {
		return;
	}
	is_loaded = true;
	LoadEntries(transaction);
}

void MySQLCatalogSet::DropEntry(MySQLTransaction &transaction, DropInfo &info) {
	string drop_query = "DROP ";
	drop_query += CatalogTypeToString(info.type) + " ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		drop_query += " IF EXISTS ";
	}
	drop_query += MySQLUtils::WriteIdentifier(info.GetQualifiedName().Name().GetIdentifierName());
	if (info.type != CatalogType::SCHEMA_ENTRY) {
		if (info.cascade) {
			drop_query += " CASCADE";
		}
	}
	transaction.GetConnection().Execute(drop_query);

	// erase the entry from the catalog set
	EraseEntryInternal(info.GetQualifiedName().Name().GetIdentifierName());
}

void MySQLCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void MySQLCatalogSet::Scan(MySQLTransaction &transaction, const std::function<void(CatalogEntry &)> &callback) {
	TryLoadEntries(transaction);
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> MySQLCatalogSet::CreateEntry(MySQLTransaction &transaction, shared_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = transaction.ReferenceEntry(entry);
	if (result->name.empty()) {
		throw InternalException("MySQLCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void MySQLCatalogSet::ClearEntries() {
	lock_guard<mutex> l(entry_lock);
	entries.clear();
	is_loaded = false;
}

MySQLInSchemaSet::MySQLInSchemaSet(MySQLSchemaEntry &schema) : MySQLCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> MySQLInSchemaSet::CreateEntry(MySQLTransaction &transaction,
                                                         shared_ptr<CatalogEntry> entry) {
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return MySQLCatalogSet::CreateEntry(transaction, std::move(entry));
}

} // namespace duckdb
