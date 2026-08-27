//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
struct DropInfo;
class MySQLSchemaEntry;
class MySQLTransaction;

class MySQLCatalogSet {
public:
	MySQLCatalogSet(Catalog &catalog);

	optional_ptr<CatalogEntry> GetEntry(MySQLTransaction &transaction, const string &name);
	virtual void DropEntry(MySQLTransaction &transaction, DropInfo &info);
	void Scan(MySQLTransaction &transaction, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(MySQLTransaction &transaction, shared_ptr<CatalogEntry> entry);
	void ClearEntries();

protected:
	virtual void LoadEntries(MySQLTransaction &transaction) = 0;

	void TryLoadEntries(MySQLTransaction &transaction);

	void EraseEntryInternal(const string &name);

protected:
	Catalog &catalog;

private:
	// lock order -> clear, load, entry
	mutex entry_lock;
	mutex load_lock;
	mutex clear_lock;
	case_insensitive_map_t<shared_ptr<CatalogEntry>> entries;
	bool is_loaded = false;
};

class MySQLInSchemaSet : public MySQLCatalogSet {
public:
	MySQLInSchemaSet(MySQLSchemaEntry &schema);

	optional_ptr<CatalogEntry> CreateEntry(MySQLTransaction &transaction, shared_ptr<CatalogEntry> entry) override;

protected:
	MySQLSchemaEntry &schema;
};

} // namespace duckdb
