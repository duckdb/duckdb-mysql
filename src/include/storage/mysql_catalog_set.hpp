//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
struct DropInfo;
class MySQLSchemaEntry;
class MySQLTransaction;

class MySQLCatalogSet {
public:
	MySQLCatalogSet(Catalog &catalog);

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	virtual void DropEntry(ClientContext &context, DropInfo &info);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
	void ClearEntries();

protected:
	virtual void LoadEntries(ClientContext &context) = 0;

	void TryLoadEntries(ClientContext &context);

	void EraseEntryInternal(const string &name);

protected:
	Catalog &catalog;

private:
	mutex entry_lock;
	mutex load_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	atomic<bool> is_loaded;
};

class MySQLInSchemaSet : public MySQLCatalogSet {
public:
	MySQLInSchemaSet(MySQLSchemaEntry &schema);

	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry) override;

protected:
	MySQLSchemaEntry &schema;
};

} // namespace duckdb
