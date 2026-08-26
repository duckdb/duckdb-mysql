//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_index_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/mysql_catalog_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/mysql_index_entry.hpp"

namespace duckdb {
class MySQLSchemaEntry;

class MySQLIndexSet : public MySQLInSchemaSet {
public:
	MySQLIndexSet(MySQLSchemaEntry &schema);

	void DropEntry(MySQLTransaction &transaction, DropInfo &info) override;

protected:
	void LoadEntries(MySQLTransaction &transaction) override;
};

} // namespace duckdb
