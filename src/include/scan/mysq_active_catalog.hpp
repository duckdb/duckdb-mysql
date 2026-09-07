#pragma once

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"

#include "storage/mysql_catalog.hpp"

namespace duckdb {

class MySQLActiveCatalog {
	shared_ptr<AttachedDatabase> database;
	MySQLCatalog &catalog;

public:
	explicit MySQLActiveCatalog(ClientContext &ctx);
};

} // namespace duckdb
