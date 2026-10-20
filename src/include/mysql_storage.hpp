//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class MySQLStorageExtension : public StorageExtension {
public:
	MySQLStorageExtension();
};

} // namespace duckdb
