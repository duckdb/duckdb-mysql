//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "dbconnector/storage/transaction_manager.hpp"

#include "storage/mysql_catalog.hpp"
#include "storage/mysql_transaction.hpp"

namespace duckdb {

using MySQLTransactionManager = dbconnector::storage::TransactionManager<MySQLCatalog, MySQLTransaction>;

} // namespace duckdb
