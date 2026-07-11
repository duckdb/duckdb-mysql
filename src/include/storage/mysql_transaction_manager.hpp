//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_transaction.hpp"

namespace duckdb {

class MySQLTransactionManager : public TransactionManager {
public:
	MySQLTransactionManager(AttachedDatabase &db_p, MySQLCatalog &mysql_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	MySQLCatalog &mysql_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<MySQLTransaction>> transactions;
};

} // namespace duckdb
