//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_transaction.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "mysql_connection.hpp"
#include "storage/mysql_connection_pool.hpp"

namespace duckdb {
class MySQLCatalog;
class MySQLSchemaEntry;
class MySQLTableEntry;

enum class MySQLTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class MySQLTransaction : public Transaction {
public:
	MySQLTransaction(MySQLCatalog &mysql_catalog, TransactionManager &manager, ClientContext &context);
	~MySQLTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	MySQLConnection &GetConnection();
	uint64_t GetConnectionId();
	//! Whether a transaction has been started on the remote server - if so, queries must be
	//! executed through the transaction's connection so they see uncommitted changes
	bool HasStartedTransaction() const {
		return transactions_enabled && transaction_state == MySQLTransactionState::TRANSACTION_STARTED;
	}
	unique_ptr<MySQLResult> Query(const string &query);
	static MySQLTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	void EnsureConnection();
	void StartTransactionInternal();

	MySQLCatalog &catalog;
	MySQLPooledConnection pooled_connection;
	mutex pooled_connection_lock;
	bool transactions_enabled = true;
	MySQLTransactionState transaction_state = MySQLTransactionState::TRANSACTION_NOT_YET_STARTED;
	mutex transaction_state_lock;
	AccessMode access_mode;
	string time_zone;
};

} // namespace duckdb
