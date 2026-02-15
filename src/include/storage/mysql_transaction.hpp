//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_transaction.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "mysql_connection.hpp"
#include "mysql_connection_pool.hpp"

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
	unique_ptr<MySQLResult> Query(const string &query);
	static MySQLTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	void EnsureConnection();

	MySQLCatalog &catalog;
	PooledConnection pooled_connection;
	bool transactions_enabled = true;
	MySQLTransactionState transaction_state = MySQLTransactionState::TRANSACTION_NOT_YET_STARTED;
	AccessMode access_mode;
	string time_zone;
};

} // namespace duckdb
