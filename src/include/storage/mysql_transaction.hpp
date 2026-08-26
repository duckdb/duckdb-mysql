//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_transaction.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"

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

	void Start();
	void Commit();
	void Rollback();

	MySQLConnection &GetConnection();
	uint64_t GetConnectionId();
	static MySQLTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

	ClientContext &GetContext();
	optional_ptr<CatalogEntry> ReferenceEntry(shared_ptr<CatalogEntry> &entry);

private:
	static bool GetTransactionsEnabled(ClientContext &context);
	static MySQLConnectionInitOptions GetConnectionInitOptions(ClientContext &context);

	MySQLCatalog &catalog;
	const bool transactions_enabled = true;
	const AccessMode access_mode = AccessMode::READ_WRITE;
	const MySQLConnectionInitOptions conn_init_opts;

	mutex transaction_lock;
	MySQLTransactionState transaction_state = MySQLTransactionState::TRANSACTION_NOT_YET_STARTED;
	MySQLPooledConnection pooled_connection;

	reference_map_t<CatalogEntry, shared_ptr<CatalogEntry>> referenced_entries;
};

} // namespace duckdb
