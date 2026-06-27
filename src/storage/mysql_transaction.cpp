#include "storage/mysql_transaction.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "mysql_result.hpp"
#include "mysql_types.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {

MySQLTransaction::MySQLTransaction(MySQLCatalog &mysql_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), catalog(mysql_catalog),
      transaction_state(MySQLTransactionState::TRANSACTION_NOT_YET_STARTED), access_mode(mysql_catalog.access_mode) {

	Value mysql_enable_transactions;
	if (context.TryGetCurrentSetting("mysql_enable_transactions", mysql_enable_transactions)) {
		this->transactions_enabled = BooleanValue::Get(mysql_enable_transactions);
	}

	Value mysql_session_time_zone;
	if (context.TryGetCurrentSetting("mysql_session_time_zone", mysql_session_time_zone)) {
		time_zone = mysql_session_time_zone.ToString();
	}

	acquire_mode = MySQLConnectionPool::GetAcquireMode(context);
}

MySQLTransaction::~MySQLTransaction() = default;

void MySQLTransaction::Start() {
	lock_guard<mutex> guard(transaction_state_lock);
	transaction_state = MySQLTransactionState::TRANSACTION_NOT_YET_STARTED;
}

void MySQLTransaction::Commit() {
	if (!transactions_enabled) {
		return;
	}

	lock_guard<mutex> guard(transaction_state_lock);

	if (transaction_state == MySQLTransactionState::TRANSACTION_STARTED) {
		try {
			pooled_connection.GetConnection().Execute("COMMIT");
			transaction_state = MySQLTransactionState::TRANSACTION_FINISHED;
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
}

void MySQLTransaction::Rollback() {
	if (!transactions_enabled) {
		return;
	}

	lock_guard<mutex> guard(transaction_state_lock);

	if (transaction_state == MySQLTransactionState::TRANSACTION_STARTED) {
		try {
			pooled_connection.GetConnection().Execute("ROLLBACK");
			transaction_state = MySQLTransactionState::TRANSACTION_FINISHED;
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
}

void MySQLTransaction::EnsureConnection() {
	lock_guard<mutex> guard(pooled_connection_lock);

	if (pooled_connection) {
		return;
	}

	auto pc = catalog.GetConnectionPool().Acquire(acquire_mode, time_zone);

	auto ctx = context.lock();
	if (ctx) {
		pc.GetConnection().SetTypeConfig(MySQLTypeConfig(*ctx));
	}

	this->pooled_connection = std::move(pc);
}

void MySQLTransaction::StartTransactionInternal() {
	if (!transactions_enabled) {
		return;
	}

	lock_guard<mutex> guard(transaction_state_lock);

	if (transaction_state == MySQLTransactionState::TRANSACTION_NOT_YET_STARTED) {
		string query = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			query += " READ ONLY";
		}
		try {
			pooled_connection.GetConnection().Execute(query);
			transaction_state = MySQLTransactionState::TRANSACTION_STARTED;
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
}

MySQLConnection &MySQLTransaction::GetConnection() {
	EnsureConnection();

	StartTransactionInternal();

	return pooled_connection.GetConnection();
}

unique_ptr<MySQLResult> MySQLTransaction::Query(const string &query) {
	EnsureConnection();

	StartTransactionInternal();

	try {
		return pooled_connection.GetConnection().Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);
	} catch (...) {
		pooled_connection.Invalidate();
		throw;
	}
}

MySQLTransaction &MySQLTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<MySQLTransaction>();
}

} // namespace duckdb
