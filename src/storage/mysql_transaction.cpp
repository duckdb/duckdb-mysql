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

	Value mysql_pool_acquire_mode;
	if (context.TryGetCurrentSetting("mysql_pool_acquire_mode", mysql_pool_acquire_mode)) {
		auto mode_str = StringUtil::Lower(mysql_pool_acquire_mode.ToString());
		if (mode_str == "force") {
			acquire_mode = MySQLPoolAcquireMode::FORCE;
		} else if (mode_str == "wait") {
			acquire_mode = MySQLPoolAcquireMode::WAIT;
		} else if (mode_str == "try") {
			acquire_mode = MySQLPoolAcquireMode::TRY;
		}
	}
}

MySQLTransaction::~MySQLTransaction() = default;

void MySQLTransaction::Start() {
	transaction_state = MySQLTransactionState::TRANSACTION_NOT_YET_STARTED;
}

void MySQLTransaction::Commit() {
	if (transactions_enabled && transaction_state == MySQLTransactionState::TRANSACTION_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_FINISHED;
		try {
			pooled_connection.GetConnection().Execute("COMMIT");
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
}

void MySQLTransaction::Rollback() {
	if (transactions_enabled && transaction_state == MySQLTransactionState::TRANSACTION_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_FINISHED;
		try {
			pooled_connection.GetConnection().Execute("ROLLBACK");
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
}

void MySQLTransaction::EnsureConnection() {
	if (pooled_connection) {
		return;
	}
	auto &pool = catalog.GetConnectionPool();
	switch (acquire_mode) {
	case MySQLPoolAcquireMode::FORCE:
		pooled_connection = pool.ForceAcquire();
		break;
	case MySQLPoolAcquireMode::WAIT:
		pooled_connection = pool.WaitAcquire();
		break;
	case MySQLPoolAcquireMode::TRY:
		pooled_connection = pool.TryAcquire();
		if (!pooled_connection) {
			throw IOException("Connection pool exhausted: no connections available (try mode)");
		}
		break;
	}

	if (!time_zone.empty()) {
		try {
			pooled_connection.GetConnection().Execute("SET TIME_ZONE = ?", {Value(time_zone)});
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
}

MySQLConnection &MySQLTransaction::GetConnection() {
	EnsureConnection();

	auto ctx = context.lock();
	if (ctx) {
		pooled_connection.GetConnection().SetTypeConfig(MySQLTypeConfig(*ctx));
	}

	if (transactions_enabled && transaction_state == MySQLTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_STARTED;
		string query = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			query += " READ ONLY";
		}
		try {
			pooled_connection.GetConnection().Execute(query);
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
	return pooled_connection.GetConnection();
}

unique_ptr<MySQLResult> MySQLTransaction::Query(const string &query) {
	EnsureConnection();

	if (transactions_enabled && transaction_state == MySQLTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_STARTED;
		string transaction_start = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			transaction_start += " READ ONLY";
		}
		try {
			pooled_connection.GetConnection().Execute(transaction_start);
			return pooled_connection.GetConnection().Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);
		} catch (...) {
			pooled_connection.Invalidate();
			throw;
		}
	}
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
