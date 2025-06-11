#include "storage/mysql_transaction.hpp"
#include "storage/mysql_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "mysql_result.hpp"

namespace duckdb {

MySQLTransaction::MySQLTransaction(MySQLCatalog &mysql_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(mysql_catalog.access_mode) {
	connection = MySQLConnection::Open(mysql_catalog.connection_string);
}

MySQLTransaction::~MySQLTransaction() = default;

void MySQLTransaction::Start() {
	transaction_state = MySQLTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void MySQLTransaction::Commit() {
	if (transaction_state == MySQLTransactionState::TRANSACTION_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_FINISHED;
		connection.Execute("COMMIT");
	}
}
void MySQLTransaction::Rollback() {
	if (transaction_state == MySQLTransactionState::TRANSACTION_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_FINISHED;
		connection.Execute("ROLLBACK");
	}
}

MySQLConnection &MySQLTransaction::GetConnection() {
	if (transaction_state == MySQLTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_STARTED;
		string query = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			query += " READ ONLY";
		}
		connection.Execute(query);
	}
	return connection;
}

unique_ptr<MySQLResult> MySQLTransaction::Query(const string &query) {
	if (transaction_state == MySQLTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = MySQLTransactionState::TRANSACTION_STARTED;
		string transaction_start = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			transaction_start += " READ ONLY";
		}
		connection.Execute(transaction_start);
		return connection.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);
	}
	return connection.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);
}

MySQLTransaction &MySQLTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<MySQLTransaction>();
}

} // namespace duckdb
