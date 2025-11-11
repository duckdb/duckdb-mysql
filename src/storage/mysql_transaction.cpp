#include "storage/mysql_transaction.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "mysql_result.hpp"
#include "mysql_types.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {

MySQLTransaction::MySQLTransaction(MySQLCatalog &mysql_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context),
      connection(
          MySQLConnection::Open(MySQLTypeConfig(context), mysql_catalog.connection_string, mysql_catalog.attach_path)),
      access_mode(mysql_catalog.access_mode) {
	string time_zone;
	{
		Value mysql_session_time_zone;
		if (context.TryGetCurrentSetting("mysql_session_time_zone", mysql_session_time_zone)) {
			time_zone = mysql_session_time_zone.ToString();
		}
	}
	if (!time_zone.empty()) {
		connection.Execute("SET TIME_ZONE = '" + time_zone + "'");
	}
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
