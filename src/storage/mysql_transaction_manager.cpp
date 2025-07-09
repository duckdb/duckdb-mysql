#include "storage/mysql_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

MySQLTransactionManager::MySQLTransactionManager(AttachedDatabase &db_p, MySQLCatalog &mysql_catalog)
    : TransactionManager(db_p), mysql_catalog(mysql_catalog) {
}

Transaction &MySQLTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<MySQLTransaction>(mysql_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData MySQLTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &mysql_transaction = transaction.Cast<MySQLTransaction>();
	mysql_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void MySQLTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &mysql_transaction = transaction.Cast<MySQLTransaction>();
	try {
		mysql_transaction.Rollback();
	} catch (...) {
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void MySQLTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = MySQLTransaction::Get(context, db.GetCatalog());
	auto &db = transaction.GetConnection();
	db.Execute("CHECKPOINT");
}

} // namespace duckdb
