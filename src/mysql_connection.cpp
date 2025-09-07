#include "mysql_connection.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "mysql_parameter.hpp"
#include "mysql_types.hpp"

namespace duckdb {

static bool debug_mysql_print_queries = false;

MySQLConnection::MySQLConnection(shared_ptr<OwnedMySQLConnection> connection_p, const std::string &dsn_p,
                                 MySQLTypeConfig type_config_p)
    : connection(std::move(connection_p)), dsn(std::move(dsn_p)), type_config(std::move(type_config_p)) {
}

MySQLConnection::~MySQLConnection() {
	Close();
}

MySQLConnection::MySQLConnection(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	std::swap(type_config, other.type_config);
}

MySQLConnection &MySQLConnection::operator=(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	std::swap(type_config, other.type_config);
	return *this;
}

MySQLConnection MySQLConnection::Open(MySQLTypeConfig type_config, const string &connection_string) {
	auto connection = make_shared_ptr<OwnedMySQLConnection>(MySQLUtils::Connect(connection_string));
	return MySQLConnection(std::move(connection), connection_string, std::move(type_config));
}

idx_t MySQLConnection::MySQLExecute(MYSQL_STMT *stmt, const string &query, vector<Value> params, bool streaming) {
	if (MySQLConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	lock_guard<mutex> l(query_lock);

	if (!stmt) { // basic interface
		auto con = GetConn();
		int res_query = mysql_real_query(con, query.c_str(), query.size());
		if (res_query != 0) {
			throw IOException("Failed to run query \"%s\": %s\n", query.c_str(), mysql_error(con));
		}
		auto result = MySQLResultPtr(mysql_store_result(con), MySQLResultDelete);
		return 0;
	}

	// prepared statement interface

	int res_prepare = mysql_stmt_prepare(stmt, query.c_str(), query.size());
	if (res_prepare != 0) {
		throw IOException("Failed to prepare MySQL query \"%s\": %s\n", query.c_str(), mysql_stmt_error(stmt));
	}

	vector<MySQLParameter> mysql_params;
	vector<MYSQL_BIND> binds;
	if (params.size() > 0) {
		size_t expected_count = mysql_stmt_param_count(stmt);
		if (expected_count != params.size()) {
			throw IOException(
			    "Incorrect query parameters count specified, expected: %zu, actual: %zu, MySQL query \"%s\": %s\n",
			    expected_count, params.size(), query.c_str(), mysql_stmt_error(stmt));
		}
		mysql_params.reserve(params.size());
		binds.reserve(params.size());
		for (Value &dp : params) {
			MySQLParameter mp(query, std::move(dp));
			mysql_params.emplace_back(std::move(mp));
			MySQLParameter &mp_ref = mysql_params.back();
			binds.push_back(mp_ref.CreateBind());
		}
		auto res_bind = mysql_stmt_bind_param(stmt, binds.data());
		if (res_bind != 0) {
			throw IOException("Failed to bind parameters, count: %zu, MySQL query \"%s\": %s\n", binds.size(),
			                  query.c_str(), mysql_stmt_error(stmt));
		}
	}

	int res_exec = mysql_stmt_execute(stmt);
	if (res_exec != 0) {
		throw IOException("Failed to execute MySQL query \"%s\": %s\n", query.c_str(), mysql_stmt_error(stmt));
	}

	idx_t affected_rows = mysql_stmt_affected_rows(stmt);

	if (!streaming && affected_rows == static_cast<idx_t>(-1)) {
		bool btrue = true;
		auto res_attr = mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &btrue);
		if (res_attr != 0) {
			throw IOException("Failed to set STMT_ATTR_UPDATE_MAX_LENGTH for MySQL query \"%s\": %s\n", query.c_str(),
			                  mysql_stmt_error(stmt));
		}

		int res_store = mysql_stmt_store_result(stmt);
		if (res_store != 0) {
			throw IOException("Failed to store result for MySQL query \"%s\": %s\n", query.c_str(),
			                  mysql_stmt_error(stmt));
		}
	}

	return affected_rows;
}

unique_ptr<MySQLResult> MySQLConnection::QueryInternal(const string &query, vector<Value> params,
                                                       MySQLResultStreaming streaming,
                                                       MySQLConnectorInterface con_interface) {
	auto con = GetConn();
	bool result_streaming = streaming == MySQLResultStreaming::ALLOW_STREAMING;
	bool basic_interface = con_interface == MySQLConnectorInterface::BASIC;

	if (basic_interface) {
		MySQLExecute(nullptr, query, params, result_streaming);
		return unique_ptr<MySQLResult>(nullptr);
	}

	auto stmt = MySQLStatementPtr(mysql_stmt_init(con), MySQLStatementDelete);
	if (!stmt) {
		throw IOException("Failed to initialize MySQL query \"%s\": %s\n", query.c_str(), mysql_error(con));
	}
	idx_t affected_rows = MySQLExecute(stmt.get(), query, params, result_streaming);
	return make_uniq<MySQLResult>(query, std::move(stmt), type_config, affected_rows);
}

unique_ptr<MySQLResult> MySQLConnection::Query(const string &query, MySQLResultStreaming streaming) {
	return Query(query, vector<Value>(), streaming);
}

unique_ptr<MySQLResult> MySQLConnection::Query(const string &query, vector<Value> params,
                                               MySQLResultStreaming streaming) {
	return QueryInternal(query, params, streaming, MySQLConnectorInterface::PREPARED_STATEMENT);
}

void MySQLConnection::Execute(const string &query) {
	Execute(query, vector<Value>());
}

void MySQLConnection::Execute(const string &query, vector<Value> params) {
	MySQLConnectorInterface con_interface =
	    params.size() > 0 ? MySQLConnectorInterface::PREPARED_STATEMENT : MySQLConnectorInterface::BASIC;
	QueryInternal(query, std::move(params), MySQLResultStreaming::FORCE_MATERIALIZATION, con_interface);
}

bool MySQLConnection::IsOpen() {
	return connection.get();
}

void MySQLConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	connection = nullptr;
}

vector<IndexInfo> MySQLConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void MySQLConnection::DebugSetPrintQueries(bool print) {
	debug_mysql_print_queries = print;
}

bool MySQLConnection::DebugPrintQueries() {
	return debug_mysql_print_queries;
}

} // namespace duckdb
