#include "mysql_connection.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "mysql_parameter.hpp"
#include "mysql_types.hpp"

namespace duckdb {

static bool debug_mysql_print_queries = false;

MySQLConnection::MySQLConnection(shared_ptr<OwnedMySQLConnection> connection_p, MySQLTypeConfig type_config_p,
                                 const string &connection_string_p)
    : connection(std::move(connection_p)), type_config(std::move(type_config_p)),
      connection_string(connection_string_p) {
}

MySQLConnection::~MySQLConnection() {
	Close();
}

MySQLConnection::MySQLConnection(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(type_config, other.type_config);
	std::swap(connection_string, other.connection_string);
}

MySQLConnection &MySQLConnection::operator=(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(type_config, other.type_config);
	std::swap(connection_string, other.connection_string);
	return *this;
}

MySQLConnection MySQLConnection::Open(MySQLTypeConfig type_config, const string &connection_string,
                                      const string &attach_path) {
	auto connection = make_shared_ptr<OwnedMySQLConnection>(MySQLUtils::Connect(connection_string, attach_path));
	return MySQLConnection(std::move(connection), std::move(type_config), connection_string);
}

// Reads and discards the result of a query executed with the basic interface (if the query produced a result),
// the result is fetched unbuffered, so the connection can be used for other queries.
// must be called with the query_lock held
void MySQLConnection::DrainBasicResult(optional_ptr<ClientContext> context, const string &query) {
	auto con = GetConn();
	auto result = MySQLResultPtr(mysql_use_result(con), MySQLResultDelete);
	if (result) {
		try {
			// rows are not read into chunks here, interrupts are checked once per the chunk size of rows
			idx_t row_count = 0;
			while (mysql_fetch_row(result.get())) {
				row_count++;
				if (context && row_count % STANDARD_VECTOR_SIZE == 0) {
					context->InterruptCheck();
				}
			}
		} catch (...) {
			// remaining rows are discarded when the result is freed, cancel the query to not read all of them
			if (context) {
				TryCancelQuery(*context, type_config, connection_string, connection->GetID());
			}
			throw;
		}
	}
	if (mysql_errno(con) != 0) {
		throw IOException("Failed to read result of query \"%s\": %s\n", query.c_str(), mysql_error(con));
	}
}

// must be called with the query_lock held
idx_t MySQLConnection::MySQLExecute(optional_ptr<ClientContext> context, MYSQL_STMT *stmt, const string &query,
                                    const vector<Value> &params, bool streaming, bool prepared) {
	if (MySQLConnection::DebugPrintQueries()) {
		string msg = StringUtil::Format("%s /* streaming=%s */\n", query, streaming ? "TRUE" : "FALSE");
		Printer::Print(msg);
	}

	if (!stmt) { // basic interface
		auto con = GetConn();
		int res_query = mysql_real_query(con, query.c_str(), query.size());
		if (res_query != 0) {
			throw IOException("Failed to run query \"%s\": %s\n", query.c_str(), mysql_error(con));
		}
		DrainBasicResult(context, query);
		return 0;
	}

	// statement interface, may or may not be already prepared

	if (!prepared) {
		int res_prepare = mysql_stmt_prepare(stmt, query.c_str(), query.size());
		if (res_prepare != 0) {
			throw IOException("Failed to prepare MySQL query \"%s\": %s\n", query.c_str(), mysql_stmt_error(stmt));
		}
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
		for (const Value &dp : params) {
			mysql_params.emplace_back(query, dp);
			binds.push_back(mysql_params.back().CreateBind());
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

	// the result is never stored by the client library, it is either streamed to the caller
	// or read into a buffer-managed collection by MySQLResult::Materialize
	return mysql_stmt_affected_rows(stmt);
}

// Reads and discards all remaining rows of a streamed result, so the connection can be used for other queries
static void DrainResult(MySQLResult &result) {
	while (!result.Exhausted()) {
		result.NextChunk();
	}
}

unique_ptr<MySQLResult> MySQLConnection::QueryInternal(optional_ptr<ClientContext> context, const string &query,
                                                       const vector<Value> &params, MySQLResultStreaming streaming,
                                                       MySQLConnectorInterface con_interface) {
	auto con = GetConn();
	bool result_streaming = streaming == MySQLResultStreaming::ALLOW_STREAMING;
	bool basic_interface = con_interface == MySQLConnectorInterface::BASIC;

	lock_guard<mutex> guard(query_lock);

	if (basic_interface) {
		MySQLExecute(context, nullptr, query, params, result_streaming);
		return unique_ptr<MySQLResult>(nullptr);
	}

	// results that are not streamed are read eagerly into a buffer-managed ColumnDataCollection
	// that is subject to the DuckDB memory limit and can be spilled to disk
	if (!result_streaming && !context) {
		throw InternalException("MySQL query result cannot be materialized without a client context, query \"%s\"",
		                        query);
	}

	auto stmt = MySQLStatementPtr(mysql_stmt_init(con), MySQLStatementDelete);
	if (!stmt) {
		throw IOException("Failed to initialize MySQL query \"%s\": %s\n", query.c_str(), mysql_error(con));
	}

	idx_t affected_rows = MySQLExecute(context, stmt.get(), query, params, result_streaming);
	unsigned long connection_id = connection->GetID();
	auto result = make_uniq<MySQLResult>(context, query, std::move(stmt), type_config, connection_string, connection_id,
	                                     streaming, affected_rows);
	if (!result_streaming) {
		result->Materialize();
	}
	return result;
}

unique_ptr<MySQLResult> MySQLConnection::Query(ClientContext &context, const string &query,
                                               MySQLResultStreaming streaming) {
	return QueryInternal(context, query, vector<Value>(), streaming, MySQLConnectorInterface::PREPARED_STATEMENT);
}

unique_ptr<MySQLResult> MySQLConnection::Query(ClientContext &context, const string &query, const vector<Value> &params,
                                               MySQLResultStreaming streaming) {
	return QueryInternal(context, query, params, streaming, MySQLConnectorInterface::PREPARED_STATEMENT);
}

unique_ptr<MySQLResult> MySQLConnection::QueryStmt(ClientContext &context, MySQLStatement &stmt,
                                                   const vector<Value> &params, MySQLResultStreaming streaming) {
	lock_guard<mutex> guard(query_lock);

	bool result_streaming = streaming == MySQLResultStreaming::ALLOW_STREAMING;
	bool prepared = true;
	idx_t affected_rows = MySQLExecute(context, stmt.get(), stmt.Query(), params, result_streaming, prepared);
	MYSQL_STMT *stmt_ptr_bare = stmt.get();
	// owning pointer here only because it is required by result interface, stmt won't be destroyed
	MySQLStatementPtr stmt_ptr(stmt_ptr_bare, MySQLStatementDelete);
	unsigned long connection_id = connection->GetID();
	auto result = make_uniq<MySQLResult>(context, stmt.Query(), std::move(stmt_ptr), type_config, connection_string,
	                                     connection_id, streaming, affected_rows, stmt.FieldsCopy(),
	                                     MySQLResultStatementLifetime::BORROWED);
	if (!result_streaming) {
		result->Materialize();
	}
	return result;
}

unique_ptr<MySQLStatement> MySQLConnection::Prepare(const string &query) {
	lock_guard<mutex> guard(query_lock);
	auto con = GetConn();

	auto stmt = MySQLStatementPtr(mysql_stmt_init(con), MySQLStatementDelete);
	if (!stmt) {
		throw IOException("Failed to initialize MySQL query \"%s\": %s\n", query.c_str(), mysql_error(con));
	}

	int res_prepare = mysql_stmt_prepare(stmt.get(), query.c_str(), query.size());
	if (res_prepare != 0) {
		throw IOException("Failed to prepare MySQL query \"%s\": %s\n", query.c_str(), mysql_stmt_error(stmt.get()));
	}

	vector<MySQLField> fields = MySQLField::ReadFields(query, stmt.get(), type_config);

	return make_uniq<MySQLStatement>(query, stmt.release(), std::move(fields));
}

void MySQLConnection::Initialize(ClientContext &context, const MySQLConnectionInitOptions &options) {
	SetTypeConfig(options.type_config);

	vector<Value> params;
	params.emplace_back(Value(options.time_zone));
	if (!options.time_zone.empty()) {
		Execute(context, "SET TIME_ZONE = ?", params);
	}
}

void MySQLConnection::Execute(ClientContext &context, const string &query) {
	Execute(context, query, vector<Value>());
}

void MySQLConnection::Execute(ClientContext &context, const string &query, const vector<Value> &params) {
	if (params.empty()) {
		QueryInternal(context, query, params, MySQLResultStreaming::ALLOW_STREAMING, MySQLConnectorInterface::BASIC);
		return;
	}
	// the result is fetched unbuffered and drained immediately
	auto result = QueryInternal(context, query, params, MySQLResultStreaming::ALLOW_STREAMING,
	                            MySQLConnectorInterface::PREPARED_STATEMENT);
	DrainResult(*result);
}

bool MySQLConnection::IsOpen() {
	return connection.get() != nullptr;
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

bool MySQLConnection::TryCancelQuery(ClientContext &context, const MySQLTypeConfig &type_config,
                                     const string &connection_string, unsigned long connection_id) {
	if (connection_string.empty()) {
		return false;
	}
	try {
		auto con = MySQLConnection::Open(type_config, connection_string, "");
		string kill_query = "KILL QUERY " + to_string(connection_id);
		con.Execute(context, kill_query);
		return true;
	} catch (std::exception &) {
		return false;
	}
}

bool MySQLConnection::IsConnectionHealthy(const string &health_check_query) {
	if (!IsOpen()) {
		return false;
	}

	if (!health_check_query.empty()) {
		try {
			// called by the connection pool, no context is available, the result is fetched unbuffered and
			// drained immediately, so the connection can be used for other queries
			auto result =
			    QueryInternal(nullptr, health_check_query, vector<Value>(), MySQLResultStreaming::ALLOW_STREAMING,
			                  MySQLConnectorInterface::PREPARED_STATEMENT);
			DrainResult(*result);
			return true;
		} catch (...) {
			return false;
		}
	}

	lock_guard<mutex> guard(query_lock);

	MYSQL *mysql_conn = GetConn();
	if (mysql_ping(mysql_conn) != 0) {
		unsigned int err = mysql_errno(mysql_conn);
		(void)err;
		return false;
	}
	return true;
}

void MySQLConnection::Reset() {
	lock_guard<mutex> guard(query_lock);

	MYSQL *mysql_conn = GetConn();

#if defined(MARIADB_VERSION_ID) || (defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 50703)
	if (mysql_reset_connection(mysql_conn) != 0) {
		throw IOException("Failed to reset MySQL connection: %s", mysql_error(mysql_conn));
	}
#else
	if (mysql_change_user(mysql_conn, nullptr, nullptr, nullptr) != 0) {
		throw IOException("Failed to reset MySQL connection: %s", mysql_error(mysql_conn));
	}
#endif

	if (mysql_query(mysql_conn, "SET autocommit=1") != 0) {
		throw IOException("Failed to set autocommit after connection reset: %s", mysql_error(mysql_conn));
	}

	if (mysql_set_character_set(mysql_conn, "utf8mb4") != 0) {
		throw IOException("Failed to set character set after connection reset: %s", mysql_error(mysql_conn));
	}

	const char *charset = mysql_character_set_name(mysql_conn);
	if (strcmp(charset, "utf8mb4") != 0) {
		throw IOException("Character set verification failed: expected utf8mb4, got %s", charset);
	}
}

string MySQLConnection::GetServerInfo() {
	lock_guard<mutex> guard(query_lock);
	auto conn = GetConn();
	const char *server_info = mysql_get_server_info(conn);
	return server_info != nullptr ? string(server_info) : string();
}

} // namespace duckdb
