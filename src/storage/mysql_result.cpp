#include "mysql_result.hpp"
#include "mysql_connection.hpp"
namespace duckdb {

MySQLResult::MySQLResult(MYSQL_RES *res_p, idx_t field_count, bool streaming_p, MySQLConnection &con)
    : res(res_p), field_count(field_count), streaming(streaming_p) {
	if (streaming) {
		dsn = con.GetDSN();
		connection = con.GetConnection();
	}
}
MySQLResult::MySQLResult(MYSQL_RES *res_p, vector<MySQLField> fields_p, bool streaming_p, MySQLConnection &con)
    : res(res_p), field_count(fields_p.size()), fields(std::move(fields_p)), streaming(streaming_p) {
	if (streaming) {
		dsn = con.GetDSN();
		connection = con.GetConnection();
	}
}
MySQLResult::MySQLResult(idx_t affected_rows) : affected_rows(affected_rows) {
}

MySQLResult::~MySQLResult() {
	if (res) {
		if (streaming) {
			// need to exhaust result if we are streaming
			if (mysql_fetch_row(res) != NULL) {
				// there's still results left - try to kill the query explicitly
				if (!TryCancelQuery()) {
					// failed to cancel: fetch the remainder of the rows
					while (mysql_fetch_row(res) != NULL)
						;
				}
			}
		}
		mysql_free_result(res);
		res = nullptr;
	}
}

bool MySQLResult::TryCancelQuery() {
	if (!connection) {
		return false;
	}
	try {
		// get the connection id to kill
		auto connection_id = mysql_thread_id(connection->connection);

		// open a new connection
		auto con = MySQLConnection::Open(dsn);

		// execute KILL QUERY [connection_id] to kill the running query
		string kill_query = "KILL " + to_string(connection_id);
		con.Execute(kill_query);

		// swap connections
		std::swap(con.GetConnection()->connection, connection->connection);

		// we cancelled the query and replaced the connection with a new one - we're
		// done
		return true;
	} catch (...) {
		return false;
	}
}

} // namespace duckdb
