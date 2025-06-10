#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "mysql_connection.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

static bool debug_mysql_print_queries = false;

MySQLConnection::MySQLConnection(shared_ptr<OwnedMySQLConnection> connection_p) : connection(std::move(connection_p)) {
}

MySQLConnection::~MySQLConnection() {
	Close();
}

MySQLConnection::MySQLConnection(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
}

MySQLConnection &MySQLConnection::operator=(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	return *this;
}

MySQLConnection MySQLConnection::Open(const string &connection_string) {
	MySQLConnection result;
	result.connection = make_shared_ptr<OwnedMySQLConnection>(MySQLUtils::Connect(connection_string));
	result.dsn = connection_string;
	return result;
}

MYSQL_RES *MySQLConnection::MySQLExecute(const string &query, bool streaming) {
	if (MySQLConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}
	auto con = GetConn();
	lock_guard<mutex> l(query_lock);
	int res = mysql_real_query(con, query.c_str(), query.size());
	if (res != 0) {
		throw IOException("Failed to run query \"%s\": %s\n", query.c_str(), mysql_error(con));
	}
	return streaming ? mysql_use_result(con) : mysql_store_result(con);
}

unique_ptr<MySQLResult> MySQLConnection::QueryInternal(const string &query, MySQLResultStreaming streaming,
                                                       optional_ptr<ClientContext> context) {
	auto con = GetConn();
	bool result_streaming = streaming == MySQLResultStreaming::ALLOW_STREAMING;
	auto result = MySQLExecute(query, result_streaming);
	auto field_count = mysql_field_count(con);
	if (!result) {
		// no result set
		// this can happen in case of a statement like CREATE TABLE, INSERT, etc
		// check if this is the case with mysql_field_count
		if (field_count != 0) {
			// no result but we expected a result
			throw IOException("Failed to fetch result for query \"%s\": %s\n", query.c_str(), mysql_error(con));
		}
		// get the affected rows
		return make_uniq<MySQLResult>(mysql_affected_rows(con));
	} else {
		// result set
		if (!context) {
			return make_uniq<MySQLResult>(result, field_count, result_streaming);
		}
		vector<MySQLField> fields;
		for (idx_t i = 0; i < field_count; i++) {
			auto field = mysql_fetch_field_direct(result, i);
			MySQLField mysql_field;
			if (field->name && field->name_length > 0) {
				mysql_field.name = string(field->name, field->name_length);
			}
			mysql_field.type = MySQLUtils::FieldToLogicalType(*context, field);
			fields.push_back(std::move(mysql_field));
		}

		return make_uniq<MySQLResult>(result, std::move(fields), result_streaming);
	}
}

unique_ptr<MySQLResult> MySQLConnection::Query(const string &query, MySQLResultStreaming streaming) {
	return QueryInternal(query, streaming, nullptr);
}

unique_ptr<MySQLResult> MySQLConnection::Query(const string &query, MySQLResultStreaming streaming,
                                               ClientContext &context) {
	return QueryInternal(query, streaming, context);
}

void MySQLConnection::Execute(const string &query) {
	QueryInternal(query, MySQLResultStreaming::FORCE_MATERIALIZATION, nullptr);
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
