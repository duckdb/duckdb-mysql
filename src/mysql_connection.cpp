#include "mysql_connection.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

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

unique_ptr<MySQLResult> MySQLConnection::QueryInternal(const string &query, MySQLResultStreaming streaming) {
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
		vector<MySQLField> fields;
		for (idx_t i = 0; i < field_count; i++) {
			auto field = mysql_fetch_field_direct(result, i);
			MySQLField mysql_field;
			if (field->name && field->name_length > 0) {
				mysql_field.name = string(field->name, field->name_length);
			}
			mysql_field.type = MySQLTypes::FieldToLogicalType(type_config, field);
			fields.push_back(std::move(mysql_field));
		}

		return make_uniq<MySQLResult>(result, std::move(fields), result_streaming, *this);
	}
}

unique_ptr<MySQLResult> MySQLConnection::Query(const string &query, MySQLResultStreaming streaming) {
	return QueryInternal(query, streaming);
}

void MySQLConnection::Execute(const string &query) {
	QueryInternal(query, MySQLResultStreaming::FORCE_MATERIALIZATION);
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
