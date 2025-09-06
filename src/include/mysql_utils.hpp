//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "mysql.h"

namespace duckdb {
class MySQLSchemaEntry;
class MySQLTransaction;

struct MySQLConnectionParameters {
	string host;
	string user;
	string passwd;
	string db;
	uint32_t port = 0;
	string unix_socket;
	idx_t client_flag = CLIENT_COMPRESS | CLIENT_IGNORE_SIGPIPE | CLIENT_MULTI_STATEMENTS;
	unsigned int ssl_mode = SSL_MODE_PREFERRED;
	string ssl_ca;
	string ssl_ca_path;
	string ssl_cert;
	string ssl_cipher;
	string ssl_crl;
	string ssl_crl_path;
	string ssl_key;
};

enum class MySQLResultStreaming { UNINITIALIZED, ALLOW_STREAMING, FORCE_MATERIALIZATION };

enum class MySQLConnectorInterface { UNINITIALIZED, BASIC, PREPARED_STATEMENT };

class MySQLUtils {
public:
	static std::tuple<MySQLConnectionParameters, unordered_set<string>> ParseConnectionParameters(const string &dsn);
	static MYSQL *Connect(const string &dsn);

	static string WriteIdentifier(const string &identifier);
	static string WriteLiteral(const string &identifier);
	static string EscapeQuotes(const string &text, char quote);
	static string WriteQuoted(const string &text, char quote);
	static string TransformConstant(const Value &val);
};

using MySQLStatementPtr = duckdb::unique_ptr<MYSQL_STMT, void (*)(MYSQL_STMT *)>;

inline void MySQLStatementDelete(MYSQL_STMT *stmt) {
	mysql_stmt_close(stmt);
}

using MySQLResultPtr = duckdb::unique_ptr<MYSQL_RES, void (*)(MYSQL_RES *)>;

inline void MySQLResultDelete(MYSQL_RES *res) {
	mysql_free_result(res);
}

} // namespace duckdb
