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

// The option remained from libmysql,
// is translated to libmariadb options on connect
enum mysql_ssl_mode_compat {
	SSL_MODE_DISABLED,
	SSL_MODE_PREFERRED,
	SSL_MODE_REQUIRED,
	SSL_MODE_VERIFY_CA,
	SSL_MODE_VERIFY_IDENTITY
};

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

enum class MySQLResultStreaming { UNINITIALIZED, ALLOW_STREAMING, FORCE_MATERIALIZATION, REQUIRE_STREAMING };

enum class MySQLConnectorInterface { UNINITIALIZED, BASIC, PREPARED_STATEMENT };

class MySQLUtils {
public:
	static std::tuple<MySQLConnectionParameters, unordered_set<string>> ParseConnectionParameters(const string &dsn);
	static MYSQL *Connect(const string &dsn, const string &attach_path);

	static string WriteIdentifier(const string &identifier);
	static string WriteLiteral(const string &identifier);
	static string EscapeQuotes(const string &text, char quote);
	static string WriteQuoted(const string &text, char quote);
	static string TransformConstant(const Value &val);
};

} // namespace duckdb
