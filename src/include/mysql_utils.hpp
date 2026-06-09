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

//! The type of server we are connected to
enum class MySQLServerType : uint8_t { MYSQL, MARIADB };

//! Server version information, parsed from the server version string on attach
struct MySQLVersion {
	idx_t major_version = 0;
	idx_t minor_version = 0;
	idx_t patch_version = 0;
	MySQLServerType server_type = MySQLServerType::MYSQL;

	//! Parse a server version string (e.g. "9.6.0", "8.0.42-log", "11.4.2-MariaDB",
	//! "5.5.5-10.11.6-MariaDB-log")
	static MySQLVersion Parse(const string &version_string);

	bool IsAtLeast(idx_t major_p, idx_t minor_p, idx_t patch_p) const {
		if (major_version != major_p) {
			return major_version > major_p;
		}
		if (minor_version != minor_p) {
			return minor_version > minor_p;
		}
		return patch_version >= patch_p;
	}

	//! Whether the server supports window functions and (recursive) CTEs
	bool SupportsWindowFunctions() const {
		switch (server_type) {
		case MySQLServerType::MARIADB:
			// window functions: MariaDB 10.2.0, CTEs: 10.2.1, recursive CTEs: 10.2.2
			return IsAtLeast(10, 2, 2);
		default:
			// window functions and CTEs were added during the MySQL 8.0 development cycle -
			// require the first GA release
			return IsAtLeast(8, 0, 11);
		}
	}

	//! Whether the server supports (recursive) common table expressions
	bool SupportsCTEs() const {
		return SupportsWindowFunctions();
	}

	//! Whether the server supports the EXCEPT / INTERSECT set operations
	bool SupportsExceptIntersect(bool all) const {
		switch (server_type) {
		case MySQLServerType::MARIADB:
			// EXCEPT / INTERSECT: MariaDB 10.3, the ALL variants: MariaDB 10.5
			return all ? IsAtLeast(10, 5, 0) : IsAtLeast(10, 3, 0);
		default:
			// EXCEPT / INTERSECT (including ALL) were added in MySQL 8.0.31
			return IsAtLeast(8, 0, 31);
		}
	}

	//! The NO PAD binary collation used to give string literals byte-wise comparison
	//! semantics, or an empty string if the server has no suitable collation
	//! (in which case string literals cannot be pushed down)
	string GetBinaryCollation() const {
		switch (server_type) {
		case MySQLServerType::MARIADB:
			// utf8mb4_nopad_bin is available since MariaDB 10.2
			return IsAtLeast(10, 2, 0) ? "utf8mb4_nopad_bin" : string();
		default:
			// utf8mb4_0900_bin is available since MySQL 8.0.17
			return IsAtLeast(8, 0, 17) ? "utf8mb4_0900_bin" : string();
		}
	}
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

enum class MySQLResultStreaming { UNINITIALIZED, ALLOW_STREAMING, FORCE_MATERIALIZATION };

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
