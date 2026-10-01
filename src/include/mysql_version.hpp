//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! The type of server we are connected to
enum class MySQLServerType { MYSQL, MARIADB };

//! Server version information, parsed from the server version string on attach
struct MySQLVersion {
	idx_t major_version = 0;
	idx_t minor_version = 0;
	idx_t patch_version = 0;
	MySQLServerType server_type = MySQLServerType::MYSQL;

	//! Parse a server version string (e.g. "9.6.0", "8.0.42-log", "11.4.2-MariaDB",
	//! "5.5.5-10.11.6-MariaDB-log")
	static MySQLVersion Parse(const string &version_string);

	bool IsAtLeast(idx_t major_p, idx_t minor_p, idx_t patch_p) const;
	bool SupportsWindowFunctions() const;
	bool SupportsCTEs() const;
	bool SupportsExceptIntersect(bool all) const;

	//! The NO PAD binary collation used to give string literals byte-wise comparison
	//! semantics
	string GetBinaryCollation() const;
};
} // namespace duckdb
