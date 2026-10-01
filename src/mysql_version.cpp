#include "mysql_version.hpp"

namespace duckdb {

MySQLVersion MySQLVersion::Parse(const string &version_string) {
	MySQLVersion result;
	if (StringUtil::Contains(StringUtil::Lower(version_string), "mariadb")) {
		result.server_type = MySQLServerType::MARIADB;
	}

	string version = version_string;
	if (result.server_type == MySQLServerType::MARIADB && StringUtil::StartsWith(version, "5.5.5-")) {
		// MariaDB servers can prefix their version with "5.5.5-" (replication compatibility)
		version = version.substr(6);
	}
	// parse the leading major.minor.patch
	idx_t numbers[3] = {0, 0, 0};
	idx_t number_index = 0;
	for (idx_t pos = 0; pos < version.size(); pos++) {
		auto c = version[pos];
		if (c >= '0' && c <= '9') {
			numbers[number_index] = numbers[number_index] * 10 + static_cast<idx_t>(c - '0');
		} else if (c == '.' && number_index < 2) {
			number_index++;
		} else {
			break;
		}
	}
	result.major_version = numbers[0];
	result.minor_version = numbers[1];
	result.patch_version = numbers[2];
	return result;
}

bool MySQLVersion::IsAtLeast(idx_t major_p, idx_t minor_p, idx_t patch_p) const {
	if (major_version != major_p) {
		return major_version > major_p;
	}
	if (minor_version != minor_p) {
		return minor_version > minor_p;
	}
	return patch_version >= patch_p;
}

//! Whether the server supports window functions and (recursive) CTEs
bool MySQLVersion::SupportsWindowFunctions() const {
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
bool MySQLVersion::SupportsCTEs() const {
	return SupportsWindowFunctions();
}

//! Whether the server supports the EXCEPT / INTERSECT set operations
bool MySQLVersion::SupportsExceptIntersect(bool all) const {
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
string MySQLVersion::GetBinaryCollation() const {
	switch (server_type) {
	case MySQLServerType::MARIADB:
		// utf8mb4_nopad_bin is available since MariaDB 10.2
		return IsAtLeast(10, 2, 0) ? "utf8mb4_nopad_bin" : string();
	default:
		// utf8mb4_0900_bin is available since MySQL 8.0.17
		return IsAtLeast(8, 0, 17) ? "utf8mb4_0900_bin" : string();
	}
}
} // namespace duckdb
