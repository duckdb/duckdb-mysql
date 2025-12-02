//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "mysql_field.hpp"

namespace duckdb {

using MySQLStatementPtr = duckdb::unique_ptr<MYSQL_STMT, void (*)(MYSQL_STMT *)>;

inline void MySQLStatementDelete(MYSQL_STMT *stmt) {
	mysql_stmt_close(stmt);
}

struct MySQLStatement {
	string query;
	MYSQL_STMT *stmt;
	vector<MySQLField> fields;

	MySQLStatement(const string &query_p, MYSQL_STMT *stmt_p, vector<MySQLField> fields_p)
	    : query(query_p), stmt(stmt_p), fields(std::move(fields_p)) {
	}

	~MySQLStatement() {
		if (stmt) {
			mysql_stmt_close(stmt);
		}
	}

	const string &Query() {
		return query;
	}

	MYSQL_STMT *get() {
		return stmt;
	}

	MySQLStatementPtr release() {
		MySQLStatementPtr ptr(stmt, MySQLStatementDelete);
		this->stmt = nullptr;
		return ptr;
	}

	const vector<MySQLField> &Fields() {
		return fields;
	}

	vector<MySQLField> FieldsCopy() {
		// performing implicit copy, fields are expected to be copyable
		return fields;
	}
};

} // namespace duckdb
