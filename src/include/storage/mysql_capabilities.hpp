//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_capabilities.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

#include "mysql_version.hpp"

namespace duckdb {

class MySQLCapabilities {
	MySQLVersion version;

public:
	MySQLCapabilities() {
	}

	explicit MySQLCapabilities(MySQLVersion version_p) : version(std::move(version_p)) {
	}

	bool Supports(RemoteCapability capability) const;
	bool SupportsPushdown(const ParsedExpression &expression);
	bool SupportsPushdown(const TableRef &ref);
	bool SupportsPushdown(const QueryNode &node);
	bool SupportsPushdown(const SQLStatement &statement);

	const MySQLVersion &GetVersion() const {
		return version;
	}

private:
	bool SupportsWindow(const WindowExpression &window);
	bool SupportsCTEMap(const CommonTableExpressionMap &cte_map);
	bool SupportsResultModifiers(const QueryNode &node,
	                             optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list = nullptr);
	bool SupportsOrderEntries(const vector<OrderByNode> &orders,
	                          optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list = nullptr);
	bool SupportsSelectAliasUsage(const SelectNode &select);
	bool SupportsValue(const Value &value);
	bool SupportsType(const LogicalType &type, bool for_cast);
};

} // namespace duckdb
