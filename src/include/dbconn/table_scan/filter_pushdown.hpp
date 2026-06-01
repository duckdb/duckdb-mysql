#pragma once

#include <string>

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace dbconnector {
namespace table_scan {

class FilterPushdown {

	struct Config {
		char identifier_quote = '"';
	};

public:
	static Config CreateConfig(char identifier_quote);

	static std::string TransformFilter(const Config &config, const std::string &column_name,
	                                   const duckdb::TableFilter &filter);

private:
	static std::string TransformExpression(const std::string &column_name, const duckdb::Expression &expr);
	static std::string TransformComparison(duckdb::ExpressionType type);
	static std::string CreateExpression(const std::string &column_name,
	                                    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters,
	                                    const std::string &op);
};

} // namespace table_scan
} // namespace dbconnector
