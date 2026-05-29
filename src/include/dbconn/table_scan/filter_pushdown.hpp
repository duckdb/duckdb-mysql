#pragma once

#include <string>

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "filter_pushdown_config.hpp"

namespace dbconnector {
namespace table_scan {

class FilterPushdown {

public:
	static std::string TransformFilter(const FilterPushdownConfig &config, const std::string &column_name,
	                                   const duckdb::TableFilter &filter);

	static bool IsInternalFilter(const duckdb::TableFilter &filter);
	static std::string ToString(const duckdb::TableFilter &filter);

private:
	static std::string TransformExpression(const std::string &column_name, const duckdb::Expression &expr);
	static std::string TransformComparison(duckdb::ExpressionType type);
	static std::string CreateExpression(const std::string &column_name,
	                                    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters,
	                                    const std::string &op);
	static bool IsDirectReference(const duckdb::Expression &expr);
	static const duckdb::Expression &GetExpression(const duckdb::TableFilter &filter, const std::string &context);
};

} // namespace table_scan
} // namespace dbconnector
