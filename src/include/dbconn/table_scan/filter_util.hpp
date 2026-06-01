#pragma once

#include <string>

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace dbconnector {
namespace table_scan {

class FilterUtil {

public:
	static const duckdb::Expression &GetExpression(const duckdb::TableFilter &filter, const std::string &call_context);

	static bool IsInternalFilter(const duckdb::TableFilter &filter);

	static std::string ToString(const duckdb::TableFilter &filter);
};

} // namespace table_scan
} // namespace dbconnector
