#pragma once

#include <string>

namespace dbconnector {
namespace optimizer {

struct AggregateBindData {
	std::string aggregate_select_list;
	std::string group_by_clause;
	std::string aggregate_where_clause;
	bool has_aggregate_pushdown = false;
};

} // namespace optimizer
} // namespace dbconnector
