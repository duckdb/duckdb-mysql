#pragma once

#include <string>

namespace dbconnector {
namespace optimizer {

struct OrderByAndLimitBindData {
	std::string limit_clause;
	std::string order_by_clause;
};

} // namespace optimizer
} // namespace dbconnector
