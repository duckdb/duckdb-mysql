#pragma once

#include "duckdb/function/function.hpp"

#include "dbconn/optimizer/order_by_and_limit_bind_data.hpp"

namespace dbconnector {

class BindData : public duckdb::FunctionData {

public:
	virtual optimizer::OrderByAndLimitBindData &GetOrderByAndLimitBindData() = 0;
};

} // namespace dbconnector
