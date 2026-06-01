#pragma once

#include <string>

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace dbconnector {
namespace optimizer {

class OrderByAndLimitOptimizer {

	struct Config {
		bool enabled = false;
		std::string table_scan_name;
	};

public:
	static Config CreateConfig(duckdb::ClientContext &ctx, const std::string &enabled_option,
	                           std::string table_scan_name);

	static void Optimize(const Config &config, duckdb::OptimizerExtensionInput &input,
	                     duckdb::unique_ptr<duckdb::LogicalOperator> &op);
};

} // namespace optimizer
} // namespace dbconnector
