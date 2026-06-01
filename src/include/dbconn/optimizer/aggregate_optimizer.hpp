#pragma once

#include <string>

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace dbconnector {
namespace optimizer {

typedef bool (*should_push_aggregate_t)(duckdb::ClientContext &context, duckdb::LogicalAggregate &aggr);

class AggregateOptimizer {

public:
	struct Config {
		bool enabled = false;
		char identifier_quote = '"';
		std::string table_scan_name;
		should_push_aggregate_t should_push_aggregate = nullptr;
	};

	static Config CreateConfig(duckdb::ClientContext &ctx, const std::string &enabled_option,

	                           char identifier_quote, std::string table_scan_name,
	                           should_push_aggregate_t should_push_aggregate = nullptr);

	static void Optimize(const Config &config, duckdb::OptimizerExtensionInput &input,
	                     duckdb::unique_ptr<duckdb::LogicalOperator> &op);
};

} // namespace optimizer
} // namespace dbconnector
