#include "storage/mysql_optimizer.hpp"

#include <map>

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "mysql_scanner.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {

struct MySQLOperators {
	std::map<string, vector<reference<LogicalGet>>> scans;
};

void GatherMySQLScans(ClientContext &ctx, LogicalOperator &op, MySQLOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (MySQLCatalog::IsMySQLScan(table_scan.name.GetIdentifierName())) {
			auto &bdata = get.bind_data->Cast<MySQLBindData>();
			result.scans[bdata.table_name.Catalog().GetIdentifierName()].push_back(get);
		}
		if (MySQLCatalog::IsMySQLQuery(table_scan.name.GetIdentifierName())) {
			auto &bdata = get.bind_data->Cast<MySQLQueryBindData>();
			result.scans[bdata.catalog_name.GetIdentifierName()].push_back(get);
		}
	}
	for (auto &child : op.children) {
		GatherMySQLScans(ctx, *child, result);
	}
}

void MySQLOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	MySQLOperators operators;
	GatherMySQLScans(input.context, *plan, operators);
	for (auto &entry : operators.scans) {
		MySQLResultStreaming result_streaming = MySQLResultStreaming::FORCE_MATERIALIZATION;
		if (entry.second.size() == 1) {
			result_streaming = MySQLResultStreaming::ALLOW_STREAMING;
		}
		for (auto &logical_get : entry.second) {
			auto &get = logical_get.get();
			auto &function_name = get.function.name.GetIdentifierName();
			if (MySQLCatalog::IsMySQLScan(function_name)) {
				auto &bind_data = get.bind_data->Cast<MySQLBindData>();
				if (bind_data.optimizer_streaming == MySQLResultStreaming::UNINITIALIZED ||
				    result_streaming == MySQLResultStreaming::FORCE_MATERIALIZATION) {
					bind_data.optimizer_streaming = result_streaming;
				}
			} else if (MySQLCatalog::IsMySQLQuery(function_name)) {
				auto &bind_data = get.bind_data->Cast<MySQLQueryBindData>();
				if (bind_data.optimizer_streaming == MySQLResultStreaming::UNINITIALIZED ||
				    result_streaming == MySQLResultStreaming::FORCE_MATERIALIZATION) {
					bind_data.optimizer_streaming = result_streaming;
				}
			}
		}
	}
}

} // namespace duckdb
