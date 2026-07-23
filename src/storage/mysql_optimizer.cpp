#include "storage/mysql_optimizer.hpp"

#include "duckdb/planner/operator/logical_get.hpp"

#include "mysql_scanner.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {

struct MySQLOperators {
	reference_map_t<MySQLCatalog, vector<reference<LogicalGet>>> scans;
};

void GatherMySQLScans(LogicalOperator &op, MySQLOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (MySQLCatalog::IsMySQLScan(table_scan.name.GetIdentifierName())) {
			auto &bind_data = get.bind_data->Cast<MySQLBindData>();
			auto &catalog = bind_data.table.ParentCatalog().Cast<MySQLCatalog>();
			result.scans[catalog].push_back(get);
		}
		if (MySQLCatalog::IsMySQLQuery(table_scan.name.GetIdentifierName())) {
			auto &bind_data = get.bind_data->Cast<MySQLQueryBindData>();
			auto &catalog = bind_data.catalog.Cast<MySQLCatalog>();
			result.scans[catalog].push_back(get);
		}
	}
	for (auto &child : op.children) {
		GatherMySQLScans(*child, result);
	}
}

void MySQLOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	MySQLOperators operators;
	GatherMySQLScans(*plan, operators);
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
