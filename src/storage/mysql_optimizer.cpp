#include "storage/mysql_optimizer.hpp"
#include "storage/mysql_catalog.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "mysql_scanner.hpp"

namespace duckdb {

struct MySQLOperators {
	reference_map_t<MySQLCatalog, vector<reference<LogicalGet>>> scans;
};

void GatherMySQLScans(LogicalOperator &op, MySQLOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (MySQLCatalog::IsMySQLScan(table_scan.name)) {
			// mysql table scan - add to the catalog
			auto &bind_data = get.bind_data->Cast<MySQLBindData>();
			auto &catalog = bind_data.table.ParentCatalog().Cast<MySQLCatalog>();
			result.scans[catalog].push_back(get);
		}
		if (MySQLCatalog::IsMySQLQuery(table_scan.name)) {
			auto &bind_data = get.bind_data->Cast<MySQLQueryBindData>();
			auto &catalog = bind_data.catalog.Cast<MySQLCatalog>();
			result.scans[catalog].push_back(get);
		}
	}
	// recurse into children
	for (auto &child : op.children) {
		GatherMySQLScans(*child, result);
	}
}

void OptimizeMySQLScan(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}
		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto &get = child.get().Cast<LogicalGet>();
		if (!MySQLCatalog::IsMySQLScan(get.function.name)) {
			return;
		}
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset limit
			return;
		}
		switch (limit.offset_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset offset
			return;
		}
		auto &bind_data = get.bind_data->Cast<MySQLBindData>();
		if (limit.limit_val.Type() != LimitNodeType::UNSET) {
			bind_data.limit += " LIMIT " + to_string(limit.limit_val.GetConstantValue());
		}
		if (limit.offset_val.Type() != LimitNodeType::UNSET) {
			bind_data.limit += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
		}
		// remove the limit
		op = std::move(op->children[0]);
		return;
	}
	// recurse into children
	for (auto &child : op->children) {
		OptimizeMySQLScan(child);
	}
}

void MySQLOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	MySQLOperators operators;
	GatherMySQLScans(*plan, operators);
	for (auto &entry : operators.scans) {
		MySQLResultStreaming result_streaming = MySQLResultStreaming::FORCE_MATERIALIZATION;
		if (entry.second.size() == 1) {
			// if we have exactly one scan for a given catalog we can stream it
			result_streaming = MySQLResultStreaming::ALLOW_STREAMING;
		}
		for (auto &logical_get : entry.second) {
			auto &get = logical_get.get();
			if (MySQLCatalog::IsMySQLScan(get.function.name)) {
				// mysql table scan - add to the catalog
				auto &bind_data = get.bind_data->Cast<MySQLBindData>();
				if (bind_data.streaming == MySQLResultStreaming::UNINITIALIZED ||
				    result_streaming == MySQLResultStreaming::FORCE_MATERIALIZATION) {
					bind_data.streaming = result_streaming;
				}
			}
			if (MySQLCatalog::IsMySQLQuery(get.function.name)) {
				auto &bind_data = get.bind_data->Cast<MySQLQueryBindData>();
				if (bind_data.streaming == MySQLResultStreaming::UNINITIALIZED ||
				    result_streaming == MySQLResultStreaming::FORCE_MATERIALIZATION) {
					bind_data.streaming = result_streaming;
				}
			}
		}
	}

	OptimizeMySQLScan(plan);
}

} // namespace duckdb
