#include "storage/mysql_optimizer.hpp"
#include "storage/mysql_catalog.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "mysql_filter_pushdown.hpp"
#include "mysql_scanner.hpp"
#include "storage/federation/cost_model.hpp"

#include "dbconn/bind_data.hpp"
#include "dbconn/optimizer/aggregate_optimizer.hpp"
#include "dbconn/optimizer/optimizer_util.hpp"
#include "dbconn/optimizer/order_by_and_limit_optimizer.hpp"
#include "dbconn/table_scan/filter_pushdown.hpp"

namespace duckdb {

struct MySQLOperators {
	reference_map_t<MySQLCatalog, vector<reference<LogicalGet>>> scans;
};

static bool ShouldPushAggregate(ClientContext &context, LogicalAggregate &aggr) {
	Value pred_analyzer_val;
	bool enable_predicate_analyzer;
	if (context.TryGetCurrentSetting("mysql_enable_predicate_analyzer", pred_analyzer_val)) {
		enable_predicate_analyzer = BooleanValue::Get(pred_analyzer_val);
	}

	if (!enable_predicate_analyzer) {
		return true;
	}

	LogicalGet *get = nullptr;
	dbconnector::BindData *dbconn_bind_data = nullptr;
	dbconnector::optimizer::OptimizerUtil::FindExtensionGet("mysql_scan", *aggr.children[0], get, dbconn_bind_data);
	auto bind_data = &dbconn_bind_data->Cast<MySQLBindData>();

	auto &catalog = bind_data->table.ParentCatalog().Cast<MySQLCatalog>();
	MySQLTableStats table_stats;
	bool have_stats =
	    catalog.GetStatsCache().GetTableStats(bind_data->table.schema.name, bind_data->table.name, table_stats);

	if (!have_stats) {
		return true;
	}

	CachedCostConstants cached_costs;
	CostModelParameters cost_params;
	if (catalog.GetStatsCache().GetCostConstants(cached_costs) && cached_costs.loaded) {
		cost_params.cpu_cost_per_row = cached_costs.row_evaluate_cost;
		double effective_block_cost = cached_costs.io_block_read_cost;
		double bpr;
		if (catalog.GetStatsCache().GetBufferPoolHitRate(bpr) && bpr >= 0) {
			effective_block_cost =
			    (bpr * cached_costs.memory_block_read_cost) + ((1.0 - bpr) * cached_costs.io_block_read_cost);
		}
		cost_params.io_cost_per_byte =
		    effective_block_cost / static_cast<double>(CostModelParameters::INNODB_PAGE_SIZE);
	}
	DefaultCostModel cost_model(cost_params);
	cost_model.SetNetworkCalibration(catalog.GetConnectionPool().GetNetworkCalibration());
	Value compression_aware_val, compression_ratio_val;
	if (context.TryGetCurrentSetting("mysql_compression_aware_costs", compression_aware_val)) {
		cost_model.SetCompressionAwareCosts(BooleanValue::Get(compression_aware_val));
	}
	if (context.TryGetCurrentSetting("mysql_compression_ratio", compression_ratio_val)) {
		cost_model.SetCompressionRatio(compression_ratio_val.GetValue<double>());
	}

	double filter_selectivity = 1.0;
	if (get->table_filters.HasFilters()) {
		MySQLTableStats cached_filter_stats;
		if (catalog.GetStatsCache().GetTableStats(bind_data->table.schema.name, bind_data->table.name,
		                                          cached_filter_stats)) {
			for (const auto &entry : get->table_filters) {
				ProjectionIndex proj_idx = entry.GetIndex();
				ColumnIndex col_idx = get->GetColumnIndex(proj_idx);
				column_t table_col_idx = col_idx.GetPrimaryIndex();
				if (table_col_idx >= bind_data->names.size()) {
					continue;
				}
				const string &col_name = bind_data->names[table_col_idx];
				auto it = cached_filter_stats.column_distinct_count.find(col_name);
				if (it != cached_filter_stats.column_distinct_count.end() && it->second > 0 &&
				    cached_filter_stats.estimated_row_count > 0) {
					double col_sel =
					    1.0 / static_cast<double>(std::min(it->second, cached_filter_stats.estimated_row_count));
					filter_selectivity *= col_sel;
				} else {
					filter_selectivity *= 0.3;
				}
			}
		} else {
			filter_selectivity = 0.3;
		}
	}
	idx_t effective_row_count =
	    static_cast<idx_t>(static_cast<double>(table_stats.estimated_row_count) * filter_selectivity);
	effective_row_count = std::max(effective_row_count, static_cast<idx_t>(1));

	vector<string> columns;
	for (auto &name : bind_data->names) {
		columns.push_back(name);
	}

	idx_t num_groups = 0;
	for (auto &group : aggr.groups) {
		if (group->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = group->Cast<BoundColumnRefExpression>();
			dbconnector::optimizer::TracedBindingColumn traced_binding =
			    dbconnector::optimizer::OptimizerUtil::TraceBindingToColumn(col_ref.binding, *aggr.children[0], *get);
			if (traced_binding.Found()) {
				auto it = table_stats.column_distinct_count.find(traced_binding.col_name);
				if (it != table_stats.column_distinct_count.end() && it->second > 0) {
					num_groups = std::max(num_groups, it->second);
				}
			}
		}
	}
	if (num_groups == 0) {
		num_groups = effective_row_count / 10;
	}
	num_groups = std::min(num_groups, effective_row_count);
	idx_t num_aggregates = aggr.expressions.size();

	return cost_model.ShouldPushAggregate(table_stats, columns, effective_row_count, num_groups, num_aggregates);
}

void MySQLOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto aggr_config = dbconnector::optimizer::AggregateOptimizer::CreateConfig(
	    input.context, "mysql_aggregate_pushdown_enabled", '`', "mysql_scan", ShouldPushAggregate);
	dbconnector::optimizer::AggregateOptimizer::Optimize(aggr_config, input, plan);

	auto order_config = dbconnector::optimizer::OrderByAndLimitOptimizer::CreateConfig(
	    input.context, "mysql_order_pushdown_enabled", '`', "mysql_scan");
	dbconnector::optimizer::OrderByAndLimitOptimizer::Optimize(order_config, input, plan);
}

} // namespace duckdb
