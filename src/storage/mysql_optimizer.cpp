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

#include "dbconn/optimizer/order_by_and_limit_optimizer.hpp"
#include "dbconn/table_scan/filter_pushdown.hpp"

namespace duckdb {

struct MySQLOperators {
	reference_map_t<MySQLCatalog, vector<reference<LogicalGet>>> scans;
};

static bool FindMySQLGet(LogicalOperator &start, LogicalGet *&get_out, MySQLBindData *&bind_out) {
	reference<LogicalOperator> current = start;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (current.get().children.empty()) {
			return false;
		}
		current = *current.get().children[0];
	}
	if (current.get().type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = current.get().Cast<LogicalGet>();
	if (!MySQLCatalog::IsMySQLScan(get.function.name)) {
		return false;
	}
	get_out = &get;
	bind_out = &get.bind_data->Cast<MySQLBindData>();
	return true;
}

static const unordered_set<string> PUSHABLE_AGGREGATES = {"count_star", "count", "sum", "avg", "min", "max"};

static bool CanPushAggregate(LogicalAggregate &aggr) {
	if (aggr.grouping_sets.size() > 1) {
		return false;
	}
	if (!aggr.grouping_functions.empty()) {
		return false;
	}
	for (auto &group : aggr.groups) {
		if (group->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
	}
	for (auto &expr : aggr.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &agg_expr = expr->Cast<BoundAggregateExpression>();
		if (agg_expr.IsDistinct()) {
			return false;
		}
		if (agg_expr.filter) {
			return false;
		}
		if (agg_expr.order_bys && !agg_expr.order_bys->orders.empty()) {
			return false;
		}
		if (PUSHABLE_AGGREGATES.find(agg_expr.function.GetName()) == PUSHABLE_AGGREGATES.end()) {
			return false;
		}
		if (agg_expr.function.GetName() != "count_star") {
			if (agg_expr.children.size() != 1) {
				return false;
			}
			if (agg_expr.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
		}
	}
	return true;
}

struct TracedBindingColumn {
	string col_name;
	LogicalType col_type;

	bool Found() {
		return !col_name.empty();
	}
};

static TracedBindingColumn TraceBindingToMySQLColumn(ColumnBinding binding, LogicalOperator &child, LogicalGet &get) {
	TracedBindingColumn res;
	reference<LogicalOperator> current = child;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = current.get().Cast<LogicalProjection>();
		if (binding.table_index != proj.table_index) {
			break;
		}
		if (binding.column_index >= proj.expressions.size()) {
			return res;
		}
		auto &proj_expr = *proj.expressions[binding.column_index];
		if (proj_expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return res;
		}
		auto &inner_ref = proj_expr.Cast<BoundColumnRefExpression>();
		if (inner_ref.depth > 0) {
			return res;
		}
		binding = inner_ref.binding;
		current = *current.get().children[0];
	}

	if (binding.table_index != get.table_index) {
		return res;
	}
	auto &column_ids = get.GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return res;
	}
	auto &col_index = column_ids[binding.column_index];
	if (col_index.IsRowIdColumn()) {
		return res;
	}
	auto actual_col_idx = col_index.GetPrimaryIndex();
	if (actual_col_idx >= get.names.size()) {
		return res;
	}
	res.col_name = get.names[actual_col_idx];
	res.col_type = get.returned_types[actual_col_idx];
	return res;
}

struct PushedAggregate {
	string select_list;
	string group_by_clause;
	string where_clause;
	bool success = false;

	bool PushedDown() {
		return success;
	}
};

static PushedAggregate TryPushAggregateToMySQL(LogicalAggregate &aggr, LogicalOperator &aggr_child, LogicalGet &get) {
	PushedAggregate res;
	if (!CanPushAggregate(aggr)) {
		return res;
	}

	vector<string> select_fragments;
	vector<string> group_names;
	vector<LogicalType> new_types;
	vector<string> new_names;

	for (auto &group : aggr.groups) {
		auto &col_ref = group->Cast<BoundColumnRefExpression>();
		TracedBindingColumn traced_binding = TraceBindingToMySQLColumn(col_ref.binding, aggr_child, get);
		if (!traced_binding.Found()) {
			return res;
		}
		string quoted = MySQLUtils::WriteIdentifier(traced_binding.col_name);
		select_fragments.push_back(quoted);
		group_names.push_back(quoted);
		new_types.push_back(traced_binding.col_type);
		new_names.push_back(traced_binding.col_name);
	}

	idx_t agg_idx = 0;
	for (auto &expr : aggr.expressions) {
		auto &agg_expr = expr->Cast<BoundAggregateExpression>();
		string fragment;
		string alias = "_agg_" + to_string(agg_idx);

		if (agg_expr.function.GetName() == "count_star") {
			fragment = "COUNT(*) AS " + MySQLUtils::WriteIdentifier(alias);
		} else {
			auto &child_ref = agg_expr.children[0]->Cast<BoundColumnRefExpression>();
			TracedBindingColumn traced_binding = TraceBindingToMySQLColumn(child_ref.binding, aggr_child, get);
			if (!traced_binding.Found()) {
				return res;
			}
			string quoted_col = MySQLUtils::WriteIdentifier(traced_binding.col_name);
			string func_upper;
			if (agg_expr.function.GetName() == "count") {
				func_upper = "COUNT";
			} else if (agg_expr.function.GetName() == "sum") {
				func_upper = "SUM";
			} else if (agg_expr.function.GetName() == "avg") {
				func_upper = "AVG";
			} else if (agg_expr.function.GetName() == "min") {
				func_upper = "MIN";
			} else if (agg_expr.function.GetName() == "max") {
				func_upper = "MAX";
			} else {
				return res;
			}
			if (func_upper == "SUM" && traced_binding.col_type.id() == LogicalTypeId::DECIMAL) {
				return res;
			}
			if (func_upper == "AVG" && traced_binding.col_type.id() == LogicalTypeId::DECIMAL) {
				return res;
			}
			string agg_sql = func_upper + "(" + quoted_col + ")";
			if (func_upper == "AVG") {
				agg_sql = "CAST(" + agg_sql + " AS DOUBLE)";
			}
			fragment = agg_sql + " AS " + MySQLUtils::WriteIdentifier(alias);
		}

		select_fragments.push_back(fragment);
		new_types.push_back(agg_expr.GetReturnType());
		new_names.push_back(alias);
		agg_idx++;
	}

	res.select_list = StringUtil::Join(select_fragments, ", ");

	if (!group_names.empty()) {
		res.group_by_clause = " GROUP BY " + StringUtil::Join(group_names, ", ");
	}

	if (get.table_filters.HasFilters()) {
		string where_clause;
		for (auto &entry : get.table_filters) {
			ProjectionIndex proj_idx = entry.GetIndex();
			ColumnIndex col_idx = get.GetColumnIndex(proj_idx);
			column_t table_col_idx = col_idx.GetPrimaryIndex();
			if (table_col_idx >= get.names.size()) {
				return res;
			}
			auto column_name = get.names[table_col_idx];
			auto config = dbconnector::table_scan::FilterPushdown::CreateConfig('`');
			auto new_filter =
			    dbconnector::table_scan::FilterPushdown::TransformFilter(config, column_name, entry.Filter());
			if (new_filter.empty()) {
				return res;
			}
			if (!where_clause.empty()) {
				where_clause += " AND ";
			}
			where_clause += new_filter;
		}
		if (!where_clause.empty()) {
			res.where_clause = where_clause;
		}
	}

	get.returned_types = new_types;
	get.names = new_names;
	vector<ColumnIndex> new_column_ids;
	for (idx_t i = 0; i < new_types.size(); i++) {
		new_column_ids.push_back(ColumnIndex(i));
	}
	get.SetColumnIds(std::move(new_column_ids));
	get.projection_ids.clear();
	get.table_filters.ClearFilters();

	res.success = true;
	return res;
}

struct AggregateRewriteInfo {
	TableIndex group_index;
	TableIndex aggregate_index;
	TableIndex scan_table_index;
	idx_t num_groups;
};

static void RewriteExpression(unique_ptr<Expression> &expr, AggregateRewriteInfo &info) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (col_ref.depth > 0) {
			return;
		}
		if (col_ref.binding.table_index == info.group_index) {
			col_ref.binding.table_index = info.scan_table_index;
		} else if (col_ref.binding.table_index == info.aggregate_index) {
			col_ref.binding.table_index = info.scan_table_index;
			col_ref.binding.column_index = ProjectionIndex(col_ref.binding.column_index.GetIndex() + info.num_groups);
		}
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { RewriteExpression(child, info); });
}

static void RewriteBindingsInOperator(LogicalOperator &op, AggregateRewriteInfo &info) {
	for (auto &expr : op.expressions) {
		RewriteExpression(expr, info);
	}
	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op.Cast<LogicalOrder>();
		for (auto &node : order.orders) {
			RewriteExpression(node.expression, info);
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &topn = op.Cast<LogicalTopN>();
		for (auto &node : topn.orders) {
			RewriteExpression(node.expression, info);
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &cond : join.conditions) {
			RewriteExpression(cond.LeftReference(), info);
			RewriteExpression(cond.RightReference(), info);
		}
	}
}

static void RewriteBindingsInTree(LogicalOperator &op, AggregateRewriteInfo &info) {
	RewriteBindingsInOperator(op, info);
	for (auto &child : op.children) {
		RewriteBindingsInTree(*child, info);
	}
}

static void OptimizeAggregates(ClientContext &context, unique_ptr<LogicalOperator> &op,
                               vector<AggregateRewriteInfo> &rewrites) {
	Value agg_enabled_val;
	if (context.TryGetCurrentSetting("mysql_aggregate_pushdown_enabled", agg_enabled_val)) {
		if (!BooleanValue::Get(agg_enabled_val)) {
			return;
		}
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		auto &child = op->children[i];
		if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &aggr = child->Cast<LogicalAggregate>();
			LogicalGet *get = nullptr;
			MySQLBindData *bind_data = nullptr;
			if (!aggr.children.empty() && FindMySQLGet(*aggr.children[0], get, bind_data)) {
				auto &catalog = bind_data->table.ParentCatalog().Cast<MySQLCatalog>();
				MySQLTableStats table_stats;
				bool have_stats = catalog.GetStatsCache().GetTableStats(bind_data->table.schema.name,
				                                                        bind_data->table.name, table_stats);

				if (have_stats) {
					CachedCostConstants cached_costs;
					CostModelParameters cost_params;
					if (catalog.GetStatsCache().GetCostConstants(cached_costs) && cached_costs.loaded) {
						cost_params.cpu_cost_per_row = cached_costs.row_evaluate_cost;
						double effective_block_cost = cached_costs.io_block_read_cost;
						double bpr;
						if (catalog.GetStatsCache().GetBufferPoolHitRate(bpr) && bpr >= 0) {
							effective_block_cost = (bpr * cached_costs.memory_block_read_cost) +
							                       ((1.0 - bpr) * cached_costs.io_block_read_cost);
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
									double col_sel = 1.0 / static_cast<double>(std::min(
									                           it->second, cached_filter_stats.estimated_row_count));
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
							TracedBindingColumn traced_binding =
							    TraceBindingToMySQLColumn(col_ref.binding, *aggr.children[0], *get);
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

					if (!cost_model.ShouldPushAggregate(table_stats, columns, effective_row_count, num_groups,
					                                    num_aggregates)) {
						OptimizeAggregates(context, child, rewrites);
						continue;
					}
				}

				PushedAggregate pushed_aggr = TryPushAggregateToMySQL(aggr, *aggr.children[0], *get);
				bind_data->aggregate_select_list = pushed_aggr.select_list;
				bind_data->group_by_clause = pushed_aggr.group_by_clause;
				bind_data->aggregate_where_clause = pushed_aggr.where_clause;
				bind_data->has_aggregate_pushdown = pushed_aggr.PushedDown();
				if (pushed_aggr.PushedDown()) {
					AggregateRewriteInfo info;
					info.group_index = aggr.group_index;
					info.aggregate_index = aggr.aggregate_index;
					info.scan_table_index = get->table_index;
					info.num_groups = aggr.groups.size();
					rewrites.push_back(info);
					op->children[i] = std::move(aggr.children[0]);
					continue;
				}
			}
		}
		OptimizeAggregates(context, child, rewrites);
	}
}

void MySQLOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	vector<AggregateRewriteInfo> rewrites;
	OptimizeAggregates(input.context, plan, rewrites);
	for (auto &info : rewrites) {
		RewriteBindingsInTree(*plan, info);
	}

	auto order_config = dbconnector::optimizer::OrderByAndLimitOptimizer::CreateConfig(
	    input.context, "mysql_order_pushdown_enabled", "mysql_scan");
	dbconnector::optimizer::OrderByAndLimitOptimizer::Optimize(order_config, input, plan);
}

} // namespace duckdb
