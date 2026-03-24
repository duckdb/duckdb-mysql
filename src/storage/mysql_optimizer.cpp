#include "storage/mysql_optimizer.hpp"
#include "storage/mysql_catalog.hpp"
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
#include "storage/mysql_transaction.hpp"
#include "storage/mysql_predicate_analyzer.hpp"

namespace duckdb {

struct MySQLOperators {
	reference_map_t<MySQLCatalog, vector<reference<LogicalGet>>> scans;
};

void GatherMySQLScans(LogicalOperator &op, MySQLOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (MySQLCatalog::IsMySQLScan(table_scan.name)) {
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
	for (auto &child : op.children) {
		GatherMySQLScans(*child, result);
	}
}

static bool TraceColumnToGet(Expression &expr, LogicalOperator &child, LogicalGet &get, MySQLBindData &bind_data,
                             string &out) {
	if (expr.expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col_ref = expr.Cast<BoundColumnRefExpression>();
	if (col_ref.depth > 0) {
		return false;
	}
	auto binding = col_ref.binding;

	reference<LogicalOperator> current = child;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = current.get().Cast<LogicalProjection>();
		if (binding.table_index != proj.table_index) {
			break;
		}
		if (binding.column_index >= proj.expressions.size()) {
			return false;
		}
		auto &proj_expr = *proj.expressions[binding.column_index];
		if (proj_expr.expression_class != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &inner_ref = proj_expr.Cast<BoundColumnRefExpression>();
		if (inner_ref.depth > 0) {
			return false;
		}
		binding = inner_ref.binding;
		current = *current.get().children[0];
	}

	if (binding.table_index != get.table_index) {
		return false;
	}
	auto &column_ids = get.GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return false;
	}
	auto &col_index = column_ids[binding.column_index];
	if (col_index.IsRowIdColumn()) {
		return false;
	}

	if (bind_data.has_aggregate_pushdown) {
		auto agg_idx = col_index.GetPrimaryIndex();
		if (agg_idx >= get.names.size()) {
			return false;
		}
		out = MySQLUtils::WriteIdentifier(get.names[agg_idx]);
		return true;
	}

	auto actual_col_idx = col_index.GetPrimaryIndex();
	if (actual_col_idx >= bind_data.names.size()) {
		return false;
	}
	out = MySQLUtils::WriteIdentifier(bind_data.names[actual_col_idx]);
	return true;
}

static bool TryBuildOrderByClause(vector<BoundOrderByNode> &orders, LogicalOperator &child, LogicalGet &get,
                                  MySQLBindData &bind_data, string &out) {
	vector<string> fragments;
	for (auto &order : orders) {
		string col_name;
		if (!TraceColumnToGet(*order.expression, child, get, bind_data, col_name)) {
			return false;
		}

		OrderType direction = order.type;
		OrderByNullType null_order = order.null_order;

		if (direction == OrderType::ORDER_DEFAULT) {
			direction = OrderType::ASCENDING;
		}
		if (null_order == OrderByNullType::ORDER_DEFAULT) {
			null_order =
			    (direction == OrderType::ASCENDING) ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		}

		if (direction == OrderType::ASCENDING) {
			if (null_order == OrderByNullType::NULLS_FIRST) {
				fragments.push_back(col_name + " ASC");
			} else {
				fragments.push_back(col_name + " IS NULL, " + col_name + " ASC");
			}
		} else {
			if (null_order == OrderByNullType::NULLS_FIRST) {
				fragments.push_back(col_name + " IS NOT NULL, " + col_name + " DESC");
			} else {
				fragments.push_back(col_name + " DESC");
			}
		}
	}
	out = " ORDER BY " + StringUtil::Join(fragments, ", ");
	return true;
}

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
		if (group->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
	}
	for (auto &expr : aggr.expressions) {
		if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
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
		if (PUSHABLE_AGGREGATES.find(agg_expr.function.name) == PUSHABLE_AGGREGATES.end()) {
			return false;
		}
		if (agg_expr.function.name != "count_star") {
			if (agg_expr.children.size() != 1) {
				return false;
			}
			if (agg_expr.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
		}
	}
	return true;
}

static bool TraceBindingToMySQLColumn(ColumnBinding binding, LogicalOperator &child, LogicalGet &get,
                                      MySQLBindData &bind_data, string &col_name, LogicalType &col_type) {
	reference<LogicalOperator> current = child;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = current.get().Cast<LogicalProjection>();
		if (binding.table_index != proj.table_index) {
			break;
		}
		if (binding.column_index >= proj.expressions.size()) {
			return false;
		}
		auto &proj_expr = *proj.expressions[binding.column_index];
		if (proj_expr.expression_class != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &inner_ref = proj_expr.Cast<BoundColumnRefExpression>();
		if (inner_ref.depth > 0) {
			return false;
		}
		binding = inner_ref.binding;
		current = *current.get().children[0];
	}

	if (binding.table_index != get.table_index) {
		return false;
	}
	auto &column_ids = get.GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return false;
	}
	auto &col_index = column_ids[binding.column_index];
	if (col_index.IsRowIdColumn()) {
		return false;
	}
	auto actual_col_idx = col_index.GetPrimaryIndex();
	if (actual_col_idx >= bind_data.names.size()) {
		return false;
	}
	col_name = bind_data.names[actual_col_idx];
	col_type = bind_data.types[actual_col_idx];
	return true;
}

static bool TryPushAggregateToMySQL(LogicalAggregate &aggr, LogicalOperator &aggr_child, LogicalGet &get,
                                    MySQLBindData &bind_data) {
	if (!CanPushAggregate(aggr)) {
		return false;
	}

	vector<string> select_fragments;
	vector<string> group_names;
	vector<LogicalType> new_types;
	vector<string> new_names;

	for (auto &group : aggr.groups) {
		auto &col_ref = group->Cast<BoundColumnRefExpression>();
		string col_name;
		LogicalType col_type;
		if (!TraceBindingToMySQLColumn(col_ref.binding, aggr_child, get, bind_data, col_name, col_type)) {
			return false;
		}
		string quoted = MySQLUtils::WriteIdentifier(col_name);
		select_fragments.push_back(quoted);
		group_names.push_back(quoted);
		new_types.push_back(col_type);
		new_names.push_back(col_name);
	}

	idx_t agg_idx = 0;
	for (auto &expr : aggr.expressions) {
		auto &agg_expr = expr->Cast<BoundAggregateExpression>();
		string fragment;
		string alias = "_agg_" + to_string(agg_idx);

		if (agg_expr.function.name == "count_star") {
			fragment = "COUNT(*) AS " + MySQLUtils::WriteIdentifier(alias);
		} else {
			auto &child_ref = agg_expr.children[0]->Cast<BoundColumnRefExpression>();
			string col_name;
			LogicalType col_type;
			if (!TraceBindingToMySQLColumn(child_ref.binding, aggr_child, get, bind_data, col_name, col_type)) {
				return false;
			}
			string quoted_col = MySQLUtils::WriteIdentifier(col_name);
			string func_upper;
			if (agg_expr.function.name == "count") {
				func_upper = "COUNT";
			} else if (agg_expr.function.name == "sum") {
				func_upper = "SUM";
			} else if (agg_expr.function.name == "avg") {
				func_upper = "AVG";
			} else if (agg_expr.function.name == "min") {
				func_upper = "MIN";
			} else if (agg_expr.function.name == "max") {
				func_upper = "MAX";
			} else {
				return false;
			}
			if (func_upper == "SUM" && col_type.id() == LogicalTypeId::DECIMAL) {
				return false;
			}
			if (func_upper == "AVG" && col_type.id() == LogicalTypeId::DECIMAL) {
				return false;
			}
			string agg_sql = func_upper + "(" + quoted_col + ")";
			if (func_upper == "AVG") {
				agg_sql = "CAST(" + agg_sql + " AS DOUBLE)";
			}
			fragment = agg_sql + " AS " + MySQLUtils::WriteIdentifier(alias);
		}

		select_fragments.push_back(fragment);
		new_types.push_back(agg_expr.return_type);
		new_names.push_back(alias);
		agg_idx++;
	}

	bind_data.aggregate_select_list = StringUtil::Join(select_fragments, ", ");

	if (!group_names.empty()) {
		bind_data.group_by_clause = " GROUP BY " + StringUtil::Join(group_names, ", ");
	}

	if (!get.table_filters.filters.empty()) {
		auto &column_ids = get.GetColumnIds();
		string where_clause;
		for (auto &entry : get.table_filters.filters) {
			auto scan_col_idx = entry.first;
			if (scan_col_idx >= column_ids.size()) {
				continue;
			}
			auto actual_col_idx = column_ids[scan_col_idx].GetPrimaryIndex();
			if (actual_col_idx >= bind_data.names.size()) {
				continue;
			}
			auto column_name = MySQLUtils::WriteIdentifier(bind_data.names[actual_col_idx]);
			auto new_filter = MySQLFilterPushdown::TransformFilter(column_name, *entry.second);
			if (new_filter.empty()) {
				return false;
			}
			if (!where_clause.empty()) {
				where_clause += " AND ";
			}
			where_clause += new_filter;
		}
		if (!where_clause.empty()) {
			bind_data.aggregate_where_clause = where_clause;
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
	get.table_filters.filters.clear();

	bind_data.has_aggregate_pushdown = true;

	return true;
}

struct AggregateRewriteInfo {
	idx_t group_index;
	idx_t aggregate_index;
	idx_t scan_table_index;
	idx_t num_groups;
};

static void RewriteExpression(unique_ptr<Expression> &expr, AggregateRewriteInfo &info) {
	if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (col_ref.depth > 0) {
			return;
		}
		if (col_ref.binding.table_index == info.group_index) {
			col_ref.binding.table_index = info.scan_table_index;
		} else if (col_ref.binding.table_index == info.aggregate_index) {
			col_ref.binding.table_index = info.scan_table_index;
			col_ref.binding.column_index += info.num_groups;
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

					double filter_selectivity = 1.0;
					if (!get->table_filters.filters.empty()) {
						try {
							auto &transaction = MySQLTransaction::Get(context, bind_data->table.catalog);
							auto &con = transaction.GetConnection();
							MySQLStatisticsCollector stats_collector(con, catalog.GetStatsCache());
							PredicateAnalyzer analyzer(stats_collector, bind_data->table.schema.name,
							                           bind_data->table.name);
							vector<column_t> col_ids;
							for (auto &cid : get->GetColumnIds()) {
								col_ids.push_back(cid.GetPrimaryIndex());
							}
							auto analysis = analyzer.AnalyzeFilters(col_ids, &get->table_filters, bind_data->names);
							filter_selectivity = analysis.combined_selectivity;
						} catch (...) {
							for (auto &entry : get->table_filters.filters) {
								(void)entry;
								filter_selectivity *= 0.3;
							}
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
						if (group->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
							auto &col_ref = group->Cast<BoundColumnRefExpression>();
							string col_name;
							LogicalType col_type;
							if (TraceBindingToMySQLColumn(col_ref.binding, *aggr.children[0], *get, *bind_data,
							                              col_name, col_type)) {
								auto it = table_stats.column_distinct_count.find(col_name);
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

				if (TryPushAggregateToMySQL(aggr, *aggr.children[0], *get, *bind_data)) {
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

static void CollectBindingRefs(Expression &expr, idx_t target_table_index, unordered_set<idx_t> &referenced) {
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		if (ref.binding.table_index == target_table_index) {
			referenced.insert(ref.binding.column_index);
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<Expression> &child) { CollectBindingRefs(*child, target_table_index, referenced); });
}

static void RewriteBindingRefs(Expression &expr, idx_t target_table_index, unordered_map<idx_t, idx_t> &old_to_new) {
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		if (ref.binding.table_index == target_table_index) {
			auto it = old_to_new.find(ref.binding.column_index);
			if (it != old_to_new.end()) {
				ref.binding.column_index = it->second;
			}
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<Expression> &child) { RewriteBindingRefs(*child, target_table_index, old_to_new); });
}

static void PruneProjectionLayer(LogicalProjection &proj, const unordered_set<idx_t> &keep_indices,
                                 vector<LogicalProjection *> &above_projs) {
	vector<unique_ptr<Expression>> new_exprs;
	unordered_map<idx_t, idx_t> old_to_new;
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		if (keep_indices.count(i)) {
			old_to_new[i] = new_exprs.size();
			new_exprs.push_back(std::move(proj.expressions[i]));
		}
	}
	proj.expressions = std::move(new_exprs);

	for (auto *above : above_projs) {
		for (auto &expr : above->expressions) {
			RewriteBindingRefs(*expr, proj.table_index, old_to_new);
		}
	}
}

static void PruneColumnsAfterOrderByRemoval(LogicalOperator &child, LogicalGet &get,
                                            const vector<idx_t> &projection_map) {
	if (child.type == LogicalOperatorType::LOGICAL_GET) {
		auto &column_ids = get.GetColumnIds();
		vector<ColumnIndex> new_ids;
		for (auto idx : projection_map) {
			new_ids.push_back(column_ids[idx]);
		}
		get.SetColumnIds(std::move(new_ids));
		get.projection_ids.clear();
		return;
	}

	if (child.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	vector<LogicalProjection *> proj_chain;
	reference<LogicalOperator> current = child;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		proj_chain.push_back(&current.get().Cast<LogicalProjection>());
		if (current.get().children.empty()) {
			break;
		}
		current = *current.get().children[0];
	}

	if (proj_chain.size() < 2) {
		return;
	}

	auto &top_proj = *proj_chain[0];
	vector<LogicalProjection *> above_projs;
	above_projs.push_back(&top_proj);

	for (idx_t layer = 1; layer < proj_chain.size(); layer++) {
		auto &prev_proj = *proj_chain[layer - 1];
		auto &curr_proj = *proj_chain[layer];

		unordered_set<idx_t> needed;
		for (auto &expr : prev_proj.expressions) {
			CollectBindingRefs(*expr, curr_proj.table_index, needed);
		}

		if (needed.size() < curr_proj.expressions.size()) {
			PruneProjectionLayer(curr_proj, needed, above_projs);
		}
		above_projs.push_back(&curr_proj);
	}

	auto &bottom_proj = *proj_chain.back();
	unordered_set<idx_t> get_referenced;
	for (auto &expr : bottom_proj.expressions) {
		CollectBindingRefs(*expr, get.table_index, get_referenced);
	}

	auto &column_ids = get.GetColumnIds();
	if (get_referenced.size() < column_ids.size()) {
		vector<ColumnIndex> new_ids;
		unordered_map<idx_t, idx_t> get_old_to_new;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (get_referenced.count(i)) {
				get_old_to_new[i] = new_ids.size();
				new_ids.push_back(column_ids[i]);
			}
		}
		get.SetColumnIds(std::move(new_ids));
		get.projection_ids.clear();

		for (auto *proj : proj_chain) {
			for (auto &expr : proj->expressions) {
				RewriteBindingRefs(*expr, get.table_index, get_old_to_new);
			}
		}
	}
}

void OptimizeOrderByAndLimit(ClientContext &context, unique_ptr<LogicalOperator> &op) {
	Value order_enabled_val;
	if (context.TryGetCurrentSetting("mysql_order_pushdown_enabled", order_enabled_val)) {
		if (!BooleanValue::Get(order_enabled_val)) {
			for (auto &child : op->children) {
				OptimizeOrderByAndLimit(context, child);
			}
			return;
		}
	}

	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &topn = op->Cast<LogicalTopN>();
		LogicalGet *get = nullptr;
		MySQLBindData *bind_data = nullptr;
		if (FindMySQLGet(*op->children[0], get, bind_data)) {
			string order_clause;
			if (TryBuildOrderByClause(topn.orders, *op->children[0], *get, *bind_data, order_clause)) {
				bind_data->order_by_clause = order_clause;
				bind_data->limit = " LIMIT " + to_string(topn.limit);
				if (topn.offset > 0) {
					bind_data->limit += " OFFSET " + to_string(topn.offset);
				}
				op = std::move(op->children[0]);
				return;
			}
		}
		for (auto &child : op->children) {
			OptimizeOrderByAndLimit(context, child);
		}
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op->Cast<LogicalOrder>();
		LogicalGet *get = nullptr;
		MySQLBindData *bind_data = nullptr;
		if (FindMySQLGet(*op->children[0], get, bind_data)) {
			string order_clause;
			if (TryBuildOrderByClause(order.orders, *op->children[0], *get, *bind_data, order_clause)) {
				bind_data->order_by_clause = order_clause;
				if (!order.projection_map.empty()) {
					PruneColumnsAfterOrderByRemoval(*op->children[0], *get, order.projection_map);
				}
				op = std::move(op->children[0]);
				return;
			}
		}
		for (auto &child : op->children) {
			OptimizeOrderByAndLimit(context, child);
		}
		return;
	}
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
			return;
		}
		switch (limit.offset_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			return;
		}
		auto &bind_data = get.bind_data->Cast<MySQLBindData>();
		if (!bind_data.limit.empty()) {
			return;
		}
		bool has_limit = (limit.limit_val.Type() != LimitNodeType::UNSET);
		bool has_offset = (limit.offset_val.Type() != LimitNodeType::UNSET);
		if (!has_limit && has_offset) {
			return;
		}
		if (has_limit) {
			bind_data.limit = " LIMIT " + to_string(limit.limit_val.GetConstantValue());
		}
		if (has_offset) {
			bind_data.limit += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
		}
		op = std::move(op->children[0]);
		return;
	}
	for (auto &child : op->children) {
		OptimizeOrderByAndLimit(context, child);
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
			if (MySQLCatalog::IsMySQLScan(get.function.name)) {
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

	vector<AggregateRewriteInfo> rewrites;
	OptimizeAggregates(input.context, plan, rewrites);
	for (auto &info : rewrites) {
		RewriteBindingsInTree(*plan, info);
	}

	OptimizeOrderByAndLimit(input.context, plan);
}

} // namespace duckdb
