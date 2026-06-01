#include "dbconn/optimizer/aggregate_optimizer.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include "dbconn/bind_data.hpp"
#include "dbconn/optimizer/aggregate_bind_data.hpp"
#include "dbconn/optimizer/optimizer_util.hpp"
#include "dbconn/query/query_writer.hpp"
#include "dbconn/table_scan/filter_pushdown.hpp"

namespace dbconnector {
namespace optimizer {

using namespace duckdb;

AggregateOptimizer::Config AggregateOptimizer::CreateConfig(ClientContext &ctx, const std::string &enabled_option,

                                                            char identifier_quote, std::string table_scan_name,
                                                            should_push_aggregate_t should_push_aggregate) {

	Config res;

	res.enabled = false;
	Value enabled_val;
	if (ctx.TryGetCurrentSetting(enabled_option, enabled_val) && !enabled_val.IsNull()) {
		res.enabled = BooleanValue::Get(enabled_val);
	}

	res.identifier_quote = identifier_quote;
	res.table_scan_name = std::move(table_scan_name);
	res.should_push_aggregate = should_push_aggregate;

	return res;
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

struct PushedAggregate {
	string select_list;
	string group_by_clause;
	string where_clause;
	bool success = false;

	bool PushedDown() {
		return success;
	}
};

static PushedAggregate TryPushAggregateToMySQL(const AggregateOptimizer::Config &config, LogicalAggregate &aggr,
                                               LogicalOperator &aggr_child, LogicalGet &get) {
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
		TracedBindingColumn traced_binding = OptimizerUtil::TraceBindingToColumn(col_ref.binding, aggr_child, get);
		if (!traced_binding.Found()) {
			return res;
		}
		string quoted =
		    dbconnector::query::QueryWriter::WriteIdentifier(traced_binding.col_name, config.identifier_quote);
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
			fragment =
			    "COUNT(*) AS " + dbconnector::query::QueryWriter::WriteIdentifier(alias, config.identifier_quote);
		} else {
			auto &child_ref = agg_expr.children[0]->Cast<BoundColumnRefExpression>();
			TracedBindingColumn traced_binding =
			    OptimizerUtil::TraceBindingToColumn(child_ref.binding, aggr_child, get);
			if (!traced_binding.Found()) {
				return res;
			}
			string quoted_col =
			    dbconnector::query::QueryWriter::WriteIdentifier(traced_binding.col_name, config.identifier_quote);
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
			fragment =
			    agg_sql + " AS " + dbconnector::query::QueryWriter::WriteIdentifier(alias, config.identifier_quote);
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

static void OptimizeAggregates(const AggregateOptimizer::Config &config, ClientContext &context,
                               unique_ptr<LogicalOperator> &op, vector<AggregateRewriteInfo> &rewrites) {
	if (!config.enabled) {
		return;
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		auto &child = op->children[i];
		if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &aggr = child->Cast<LogicalAggregate>();
			LogicalGet *get = nullptr;
			dbconnector::BindData *bind_data = nullptr;
			if (!aggr.children.empty() &&
			    OptimizerUtil::FindExtensionGet(config.table_scan_name, *aggr.children[0], get, bind_data)) {

				if (config.should_push_aggregate && !config.should_push_aggregate(context, aggr)) {
					OptimizeAggregates(config, context, child, rewrites);
					continue;
				}

				PushedAggregate pushed_aggr = TryPushAggregateToMySQL(config, aggr, *aggr.children[0], *get);
				auto &aggr_bind_data = bind_data->GetAggregateBindData();
				aggr_bind_data.aggregate_select_list = pushed_aggr.select_list;
				aggr_bind_data.group_by_clause = pushed_aggr.group_by_clause;
				aggr_bind_data.aggregate_where_clause = pushed_aggr.where_clause;
				aggr_bind_data.has_aggregate_pushdown = pushed_aggr.PushedDown();
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
		OptimizeAggregates(config, context, child, rewrites);
	}
}

void AggregateOptimizer::Optimize(const AggregateOptimizer::Config &config, OptimizerExtensionInput &input,
                                  unique_ptr<LogicalOperator> &op) {

	vector<AggregateRewriteInfo> rewrites;
	OptimizeAggregates(config, input.context, op, rewrites);
	for (auto &info : rewrites) {
		RewriteBindingsInTree(*op, info);
	}
}

} // namespace optimizer
} // namespace dbconnector
