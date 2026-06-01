#include "dbconn/optimizer/order_by_and_limit_optimizer.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include "dbconn/bind_data.hpp"
#include "dbconn/query/query_writer.hpp"

// todo: removme
#include <iostream>

namespace dbconnector {
namespace optimizer {

using namespace duckdb;

OrderByAndLimitOptimizer::Config OrderByAndLimitOptimizer::CreateConfig(ClientContext &ctx,
                                                                        const std::string &enabled_option,
                                                                        std::string table_scan_name) {
	Config res;

	res.enabled = false;
	Value enabled_val;
	if (ctx.TryGetCurrentSetting(enabled_option, enabled_val) && !enabled_val.IsNull()) {
		res.enabled = BooleanValue::Get(enabled_val);
	}

	res.table_scan_name = std::move(table_scan_name);

	return res;
}

static bool FindExtensionGet(const std::string &table_scan_name, LogicalOperator &start, LogicalGet *&get_out,
                             dbconnector::BindData *&bind_out) {
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
	if (get.function.name != table_scan_name) {
		return false;
	}
	get_out = &get;
	bind_out = &get.bind_data->Cast<dbconnector::BindData>();
	return true;
}

static string TraceColumnToGet(Expression &expr, LogicalOperator &child, LogicalGet &get) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return std::string();
	}
	auto &col_ref = expr.Cast<BoundColumnRefExpression>();
	if (col_ref.depth > 0) {
		return std::string();
	}
	auto binding = col_ref.binding;

	reference<LogicalOperator> current = child;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = current.get().Cast<LogicalProjection>();
		if (binding.table_index != proj.table_index) {
			break;
		}
		if (binding.column_index >= proj.expressions.size()) {
			return std::string();
		}
		auto &proj_expr = *proj.expressions[binding.column_index];
		if (proj_expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return std::string();
		}
		auto &inner_ref = proj_expr.Cast<BoundColumnRefExpression>();
		if (inner_ref.depth > 0) {
			return std::string();
		}
		binding = inner_ref.binding;
		current = *current.get().children[0];
	}

	if (binding.table_index != get.table_index) {
		return std::string();
	}
	auto &column_ids = get.GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return std::string();
	}
	auto &col_index = column_ids[binding.column_index];
	if (col_index.IsRowIdColumn()) {
		return std::string();
	}

	auto actual_col_idx = col_index.GetPrimaryIndex();
	if (actual_col_idx >= get.names.size()) {
		return std::string();
	}
	// TODO: quote
	return dbconnector::query::QueryWriter::WriteIdentifier(get.names[actual_col_idx], '`');
}

static string TryBuildOrderByClause(vector<BoundOrderByNode> &orders, LogicalOperator &child, LogicalGet &get) {
	vector<string> fragments;
	for (auto &order : orders) {
		string col_name = TraceColumnToGet(*order.expression, child, get);
		if (col_name.empty()) {
			return std::string();
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
	return " ORDER BY " + StringUtil::Join(fragments, ", ");
}

static void CollectBindingRefs(Expression &expr, idx_t target_table_index, unordered_set<idx_t> &referenced) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		if (ref.binding.table_index.index == target_table_index) {
			referenced.insert(ref.binding.column_index);
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<Expression> &child) { CollectBindingRefs(*child, target_table_index, referenced); });
}

static void RewriteBindingRefs(Expression &expr, idx_t target_table_index, unordered_map<idx_t, idx_t> &old_to_new) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		if (ref.binding.table_index.index == target_table_index) {
			auto it = old_to_new.find(ref.binding.column_index);
			if (it != old_to_new.end()) {
				ref.binding.column_index = ProjectionIndex(it->second);
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
			RewriteBindingRefs(*expr, proj.table_index.index, old_to_new);
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
			CollectBindingRefs(*expr, curr_proj.table_index.index, needed);
		}

		if (needed.size() < curr_proj.expressions.size()) {
			PruneProjectionLayer(curr_proj, needed, above_projs);
		}
		above_projs.push_back(&curr_proj);
	}

	auto &bottom_proj = *proj_chain.back();
	unordered_set<idx_t> get_referenced;
	for (auto &expr : bottom_proj.expressions) {
		CollectBindingRefs(*expr, get.table_index.index, get_referenced);
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
				RewriteBindingRefs(*expr, get.table_index.index, get_old_to_new);
			}
		}
	}
}

void OrderByAndLimitOptimizer::Optimize(const OrderByAndLimitOptimizer::Config &config, OptimizerExtensionInput &input,
                                        unique_ptr<LogicalOperator> &op) {
	if (!config.enabled) {
		return;
	}

	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &topn = op->Cast<LogicalTopN>();
		LogicalGet *get = nullptr;
		dbconnector::BindData *bind_data = nullptr;
		if (FindExtensionGet(config.table_scan_name, *op->children[0], get, bind_data)) {
			string order_clause = TryBuildOrderByClause(topn.orders, *op->children[0], *get);
			if (!order_clause.empty()) {
				auto &order_by_and_limit_bind_data = bind_data->GetOrderByAndLimitBindData();
				order_by_and_limit_bind_data.order_by_clause = order_clause;
				order_by_and_limit_bind_data.limit_clause = " LIMIT " + to_string(topn.limit);
				if (topn.offset > 0) {
					order_by_and_limit_bind_data.limit_clause += " OFFSET " + to_string(topn.offset);
				}
				op = std::move(op->children[0]);
				return;
			}
		}
		for (auto &child : op->children) {
			Optimize(config, input, child);
		}
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op->Cast<LogicalOrder>();
		LogicalGet *get = nullptr;
		dbconnector::BindData *bind_data = nullptr;
		if (FindExtensionGet(config.table_scan_name, *op->children[0], get, bind_data)) {
			string order_clause = TryBuildOrderByClause(order.orders, *op->children[0], *get);
			if (!order_clause.empty()) {
				auto &order_by_and_limit_bind_data = bind_data->GetOrderByAndLimitBindData();
				order_by_and_limit_bind_data.order_by_clause = order_clause;
				if (!order.projection_map.empty()) {
					vector<column_t> indices;
					indices.reserve(order.projection_map.size());
					for (auto proj_idx : order.projection_map) {
						ColumnIndex col_idx = get->GetColumnIndex(proj_idx);
						column_t table_col_idx = col_idx.GetPrimaryIndex();
						indices.emplace_back(table_col_idx);
					}
					PruneColumnsAfterOrderByRemoval(*op->children[0], *get, indices);
				}
				op = std::move(op->children[0]);
				return;
			}
		}
		for (auto &child : op->children) {
			Optimize(config, input, child);
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
		if (get.function.name != config.table_scan_name) {
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
		auto &bind_data = get.bind_data->Cast<dbconnector::BindData>();
		auto &order_by_and_limit_bind_data = bind_data.GetOrderByAndLimitBindData();
		if (!order_by_and_limit_bind_data.limit_clause.empty()) {
			return;
		}
		bool has_limit = (limit.limit_val.Type() != LimitNodeType::UNSET);
		bool has_offset = (limit.offset_val.Type() != LimitNodeType::UNSET);
		if (!has_limit && has_offset) {
			return;
		}
		if (has_limit) {
			order_by_and_limit_bind_data.limit_clause = " LIMIT " + to_string(limit.limit_val.GetConstantValue());
		}
		if (has_offset) {
			order_by_and_limit_bind_data.limit_clause += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
		}
		op = std::move(op->children[0]);
		return;
	}
	for (auto &child : op->children) {
		Optimize(config, input, child);
	}
}

} // namespace optimizer
} // namespace dbconnector
