#include "storage/mysql_predicate_analyzer.hpp"
#include "mysql_utils.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"

#include "dbconnector/table_scan/filter_pushdown.hpp"

#include <cmath>
#include <unordered_set>

namespace duckdb {

PredicateAnalyzer::PredicateAnalyzer(MySQLStatisticsCollector &stats, const string &schema, const string &table)
    : stats_(stats), schema_(schema), table_(table) {
	table_stats_ = stats_.get().GetTableStats(schema, table);
}

void PredicateAnalyzer::SetHintInjectionEnabled(bool enabled, double staleness_threshold) {
	hint_injection_enabled_ = enabled;
	hint_staleness_threshold_ = staleness_threshold;
}

void PredicateAnalyzer::SetPushThresholds(double with_index, double no_index) {
	push_threshold_with_index_ = with_index;
	push_threshold_no_index_ = no_index;
}

static bool IsDirectReference(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	default:
		return false;
	}
}

double PredicateAnalyzer::EstimateExpressionSelectivity(const string &column_name, const Expression &expr) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		const Value *constant = nullptr;
		if (IsDirectReference(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().GetValue();
		} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && IsDirectReference(right)) {
			constant = &left.Cast<BoundConstantExpression>().GetValue();
			comparison_type = FlipComparisonExpression(comparison_type);
		} else {
			return DEFAULT_SELECTIVITY;
		}
		return stats_.get().EstimateSelectivity(schema_, table_, column_name, comparison_type, *constant);
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND: {
			double selectivity = 1.0;
			for (const auto &child : conjunction.GetChildren()) {
				selectivity *= EstimateExpressionSelectivity(column_name, *child);
			}
			return selectivity;
		}
		case ExpressionType::CONJUNCTION_OR: {
			double selectivity = 1.0;
			for (const auto &child : conjunction.GetChildren()) {
				selectivity *= EstimateExpressionSelectivity(column_name, *child);
			}
			return selectivity;
		}
		default:
			return DEFAULT_SELECTIVITY;
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL: {

			auto it = table_stats_.column_null_fraction.find(column_name);
			if (it != table_stats_.column_null_fraction.end()) {
				return it->second;
			}
			return 0.01;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			auto it = table_stats_.column_null_fraction.find(column_name);
			if (it != table_stats_.column_null_fraction.end()) {
				return 1.0 - it->second;
			}
			return 0.99;
		}
		case ExpressionType::COMPARE_IN: {
			if (op.GetChildren().empty() || !IsDirectReference(*op.GetChildren()[0])) {
				return DEFAULT_SELECTIVITY;
			}
			double per_value_selectivity = 0.01;
			if (op.GetChildren().size() > 10) {
				per_value_selectivity = 0.005;
			}
			return std::min(static_cast<double>(op.GetChildren().size()) * per_value_selectivity, 0.8);
		}
		default:
			return DEFAULT_SELECTIVITY;
		}
	}
	default:
		return DEFAULT_SELECTIVITY;
	}
}

PushdownDecision PredicateAnalyzer::MakePushdownDecision(double selectivity, bool has_index) const {
	if (has_index && selectivity < push_threshold_with_index_) {
		return PushdownDecision::PUSH_TO_MYSQL;
	}
	if (!has_index && selectivity < push_threshold_no_index_) {
		return PushdownDecision::PUSH_TO_MYSQL;
	}
	return PushdownDecision::EXECUTE_IN_DUCKDB;
}

#ifndef NDEBUG
string PredicateAnalyzer::GenerateReasoning(double selectivity, bool has_index, PushdownDecision decision) const {
	string reason;
	reason += "selectivity=" + std::to_string(selectivity);
	reason += ", has_index=" + string(has_index ? "true" : "false");
	reason += ", decision=";
	switch (decision) {
	case PushdownDecision::PUSH_TO_MYSQL:
		reason += "PUSH";
		break;
	case PushdownDecision::EXECUTE_IN_DUCKDB:
		reason += "LOCAL";
		break;
	case PushdownDecision::PUSH_PARTIAL:
		reason += "PARTIAL";
		break;
	}
	return reason;
}
#endif

PredicateAnalysis PredicateAnalyzer::AnalyzeFilter(const string &column_name, const TableFilter &filter,
                                                   column_t column_id) {
	using namespace dbconnector;
	PredicateAnalysis result;
	result.column_name = column_name;

	auto config = table_scan::FilterPushdown::CreateConfig('`', '\'', query::QuoteEscapeStyle::BACKSLASH);
	result.mysql_predicate = table_scan::FilterPushdown::TransformFilter(config, column_name, filter, column_id);
	if (result.mysql_predicate.empty()) {
		result.decision = PushdownDecision::EXECUTE_IN_DUCKDB;
		result.estimated_selectivity = DEFAULT_SELECTIVITY;
#ifndef NDEBUG
		result.reasoning = "unpushable filter type";
#endif
		return result;
	}

	result.has_index = table_stats_.HasIndexOnColumn(column_name);
	result.is_partition_key = table_stats_.partition_info.IsPartitionColumn(column_name);
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "PredicateAnalyzer::AnalyzeFilter");
	auto &expr = *expr_filter.expr;
	result.estimated_selectivity = EstimateExpressionSelectivity(column_name, expr);

	if (result.is_partition_key) {
		result.decision = PushdownDecision::PUSH_TO_MYSQL;
	} else {
		result.decision = MakePushdownDecision(result.estimated_selectivity, result.has_index);
	}

#ifndef NDEBUG
	result.reasoning = GenerateReasoning(result.estimated_selectivity, result.has_index, result.decision);
	if (result.is_partition_key) {
		result.reasoning += ", partition_key=true (force push)";
	}
#endif

	return result;
}

void PredicateAnalyzer::ReorderPredicatesForIndex(vector<PredicateAnalysis> &analyses,
                                                  vector<string> &pushed_predicates) const {
	if (pushed_predicates.size() < 2 || table_stats_.indexes.empty()) {
		return;
	}

	vector<string> filter_columns;
	for (const auto &analysis : analyses) {
		if (analysis.ShouldPush() && !analysis.mysql_predicate.empty()) {
			filter_columns.push_back(analysis.column_name);
		}
	}

	if (filter_columns.size() < 2) {
		return;
	}

	optional_ptr<const MySQLIndexInfo> best_index = table_stats_.FindBestIndex(filter_columns);
	if (!best_index) {
		for (const auto &index : table_stats_.indexes) {
			idx_t matched = 0;
			for (const auto &col : filter_columns) {
				for (const auto &idx_col : index.columns) {
					if (StringUtil::CIEquals(col, idx_col)) {
						matched++;
						break;
					}
				}
			}
			if (matched >= 2 && (!best_index || matched > filter_columns.size() / 2)) {
				best_index = &index;
			}
		}
	}

	if (!best_index || best_index->columns.size() < 2) {
		return;
	}

	case_insensitive_map_t<vector<idx_t>> column_to_analysis_indices;
	for (idx_t i = 0; i < analyses.size(); i++) {
		if (analyses[i].ShouldPush() && !analyses[i].mysql_predicate.empty()) {
			column_to_analysis_indices[analyses[i].column_name].push_back(i);
		}
	}

	vector<string> reordered_predicates;
	unordered_set<idx_t> used_indices;

	for (const auto &idx_col : best_index->columns) {
		auto it = column_to_analysis_indices.find(idx_col);
		if (it != column_to_analysis_indices.end()) {
			for (auto analysis_idx : it->second) {
				if (used_indices.find(analysis_idx) == used_indices.end()) {
					reordered_predicates.push_back(analyses[analysis_idx].mysql_predicate);
					used_indices.insert(analysis_idx);
				}
			}
		}
	}

	for (idx_t i = 0; i < analyses.size(); i++) {
		if (analyses[i].ShouldPush() && !analyses[i].mysql_predicate.empty() &&
		    used_indices.find(i) == used_indices.end()) {
			reordered_predicates.push_back(analyses[i].mysql_predicate);
		}
	}

	if (reordered_predicates.size() == pushed_predicates.size()) {
		pushed_predicates = std::move(reordered_predicates);
	}
}

FilterAnalysisResult PredicateAnalyzer::AnalyzeFilters(const vector<column_t> &column_ids,
                                                       optional_ptr<TableFilterSet> filters,
                                                       const vector<string> &names) {
	FilterAnalysisResult result;

	if (!filters || !filters->HasFilters()) {
		return result;
	}

	vector<string> pushed_predicates;

	idx_t filter_count = 0;
	for (const auto &entry : *filters) {
		column_t col_idx = entry.GetIndex().GetIndex();
		if (col_idx >= column_ids.size()) {
			continue;
		}
		column_t actual_col_idx = column_ids[col_idx];
		if (actual_col_idx >= names.size()) {
			continue;
		}
		string column_name = names[actual_col_idx];
		auto &filter = entry.Filter();

		auto analysis = AnalyzeFilter(column_name, filter, actual_col_idx);
		analysis.column_index = col_idx;
		filter_count++;
		if (filter_count <= 2) {
			result.combined_selectivity *= analysis.estimated_selectivity;
		} else {
			result.combined_selectivity *= std::pow(analysis.estimated_selectivity, SELECTIVITY_DAMPENING_EXPONENT);
		}

		if (analysis.ShouldPush() && !analysis.mysql_predicate.empty()) {
			pushed_predicates.push_back(analysis.mysql_predicate);
			result.filters_pushed++;
		} else {
			result.filters_local++;
		}

		result.analyses.push_back(std::move(analysis));
	}

	if (pushed_predicates.size() >= 2) {
		ReorderPredicatesForIndex(result.analyses, pushed_predicates);
	}

	if (!pushed_predicates.empty()) {
		result.combined_mysql_predicate = StringUtil::Join(pushed_predicates, " AND ");
	}

	if (hint_injection_enabled_) {
		RecommendIndexHint(result);
	}

	return result;
}

void PredicateAnalyzer::RecommendIndexHint(FilterAnalysisResult &result) const {
	if (table_stats_.staleness_score < hint_staleness_threshold_) {
		return;
	}

	vector<string> filter_columns;
	for (const auto &analysis : result.analyses) {
		if (analysis.ShouldPush() && analysis.has_index) {
			filter_columns.push_back(analysis.column_name);
		}
	}

	if (filter_columns.empty()) {
		return;
	}

	auto best_index = table_stats_.FindBestIndex(filter_columns);
	if (!best_index) {
		return;
	}

	result.recommended_index = best_index->index_name;
	result.suggest_force_index = (table_stats_.staleness_score >= FORCE_INDEX_STALENESS_THRESHOLD);
}

} // namespace duckdb
