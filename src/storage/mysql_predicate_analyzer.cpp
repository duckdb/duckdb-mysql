#include "storage/mysql_predicate_analyzer.hpp"
#include "mysql_utils.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

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

bool PredicateAnalyzer::IsPushableFilterType(TableFilterType type) const {
	switch (type) {
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
	case TableFilterType::CONJUNCTION_AND:
	case TableFilterType::CONJUNCTION_OR:
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::OPTIONAL_FILTER:
	case TableFilterType::IN_FILTER:
		return true;
	case TableFilterType::DYNAMIC_FILTER:
		return false;
	default:
		return false;
	}
}

bool PredicateAnalyzer::CanPushFilter(TableFilter &filter) const {
	return IsPushableFilterType(filter.filter_type);
}

string PredicateAnalyzer::TransformComparisonOperator(ExpressionType type) const {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		return "";
	}
}

string PredicateAnalyzer::CreateConjunction(const vector<string> &predicates, const string &op) const {
	if (predicates.empty()) {
		return "";
	}
	if (predicates.size() == 1) {
		return predicates[0];
	}
	return "(" + StringUtil::Join(predicates, " " + op + " ") + ")";
}

string PredicateAnalyzer::TransformFilterToMySQL(const string &column_name, TableFilter &filter) const {
	string col_escaped = MySQLUtils::WriteIdentifier(column_name);

	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		return col_escaped + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return col_escaped + " IS NOT NULL";
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = filter.Cast<ConjunctionAndFilter>();
		vector<string> child_predicates;
		for (const auto &child : conjunction.child_filters) {
			auto pred = TransformFilterToMySQL(column_name, *child);
			if (!pred.empty()) {
				child_predicates.push_back(pred);
			}
		}
		return CreateConjunction(child_predicates, "AND");
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = filter.Cast<ConjunctionOrFilter>();
		vector<string> child_predicates;
		for (const auto &child : conjunction.child_filters) {
			auto pred = TransformFilterToMySQL(column_name, *child);
			if (!pred.empty()) {
				child_predicates.push_back(pred);
			}
		}
		return CreateConjunction(child_predicates, "OR");
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto constant_string = MySQLUtils::TransformConstant(constant_filter.constant);
		auto operator_string = TransformComparisonOperator(constant_filter.comparison_type);
		if (operator_string.empty()) {
			return "";
		}
		return StringUtil::Format("%s %s %s", col_escaped, operator_string, constant_string);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return TransformFilterToMySQL(column_name, *optional_filter.child_filter);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		if (in_filter.values.empty()) {
			return "";
		}
		string in_list;
		for (const auto &val : in_filter.values) {
			if (!in_list.empty()) {
				in_list += ", ";
			}
			in_list += MySQLUtils::TransformConstant(val);
		}
		return col_escaped + " IN (" + in_list + ")";
	}
	case TableFilterType::DYNAMIC_FILTER:
		return "";
	default:
		return "";
	}
}

double PredicateAnalyzer::EstimateFilterSelectivity(const string &column_name, TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL: {
		auto it = table_stats_.column_null_fraction.find(column_name);
		if (it != table_stats_.column_null_fraction.end()) {
			return it->second;
		}
		return 0.01;
	}
	case TableFilterType::IS_NOT_NULL: {
		auto it = table_stats_.column_null_fraction.find(column_name);
		if (it != table_stats_.column_null_fraction.end()) {
			return 1.0 - it->second;
		}
		return 0.99;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = filter.Cast<ConjunctionAndFilter>();
		double selectivity = 1.0;
		for (const auto &child : conjunction.child_filters) {
			selectivity *= EstimateFilterSelectivity(column_name, *child);
		}
		return selectivity;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = filter.Cast<ConjunctionOrFilter>();
		double combined = 1.0;
		for (const auto &child : conjunction.child_filters) {
			double child_sel = EstimateFilterSelectivity(column_name, *child);
			combined *= (1.0 - child_sel);
		}
		return 1.0 - combined;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return stats_.get().EstimateSelectivity(schema_, table_, column_name, constant_filter.comparison_type,
		                                        constant_filter.constant);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return EstimateFilterSelectivity(column_name, *optional_filter.child_filter);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		auto it = table_stats_.column_distinct_count.find(column_name);
		if (it != table_stats_.column_distinct_count.end() && it->second > 0) {
			return std::min(static_cast<double>(in_filter.values.size()) / static_cast<double>(it->second), 1.0);
		}
		double per_value_selectivity = 0.01;
		if (in_filter.values.size() > 10) {
			per_value_selectivity = 0.005;
		}
		return std::min(static_cast<double>(in_filter.values.size()) * per_value_selectivity, 0.8);
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

PredicateAnalysis PredicateAnalyzer::AnalyzeFilter(const string &column_name, TableFilter &filter) {
	PredicateAnalysis result;
	result.column_name = column_name;

	if (!CanPushFilter(filter)) {
		result.decision = PushdownDecision::EXECUTE_IN_DUCKDB;
		result.estimated_selectivity = DEFAULT_SELECTIVITY;
#ifndef NDEBUG
		result.reasoning = "unpushable filter type";
#endif
		return result;
	}

	result.has_index = table_stats_.HasIndexOnColumn(column_name);
	result.is_partition_key = table_stats_.partition_info.IsPartitionColumn(column_name);
	result.estimated_selectivity = EstimateFilterSelectivity(column_name, filter);

	if (result.is_partition_key) {
		result.decision = PushdownDecision::PUSH_TO_MYSQL;
	} else {
		result.decision = MakePushdownDecision(result.estimated_selectivity, result.has_index);
	}

	result.mysql_predicate = TransformFilterToMySQL(column_name, filter);
	if (result.mysql_predicate.empty()) {
		result.decision = PushdownDecision::EXECUTE_IN_DUCKDB;
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

	if (!filters || filters->filters.empty()) {
		return result;
	}

	vector<string> pushed_predicates;

	idx_t filter_count = 0;
	for (const auto &entry : filters->filters) {
		column_t col_idx = entry.first;
		if (col_idx >= column_ids.size()) {
			continue;
		}
		column_t actual_col_idx = column_ids[col_idx];
		if (actual_col_idx >= names.size()) {
			continue;
		}
		string column_name = names[actual_col_idx];
		auto &filter = *entry.second;

		auto analysis = AnalyzeFilter(column_name, filter);
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
