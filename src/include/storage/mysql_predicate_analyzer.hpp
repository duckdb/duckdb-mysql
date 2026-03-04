//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_predicate_analyzer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "storage/mysql_statistics.hpp"

namespace duckdb {

enum class PushdownDecision : uint8_t { PUSH_TO_MYSQL, EXECUTE_IN_DUCKDB, PUSH_PARTIAL };

struct PredicateAnalysis {
	PushdownDecision decision = PushdownDecision::PUSH_TO_MYSQL;
	string mysql_predicate;
	double estimated_selectivity = 1.0;
	bool has_index = false;
	bool is_partition_key = false;
	string column_name;
	column_t column_index = DConstants::INVALID_INDEX;
#ifndef NDEBUG
	string reasoning;
#endif

	bool ShouldPush() const {
		return decision == PushdownDecision::PUSH_TO_MYSQL || decision == PushdownDecision::PUSH_PARTIAL;
	}
};

struct FilterAnalysisResult {
	vector<PredicateAnalysis> analyses;
	string combined_mysql_predicate;
	double combined_selectivity = 1.0;
	idx_t filters_pushed = 0;
	idx_t filters_local = 0;
	string recommended_index;
	bool suggest_force_index = false;

	bool HasPushedFilters() const {
		return !combined_mysql_predicate.empty();
	}
};

class PredicateAnalyzer {
public:
	PredicateAnalyzer(MySQLStatisticsCollector &stats, const string &schema, const string &table);

	PredicateAnalysis AnalyzeFilter(const string &column_name, TableFilter &filter);
	FilterAnalysisResult AnalyzeFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                                    const vector<string> &names);

	void SetHintInjectionEnabled(bool enabled, double staleness_threshold = 0.5);
	void SetPushThresholds(double with_index, double no_index);

	static constexpr double DEFAULT_PUSH_THRESHOLD_WITH_INDEX = 0.5;
	static constexpr double DEFAULT_PUSH_THRESHOLD_NO_INDEX = 0.3;
	static constexpr double DEFAULT_SELECTIVITY = 0.10;
	static constexpr double SELECTIVITY_DAMPENING_EXPONENT = 0.75;
	static constexpr double FORCE_INDEX_STALENESS_THRESHOLD = 0.7;

private:
	reference<MySQLStatisticsCollector> stats_;
	string schema_;
	string table_;
	MySQLTableStats table_stats_;
	bool hint_injection_enabled_ = false;
	double hint_staleness_threshold_ = 0.5;
	double push_threshold_with_index_ = DEFAULT_PUSH_THRESHOLD_WITH_INDEX;
	double push_threshold_no_index_ = DEFAULT_PUSH_THRESHOLD_NO_INDEX;

	bool CanPushFilter(TableFilter &filter) const;
	bool IsPushableFilterType(TableFilterType type) const;
	double EstimateFilterSelectivity(const string &column_name, TableFilter &filter);
	PushdownDecision MakePushdownDecision(double selectivity, bool has_index) const;
	string TransformFilterToMySQL(const string &column_name, TableFilter &filter) const;
	string TransformComparisonOperator(ExpressionType type) const;
	string CreateConjunction(const vector<string> &predicates, const string &op) const;
	void ReorderPredicatesForIndex(vector<PredicateAnalysis> &analyses, vector<string> &pushed_predicates) const;
	void RecommendIndexHint(FilterAnalysisResult &result) const;

#ifndef NDEBUG
	string GenerateReasoning(double selectivity, bool has_index, PushdownDecision decision) const;
#endif
};

} // namespace duckdb
