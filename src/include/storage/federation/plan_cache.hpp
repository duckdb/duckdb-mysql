//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/federation/plan_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/federation/cost_model.hpp"
#include "duckdb/common/types/hash.hpp"

#include <chrono>
#include <cmath>
#include <deque>
#include <mutex>
#include <unordered_map>

namespace duckdb {

struct ExecutionPlanCacheKey {
	string schema_name;
	string table_name;
	vector<string> column_names;
	vector<string> filter_columns;
	vector<double> selectivities;

	ExecutionPlanCacheKey() = default;

	ExecutionPlanCacheKey(string schema, string table, vector<string> columns, vector<string> filters,
	                      vector<double> sels);

	bool operator==(const ExecutionPlanCacheKey &other) const;
};

struct ExecutionPlanCacheKeyHash {
	static constexpr uint64_t GOLDEN_RATIO_64 = 0x9e3779b97f4a7c15ULL;

	static void HashCombine(size_t &seed, size_t value) {
		seed ^= value + GOLDEN_RATIO_64 + (seed << 6) + (seed >> 2);
	}

	static int64_t QuantizeSel(double selectivity) {
		if (!std::isfinite(selectivity)) {
			return 0;
		}
		double clamped = selectivity < 0.0 ? 0.0 : (selectivity > 1.0 ? 1.0 : selectivity);
		return static_cast<int64_t>(clamped * 1000.0);
	}

	size_t operator()(const ExecutionPlanCacheKey &key) const;
};

struct CachedExecutionPlan {
	ExecutionPlan plan;
	std::chrono::steady_clock::time_point cached_at;
	idx_t hit_count = 0;
	idx_t smoothed_actual_rows = 0;
	idx_t execution_count = 0;
	idx_t plan_generation = 0;
	idx_t eviction_seq = 0;
};

struct EvictionEntry {
	ExecutionPlanCacheKey key;
	idx_t seq;
};

struct PlanCacheEntry {
	string schema_name;
	string table_name;
	ExecutionStrategy strategy = ExecutionStrategy::PUSH_ALL_FILTERS;
	idx_t filters_pushed = 0;
	idx_t filters_local = 0;
	idx_t hit_count = 0;
	idx_t estimated_rows = 0;
	double io_cost = 0.0;
	double network_cost = 0.0;
	double cpu_cost = 0.0;
	double total_cost = 0.0;
	double push_all_cost = 0.0;
	double local_all_cost = 0.0;
	double hybrid_cost = 0.0;
	double combined_selectivity = 0.0;
	string partition_clause;
	idx_t execution_count = 0;
	idx_t smoothed_actual_rows = 0;
	string recommended_index;
};

class PlanCache {
public:
	static constexpr idx_t MAX_PLAN_CACHE_SIZE = 1000;
	static constexpr idx_t PLAN_CACHE_TTL_SECONDS = 300;
	static constexpr double EMA_NEW_WEIGHT = 0.3;
	static constexpr double EMA_OLD_WEIGHT = 0.7;
	static constexpr idx_t MIN_ROWS_FOR_FEEDBACK = 1000;
	static constexpr idx_t MIN_EXECUTIONS_FOR_REPLAN = 3;
	static constexpr double REPLAN_OVERESTIMATE_RATIO = 2.0;
	static constexpr double REPLAN_UNDERESTIMATE_RATIO = 0.3;

	PlanCache() = default;

	bool GetCachedPlan(const ExecutionPlanCacheKey &key, CachedExecutionPlan &out);

	idx_t CachePlan(const ExecutionPlanCacheKey &key, const ExecutionPlan &plan);

	bool UpdatePlanFeedback(const ExecutionPlanCacheKey &key, idx_t actual_rows, idx_t expected_generation);

	vector<PlanCacheEntry> GetPlanCacheEntries() const;

	void Clear();
	void InvalidateTable(const string &schema, const string &table);

	idx_t GetGeneration() const;

	idx_t Size() const;

private:
	mutable std::mutex mutex_;
	std::unordered_map<ExecutionPlanCacheKey, CachedExecutionPlan, ExecutionPlanCacheKeyHash> cache_;
	std::unordered_map<ExecutionPlanCacheKey, idx_t, ExecutionPlanCacheKeyHash> replan_counts_;
	std::deque<EvictionEntry> eviction_queue_;
	idx_t generation_ = 0;
	idx_t next_eviction_seq_ = 0;

	void CompactEvictionQueue();
	void EvictOldest();
};

} // namespace duckdb
