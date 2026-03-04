#include "storage/federation/plan_cache.hpp"

#include <algorithm>

namespace duckdb {

ExecutionPlanCacheKey::ExecutionPlanCacheKey(string schema, string table, vector<string> columns,
                                             vector<string> filters, vector<double> sels)
    : schema_name(std::move(schema)), table_name(std::move(table)), column_names(std::move(columns)),
      filter_columns(std::move(filters)), selectivities(std::move(sels)) {
	D_ASSERT(filter_columns.size() == selectivities.size());
	std::sort(column_names.begin(), column_names.end());

	if (!filter_columns.empty()) {
		vector<idx_t> indices(filter_columns.size());
		for (idx_t i = 0; i < indices.size(); i++) {
			indices[i] = i;
		}
		std::sort(indices.begin(), indices.end(),
		          [this](idx_t a, idx_t b) { return filter_columns[a] < filter_columns[b]; });

		vector<string> sorted_filters(filter_columns.size());
		vector<double> sorted_sels(selectivities.size());
		for (idx_t i = 0; i < indices.size(); i++) {
			sorted_filters[i] = filter_columns[indices[i]];
			sorted_sels[i] = selectivities[indices[i]];
		}
		filter_columns = std::move(sorted_filters);
		selectivities = std::move(sorted_sels);
	}
}

bool ExecutionPlanCacheKey::operator==(const ExecutionPlanCacheKey &other) const {
	if (schema_name != other.schema_name || table_name != other.table_name) {
		return false;
	}
	if (column_names.size() != other.column_names.size()) {
		return false;
	}
	for (idx_t i = 0; i < column_names.size(); i++) {
		if (column_names[i] != other.column_names[i]) {
			return false;
		}
	}
	if (filter_columns.size() != other.filter_columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < filter_columns.size(); i++) {
		if (filter_columns[i] != other.filter_columns[i]) {
			return false;
		}
	}
	if (selectivities.size() != other.selectivities.size()) {
		return false;
	}
	for (idx_t i = 0; i < selectivities.size(); i++) {
		if (ExecutionPlanCacheKeyHash::QuantizeSel(selectivities[i]) !=
		    ExecutionPlanCacheKeyHash::QuantizeSel(other.selectivities[i])) {
			return false;
		}
	}
	return true;
}

size_t ExecutionPlanCacheKeyHash::operator()(const ExecutionPlanCacheKey &key) const {
	size_t seed = 0;

	HashCombine(seed, Hash(key.schema_name.c_str(), key.schema_name.size()));
	HashCombine(seed, Hash(key.table_name.c_str(), key.table_name.size()));

	HashCombine(seed, Hash(static_cast<uint64_t>(key.column_names.size())));
	for (const auto &col : key.column_names) {
		HashCombine(seed, Hash(col.c_str(), col.size()));
	}

	HashCombine(seed, Hash(static_cast<uint64_t>(key.filter_columns.size())));
	for (const auto &fc : key.filter_columns) {
		HashCombine(seed, Hash(fc.c_str(), fc.size()));
	}

	for (const auto &sel : key.selectivities) {
		auto quantized = static_cast<uint64_t>(QuantizeSel(sel));
		HashCombine(seed, Hash(quantized));
	}

	return seed;
}

bool PlanCache::GetCachedPlan(const ExecutionPlanCacheKey &key, CachedExecutionPlan &out) {
	lock_guard<std::mutex> lock(mutex_);

	auto it = cache_.find(key);
	if (it == cache_.end()) {
		return false;
	}

	auto &cached = it->second;
	auto now = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - cached.cached_at).count();
	if (static_cast<idx_t>(elapsed) >= PLAN_CACHE_TTL_SECONDS) {
		cache_.erase(it);
		return false;
	}

	cached.hit_count++;
	out = cached;
	return true;
}

idx_t PlanCache::CachePlan(const ExecutionPlanCacheKey &key, const ExecutionPlan &plan) {
	lock_guard<std::mutex> lock(mutex_);

	auto seq = next_eviction_seq_++;

	auto it = cache_.find(key);
	if (it != cache_.end()) {
		it->second.plan = plan;
		it->second.cached_at = std::chrono::steady_clock::now();
		it->second.hit_count = 0;
		it->second.smoothed_actual_rows = 0;
		it->second.execution_count = 0;
		it->second.plan_generation = generation_;
		it->second.eviction_seq = seq;
		EvictionEntry entry;
		entry.key = key;
		entry.seq = seq;
		eviction_queue_.push_back(std::move(entry));
		CompactEvictionQueue();
		return generation_;
	}

	while (cache_.size() >= MAX_PLAN_CACHE_SIZE) {
		if (eviction_queue_.empty()) {
			auto oldest = cache_.begin();
			if (oldest != cache_.end()) {
				cache_.erase(oldest);
			}
			break;
		}
		EvictOldest();
	}

	CachedExecutionPlan cached;
	cached.plan = plan;
	cached.cached_at = std::chrono::steady_clock::now();
	cached.plan_generation = generation_;
	cached.eviction_seq = seq;
	cache_.emplace(key, std::move(cached));
	EvictionEntry entry;
	entry.key = key;
	entry.seq = seq;
	eviction_queue_.push_back(std::move(entry));
	return generation_;
}

bool PlanCache::UpdatePlanFeedback(const ExecutionPlanCacheKey &key, idx_t actual_rows, idx_t expected_generation) {
	lock_guard<std::mutex> lock(mutex_);

	auto it = cache_.find(key);
	if (it == cache_.end()) {
		return false;
	}

	auto &cached = it->second;

	if (cached.plan_generation != expected_generation) {
		return false;
	}

	if (actual_rows < MIN_ROWS_FOR_FEEDBACK) {
		return false;
	}

	if (cached.execution_count == 0) {
		cached.smoothed_actual_rows = actual_rows;
	} else {
		cached.smoothed_actual_rows =
		    static_cast<idx_t>(EMA_OLD_WEIGHT * static_cast<double>(cached.smoothed_actual_rows) +
		                       EMA_NEW_WEIGHT * static_cast<double>(actual_rows));
	}
	cached.execution_count++;

	idx_t replan_count = 0;
	auto rc_it = replan_counts_.find(key);
	if (rc_it != replan_counts_.end()) {
		replan_count = rc_it->second;
	}
	idx_t replan_threshold = MIN_EXECUTIONS_FOR_REPLAN;
	for (idx_t i = 0; i < replan_count && replan_threshold < 1000; i++) {
		replan_threshold *= 2;
	}
	if (cached.execution_count >= replan_threshold) {
		auto estimated = cached.plan.estimated_rows_from_mysql;
		if (estimated > 0) {
			double ratio = static_cast<double>(cached.smoothed_actual_rows) / static_cast<double>(estimated);
			if (ratio > REPLAN_OVERESTIMATE_RATIO || ratio < REPLAN_UNDERESTIMATE_RATIO) {
				replan_counts_[key] = replan_count + 1;
				cache_.erase(it);
				generation_++;
				return true;
			}
		}
	}

	return false;
}

vector<PlanCacheEntry> PlanCache::GetPlanCacheEntries() const {
	lock_guard<std::mutex> lock(mutex_);

	vector<PlanCacheEntry> entries;
	entries.reserve(cache_.size());

	for (const auto &pair : cache_) {
		PlanCacheEntry entry;
		entry.schema_name = pair.first.schema_name;
		entry.table_name = pair.first.table_name;
		entry.strategy = pair.second.plan.strategy;
		entry.filters_pushed = pair.second.plan.pushed_filter_indices.size();
		entry.filters_local = pair.second.plan.local_filter_indices.size();
		entry.hit_count = pair.second.hit_count;
		entry.estimated_rows = pair.second.plan.estimated_rows_from_mysql;
		entry.io_cost = pair.second.plan.estimated_cost.io_cost;
		entry.network_cost = pair.second.plan.estimated_cost.network_cost;
		entry.cpu_cost = pair.second.plan.estimated_cost.cpu_cost;
		entry.total_cost = pair.second.plan.estimated_cost.Total();
		entry.push_all_cost = pair.second.plan.comparison_costs.push_all_cost.Total();
		entry.local_all_cost = pair.second.plan.comparison_costs.local_all_cost.Total();
		entry.hybrid_cost = pair.second.plan.comparison_costs.hybrid_cost.Total();
		entry.combined_selectivity = pair.second.plan.combined_selectivity;
		entry.partition_clause = pair.second.plan.partition_clause;
		entry.execution_count = pair.second.execution_count;
		entry.smoothed_actual_rows = pair.second.smoothed_actual_rows;
		entry.recommended_index = pair.second.plan.recommended_index;
		entries.push_back(std::move(entry));
	}

	return entries;
}

void PlanCache::Clear() {
	lock_guard<std::mutex> lock(mutex_);
	cache_.clear();
	eviction_queue_.clear();
	replan_counts_.clear();
	generation_++;
}

void PlanCache::InvalidateTable(const string &schema, const string &table) {
	lock_guard<std::mutex> lock(mutex_);
	for (auto it = cache_.begin(); it != cache_.end();) {
		if (it->first.schema_name == schema && it->first.table_name == table) {
			it = cache_.erase(it);
		} else {
			++it;
		}
	}
	for (auto it = replan_counts_.begin(); it != replan_counts_.end();) {
		if (it->first.schema_name == schema && it->first.table_name == table) {
			it = replan_counts_.erase(it);
		} else {
			++it;
		}
	}
	generation_++;
}

idx_t PlanCache::GetGeneration() const {
	lock_guard<std::mutex> lock(mutex_);
	return generation_;
}

idx_t PlanCache::Size() const {
	lock_guard<std::mutex> lock(mutex_);
	return cache_.size();
}

void PlanCache::CompactEvictionQueue() {
	if (eviction_queue_.size() <= 2 * MAX_PLAN_CACHE_SIZE) {
		return;
	}
	std::deque<EvictionEntry> compacted;
	for (auto &entry : eviction_queue_) {
		auto it = cache_.find(entry.key);
		if (it != cache_.end() && it->second.eviction_seq == entry.seq) {
			compacted.push_back(std::move(entry));
		}
	}
	eviction_queue_ = std::move(compacted);
}

void PlanCache::EvictOldest() {
	while (!eviction_queue_.empty()) {
		auto entry = std::move(eviction_queue_.front());
		eviction_queue_.pop_front();
		auto it = cache_.find(entry.key);
		if (it != cache_.end() && it->second.eviction_seq == entry.seq) {
			cache_.erase(it);
			return;
		}
	}
}

} // namespace duckdb
