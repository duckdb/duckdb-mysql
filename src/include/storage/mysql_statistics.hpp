//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <chrono>
#include <mutex>

namespace duckdb {

class MySQLConnection;

enum class MySQLPartitionType : uint8_t { NONE, RANGE, RANGE_COLUMNS, LIST, LIST_COLUMNS, HASH, KEY };

struct MySQLPartitionDetail {
	string partition_name;
	idx_t estimated_rows = 0;
	string subpartition_name;
	bool is_subpartition = false;
	string description;
};

struct MySQLPartitionInfo {
	MySQLPartitionType type = MySQLPartitionType::NONE;
	vector<string> partition_columns;
	idx_t partition_count = 0;
	vector<MySQLPartitionDetail> partitions;

	bool IsPartitioned() const {
		return type != MySQLPartitionType::NONE;
	}

	bool IsPartitionColumn(const string &column) const {
		for (const auto &part_col : partition_columns) {
			if (StringUtil::CIEquals(part_col, column)) {
				return true;
			}
		}
		return false;
	}

	idx_t GetTotalPartitionCount() const {
		return partitions.size();
	}

	bool IsRangeOrList() const {
		return type == MySQLPartitionType::RANGE || type == MySQLPartitionType::RANGE_COLUMNS ||
		       type == MySQLPartitionType::LIST || type == MySQLPartitionType::LIST_COLUMNS;
	}

	vector<string> GetMatchingPartitions(const Value &filter_value, ExpressionType comparison) const;
};

struct MySQLIndexInfo {
	string index_name;
	vector<string> columns;
	bool is_unique = false;
	bool is_primary = false;
	idx_t cardinality = DConstants::INVALID_INDEX;
	string index_type;
	idx_t index_size_pages = 0;
	vector<idx_t> prefix_distinct_counts;

	bool CoversColumns(const vector<string> &required_columns) const;
	bool IsLeadingColumn(const string &column) const;
};

struct MySQLHistogramBucket {
	double cumulative_frequency = 0.0;
	double num_distinct = 1.0;
};

struct MySQLHistogram {
	string column_name;
	string histogram_type;
	idx_t num_buckets = 0;
	idx_t null_count = 0;
	double sampling_rate = 1.0;
	vector<Value> bucket_bounds;
	vector<MySQLHistogramBucket> buckets;

	double EstimateSelectivity(ExpressionType comparison, const Value &value, idx_t total_rows) const;
	double EstimateRangeSelectivity(const Value &low, const Value &high, bool low_inclusive, bool high_inclusive) const;
};

struct MySQLTableStats {
	string schema_name;
	string table_name;
	idx_t estimated_row_count = 0;
	idx_t innodb_row_count = 0;
	idx_t avg_row_length = 0;
	idx_t data_length = 0;
	idx_t index_length = 0;
	idx_t clustered_index_size_pages = 0;
	vector<MySQLIndexInfo> indexes;
	case_insensitive_map_t<double> column_null_fraction;
	case_insensitive_map_t<idx_t> column_distinct_count;
	case_insensitive_map_t<MySQLHistogram> histograms;
	bool is_partitioned = false;
	bool is_view = false;
	bool has_innodb_stats = false;
	string engine;
	MySQLPartitionInfo partition_info;
	string row_format;
	int64_t update_time_epoch_us = 0;
	bool has_update_time = false;
	double staleness_score = 0.0;

	bool IsInnoDBCompressed() const {
		return StringUtil::CIEquals(row_format, "COMPRESSED");
	}

	optional_ptr<const MySQLIndexInfo> FindBestIndex(const vector<string> &columns) const;
	optional_ptr<MySQLIndexInfo> FindBestIndex(const vector<string> &columns);
	optional_ptr<const MySQLIndexInfo> FindIndexForColumn(const string &column) const;
	optional_ptr<MySQLIndexInfo> FindIndexForColumn(const string &column);
	bool HasIndexOnColumn(const string &column) const;
	optional_ptr<const MySQLHistogram> GetHistogram(const string &column) const;
	optional_ptr<const MySQLIndexInfo> FindCoveringIndex(const vector<string> &select_columns,
	                                                     const vector<string> &filter_columns) const;
};

struct CachedTableStats {
	MySQLTableStats stats;
	std::chrono::steady_clock::time_point cached_at;
};

class MySQLStatisticsCollector {
public:
	explicit MySQLStatisticsCollector(MySQLConnection &connection);

	static constexpr int64_t STATS_CACHE_TTL_SECONDS = 300;
	static constexpr idx_t MAX_STATS_CACHE_SIZE = 1000;

	MySQLTableStats GetTableStats(const string &schema, const string &table);
	bool HasUsableIndex(const string &schema, const string &table, const vector<string> &columns);
	bool ValidateWithExplain(const string &query, const string &expected_index, bool expect_index_scan);

	struct MySQLCostConstants {
		double io_block_read_cost = 1.0;
		double memory_block_read_cost = 0.25;
		double row_evaluate_cost = 0.1;
		double buffer_pool_hit_rate = -1.0;
		bool loaded = false;
	};
	MySQLCostConstants FetchMySQLCostConstants();

	struct ExplainAnalyzeResult {
		idx_t actual_rows = 0;
		double actual_time_ms = 0.0;
		string access_type;
		bool valid = false;
	};
	ExplainAnalyzeResult RunExplainAnalyze(const string &query);

	bool SupportsExplainAnalyze() const;
	int GetMajorVersion() const;
	int GetMinorVersion() const;
	int GetPatchVersion() const;
	double EstimateSelectivity(const string &schema, const string &table, const string &column,
	                           ExpressionType comparison, const Value &value);

	void ClearCache();
	void ClearCache(const string &schema, const string &table);

private:
	mutable std::mutex mutex_;
	reference<MySQLConnection> connection_;
	case_insensitive_map_t<CachedTableStats> stats_cache_;
	bool has_histogram_support_ = false;
	bool version_detected_ = false;
	bool stats_expiry_set_ = false;
	string mysql_version_;

	void DetectVersion();
	void EnsureFreshStats();
	void FetchTableMetadata(const string &schema, const string &table, MySQLTableStats &stats);
	void FetchInnoDBStats(const string &schema, const string &table, MySQLTableStats &stats);
	void FetchIndexInfo(const string &schema, const string &table, MySQLTableStats &stats);
	void FetchColumnStats(const string &schema, const string &table, MySQLTableStats &stats);
	void FetchPartitionInfo(const string &schema, const string &table, MySQLTableStats &stats);
	void FetchHistograms(const string &schema, const string &table, MySQLTableStats &stats);

	string GetCacheKey(const string &schema, const string &table) const;
	double GetDefaultSelectivity(ExpressionType comparison) const;
	double EstimateSelectivityFromCardinality(idx_t cardinality, idx_t total_rows) const;
	static double ComputeStalenessScore(const MySQLTableStats &stats);
};

} // namespace duckdb
