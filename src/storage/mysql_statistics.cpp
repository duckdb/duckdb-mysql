#include "storage/mysql_statistics.hpp"
#include "mysql_connection.hpp"
#include "mysql_result.hpp"
#include "yyjson.hpp"

namespace duckdb {

bool MySQLIndexInfo::CoversColumns(const vector<string> &required_columns) const {
	if (required_columns.empty()) {
		return false;
	}
	for (const auto &req_col : required_columns) {
		bool found = false;
		for (const auto &idx_col : columns) {
			if (StringUtil::CIEquals(req_col, idx_col)) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

bool MySQLIndexInfo::IsLeadingColumn(const string &column) const {
	if (columns.empty()) {
		return false;
	}
	return StringUtil::CIEquals(columns[0], column);
}

optional_ptr<const MySQLIndexInfo> MySQLTableStats::FindBestIndex(const vector<string> &required_columns) const {
	if (required_columns.empty()) {
		return nullptr;
	}
	optional_ptr<const MySQLIndexInfo> best_match = nullptr;
	idx_t best_match_columns = 0;

	case_insensitive_set_t required_set(required_columns.begin(), required_columns.end());

	for (const auto &index : indexes) {
		idx_t matched_columns = 0;
		for (const auto &idx_col : index.columns) {
			if (required_set.count(idx_col)) {
				matched_columns++;
			} else {
				break;
			}
		}
		if (matched_columns > best_match_columns) {
			best_match_columns = matched_columns;
			best_match = &index;
		} else if (matched_columns == best_match_columns && matched_columns > 0 && index.is_primary) {
			best_match = &index;
		}
	}
	return best_match;
}

optional_ptr<MySQLIndexInfo> MySQLTableStats::FindBestIndex(const vector<string> &required_columns) {
	auto result = const_cast<const MySQLTableStats *>(this)->FindBestIndex(required_columns);
	return const_cast<MySQLIndexInfo *>(result.get());
}

optional_ptr<const MySQLIndexInfo> MySQLTableStats::FindIndexForColumn(const string &column) const {
	for (const auto &index : indexes) {
		if (index.IsLeadingColumn(column)) {
			return &index;
		}
	}
	return nullptr;
}

optional_ptr<MySQLIndexInfo> MySQLTableStats::FindIndexForColumn(const string &column) {
	auto result = const_cast<const MySQLTableStats *>(this)->FindIndexForColumn(column);
	return const_cast<MySQLIndexInfo *>(result.get());
}

bool MySQLTableStats::HasIndexOnColumn(const string &column) const {
	return FindIndexForColumn(column) != nullptr;
}

optional_ptr<const MySQLHistogram> MySQLTableStats::GetHistogram(const string &column) const {
	auto it = histograms.find(column);
	if (it != histograms.end()) {
		return &it->second;
	}
	return nullptr;
}

optional_ptr<const MySQLIndexInfo> MySQLTableStats::FindCoveringIndex(const vector<string> &select_columns,
                                                                      const vector<string> &filter_columns) const {
	vector<string> all_columns;
	all_columns.reserve(select_columns.size() + filter_columns.size());

	for (const auto &col : select_columns) {
		all_columns.push_back(col);
	}
	for (const auto &col : filter_columns) {
		bool already_added = false;
		for (const auto &existing : all_columns) {
			if (StringUtil::CIEquals(existing, col)) {
				already_added = true;
				break;
			}
		}
		if (!already_added) {
			all_columns.push_back(col);
		}
	}

	if (all_columns.empty()) {
		return nullptr;
	}

	vector<string> pk_columns;
	if (StringUtil::CIEquals(engine, "InnoDB")) {
		for (const auto &index : indexes) {
			if (index.is_primary) {
				pk_columns = index.columns;
				break;
			}
		}
	}

	vector<string> non_pk_columns;
	if (!pk_columns.empty()) {
		for (const auto &col : all_columns) {
			bool is_pk = false;
			for (const auto &pk_col : pk_columns) {
				if (StringUtil::CIEquals(col, pk_col)) {
					is_pk = true;
					break;
				}
			}
			if (!is_pk) {
				non_pk_columns.push_back(col);
			}
		}
	}

	const auto &check_columns = pk_columns.empty() ? all_columns : non_pk_columns;

	if (check_columns.empty()) {
		for (const auto &index : indexes) {
			if (index.is_primary) {
				return &index;
			}
		}
		return indexes.empty() ? nullptr : &indexes[0];
	}

	for (const auto &index : indexes) {
		if (index.CoversColumns(check_columns)) {
			bool has_leading_filter = false;
			for (const auto &filter_col : filter_columns) {
				if (index.IsLeadingColumn(filter_col)) {
					has_leading_filter = true;
					break;
				}
			}
			if (has_leading_filter) {
				return &index;
			}
		}
	}

	for (const auto &index : indexes) {
		if (index.CoversColumns(check_columns)) {
			return &index;
		}
	}

	return nullptr;
}

vector<string> MySQLPartitionInfo::GetMatchingPartitions(const Value &filter_value, ExpressionType comparison) const {
	vector<string> matching;

	if (!IsRangeOrList() || partitions.empty()) {
		return matching;
	}

	if (type == MySQLPartitionType::RANGE || type == MySQLPartitionType::RANGE_COLUMNS) {
		idx_t num_parts = partitions.size();
		bool found_containing = false;

		for (idx_t i = 0; i < num_parts; i++) {
			const auto &part = partitions[i];
			if (part.description.empty() || part.is_subpartition) {
				continue;
			}

			bool is_maxvalue = (part.description == "MAXVALUE");
			Value bound;
			if (!is_maxvalue) {
				if (type == MySQLPartitionType::RANGE) {
					try {
						bound = Value(static_cast<int64_t>(std::stoll(part.description)));
					} catch (const std::exception &) {
						return {};
					}
					Value test_val = filter_value;
					if (!test_val.DefaultTryCastAs(bound.type())) {
						return {};
					}
				} else {
					try {
						bound = Value(part.description).DefaultCastAs(filter_value.type());
					} catch (const std::exception &) {
						return {};
					}
				}
			}

			bool value_in_partition = is_maxvalue || (filter_value < bound);

			switch (comparison) {
			case ExpressionType::COMPARE_EQUAL:
				if (value_in_partition && !found_containing) {
					matching.push_back(part.partition_name);
					return matching;
				}
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				if (!found_containing) {
					if (is_maxvalue || filter_value <= bound) {
						matching.push_back(part.partition_name);
						found_containing = true;
					} else {
						matching.push_back(part.partition_name);
					}
				}
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				if (!found_containing) {
					matching.push_back(part.partition_name);
					if (is_maxvalue || filter_value < bound) {
						found_containing = true;
					}
				}
				break;
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				if (value_in_partition && !found_containing) {
					found_containing = true;
					matching.push_back(part.partition_name);
				} else if (found_containing) {
					matching.push_back(part.partition_name);
				}
				break;
			default:
				return {};
			}
		}
	} else if (type == MySQLPartitionType::LIST || type == MySQLPartitionType::LIST_COLUMNS) {
		if (comparison != ExpressionType::COMPARE_EQUAL) {
			return {};
		}
		string filter_str = filter_value.ToString();
		for (const auto &part : partitions) {
			if (part.description.empty() || part.is_subpartition) {
				continue;
			}
			auto values = StringUtil::Split(part.description, ",");
			for (auto &val : values) {
				StringUtil::Trim(val);
				if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
					val = val.substr(1, val.size() - 2);
				}
				if (val == filter_str) {
					matching.push_back(part.partition_name);
					break;
				}
			}
		}
	}

	return matching;
}

double MySQLHistogram::EstimateSelectivity(ExpressionType comparison, const Value &value, idx_t total_rows) const {
	if (buckets.empty() || bucket_bounds.empty()) {
		return -1.0;
	}

	if (histogram_type == "singleton") {
		for (idx_t i = 0; i < bucket_bounds.size() && i < buckets.size(); i++) {
			if (bucket_bounds[i] == value) {
				double prev_freq = (i > 0) ? buckets[i - 1].cumulative_frequency : 0.0;
				return buckets[i].cumulative_frequency - prev_freq;
			}
		}
		switch (comparison) {
		case ExpressionType::COMPARE_EQUAL:
			return 0.0;
		case ExpressionType::COMPARE_NOTEQUAL:
			return 1.0;
		default:
			break;
		}
	}

	if (histogram_type == "equi-height") {
		idx_t bucket_idx = 0;
		for (idx_t i = 0; i < bucket_bounds.size(); i++) {
			if (value <= bucket_bounds[i]) {
				bucket_idx = i;
				break;
			}
			bucket_idx = i;
		}

		double bucket_freq = (bucket_idx < buckets.size())
		                         ? (bucket_idx > 0 ? buckets[bucket_idx].cumulative_frequency -
		                                                 buckets[bucket_idx - 1].cumulative_frequency
		                                           : buckets[bucket_idx].cumulative_frequency)
		                         : 0.0;
		double bucket_distinct = (bucket_idx < buckets.size() && buckets[bucket_idx].num_distinct > 0)
		                             ? buckets[bucket_idx].num_distinct
		                             : 1;

		switch (comparison) {
		case ExpressionType::COMPARE_EQUAL:
			return bucket_freq / bucket_distinct;
		case ExpressionType::COMPARE_NOTEQUAL:
			return 1.0 - (bucket_freq / bucket_distinct);
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return (bucket_idx < buckets.size()) ? buckets[bucket_idx].cumulative_frequency : 1.0;
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return 1.0 - ((bucket_idx < buckets.size()) ? buckets[bucket_idx].cumulative_frequency : 1.0);
		default:
			return -1.0;
		}
	}

	return -1.0;
}

double MySQLHistogram::EstimateRangeSelectivity(const Value &low, const Value &high, bool low_inclusive,
                                                bool high_inclusive) const {
	if (buckets.empty() || bucket_bounds.empty()) {
		return -1.0;
	}

	double low_freq = 0.0;
	double high_freq = 1.0;

	for (idx_t i = 0; i < bucket_bounds.size() && i < buckets.size(); i++) {
		if (low <= bucket_bounds[i]) {
			low_freq = (i > 0) ? buckets[i - 1].cumulative_frequency : 0.0;
			break;
		}
	}

	for (idx_t i = 0; i < bucket_bounds.size() && i < buckets.size(); i++) {
		if (high <= bucket_bounds[i]) {
			high_freq = buckets[i].cumulative_frequency;
			break;
		}
	}

	return std::max(0.0, high_freq - low_freq);
}

MySQLStatisticsCollector::MySQLStatisticsCollector(MySQLConnection &connection) : connection_(connection) {
}

double MySQLStatisticsCollector::ComputeStalenessScore(const MySQLTableStats &stats) {
	double score = 0.0;

	if (stats.has_update_time) {
		auto now_us = Timestamp::GetCurrentTimestamp().value;
		int64_t age_us = now_us - stats.update_time_epoch_us;
		int64_t days = age_us / (1000000LL * 60 * 60 * 24);
		if (days > 30) {
			score += 0.3;
		} else if (days > 7) {
			score += 0.1;
		}
	} else if (!stats.is_partitioned) {
		score += 0.2;
	}

	if (stats.histograms.empty() && stats.estimated_row_count > 10000) {
		score += 0.2;
	}

	for (const auto &idx : stats.indexes) {
		if (idx.is_primary && idx.cardinality != DConstants::INVALID_INDEX &&
		    idx.cardinality > stats.estimated_row_count + stats.estimated_row_count / 2) {
			score += 0.3;
			break;
		}
	}

	return std::min(score, 1.0);
}

void MySQLStatisticsCollector::DetectVersion() {
	if (version_detected_) {
		return;
	}
	version_detected_ = true;

	auto result =
	    connection_.get().Query("SELECT @@version AS mysql_version", MySQLResultStreaming::FORCE_MATERIALIZATION);

	if (result->Exhausted()) {
		return;
	}

	auto &chunk = result->NextChunk();
	if (chunk.size() == 0) {
		return;
	}

	mysql_version_ = chunk.data[0].GetValue(0).ToString();
	bool is_mariadb = StringUtil::Contains(StringUtil::Lower(mysql_version_), "mariadb");
	has_histogram_support_ = (!is_mariadb && GetMajorVersion() >= 8);
}

string MySQLStatisticsCollector::GetCacheKey(const string &schema, const string &table) const {
	string key;
	key.reserve(schema.size() + 1 + table.size());
	key += schema;
	key += ".";
	key += table;
	return key;
}

void MySQLStatisticsCollector::ClearCache() {
	std::lock_guard<std::mutex> lock(mutex_);
	stats_cache_.clear();
}

void MySQLStatisticsCollector::ClearCache(const string &schema, const string &table) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto key = GetCacheKey(schema, table);
	stats_cache_.erase(key);
}

void MySQLStatisticsCollector::EnsureFreshStats() {
	if (stats_expiry_set_) {
		return;
	}
	stats_expiry_set_ = true;
	try {
		connection_.get().Query("SET SESSION information_schema_stats_expiry = 0",
		                        MySQLResultStreaming::FORCE_MATERIALIZATION);
	} catch (const std::exception &) {
	}
}

MySQLTableStats MySQLStatisticsCollector::GetTableStats(const string &schema, const string &table) {
	std::lock_guard<std::mutex> lock(mutex_);
	DetectVersion();
	EnsureFreshStats();

	auto cache_key = GetCacheKey(schema, table);
	auto it = stats_cache_.find(cache_key);
	if (it != stats_cache_.end()) {
		auto elapsed = std::chrono::steady_clock::now() - it->second.cached_at;
		auto seconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
		if (seconds < STATS_CACHE_TTL_SECONDS) {
			return it->second.stats;
		}
		stats_cache_.erase(it);
	}

	MySQLTableStats stats;
	stats.schema_name = schema;
	stats.table_name = table;

	FetchTableMetadata(schema, table, stats);
	FetchIndexInfo(schema, table, stats);
	FetchInnoDBStats(schema, table, stats);
	FetchColumnStats(schema, table, stats);
	FetchPartitionInfo(schema, table, stats);
	if (has_histogram_support_) {
		FetchHistograms(schema, table, stats);
		if (stats.estimated_row_count > 0) {
			for (const auto &hist_entry : stats.histograms) {
				const auto &hist = hist_entry.second;
				if (hist.null_count > 0) {
					double null_frac =
					    static_cast<double>(hist.null_count) / static_cast<double>(stats.estimated_row_count);
					stats.column_null_fraction[hist.column_name] = std::min(null_frac, 1.0);
				} else {
					stats.column_null_fraction[hist.column_name] = 0.0;
				}
			}
		}
	}

	stats.staleness_score = ComputeStalenessScore(stats);

	if (stats_cache_.size() >= MAX_STATS_CACHE_SIZE) {
		auto oldest = stats_cache_.begin();
		for (auto sit = stats_cache_.begin(); sit != stats_cache_.end(); ++sit) {
			if (sit->second.cached_at < oldest->second.cached_at) {
				oldest = sit;
			}
		}
		stats_cache_.erase(oldest);
	}

	CachedTableStats cached;
	cached.stats = stats;
	cached.cached_at = std::chrono::steady_clock::now();
	stats_cache_[cache_key] = std::move(cached);
	return stats;
}

void MySQLStatisticsCollector::FetchTableMetadata(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH, ENGINE, "
	               "IF(CREATE_OPTIONS LIKE '%partitioned%', 1, 0) AS is_partitioned, "
	               "IF(TABLE_TYPE = 'VIEW', 1, 0) AS is_view, "
	               "ROW_FORMAT, UPDATE_TIME "
	               "FROM information_schema.TABLES "
	               "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (result->Exhausted()) {
		return;
	}

	auto &chunk = result->NextChunk();
	if (chunk.size() == 0) {
		return;
	}

	auto row_count_val = chunk.data[0].GetValue(0);
	if (!row_count_val.IsNull()) {
		int64_t row_count = row_count_val.GetValue<int64_t>();
		stats.estimated_row_count = (row_count > 0) ? static_cast<idx_t>(row_count) : 0;
	}

	auto avg_row_val = chunk.data[1].GetValue(0);
	if (!avg_row_val.IsNull()) {
		int64_t avg_row_length = avg_row_val.GetValue<int64_t>();
		stats.avg_row_length = (avg_row_length > 0) ? static_cast<idx_t>(avg_row_length) : 0;
	}

	auto data_len_val = chunk.data[2].GetValue(0);
	if (!data_len_val.IsNull()) {
		int64_t data_length = data_len_val.GetValue<int64_t>();
		stats.data_length = (data_length > 0) ? static_cast<idx_t>(data_length) : 0;
	}

	auto index_len_val = chunk.data[3].GetValue(0);
	if (!index_len_val.IsNull()) {
		int64_t index_length = index_len_val.GetValue<int64_t>();
		stats.index_length = (index_length > 0) ? static_cast<idx_t>(index_length) : 0;
	}

	auto engine_val = chunk.data[4].GetValue(0);
	if (!engine_val.IsNull()) {
		stats.engine = engine_val.ToString();
	}

	auto partitioned_val = chunk.data[5].GetValue(0);
	if (!partitioned_val.IsNull()) {
		stats.is_partitioned = partitioned_val.GetValue<int64_t>() != 0;
	}

	auto view_val = chunk.data[6].GetValue(0);
	if (!view_val.IsNull()) {
		stats.is_view = view_val.GetValue<int64_t>() != 0;
	}

	auto row_format_val = chunk.data[7].GetValue(0);
	if (!row_format_val.IsNull()) {
		stats.row_format = row_format_val.ToString();
	}

	auto update_time_val = chunk.data[8].GetValue(0);
	if (!update_time_val.IsNull()) {
		auto ts = update_time_val.GetValue<timestamp_t>();
		stats.update_time_epoch_us = Timestamp::GetEpochMicroSeconds(ts);
		stats.has_update_time = true;
	}
}

void MySQLStatisticsCollector::FetchInnoDBStats(const string &schema, const string &table, MySQLTableStats &stats) {
	if (!StringUtil::CIEquals(stats.engine, "InnoDB")) {
		return;
	}

	try {
		string table_stats_query = "SELECT n_rows, clustered_index_size, sum_of_other_index_sizes "
		                           "FROM mysql.innodb_table_stats "
		                           "WHERE database_name = ? AND table_name = ?";
		vector<Value> params;
		params.push_back(Value(schema));
		params.push_back(Value(table));

		auto result = connection_.get().Query(table_stats_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
		if (!result->Exhausted()) {
			auto &chunk = result->NextChunk();
			if (chunk.size() > 0) {
				auto n_rows_val = chunk.data[0].GetValue(0);
				if (!n_rows_val.IsNull()) {
					int64_t n_rows = n_rows_val.GetValue<int64_t>();
					stats.innodb_row_count = (n_rows > 0) ? static_cast<idx_t>(n_rows) : 0;
					if (stats.innodb_row_count > 0) {
						stats.estimated_row_count = stats.innodb_row_count;
					}
				}
				auto clustered_size_val = chunk.data[1].GetValue(0);
				if (!clustered_size_val.IsNull()) {
					int64_t cs = clustered_size_val.GetValue<int64_t>();
					stats.clustered_index_size_pages = (cs > 0) ? static_cast<idx_t>(cs) : 0;
				}
				stats.has_innodb_stats = true;
			}
		}
	} catch (const std::exception &) {
		return;
	}

	try {
		for (auto &index : stats.indexes) {
			string idx_name = index.is_primary ? "PRIMARY" : index.index_name;

			string index_stats_query = "SELECT stat_name, stat_value "
			                           "FROM mysql.innodb_index_stats "
			                           "WHERE database_name = ? AND table_name = ? AND index_name = ? "
			                           "AND (stat_name LIKE 'n_diff_pfx%' OR stat_name = 'size')";
			vector<Value> params;
			params.push_back(Value(schema));
			params.push_back(Value(table));
			params.push_back(Value(idx_name));

			auto result =
			    connection_.get().Query(index_stats_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
			if (result->Exhausted()) {
				continue;
			}

			while (!result->Exhausted()) {
				auto &chunk = result->NextChunk();
				for (idx_t row = 0; row < chunk.size(); row++) {
					string stat_name = chunk.data[0].GetValue(row).ToString();
					int64_t stat_value = chunk.data[1].GetValue(row).GetValue<int64_t>();

					if (stat_name == "size") {
						index.index_size_pages = (stat_value > 0) ? static_cast<idx_t>(stat_value) : 0;
					} else if (StringUtil::StartsWith(stat_name, "n_diff_pfx")) {
						string num_str = stat_name.substr(10);
						try {
							idx_t prefix_idx = static_cast<idx_t>(std::stoull(num_str));
							if (prefix_idx > 0) {
								idx_t arr_idx = prefix_idx - 1;
								if (index.prefix_distinct_counts.size() <= arr_idx) {
									index.prefix_distinct_counts.resize(arr_idx + 1, 0);
								}
								index.prefix_distinct_counts[arr_idx] =
								    (stat_value > 0) ? static_cast<idx_t>(stat_value) : 0;
							}
						} catch (const std::exception &) {
						}
					}
				}
			}

			if (!index.prefix_distinct_counts.empty() && index.prefix_distinct_counts[0] > 0) {
				if (!index.columns.empty()) {
					stats.column_distinct_count[index.columns[0]] = index.prefix_distinct_counts[0];
				}
			}
		}
	} catch (const std::exception &) {
	}
}

void MySQLStatisticsCollector::FetchIndexInfo(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, IFNULL(CARDINALITY, 0), "
	               "INDEX_TYPE, IF(INDEX_NAME = 'PRIMARY', 1, 0) AS is_primary, IF(NON_UNIQUE = 0, 1, 0) AS is_unique "
	               "FROM information_schema.STATISTICS "
	               "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
	               "ORDER BY INDEX_NAME, SEQ_IN_INDEX";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (result->Exhausted()) {
		return;
	}

	case_insensitive_map_t<MySQLIndexInfo> index_map;

	while (!result->Exhausted()) {
		auto &chunk = result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			string index_name = chunk.data[0].GetValue(row).ToString();
			string column_name = chunk.data[2].GetValue(row).ToString();
			int64_t cardinality_val = chunk.data[3].GetValue(row).GetValue<int64_t>();
			idx_t cardinality = (cardinality_val > 0) ? static_cast<idx_t>(cardinality_val) : 0;
			string index_type = chunk.data[4].GetValue(row).ToString();
			bool is_primary = chunk.data[5].GetValue(row).GetValue<int64_t>() != 0;
			bool is_unique = chunk.data[6].GetValue(row).GetValue<int64_t>() != 0;

			auto it = index_map.find(index_name);
			if (it == index_map.end()) {
				MySQLIndexInfo info;
				info.index_name = index_name;
				info.index_type = index_type;
				info.is_primary = is_primary;
				info.is_unique = is_unique;
				info.cardinality = cardinality;
				info.columns.push_back(column_name);
				index_map[index_name] = std::move(info);
			} else {
				it->second.columns.push_back(column_name);
			}
		}
	}

	for (auto &entry : index_map) {
		stats.indexes.push_back(std::move(entry.second));
	}
}

void MySQLStatisticsCollector::FetchPartitionInfo(const string &schema, const string &table, MySQLTableStats &stats) {
	if (!stats.is_partitioned) {
		return;
	}

	string method_query = "SELECT PARTITION_METHOD, PARTITION_EXPRESSION "
	                      "FROM information_schema.PARTITIONS "
	                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL "
	                      "LIMIT 1";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto method_result = connection_.get().Query(method_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (method_result->Exhausted()) {
		return;
	}

	auto &method_chunk = method_result->NextChunk();
	if (method_chunk.size() == 0) {
		return;
	}

	auto method_val = method_chunk.data[0].GetValue(0);
	if (!method_val.IsNull()) {
		string method = method_val.ToString();
		if (method == "RANGE") {
			stats.partition_info.type = MySQLPartitionType::RANGE;
		} else if (method == "RANGE COLUMNS") {
			stats.partition_info.type = MySQLPartitionType::RANGE_COLUMNS;
		} else if (method == "LIST") {
			stats.partition_info.type = MySQLPartitionType::LIST;
		} else if (method == "LIST COLUMNS") {
			stats.partition_info.type = MySQLPartitionType::LIST_COLUMNS;
		} else if (method == "HASH") {
			stats.partition_info.type = MySQLPartitionType::HASH;
		} else if (method == "KEY") {
			stats.partition_info.type = MySQLPartitionType::KEY;
		}
	}

	auto expr_val = method_chunk.data[1].GetValue(0);
	if (!expr_val.IsNull()) {
		string expr = expr_val.ToString();
		if (stats.partition_info.type == MySQLPartitionType::RANGE_COLUMNS ||
		    stats.partition_info.type == MySQLPartitionType::LIST_COLUMNS ||
		    stats.partition_info.type == MySQLPartitionType::KEY) {
			auto columns = StringUtil::Split(expr, ",");
			for (auto &col : columns) {
				StringUtil::Trim(col);
				if (!col.empty() && col.front() == '`' && col.back() == '`') {
					col = col.substr(1, col.size() - 2);
				}
				stats.partition_info.partition_columns.push_back(col);
			}
		} else {
			size_t start = expr.find('(');
			size_t end = expr.rfind(')');
			if (start != string::npos && end != string::npos && start < end) {
				string col = expr.substr(start + 1, end - start - 1);
				StringUtil::Trim(col);
				if (!col.empty() && col.front() == '`' && col.back() == '`') {
					col = col.substr(1, col.size() - 2);
				}
				stats.partition_info.partition_columns.push_back(col);
			} else {
				string col = expr;
				StringUtil::Trim(col);
				if (!col.empty() && col.front() == '`' && col.back() == '`') {
					col = col.substr(1, col.size() - 2);
				}
				if (!col.empty()) {
					stats.partition_info.partition_columns.push_back(col);
				}
			}
		}
	}

	string partition_query = "SELECT PARTITION_NAME, SUBPARTITION_NAME, TABLE_ROWS, PARTITION_DESCRIPTION "
	                         "FROM information_schema.PARTITIONS "
	                         "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL "
	                         "ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION";

	auto partition_result =
	    connection_.get().Query(partition_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (partition_result->Exhausted()) {
		return;
	}

	while (!partition_result->Exhausted()) {
		auto &chunk = partition_result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			MySQLPartitionDetail detail;

			auto part_name_val = chunk.data[0].GetValue(row);
			if (!part_name_val.IsNull()) {
				detail.partition_name = part_name_val.ToString();
			}

			auto subpart_name_val = chunk.data[1].GetValue(row);
			if (!subpart_name_val.IsNull()) {
				detail.subpartition_name = subpart_name_val.ToString();
				detail.is_subpartition = true;
			}

			auto rows_val = chunk.data[2].GetValue(row);
			if (!rows_val.IsNull()) {
				int64_t rows = rows_val.GetValue<int64_t>();
				detail.estimated_rows = (rows > 0) ? static_cast<idx_t>(rows) : 0;
			}

			auto desc_val = chunk.data[3].GetValue(row);
			if (!desc_val.IsNull()) {
				detail.description = desc_val.ToString();
			}

			stats.partition_info.partitions.push_back(std::move(detail));
		}
	}

	stats.partition_info.partition_count = stats.partition_info.partitions.size();
}

void MySQLStatisticsCollector::FetchColumnStats(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT COLUMN_NAME, IS_NULLABLE "
	               "FROM information_schema.COLUMNS "
	               "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (result->Exhausted()) {
		return;
	}

	while (!result->Exhausted()) {
		auto &chunk = result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			string column_name = chunk.data[0].GetValue(row).ToString();
			string is_nullable = chunk.data[1].GetValue(row).ToString();
			stats.column_null_fraction[column_name] = (is_nullable == "YES") ? 0.01 : 0.0;
		}
	}

	for (const auto &index : stats.indexes) {
		if (!index.columns.empty() && index.cardinality != DConstants::INVALID_INDEX && index.cardinality > 0) {
			if (stats.column_distinct_count.find(index.columns[0]) == stats.column_distinct_count.end()) {
				stats.column_distinct_count[index.columns[0]] = index.cardinality;
			}
		}
	}
}

void MySQLStatisticsCollector::FetchHistograms(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT COLUMN_NAME, HISTOGRAM "
	               "FROM information_schema.COLUMN_STATISTICS "
	               "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	unique_ptr<MySQLResult> result;
	try {
		result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	} catch (const std::exception &) {
		return;
	}

	if (!result || result->Exhausted()) {
		return;
	}

	while (!result->Exhausted()) {
		auto &chunk = result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			string column_name = chunk.data[0].GetValue(row).ToString();
			string histogram_json = chunk.data[1].GetValue(row).ToString();

			MySQLHistogram hist;
			hist.column_name = column_name;

			using namespace duckdb_yyjson; // NOLINT
			unique_ptr<yyjson_doc, void (*)(yyjson_doc *)> doc(
			    yyjson_read(histogram_json.c_str(), histogram_json.size(), YYJSON_READ_NOFLAG), yyjson_doc_free);
			if (!doc) {
				continue;
			}
			yyjson_val *root = yyjson_doc_get_root(doc.get());
			if (!root || !yyjson_is_obj(root)) {
				continue;
			}

			yyjson_val *type_val = yyjson_obj_get(root, "histogram-type");
			if (type_val && yyjson_is_str(type_val)) {
				hist.histogram_type = yyjson_get_str(type_val);
			}

			yyjson_val *num_buckets_val = yyjson_obj_get(root, "number-of-buckets-specified");
			if (num_buckets_val) {
				hist.num_buckets = static_cast<idx_t>(yyjson_get_uint(num_buckets_val));
			}

			yyjson_val *sampling_val = yyjson_obj_get(root, "sampling-rate");
			if (sampling_val) {
				hist.sampling_rate = yyjson_get_num(sampling_val);
			}

			yyjson_val *null_val = yyjson_obj_get(root, "null-values");
			if (null_val) {
				double null_frac = yyjson_get_num(null_val);
				hist.null_count = static_cast<idx_t>(null_frac * static_cast<double>(stats.estimated_row_count));
			}

			yyjson_val *buckets_arr = yyjson_obj_get(root, "buckets");
			if (buckets_arr && yyjson_is_arr(buckets_arr)) {
				size_t bucket_idx, bucket_max;
				yyjson_val *bucket_elem;
				yyjson_arr_foreach(buckets_arr, bucket_idx, bucket_max, bucket_elem) {
					if (!yyjson_is_arr(bucket_elem)) {
						continue;
					}
					size_t elem_count = yyjson_arr_size(bucket_elem);
					MySQLHistogramBucket bucket;

					if (hist.histogram_type == "singleton" && elem_count >= 2) {
						yyjson_val *val_elem = yyjson_arr_get(bucket_elem, 0);
						yyjson_val *freq_elem = yyjson_arr_get(bucket_elem, 1);

						if (yyjson_is_str(val_elem)) {
							hist.bucket_bounds.push_back(Value(string(yyjson_get_str(val_elem))));
						} else if (yyjson_is_int(val_elem)) {
							hist.bucket_bounds.push_back(Value(yyjson_get_sint(val_elem)));
						} else {
							hist.bucket_bounds.push_back(Value(yyjson_get_num(val_elem)));
						}
						bucket.cumulative_frequency = yyjson_get_num(freq_elem);

					} else if (hist.histogram_type == "equi-height" && elem_count >= 4) {
						yyjson_val *upper_elem = yyjson_arr_get(bucket_elem, 1);
						yyjson_val *freq_elem = yyjson_arr_get(bucket_elem, 2);
						yyjson_val *distinct_elem = yyjson_arr_get(bucket_elem, 3);

						if (yyjson_is_str(upper_elem)) {
							hist.bucket_bounds.push_back(Value(string(yyjson_get_str(upper_elem))));
						} else if (yyjson_is_int(upper_elem)) {
							hist.bucket_bounds.push_back(Value(yyjson_get_sint(upper_elem)));
						} else {
							hist.bucket_bounds.push_back(Value(yyjson_get_num(upper_elem)));
						}
						bucket.cumulative_frequency = yyjson_get_num(freq_elem);
						bucket.num_distinct = yyjson_get_num(distinct_elem);
					}

					hist.buckets.push_back(bucket);
				}
			}

			if (!hist.buckets.empty()) {
				stats.histograms[column_name] = std::move(hist);
			}
		}
	}
}

MySQLStatisticsCollector::MySQLCostConstants MySQLStatisticsCollector::FetchMySQLCostConstants() {
	MySQLCostConstants constants;

	try {
		auto result = connection_.get().Query("SELECT cost_name, cost_value, default_value "
		                                      "FROM mysql.server_cost",
		                                      MySQLResultStreaming::FORCE_MATERIALIZATION);
		while (!result->Exhausted()) {
			auto &chunk = result->NextChunk();
			for (idx_t row = 0; row < chunk.size(); row++) {
				string name = chunk.data[0].GetValue(row).ToString();
				auto cost_val = chunk.data[1].GetValue(row);
				auto default_val = chunk.data[2].GetValue(row);
				double value = 0.0;
				if (!cost_val.IsNull()) {
					value = cost_val.GetValue<double>();
				} else if (!default_val.IsNull()) {
					value = default_val.GetValue<double>();
				}
				if (name == "row_evaluate_cost" && value > 0) {
					constants.row_evaluate_cost = value;
				}
			}
		}
	} catch (const std::exception &) {
		return constants;
	}

	try {
		auto result = connection_.get().Query("SELECT cost_name, cost_value, default_value "
		                                      "FROM mysql.engine_cost",
		                                      MySQLResultStreaming::FORCE_MATERIALIZATION);
		while (!result->Exhausted()) {
			auto &chunk = result->NextChunk();
			for (idx_t row = 0; row < chunk.size(); row++) {
				string name = chunk.data[0].GetValue(row).ToString();
				auto cost_val = chunk.data[1].GetValue(row);
				auto default_val = chunk.data[2].GetValue(row);
				double value = 0.0;
				if (!cost_val.IsNull()) {
					value = cost_val.GetValue<double>();
				} else if (!default_val.IsNull()) {
					value = default_val.GetValue<double>();
				}
				if (name == "io_block_read_cost" && value > 0) {
					constants.io_block_read_cost = value;
				} else if (name == "memory_block_read_cost" && value > 0) {
					constants.memory_block_read_cost = value;
				}
			}
		}
	} catch (const std::exception &) {
		return constants;
	}

	try {
		auto bp_result = connection_.get().Query("SHOW GLOBAL STATUS WHERE variable_name IN "
		                                         "('Innodb_buffer_pool_read_requests', 'Innodb_buffer_pool_reads')",
		                                         MySQLResultStreaming::FORCE_MATERIALIZATION);
		idx_t read_requests = 0, reads = 0;
		while (!bp_result->Exhausted()) {
			auto &chunk = bp_result->NextChunk();
			for (idx_t row = 0; row < chunk.size(); row++) {
				string name = chunk.data[0].GetValue(row).ToString();
				auto val_str = chunk.data[1].GetValue(row).ToString();
				idx_t val = 0;
				try {
					val = static_cast<idx_t>(std::stoull(val_str));
				} catch (...) {
					continue;
				}
				if (name == "Innodb_buffer_pool_read_requests") {
					read_requests = val;
				} else if (name == "Innodb_buffer_pool_reads") {
					reads = val;
				}
			}
		}
		static constexpr idx_t MIN_READ_REQUESTS = 1000;
		if (read_requests >= MIN_READ_REQUESTS && reads <= read_requests) {
			constants.buffer_pool_hit_rate = 1.0 - (static_cast<double>(reads) / static_cast<double>(read_requests));
			constants.buffer_pool_hit_rate = std::max(0.0, std::min(1.0, constants.buffer_pool_hit_rate));
		}
	} catch (const std::exception &) {
	}

	constants.loaded = true;
	return constants;
}

bool MySQLStatisticsCollector::ValidateWithExplain(const string &query, const string &expected_index,
                                                   bool expect_index_scan) {
	try {
		string explain_query = "EXPLAIN FORMAT=JSON " + query;
		auto result = connection_.get().Query(explain_query, MySQLResultStreaming::FORCE_MATERIALIZATION);
		if (result->Exhausted()) {
			return true;
		}

		auto &chunk = result->NextChunk();
		if (chunk.size() == 0) {
			return true;
		}

		string explain_json = chunk.data[0].GetValue(0).ToString();

		size_t access_type_pos = explain_json.find("\"access_type\"");
		if (access_type_pos == string::npos) {
			return true;
		}

		size_t colon = explain_json.find(':', access_type_pos);
		size_t quote_start = explain_json.find('"', colon);
		size_t quote_end = explain_json.find('"', quote_start + 1);
		if (quote_start == string::npos || quote_end == string::npos) {
			return true;
		}
		string access_type = explain_json.substr(quote_start + 1, quote_end - quote_start - 1);

		if (expect_index_scan && access_type == "ALL") {
			return false;
		}

		if (!expected_index.empty()) {
			size_t key_pos = explain_json.find("\"key\"");
			if (key_pos != string::npos) {
				size_t kcolon = explain_json.find(':', key_pos);
				size_t kquote_start = explain_json.find('"', kcolon);
				size_t kquote_end = explain_json.find('"', kquote_start + 1);
				if (kquote_start != string::npos && kquote_end != string::npos) {
					string actual_key = explain_json.substr(kquote_start + 1, kquote_end - kquote_start - 1);
					if (!StringUtil::CIEquals(actual_key, expected_index)) {
						return false;
					}
				}
			}
		}
	} catch (const std::exception &) {
	}
	return true;
}

int MySQLStatisticsCollector::GetMajorVersion() const {
	if (mysql_version_.empty()) {
		return 0;
	}
	auto dot = mysql_version_.find('.');
	if (dot == string::npos) {
		return 0;
	}
	try {
		return std::stoi(mysql_version_.substr(0, dot));
	} catch (...) {
		return 0;
	}
}

int MySQLStatisticsCollector::GetMinorVersion() const {
	if (mysql_version_.empty()) {
		return 0;
	}
	auto first_dot = mysql_version_.find('.');
	if (first_dot == string::npos) {
		return 0;
	}
	auto second_dot = mysql_version_.find('.', first_dot + 1);
	if (second_dot == string::npos) {
		return 0;
	}
	try {
		return std::stoi(mysql_version_.substr(first_dot + 1, second_dot - first_dot - 1));
	} catch (...) {
		return 0;
	}
}

int MySQLStatisticsCollector::GetPatchVersion() const {
	if (mysql_version_.empty()) {
		return 0;
	}
	auto first_dot = mysql_version_.find('.');
	if (first_dot == string::npos) {
		return 0;
	}
	auto second_dot = mysql_version_.find('.', first_dot + 1);
	if (second_dot == string::npos) {
		return 0;
	}
	auto rest = mysql_version_.substr(second_dot + 1);
	auto end = rest.find_first_not_of("0123456789");
	try {
		return std::stoi(rest.substr(0, end));
	} catch (...) {
		return 0;
	}
}

bool MySQLStatisticsCollector::SupportsExplainAnalyze() const {
	int major = GetMajorVersion();
	int minor = GetMinorVersion();
	int patch = GetPatchVersion();
	if (major > 8) {
		return true;
	}
	if (major == 8 && minor > 0) {
		return true;
	}
	if (major == 8 && minor == 0 && patch >= 18) {
		return true;
	}
	return false;
}

MySQLStatisticsCollector::ExplainAnalyzeResult MySQLStatisticsCollector::RunExplainAnalyze(const string &query) {
	ExplainAnalyzeResult result;
	if (!SupportsExplainAnalyze()) {
		return result;
	}

	try {
		string explain_query = "EXPLAIN ANALYZE FORMAT=JSON " + query;
		auto res = connection_.get().Query(explain_query, MySQLResultStreaming::FORCE_MATERIALIZATION);
		if (res->Exhausted()) {
			return result;
		}

		auto &chunk = res->NextChunk();
		if (chunk.size() == 0) {
			return result;
		}

		string json = chunk.data[0].GetValue(0).ToString();

		size_t rows_pos = json.find("\"rows_examined_per_scan\"");
		if (rows_pos != string::npos) {
			size_t colon = json.find(':', rows_pos);
			size_t num_start = colon + 1;
			while (num_start < json.size() && (json[num_start] == ' ' || json[num_start] == ':')) {
				num_start++;
			}
			size_t num_end = num_start;
			while (num_end < json.size() && json[num_end] >= '0' && json[num_end] <= '9') {
				num_end++;
			}
			if (num_end > num_start) {
				result.actual_rows = static_cast<idx_t>(std::stoull(json.substr(num_start, num_end - num_start)));
			}
		}

		size_t time_pos = json.find("\"actual_time\"");
		if (time_pos == string::npos) {
			time_pos = json.find("\"query_cost\"");
		}
		if (time_pos != string::npos) {
			size_t colon = json.find(':', time_pos);
			size_t num_start = colon + 1;
			while (num_start < json.size() && (json[num_start] == ' ' || json[num_start] == '"')) {
				num_start++;
			}
			size_t num_end = num_start;
			while (num_end < json.size() && ((json[num_end] >= '0' && json[num_end] <= '9') || json[num_end] == '.')) {
				num_end++;
			}
			if (num_end > num_start) {
				result.actual_time_ms = std::stod(json.substr(num_start, num_end - num_start));
			}
		}

		size_t access_pos = json.find("\"access_type\"");
		if (access_pos != string::npos) {
			size_t colon = json.find(':', access_pos);
			size_t q_start = json.find('"', colon);
			size_t q_end = json.find('"', q_start + 1);
			if (q_start != string::npos && q_end != string::npos) {
				result.access_type = json.substr(q_start + 1, q_end - q_start - 1);
			}
		}

		result.valid = true;
	} catch (const std::exception &) {
	}
	return result;
}

bool MySQLStatisticsCollector::HasUsableIndex(const string &schema, const string &table,
                                              const vector<string> &columns) {
	auto stats = GetTableStats(schema, table);
	return stats.FindBestIndex(columns) != nullptr;
}

double MySQLStatisticsCollector::GetDefaultSelectivity(ExpressionType comparison) const {
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
		return 0.01;
	case ExpressionType::COMPARE_NOTEQUAL:
		return 0.99;
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
		return 0.33;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return 0.33;
	default:
		return 0.10;
	}
}

double MySQLStatisticsCollector::EstimateSelectivityFromCardinality(idx_t cardinality, idx_t total_rows) const {
	if (cardinality == 0 || cardinality == DConstants::INVALID_INDEX || total_rows == 0) {
		return 0.10;
	}
	return 1.0 / static_cast<double>(cardinality);
}

double MySQLStatisticsCollector::EstimateSelectivity(const string &schema, const string &table, const string &column,
                                                     ExpressionType comparison, const Value &value) {
	auto stats = GetTableStats(schema, table);

	auto histogram = stats.GetHistogram(column);
	if (histogram) {
		double hist_selectivity = histogram->EstimateSelectivity(comparison, value, stats.estimated_row_count);
		if (hist_selectivity >= 0.0) {
			return hist_selectivity;
		}
	}

	auto it = stats.column_distinct_count.find(column);
	if (it != stats.column_distinct_count.end() && it->second > 0) {
		double base_selectivity = EstimateSelectivityFromCardinality(it->second, stats.estimated_row_count);
		switch (comparison) {
		case ExpressionType::COMPARE_EQUAL:
			return base_selectivity;
		case ExpressionType::COMPARE_NOTEQUAL:
			return 1.0 - base_selectivity;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return 0.33;
		default:
			return GetDefaultSelectivity(comparison);
		}
	}

	return GetDefaultSelectivity(comparison);
}

} // namespace duckdb
