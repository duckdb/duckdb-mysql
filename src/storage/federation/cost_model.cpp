#include "storage/federation/cost_model.hpp"

namespace duckdb {

DefaultCostModel::DefaultCostModel(CostModelParameters params) : params_(std::move(params)) {
}

idx_t DefaultCostModel::EstimateRowWidth(const MySQLTableStats &stats, const vector<string> &columns) const {
	idx_t base_width = stats.avg_row_length > 0 ? stats.avg_row_length : 100;
	idx_t total_columns = stats.column_null_fraction.size();
	if (total_columns > 0 && columns.size() < total_columns) {
		base_width = static_cast<idx_t>(static_cast<double>(base_width) * static_cast<double>(columns.size()) /
		                                static_cast<double>(total_columns));
		return std::max(base_width, static_cast<idx_t>(1));
	}
	return base_width;
}

idx_t DefaultCostModel::EstimateResultRows(const MySQLTableStats &stats,
                                           const FilterAnalysisResult &filter_result) const {
	return EstimateResultRows(stats, filter_result.combined_selectivity);
}

idx_t DefaultCostModel::EstimateResultRows(const MySQLTableStats &stats, double selectivity) const {
	if (stats.estimated_row_count == 0) {
		return 0;
	}
	idx_t estimated = static_cast<idx_t>(static_cast<double>(stats.estimated_row_count) * selectivity);
	return std::max(estimated, static_cast<idx_t>(1));
}

double DefaultCostModel::EstimateIndexIOCost(const MySQLTableStats &stats, const vector<string> &filter_columns,
                                             idx_t result_rows,
                                             optional_ptr<const MySQLIndexInfo> covering_index) const {
	if (stats.estimated_row_count == 0 || result_rows == 0) {
		return 0.0;
	}

	double per_page_cost = params_.io_cost_per_byte * static_cast<double>(CostModelParameters::INNODB_PAGE_SIZE);
	double total_rows = static_cast<double>(stats.estimated_row_count);
	double selectivity = static_cast<double>(result_rows) / total_rows;

	auto scan_index = covering_index ? covering_index : stats.FindBestIndex(filter_columns);
	if (!scan_index) {
		idx_t row_width = stats.avg_row_length > 0 ? stats.avg_row_length : 100;
		double total_bytes = total_rows * static_cast<double>(row_width);
		return selectivity * total_bytes * params_.io_cost_per_byte * params_.seq_scan_cost_factor;
	}

	idx_t leaf_page_count = scan_index->leaf_pages;
	if (leaf_page_count == 0) {
		leaf_page_count = scan_index->index_size_pages;
	}
	if (leaf_page_count == 0 && stats.clustered_index_size_pages > 0) {
		leaf_page_count = std::max(static_cast<idx_t>(1),
		                           static_cast<idx_t>(static_cast<double>(stats.clustered_index_size_pages) * 0.15));
	}
	if (leaf_page_count == 0 && stats.data_length > 0) {
		leaf_page_count =
		    std::max(static_cast<idx_t>(1),
		             static_cast<idx_t>(static_cast<double>(stats.data_length) /
		                                static_cast<double>(CostModelParameters::INNODB_PAGE_SIZE) * 0.15));
	}
	if (leaf_page_count == 0) {
		leaf_page_count = std::max(
		    static_cast<idx_t>(1),
		    static_cast<idx_t>(total_rows * 20.0 / static_cast<double>(CostModelParameters::INNODB_PAGE_SIZE)));
	}

	double index_pages_to_read = selectivity * static_cast<double>(leaf_page_count);

	double btree_depth = 1.0;
	if (total_rows > 1.0) {
		btree_depth = std::ceil(std::log(total_rows) / std::log(500.0));
		btree_depth = std::max(1.0, std::min(btree_depth, 6.0));
	}
	index_pages_to_read = std::max(index_pages_to_read, btree_depth);

	double index_io = index_pages_to_read * per_page_cost;

	if (!covering_index) {
		idx_t clustered_pages = stats.clustered_index_size_pages;
		if (clustered_pages == 0 && stats.avg_row_length > 0) {
			clustered_pages = static_cast<idx_t>(total_rows * static_cast<double>(stats.avg_row_length) /
			                                     static_cast<double>(CostModelParameters::INNODB_PAGE_SIZE));
			clustered_pages = std::max(clustered_pages, static_cast<idx_t>(1));
		}
		double bookmark_pages;
		if (clustered_pages > 0) {
			double cp = static_cast<double>(clustered_pages);
			bookmark_pages = cp * (1.0 - std::exp(-static_cast<double>(result_rows) / cp));
		} else {
			bookmark_pages = static_cast<double>(result_rows);
		}
		index_io += bookmark_pages * per_page_cost;
	}

	return index_io;
}

OperationCost DefaultCostModel::MySQLScanCost(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
                                              const vector<string> &columns) const {
	OperationCost cost;

	cost.cpu_cost = params_.mysql_overhead;

	idx_t result_rows = EstimateResultRows(stats, filter_result);
	idx_t row_width = EstimateRowWidth(stats, columns);

	bool uses_index = false;
	vector<string> filter_columns;
	for (const auto &analysis : filter_result.analyses) {
		if (analysis.has_index && analysis.ShouldPush()) {
			uses_index = true;
		}
		if (!analysis.column_name.empty()) {
			filter_columns.push_back(analysis.column_name);
		}
	}

	auto covering_index = stats.FindCoveringIndex(columns, filter_columns);

	if (uses_index) {
		cost.io_cost = EstimateIndexIOCost(stats, filter_columns, result_rows, covering_index);
	} else {
		double total_bytes = static_cast<double>(stats.estimated_row_count) * static_cast<double>(row_width);
		cost.io_cost = total_bytes * params_.io_cost_per_byte * params_.seq_scan_cost_factor;
	}

	cost.cpu_cost += static_cast<double>(result_rows) * params_.cpu_cost_per_row;

	return cost;
}

OperationCost DefaultCostModel::TransferCost(idx_t row_count, idx_t row_width_bytes, bool innodb_compressed) const {
	OperationCost cost;
	double total_bytes = static_cast<double>(row_count) * static_cast<double>(row_width_bytes);

	if (compression_aware_costs_) {
		double effective_ratio = 1.0;
		if (innodb_compressed) {
			effective_ratio *= compression_ratio_;
		}
		if (network_calibration_.has_network_compression) {
			effective_ratio *= network_calibration_.network_compression_ratio;
		}
		effective_ratio = std::max(0.01, effective_ratio);
		total_bytes *= effective_ratio;
	}

	cost.network_cost = network_calibration_.ByteTransferTime(static_cast<idx_t>(total_bytes)) * 1000.0;

	cost.network_cost += params_.network_round_trip_cost;

	return cost;
}

OperationCost DefaultCostModel::LocalFilterCost(idx_t row_count, idx_t num_filters) const {
	OperationCost cost;
	cost.cpu_cost = static_cast<double>(row_count) * static_cast<double>(num_filters) * params_.local_filter_cost;
	return cost;
}

OperationCost DefaultCostModel::CalculatePushAllCost(const MySQLTableStats &stats,
                                                     const FilterAnalysisResult &filter_result,
                                                     const vector<string> &columns) const {
	auto scan_cost = MySQLScanCost(stats, filter_result, columns);

	idx_t result_rows = EstimateResultRows(stats, filter_result);
	idx_t row_width = EstimateRowWidth(stats, columns);
	auto transfer_cost = TransferCost(result_rows, row_width, stats.IsInnoDBCompressed());

	return scan_cost + transfer_cost;
}

OperationCost DefaultCostModel::CalculateLocalAllCost(const MySQLTableStats &stats,
                                                      const FilterAnalysisResult &filter_result,
                                                      const vector<string> &columns) const {
	FilterAnalysisResult no_filters;
	auto scan_cost = MySQLScanCost(stats, no_filters, columns);

	idx_t all_rows = stats.estimated_row_count;
	idx_t row_width = EstimateRowWidth(stats, columns);
	auto transfer_cost = TransferCost(all_rows, row_width, stats.IsInnoDBCompressed());

	idx_t num_filters = filter_result.analyses.size();
	auto local_cost = LocalFilterCost(all_rows, num_filters);

	return scan_cost + transfer_cost + local_cost;
}

OperationCost DefaultCostModel::CalculateHybridCost(const MySQLTableStats &stats,
                                                    const FilterAnalysisResult &filter_result,
                                                    const vector<string> &columns, vector<idx_t> &pushed_indices,
                                                    vector<idx_t> &local_indices,
                                                    double &out_pushed_selectivity) const {
	pushed_indices.clear();
	local_indices.clear();

	const idx_t filter_count = filter_result.analyses.size();
	pushed_indices.reserve(filter_count);
	local_indices.reserve(filter_count);

	double pushed_selectivity = 1.0;
	idx_t pushed_filter_count = 0;
	for (idx_t i = 0; i < filter_count; i++) {
		const auto &analysis = filter_result.analyses[i];
		if (analysis.ShouldPush() && !analysis.mysql_predicate.empty()) {
			pushed_indices.push_back(i);
			pushed_filter_count++;
			if (pushed_filter_count <= 2) {
				pushed_selectivity *= analysis.estimated_selectivity;
			} else {
				pushed_selectivity *=
				    std::pow(analysis.estimated_selectivity, PredicateAnalyzer::SELECTIVITY_DAMPENING_EXPONENT);
			}
		} else {
			local_indices.push_back(i);
		}
	}

	out_pushed_selectivity = pushed_selectivity;

	if (pushed_indices.empty()) {
		return CalculateLocalAllCost(stats, filter_result, columns);
	}
	if (local_indices.empty()) {
		return CalculatePushAllCost(stats, filter_result, columns);
	}

	FilterAnalysisResult pushed_only;
	pushed_only.combined_selectivity = pushed_selectivity;
	for (auto idx : pushed_indices) {
		pushed_only.analyses.push_back(filter_result.analyses[idx]);
	}

	auto scan_cost = MySQLScanCost(stats, pushed_only, columns);

	idx_t rows_from_mysql = EstimateResultRows(stats, pushed_selectivity);
	idx_t row_width = EstimateRowWidth(stats, columns);
	auto transfer_cost = TransferCost(rows_from_mysql, row_width, stats.IsInnoDBCompressed());

	auto local_cost = LocalFilterCost(rows_from_mysql, local_indices.size());

	return scan_cost + transfer_cost + local_cost;
}

OperationCost DefaultCostModel::AggregatePushedCost(const MySQLTableStats &stats, const vector<string> &columns,
                                                    idx_t effective_row_count, idx_t num_groups,
                                                    idx_t num_aggregates) const {
	OperationCost cost;

	cost.cpu_cost = params_.mysql_overhead;
	idx_t row_width = EstimateRowWidth(stats, columns);
	double total_bytes = static_cast<double>(stats.estimated_row_count) * static_cast<double>(row_width);
	cost.io_cost = total_bytes * params_.io_cost_per_byte * params_.seq_scan_cost_factor;

	cost.cpu_cost += static_cast<double>(effective_row_count) * params_.cpu_cost_per_row * 2.0;

	idx_t result_rows = std::max(num_groups, static_cast<idx_t>(1));
	idx_t agg_row_width = (num_groups > 0 ? 8 : 0) + num_aggregates * 8;
	agg_row_width = std::max(agg_row_width, static_cast<idx_t>(8));
	auto transfer = TransferCost(result_rows, agg_row_width, stats.IsInnoDBCompressed());
	cost += transfer;

	return cost;
}

OperationCost DefaultCostModel::AggregateLocalCost(const MySQLTableStats &stats, const vector<string> &columns,
                                                   idx_t effective_row_count) const {
	OperationCost cost;

	cost.cpu_cost = params_.mysql_overhead;
	idx_t row_width = EstimateRowWidth(stats, columns);
	double total_bytes = static_cast<double>(stats.estimated_row_count) * static_cast<double>(row_width);
	cost.io_cost = total_bytes * params_.io_cost_per_byte * params_.seq_scan_cost_factor;

	cost.cpu_cost += static_cast<double>(effective_row_count) * params_.cpu_cost_per_row;

	auto transfer = TransferCost(effective_row_count, row_width, stats.IsInnoDBCompressed());
	cost += transfer;

	cost.cpu_cost += static_cast<double>(effective_row_count) * params_.local_aggregate_cost_per_row;

	return cost;
}

bool DefaultCostModel::ShouldPushAggregate(const MySQLTableStats &stats, const vector<string> &columns,
                                           idx_t effective_row_count, idx_t num_groups, idx_t num_aggregates) const {
	auto pushed = AggregatePushedCost(stats, columns, effective_row_count, num_groups, num_aggregates);
	auto local = AggregateLocalCost(stats, columns, effective_row_count);
	return pushed.Total() < local.Total();
}

ExecutionPlan DefaultCostModel::ComparePlans(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
                                             const vector<string> &columns) const {
	ExecutionPlan plan;

	if (filter_result.analyses.empty()) {
		plan.strategy = ExecutionStrategy::PUSH_ALL_FILTERS;
		plan.estimated_cost = CalculatePushAllCost(stats, filter_result, columns);
		plan.estimated_rows_from_mysql = stats.estimated_row_count;
		plan.estimated_final_rows = stats.estimated_row_count;
#ifndef NDEBUG
		plan.reasoning = "no filters to analyze";
#endif
		return plan;
	}

	auto push_all_cost = CalculatePushAllCost(stats, filter_result, columns);

	vector<idx_t> hybrid_pushed, hybrid_local;
	double hybrid_pushed_selectivity = 1.0;
	auto hybrid_cost =
	    CalculateHybridCost(stats, filter_result, columns, hybrid_pushed, hybrid_local, hybrid_pushed_selectivity);

	auto local_all_cost = CalculateLocalAllCost(stats, filter_result, columns);

	plan.comparison_costs.push_all_cost = push_all_cost;
	plan.comparison_costs.local_all_cost = local_all_cost;
	plan.comparison_costs.hybrid_cost = hybrid_cost;

	ExecutionStrategy best_strategy = ExecutionStrategy::PUSH_ALL_FILTERS;
	OperationCost best_cost = push_all_cost;
	if (local_all_cost < best_cost) {
		best_strategy = ExecutionStrategy::EXECUTE_ALL_LOCALLY;
		best_cost = local_all_cost;
	}

	if (!hybrid_pushed.empty() && !hybrid_local.empty() && hybrid_cost < best_cost) {
		best_strategy = ExecutionStrategy::HYBRID;
		best_cost = hybrid_cost;
	}

	plan.strategy = best_strategy;
	plan.estimated_cost = best_cost;

	switch (plan.strategy) {
	case ExecutionStrategy::PUSH_ALL_FILTERS:
		plan.estimated_rows_from_mysql = EstimateResultRows(stats, filter_result);
		plan.estimated_final_rows = plan.estimated_rows_from_mysql;
		plan.pushed_filter_indices.reserve(filter_result.analyses.size());
		for (idx_t i = 0; i < filter_result.analyses.size(); i++) {
			plan.pushed_filter_indices.push_back(i);
		}
		break;
	case ExecutionStrategy::EXECUTE_ALL_LOCALLY:
		plan.estimated_rows_from_mysql = stats.estimated_row_count;
		plan.estimated_final_rows = EstimateResultRows(stats, filter_result);
		plan.local_filter_indices.reserve(filter_result.analyses.size());
		for (idx_t i = 0; i < filter_result.analyses.size(); i++) {
			plan.local_filter_indices.push_back(i);
		}
		break;
	case ExecutionStrategy::HYBRID: {
		plan.pushed_filter_indices = std::move(hybrid_pushed);
		plan.local_filter_indices = std::move(hybrid_local);
		plan.estimated_rows_from_mysql = EstimateResultRows(stats, hybrid_pushed_selectivity);
		plan.estimated_final_rows = EstimateResultRows(stats, filter_result);
		break;
	}
	}

#ifndef NDEBUG
	plan.reasoning = "push_all=" + std::to_string(push_all_cost.Total()) +
	                 ", local_all=" + std::to_string(local_all_cost.Total()) +
	                 ", hybrid=" + std::to_string(hybrid_cost.Total()) + ", chose=";
	switch (plan.strategy) {
	case ExecutionStrategy::PUSH_ALL_FILTERS:
		plan.reasoning += "PUSH_ALL";
		break;
	case ExecutionStrategy::EXECUTE_ALL_LOCALLY:
		plan.reasoning += "LOCAL_ALL";
		break;
	case ExecutionStrategy::HYBRID:
		plan.reasoning += "HYBRID";
		break;
	}
#endif

	return plan;
}

} // namespace duckdb
