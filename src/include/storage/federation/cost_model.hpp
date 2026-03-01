//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/federation/cost_model.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "storage/mysql_statistics.hpp"
#include "storage/mysql_predicate_analyzer.hpp"

#include <cmath>

namespace duckdb {

struct OperationCost {
	double cpu_cost = 0.0;
	double io_cost = 0.0;
	double network_cost = 0.0;

	OperationCost() = default;
	OperationCost(double cpu, double io, double network) : cpu_cost(cpu), io_cost(io), network_cost(network) {
	}

	double Total() const {
		return cpu_cost + io_cost + network_cost;
	}

	bool operator<(const OperationCost &other) const {
		return Total() < other.Total();
	}

	OperationCost operator+(const OperationCost &other) const {
		return OperationCost(cpu_cost + other.cpu_cost, io_cost + other.io_cost, network_cost + other.network_cost);
	}

	OperationCost &operator+=(const OperationCost &other) {
		cpu_cost += other.cpu_cost;
		io_cost += other.io_cost;
		network_cost += other.network_cost;
		return *this;
	}
};

enum class ExecutionStrategy : uint8_t { PUSH_ALL_FILTERS, EXECUTE_ALL_LOCALLY, HYBRID };

inline const char *ExecutionStrategyToString(ExecutionStrategy strategy) {
	switch (strategy) {
	case ExecutionStrategy::PUSH_ALL_FILTERS:
		return "PUSH_ALL";
	case ExecutionStrategy::EXECUTE_ALL_LOCALLY:
		return "LOCAL_ALL";
	case ExecutionStrategy::HYBRID:
		return "HYBRID";
	default:
		return "UNKNOWN";
	}
}

struct ExecutionPlan {
	ExecutionStrategy strategy = ExecutionStrategy::PUSH_ALL_FILTERS;
	OperationCost estimated_cost;
	vector<idx_t> pushed_filter_indices;
	vector<idx_t> local_filter_indices;
	idx_t estimated_rows_from_mysql = 0;
	idx_t estimated_final_rows = 0;
#ifndef NDEBUG
	string reasoning;
#endif
};

struct NetworkCalibration {
	static constexpr double DEFAULT_LATENCY_MS = 1.0;
	static constexpr double DEFAULT_BANDWIDTH_MBPS = 100.0;
	static constexpr double MBPS_TO_BYTES_PER_SEC = 125000.0;
	static constexpr double DEFAULT_COMPRESSION_RATIO = 0.7;

	double latency_ms = DEFAULT_LATENCY_MS;
	double bandwidth_mbps = DEFAULT_BANDWIDTH_MBPS;
	bool is_calibrated = false;
	bool calibration_failed = false;
	bool has_network_compression = false;
	double network_compression_ratio = DEFAULT_COMPRESSION_RATIO;

	double ByteTransferTime(idx_t bytes) const {
		double transfer_seconds = static_cast<double>(bytes) / (bandwidth_mbps * MBPS_TO_BYTES_PER_SEC);
		return (latency_ms / 1000.0) + transfer_seconds;
	}
};

class CostModel {
public:
	virtual ~CostModel() = default;

	virtual OperationCost MySQLScanCost(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
	                                    const vector<string> &columns) const = 0;

	virtual OperationCost TransferCost(idx_t row_count, idx_t row_width_bytes,
	                                   bool innodb_compressed = false) const = 0;

	virtual OperationCost LocalFilterCost(idx_t row_count, idx_t num_filters) const = 0;

	virtual ExecutionPlan ComparePlans(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
	                                   const vector<string> &columns) const = 0;

protected:
	CostModel() = default;
};

struct CostModelParameters {
	double cpu_cost_per_row = 0.1;
	double io_cost_per_byte = 0.000001;
	double network_cost_per_byte = 0.00001;
	double mysql_overhead = 10.0;
	double local_filter_cost = 0.001;
	double index_lookup_cost = 0.5;
	double seq_scan_cost_factor = 0.8;
	double network_round_trip_cost = 1.0;
};

class DefaultCostModel : public CostModel {
public:
	explicit DefaultCostModel(CostModelParameters params = {});

	OperationCost MySQLScanCost(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
	                            const vector<string> &columns) const override;

	OperationCost TransferCost(idx_t row_count, idx_t row_width_bytes, bool innodb_compressed = false) const override;

	OperationCost LocalFilterCost(idx_t row_count, idx_t num_filters) const override;

	ExecutionPlan ComparePlans(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
	                           const vector<string> &columns) const override;

	const CostModelParameters &GetParameters() const {
		return params_;
	}

	void SetNetworkCalibration(const NetworkCalibration &calibration) {
		network_calibration_ = calibration;
	}

	const NetworkCalibration &GetNetworkCalibration() const {
		return network_calibration_;
	}

	void SetCompressionAwareCosts(bool enabled) {
		compression_aware_costs_ = enabled;
	}

	void SetCompressionRatio(double ratio) {
		compression_ratio_ = ratio;
	}

private:
	CostModelParameters params_;
	NetworkCalibration network_calibration_;
	bool compression_aware_costs_ = true;
	double compression_ratio_ = NetworkCalibration::DEFAULT_COMPRESSION_RATIO;

	idx_t EstimateRowWidth(const MySQLTableStats &stats, const vector<string> &columns) const;
	idx_t EstimateResultRows(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result) const;
	idx_t EstimateResultRows(const MySQLTableStats &stats, double selectivity) const;

	OperationCost CalculatePushAllCost(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
	                                   const vector<string> &columns) const;
	OperationCost CalculateLocalAllCost(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
	                                    const vector<string> &columns) const;
	OperationCost CalculateHybridCost(const MySQLTableStats &stats, const FilterAnalysisResult &filter_result,
	                                  const vector<string> &columns, vector<idx_t> &pushed_indices,
	                                  vector<idx_t> &local_indices, double &out_pushed_selectivity) const;
};

} // namespace duckdb
