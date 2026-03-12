//===----------------------------------------------------------------------===//
//                         DuckDB
//
// network_calibration.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

#include <algorithm>

namespace duckdb {

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
		double effective_bw = std::max(bandwidth_mbps, 1.0);
		double effective_lat = std::max(latency_ms, 0.0);
		double transfer_seconds = static_cast<double>(bytes) / (effective_bw * MBPS_TO_BYTES_PER_SEC);
		return (effective_lat / 1000.0) + transfer_seconds;
	}
};

} // namespace duckdb
