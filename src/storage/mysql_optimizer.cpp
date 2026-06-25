#include "storage/mysql_optimizer.hpp"

namespace duckdb {

void MySQLOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// TODO: streaming check
}

} // namespace duckdb
