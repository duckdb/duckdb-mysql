#include "duckdb.hpp"

#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "mysql_scanner.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {

struct DebugExecutionPlanData : public TableFunctionData {
	vector<PlanCacheEntry> entries;
	vector<string> database_names;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DebugExecutionPlanBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DebugExecutionPlanData>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "mysql") {
			continue;
		}
		auto &mysql_catalog = catalog.Cast<MySQLCatalog>();
		auto entries = mysql_catalog.GetPlanCache().GetPlanCacheEntries();
		for (auto &entry : entries) {
			result->database_names.push_back(db.GetName());
			result->entries.push_back(std::move(entry));
		}
	}

	names.emplace_back("database_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("strategy");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("filters_pushed");
	return_types.push_back(LogicalType::UBIGINT);

	names.emplace_back("filters_local");
	return_types.push_back(LogicalType::UBIGINT);

	names.emplace_back("hit_count");
	return_types.push_back(LogicalType::UBIGINT);

	names.emplace_back("estimated_rows");
	return_types.push_back(LogicalType::UBIGINT);

	names.emplace_back("io_cost");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("network_cost");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("cpu_cost");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("total_cost");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("push_all_cost");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("local_all_cost");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("hybrid_cost");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("combined_selectivity");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("partition_clause");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("execution_count");
	return_types.push_back(LogicalType::UBIGINT);

	names.emplace_back("smoothed_actual_rows");
	return_types.push_back(LogicalType::UBIGINT);

	names.emplace_back("recommended_index");
	return_types.push_back(LogicalType::VARCHAR);

	return result;
}

static void DebugExecutionPlanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DebugExecutionPlanData>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];
		auto &db_name = data.database_names[data.offset];

		output.SetValue(0, count, Value(db_name));
		output.SetValue(1, count, Value(entry.schema_name));
		output.SetValue(2, count, Value(entry.table_name));
		output.SetValue(3, count, Value(ExecutionStrategyToString(entry.strategy)));
		output.SetValue(4, count, Value::UBIGINT(entry.filters_pushed));
		output.SetValue(5, count, Value::UBIGINT(entry.filters_local));
		output.SetValue(6, count, Value::UBIGINT(entry.hit_count));
		output.SetValue(7, count, Value::UBIGINT(entry.estimated_rows));
		output.SetValue(8, count, Value::DOUBLE(entry.io_cost));
		output.SetValue(9, count, Value::DOUBLE(entry.network_cost));
		output.SetValue(10, count, Value::DOUBLE(entry.cpu_cost));
		output.SetValue(11, count, Value::DOUBLE(entry.total_cost));
		output.SetValue(12, count, Value::DOUBLE(entry.push_all_cost));
		output.SetValue(13, count, Value::DOUBLE(entry.local_all_cost));
		output.SetValue(14, count, Value::DOUBLE(entry.hybrid_cost));
		output.SetValue(15, count, Value::DOUBLE(entry.combined_selectivity));
		output.SetValue(16, count, entry.partition_clause.empty() ? Value() : Value(entry.partition_clause));
		output.SetValue(17, count, Value::UBIGINT(entry.execution_count));
		output.SetValue(18, count, Value::UBIGINT(entry.smoothed_actual_rows));
		output.SetValue(19, count, entry.recommended_index.empty() ? Value() : Value(entry.recommended_index));

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

MySQLExplainFederatedFunction::MySQLExplainFederatedFunction()
    : TableFunction("mysql_explain_federated", {}, DebugExecutionPlanFunction, DebugExecutionPlanBind) {
}

MySQLDebugExecutionPlanFunction::MySQLDebugExecutionPlanFunction()
    : TableFunction("mysql_debug_execution_plan", {}, DebugExecutionPlanFunction, DebugExecutionPlanBind) {
}

} // namespace duckdb
