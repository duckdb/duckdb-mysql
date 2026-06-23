#include "storage/mysql_execute_query.hpp"
#include "storage/mysql_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_transaction.hpp"
#include "mysql_connection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"

namespace duckdb {

MySQLExecuteQuery::MySQLExecuteQuery(PhysicalPlan &physical_plan, LogicalOperator &op, string op_name_p,
                                     TableCatalogEntry &table, string query_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), op_name(std::move(op_name_p)),
      table(table), query(std::move(query_p)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class MySQLExecuteQueryGlobalState : public GlobalSinkState {
public:
	explicit MySQLExecuteQueryGlobalState() : affected_rows(0) {
	}

	idx_t affected_rows;
};

unique_ptr<GlobalSinkState> MySQLExecuteQuery::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<MySQLExecuteQueryGlobalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType MySQLExecuteQuery::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return SinkResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType MySQLExecuteQuery::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<MySQLExecuteQueryGlobalState>();
	auto &transaction = MySQLTransaction::Get(context, table.catalog);
	auto &connection = transaction.GetConnection();
	auto result = connection.Query(query, MySQLResultStreaming::FORCE_MATERIALIZATION);
	gstate.affected_rows = result->AffectedRows();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType MySQLExecuteQuery::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<MySQLExecuteQueryGlobalState>();
	chunk.SetChildCardinality(1);
	chunk.data[0].SetValue(0, Value::BIGINT(insert_gstate.affected_rows));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string MySQLExecuteQuery::GetName() const {
	return op_name;
}

InsertionOrderPreservingMap<string> MySQLExecuteQuery::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name.GetIdentifierName();
	return result;
}

PhysicalOperator &MySQLCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw BinderException("DELETE syntax not supported");
}

PhysicalOperator &MySQLCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw BinderException("UPDATE syntax not supported");
}

} // namespace duckdb
