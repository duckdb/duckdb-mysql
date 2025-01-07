#include "storage/mysql_execute_query.hpp"
#include "storage/mysql_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_transaction.hpp"
#include "mysql_connection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"

namespace duckdb {

MySQLExecuteQuery::MySQLExecuteQuery(LogicalOperator &op, string op_name_p, TableCatalogEntry &table, string query_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), op_name(std::move(op_name_p)), table(table),
      query(std::move(query_p)) {
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
	auto result = connection.Query(query);
	gstate.affected_rows = result->AffectedRows();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType MySQLExecuteQuery::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<MySQLExecuteQueryGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.affected_rows));

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
	result["Table Name"] = table.name;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
string ExtractFilters(PhysicalOperator &child, const string &statement) {
	// FIXME - all of this is pretty gnarly, we should provide a hook earlier on
	// in the planning process to convert this into a SQL statement
	if (child.type == PhysicalOperatorType::FILTER) {
		auto &filter = child.Cast<PhysicalFilter>();
		auto result = ExtractFilters(*child.children[0], statement);
		auto filter_str = filter.expression->ToString();
		if (result.empty()) {
			return filter_str;
		} else {
			return result + " AND " + filter_str;
		}
	} else if (child.type == PhysicalOperatorType::PROJECTION) {
		auto &proj = child.Cast<PhysicalProjection>();
		for (auto &expr : proj.select_list) {
			switch (expr->type) {
			case ExpressionType::BOUND_REF:
			case ExpressionType::BOUND_COLUMN_REF:
			case ExpressionType::VALUE_CONSTANT:
				break;
			default:
				throw NotImplementedException("Unsupported expression type in projection - only simple deletes/updates "
				                              "are supported in the MySQL connector");
			}
		}
		return ExtractFilters(*child.children[0], statement);
	} else if (child.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = child.Cast<PhysicalTableScan>();
		if (!table_scan.table_filters) {
			return string();
		}
		throw NotImplementedException("Pushed down table filters not supported currently");
	} else {
		throw NotImplementedException("Unsupported operator type %s in %s statement - only simple deletes "
		                              "(e.g. %s "
		                              "FROM tbl WHERE x=y) are supported in the MySQL connector",
		                              PhysicalOperatorToString(child.type), statement, statement);
	}
}

string ConstructDeleteStatement(LogicalDelete &op, PhysicalOperator &child) {
	string result = "DELETE FROM ";
	result += MySQLUtils::WriteIdentifier(op.table.schema.name);
	result += ".";
	result += MySQLUtils::WriteIdentifier(op.table.name);
	auto filters = ExtractFilters(child, "DELETE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

unique_ptr<PhysicalOperator> MySQLCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                      unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a MySQL table");
	}

	auto result = make_uniq<MySQLExecuteQuery>(op, "DELETE", op.table, ConstructDeleteStatement(op, *plan));
	result->children.push_back(std::move(plan));
	return std::move(result);
}

string ConstructUpdateStatement(LogicalUpdate &op, PhysicalOperator &child) {
	// FIXME - all of this is pretty gnarly, we should provide a hook earlier on
	// in the planning process to convert this into a SQL statement
	string result = "UPDATE";
	result += MySQLUtils::WriteIdentifier(op.table.schema.name);
	result += ".";
	result += MySQLUtils::WriteIdentifier(op.table.name);
	result += " SET ";
	if (child.type != PhysicalOperatorType::PROJECTION) {
		throw NotImplementedException("MySQL Update not supported - Expected the "
		                              "child of an update to be a projection");
	}
	auto &proj = child.Cast<PhysicalProjection>();
	for (idx_t c = 0; c < op.columns.size(); c++) {
		if (c > 0) {
			result += ", ";
		}
		auto &col = op.table.GetColumn(op.table.GetColumns().PhysicalToLogical(op.columns[c]));
		result += MySQLUtils::WriteIdentifier(col.GetName());
		result += " = ";
		if (op.expressions[c]->type == ExpressionType::VALUE_DEFAULT) {
			result += "DEFAULT";
			continue;
		}
		if (op.expressions[c]->type != ExpressionType::BOUND_REF) {
			throw NotImplementedException("MySQL Update not supported - Expected a bound reference expression");
		}
		auto &ref = op.expressions[c]->Cast<BoundReferenceExpression>();
		result += proj.select_list[ref.index]->ToString();
	}
	result += " ";
	auto filters = ExtractFilters(*child.children[0], "UPDATE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

unique_ptr<PhysicalOperator> MySQLCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                      unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a MySQL table");
	}
	auto result = make_uniq<MySQLExecuteQuery>(op, "UPDATE", op.table, ConstructUpdateStatement(op, *plan));
	result->children.push_back(std::move(plan));
	return std::move(result);
}

} // namespace duckdb
