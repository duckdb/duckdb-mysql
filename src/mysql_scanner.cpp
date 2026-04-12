#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "mysql_scanner.hpp"
#include "mysql_result.hpp"
#include "mysql_connection_pool.hpp"
#include "storage/mysql_transaction.hpp"
#include "storage/mysql_table_set.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_statistics.hpp"
#include "storage/mysql_predicate_analyzer.hpp"
#include "storage/federation/cost_model.hpp"
#include "mysql_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

static constexpr idx_t MIN_QUERY_TIMEOUT_MS = 5000;
static constexpr idx_t MAX_QUERY_TIMEOUT_MS = 300000;
static constexpr double QUERY_TIMEOUT_COST_MULTIPLIER = 10.0;
static constexpr idx_t SQL_BUFFER_RESULT_ROW_THRESHOLD = 10000;

struct MySQLGlobalState;

struct MySQLLocalState : public LocalTableFunctionState {};

struct FederationState {
	FilterAnalysisResult filter_analysis;
	ExecutionPlan execution_plan;
	string partition_clause;
	unique_ptr<Expression> local_filter_expression;
	ExecutionPlanCacheKey adaptive_cache_key;
	idx_t adaptive_cache_generation = 0;
	idx_t adaptive_estimated_rows = 0;
};

struct MySQLGlobalState : public GlobalTableFunctionState {
	explicit MySQLGlobalState(unique_ptr<MySQLResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<MySQLResult> result;
	unique_ptr<ExpressionExecutor> local_filter_executor;
	unique_ptr<Expression> owned_filter_expression;

	idx_t total_rows_fetched = 0;
	ExecutionPlanCacheKey cache_key;
	idx_t estimated_rows = 0;
	idx_t cache_generation = 0;
	bool adaptive_feedback_enabled = false;
	bool feedback_submitted = false;
	optional_ptr<MySQLCatalog> catalog;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> MySQLBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("MySQLBind");
}

static void ConfigurePredicateAnalyzer(ClientContext &context, PredicateAnalyzer &analyzer) {
	Value hint_enabled_val, hint_threshold_val;
	bool hint_injection_enabled = false;
	double hint_staleness_threshold = 0.5;
	if (context.TryGetCurrentSetting("mysql_hint_injection_enabled", hint_enabled_val)) {
		hint_injection_enabled = BooleanValue::Get(hint_enabled_val);
	}
	if (context.TryGetCurrentSetting("mysql_hint_staleness_threshold", hint_threshold_val)) {
		hint_staleness_threshold = hint_threshold_val.GetValue<double>();
	}
	analyzer.SetHintInjectionEnabled(hint_injection_enabled, hint_staleness_threshold);

	Value push_idx_val, push_noidx_val;
	if (context.TryGetCurrentSetting("mysql_push_threshold_with_index", push_idx_val)) {
		double with_idx = push_idx_val.GetValue<double>();
		double no_idx = PredicateAnalyzer::DEFAULT_PUSH_THRESHOLD_NO_INDEX;
		if (context.TryGetCurrentSetting("mysql_push_threshold_no_index", push_noidx_val)) {
			no_idx = push_noidx_val.GetValue<double>();
		}
		analyzer.SetPushThresholds(with_idx, no_idx);
	} else if (context.TryGetCurrentSetting("mysql_push_threshold_no_index", push_noidx_val)) {
		analyzer.SetPushThresholds(PredicateAnalyzer::DEFAULT_PUSH_THRESHOLD_WITH_INDEX,
		                           push_noidx_val.GetValue<double>());
	}
}

static void ResolveExecutionPlan(ClientContext &context, FederationState &fed, const MySQLBindData &bind_data,
                                 MySQLStatisticsCollector &stats_collector, MySQLCatalog &mysql_catalog,
                                 MySQLConnection &con, const vector<string> &column_names,
                                 const ExecutionPlanCacheKey &cache_key) {
	CachedExecutionPlan cached;
	if (mysql_catalog.GetPlanCache().GetCachedPlan(cache_key, cached)) {
		fed.execution_plan = cached.plan;
		fed.adaptive_cache_generation = cached.plan_generation;
		fed.adaptive_estimated_rows = cached.plan.estimated_rows_from_mysql;
	} else {
		auto table_stats = stats_collector.GetTableStats(bind_data.table.schema.name, bind_data.table.name);
		auto mysql_costs = stats_collector.FetchMySQLCostConstants();
		CostModelParameters cost_params;
		if (mysql_costs.loaded) {
			cost_params.cpu_cost_per_row = mysql_costs.row_evaluate_cost;
			double effective_block_cost = mysql_costs.io_block_read_cost;
			if (mysql_costs.buffer_pool_hit_rate >= 0) {
				effective_block_cost = (mysql_costs.buffer_pool_hit_rate * mysql_costs.memory_block_read_cost) +
				                       ((1.0 - mysql_costs.buffer_pool_hit_rate) * mysql_costs.io_block_read_cost);
			}
			cost_params.io_cost_per_byte =
			    effective_block_cost / static_cast<double>(CostModelParameters::INNODB_PAGE_SIZE);
		}
		DefaultCostModel cost_model(cost_params);
		mysql_catalog.GetConnectionPool().EnsureCalibrated(con);
		cost_model.SetNetworkCalibration(mysql_catalog.GetConnectionPool().GetNetworkCalibration());
		Value compression_aware_val, compression_ratio_val;
		if (context.TryGetCurrentSetting("mysql_compression_aware_costs", compression_aware_val)) {
			cost_model.SetCompressionAwareCosts(BooleanValue::Get(compression_aware_val));
		}
		if (context.TryGetCurrentSetting("mysql_compression_ratio", compression_ratio_val)) {
			cost_model.SetCompressionRatio(compression_ratio_val.GetValue<double>());
		}
		fed.execution_plan = cost_model.ComparePlans(table_stats, fed.filter_analysis, column_names);

		fed.execution_plan.combined_selectivity = fed.filter_analysis.combined_selectivity;
		fed.execution_plan.recommended_index = fed.filter_analysis.recommended_index;

		fed.adaptive_estimated_rows = fed.execution_plan.estimated_rows_from_mysql;
		fed.adaptive_cache_generation = mysql_catalog.GetPlanCache().CachePlan(cache_key, fed.execution_plan);
	}
	fed.adaptive_cache_key = cache_key;
}

static void ResolvePartitionPruning(FederationState &fed, const MySQLBindData &bind_data,
                                    MySQLStatisticsCollector &stats_collector, const vector<column_t> &column_ids,
                                    optional_ptr<TableFilterSet> filters) {
	auto part_stats = stats_collector.GetTableStats(bind_data.table.schema.name, bind_data.table.name);
	const auto &part_info = part_stats.partition_info;
	if (!part_info.IsRangeOrList() || part_info.partitions.empty()) {
		return;
	}
	for (const auto &analysis : fed.filter_analysis.analyses) {
		if (!analysis.is_partition_key || !analysis.ShouldPush()) {
			continue;
		}
		for (const auto &entry : filters->filters) {
			column_t col_idx = entry.first;
			if (col_idx >= column_ids.size()) {
				continue;
			}
			column_t actual_col_idx = column_ids[col_idx];
			if (actual_col_idx >= bind_data.names.size()) {
				continue;
			}
			if (!StringUtil::CIEquals(bind_data.names[actual_col_idx], analysis.column_name)) {
				continue;
			}
			auto &filter = *entry.second;
			if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
				auto &cf = filter.Cast<ConstantFilter>();
				auto matching = part_info.GetMatchingPartitions(cf.constant, cf.comparison_type);
				if (!matching.empty() && matching.size() < part_info.partitions.size()) {
					vector<string> escaped;
					for (const auto &p : matching) {
						escaped.push_back(MySQLUtils::WriteIdentifier(p));
					}
					fed.partition_clause = "PARTITION (" + StringUtil::Join(escaped, ", ") + ")";
					fed.execution_plan.partition_clause = fed.partition_clause;
				}
			}
			break;
		}
		if (!fed.partition_clause.empty()) {
			break;
		}
	}
}

static string BuildFilterString(const FederationState &fed) {
	switch (fed.execution_plan.strategy) {
	case ExecutionStrategy::PUSH_ALL_FILTERS: {
		vector<string> all_predicates;
		for (const auto &analysis : fed.filter_analysis.analyses) {
			if (!analysis.mysql_predicate.empty()) {
				all_predicates.push_back(analysis.mysql_predicate);
			}
		}
		return StringUtil::Join(all_predicates, " AND ");
	}
	case ExecutionStrategy::EXECUTE_ALL_LOCALLY:
		return "";
	case ExecutionStrategy::HYBRID: {
		vector<string> pushed_predicates;
		for (const auto idx : fed.execution_plan.pushed_filter_indices) {
			if (idx < fed.filter_analysis.analyses.size()) {
				const auto &analysis = fed.filter_analysis.analyses[idx];
				if (!analysis.mysql_predicate.empty()) {
					pushed_predicates.push_back(analysis.mysql_predicate);
				}
			}
		}
		return StringUtil::Join(pushed_predicates, " AND ");
	}
	default:
		return "";
	}
}

static void BuildLocalFilterExpression(FederationState &fed, const MySQLBindData &bind_data,
                                       const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters) {
	if ((fed.execution_plan.strategy != ExecutionStrategy::HYBRID &&
	     fed.execution_plan.strategy != ExecutionStrategy::EXECUTE_ALL_LOCALLY) ||
	    fed.execution_plan.local_filter_indices.empty()) {
		return;
	}

	vector<unique_ptr<Expression>> local_exprs;
	for (auto filter_idx : fed.execution_plan.local_filter_indices) {
		if (filter_idx >= fed.filter_analysis.analyses.size()) {
			continue;
		}
		const auto &analysis = fed.filter_analysis.analyses[filter_idx];
		column_t output_col_idx = analysis.column_index;

		auto it = filters->filters.find(output_col_idx);
		if (it == filters->filters.end()) {
			continue;
		}
		if (output_col_idx >= column_ids.size()) {
			continue;
		}
		auto actual_table_col = column_ids[output_col_idx];
		auto &col = bind_data.table.GetColumn(LogicalIndex(actual_table_col));

		auto col_ref = make_uniq<BoundReferenceExpression>(col.GetType(), output_col_idx);
		auto expr = it->second->ToExpression(*col_ref);
		if (expr) {
			local_exprs.push_back(std::move(expr));
		}
	}

	if (local_exprs.size() == 1) {
		fed.local_filter_expression = std::move(local_exprs[0]);
	} else if (local_exprs.size() > 1) {
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : local_exprs) {
			conjunction->children.push_back(std::move(expr));
		}
		fed.local_filter_expression = std::move(conjunction);
	}
}

static void InjectQueryHints(ClientContext &context, string &select, const FederationState &fed,
                             const MySQLBindData &bind_data, MySQLConnection &con, MySQLStatsCache &shared_cache) {
	Value explain_val;
	if (context.TryGetCurrentSetting("mysql_explain_validation_enabled", explain_val) &&
	    BooleanValue::Get(explain_val) && !fed.filter_analysis.recommended_index.empty()) {
		MySQLStatisticsCollector explain_stats(con, shared_cache);
		bool uses_index = false;
		for (const auto &analysis : fed.filter_analysis.analyses) {
			if (analysis.has_index && analysis.ShouldPush()) {
				uses_index = true;
				break;
			}
		}
		if (uses_index) {
			bool valid = explain_stats.ValidateWithExplain(select, fed.filter_analysis.recommended_index, true);
			if (!valid) {
				auto &mysql_catalog = bind_data.table.catalog.Cast<MySQLCatalog>();
				mysql_catalog.GetPlanCache().InvalidateTable(bind_data.table.schema.name, bind_data.table.name);
				mysql_catalog.GetStatsCache().Invalidate(bind_data.table.schema.name, bind_data.table.name);
			}
		}
	}

	string prefix;

	Value timeout_val;
	bool timeout_enabled = true;
	if (context.TryGetCurrentSetting("mysql_query_timeout_enabled", timeout_val)) {
		timeout_enabled = BooleanValue::Get(timeout_val);
	}
	string cached_version;
	bool cached_histogram = false;
	bool version_supports_timeout = true;
	if (shared_cache.GetVersionInfo(cached_version, cached_histogram)) {
		bool is_mariadb = StringUtil::Contains(StringUtil::Lower(cached_version), "mariadb");
		if (is_mariadb) {
			version_supports_timeout = false;
		} else {
			auto dot = cached_version.find('.');
			if (dot != string::npos) {
				int major = 0;
				try {
					major = std::stoi(cached_version.substr(0, dot));
				} catch (...) {
				}
				if (major < 8) {
					version_supports_timeout = false;
				}
			}
		}
	}
	if (timeout_enabled && version_supports_timeout && fed.execution_plan.estimated_cost.Total() > 0) {
		idx_t min_timeout = MIN_QUERY_TIMEOUT_MS;
		idx_t max_timeout = MAX_QUERY_TIMEOUT_MS;
		Value min_val, max_val;
		if (context.TryGetCurrentSetting("mysql_query_timeout_min_ms", min_val)) {
			min_timeout = UBigIntValue::Get(min_val);
		}
		if (context.TryGetCurrentSetting("mysql_query_timeout_max_ms", max_val)) {
			max_timeout = UBigIntValue::Get(max_val);
		}
		double cost_total = fed.execution_plan.estimated_cost.Total();
		idx_t timeout_ms = min_timeout;
		if (std::isfinite(cost_total) && cost_total > 0.0) {
			double raw_timeout = cost_total * QUERY_TIMEOUT_COST_MULTIPLIER;
			if (raw_timeout < static_cast<double>(NumericLimits<idx_t>::Maximum())) {
				timeout_ms = std::min(max_timeout, std::max(min_timeout, static_cast<idx_t>(raw_timeout)));
			} else {
				timeout_ms = max_timeout;
			}
		}
		prefix += "/*+ MAX_EXECUTION_TIME(" + std::to_string(timeout_ms) + ") */ ";
	}

	Value buffer_val;
	bool buffer_enabled = true;
	if (context.TryGetCurrentSetting("mysql_sql_buffer_result", buffer_val)) {
		buffer_enabled = BooleanValue::Get(buffer_val);
	}
	if (buffer_enabled && fed.adaptive_estimated_rows > SQL_BUFFER_RESULT_ROW_THRESHOLD &&
	    bind_data.order_by_clause.empty()) {
		prefix += "SQL_BUFFER_RESULT ";
	}

	if (!prefix.empty()) {
		auto pos = select.find("SELECT ");
		if (pos != string::npos) {
			select.insert(pos + 7, prefix);
		}
	}
}

static void ConfigureAdaptiveFeedback(ClientContext &context, MySQLGlobalState &gstate, const FederationState &fed,
                                      const MySQLBindData &bind_data) {
	gstate.cache_key = fed.adaptive_cache_key;
	gstate.cache_generation = fed.adaptive_cache_generation;
	gstate.estimated_rows = fed.adaptive_estimated_rows;
	gstate.catalog = &bind_data.table.catalog.Cast<MySQLCatalog>();

	Value adaptive_enabled_val;
	if (context.TryGetCurrentSetting("mysql_adaptive_replan_enabled", adaptive_enabled_val)) {
		gstate.adaptive_feedback_enabled = BooleanValue::Get(adaptive_enabled_val);
	}
}

static unique_ptr<GlobalTableFunctionState> MySQLInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	const auto &bind_data = input.bind_data->Cast<MySQLBindData>();
	auto &transaction = MySQLTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();

	FederationState fed;
	auto &mysql_catalog = bind_data.table.catalog.Cast<MySQLCatalog>();

	if (bind_data.has_aggregate_pushdown) {
		auto build_aggregate_query = [&]() {
			string sql = "SELECT " + bind_data.aggregate_select_list;
			sql += " FROM ";
			sql += MySQLUtils::WriteIdentifier(bind_data.table.schema.name);
			sql += ".";
			sql += MySQLUtils::WriteIdentifier(bind_data.table.name);
			if (!bind_data.aggregate_where_clause.empty()) {
				sql += " WHERE " + bind_data.aggregate_where_clause;
			}
			if (!bind_data.group_by_clause.empty()) {
				sql += bind_data.group_by_clause;
			}
			if (!bind_data.order_by_clause.empty()) {
				sql += bind_data.order_by_clause;
			}
			if (!bind_data.limit.empty()) {
				sql += bind_data.limit;
			}
			return sql;
		};

		string select = build_aggregate_query();
		FederationState agg_fed;
		agg_fed.adaptive_estimated_rows = SQL_BUFFER_RESULT_ROW_THRESHOLD + 1;
		agg_fed.execution_plan.estimated_cost.cpu_cost = static_cast<double>(MIN_QUERY_TIMEOUT_MS);
		InjectQueryHints(context, select, agg_fed, bind_data, con, mysql_catalog.GetStatsCache());
		try {
			auto query_result = con.Query(select, MySQLResultStreaming::FORCE_MATERIALIZATION);
			return make_uniq<MySQLGlobalState>(std::move(query_result));
		} catch (std::bad_alloc &) {
			throw;
		} catch (std::exception &) {
			string fallback = build_aggregate_query();
			auto query_result = con.Query(fallback, MySQLResultStreaming::FORCE_MATERIALIZATION);
			return make_uniq<MySQLGlobalState>(std::move(query_result));
		}
	}

	string select;
	select += "SELECT ";
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			select += "NULL";
		} else {
			auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
			auto col_name = col.GetName();
			select += MySQLUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += MySQLUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += MySQLUtils::WriteIdentifier(bind_data.table.name);

	string filter_string;

	if (bind_data.use_predicate_analyzer && input.filters && !input.filters->filters.empty()) {
		try {
			MySQLStatisticsCollector stats_collector(con, mysql_catalog.GetStatsCache());
			PredicateAnalyzer analyzer(stats_collector, bind_data.table.schema.name, bind_data.table.name);
			ConfigurePredicateAnalyzer(context, analyzer);

			fed.filter_analysis = analyzer.AnalyzeFilters(input.column_ids, input.filters, bind_data.names);

			vector<string> column_names;
			for (idx_t c = 0; c < input.column_ids.size(); c++) {
				if (input.column_ids[c] != COLUMN_IDENTIFIER_ROW_ID) {
					auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
					column_names.push_back(col.GetName());
				}
			}

			if (!fed.filter_analysis.analyses.empty()) {
				vector<string> filter_cols;
				vector<double> sels;
				for (const auto &analysis : fed.filter_analysis.analyses) {
					filter_cols.push_back(analysis.column_name);
					sels.push_back(analysis.estimated_selectivity);
				}
				ExecutionPlanCacheKey cache_key(bind_data.table.schema.name, bind_data.table.name, column_names,
				                                filter_cols, sels);

				ResolveExecutionPlan(context, fed, bind_data, stats_collector, mysql_catalog, con, column_names,
				                     cache_key);
				ResolvePartitionPruning(fed, bind_data, stats_collector, input.column_ids, input.filters);

				filter_string = BuildFilterString(fed);
				BuildLocalFilterExpression(fed, bind_data, input.column_ids, input.filters);
			} else {
				filter_string = MySQLFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
			}
		} catch (std::bad_alloc &) {
			throw;
		} catch (std::exception &e) {
#ifndef NDEBUG
			Printer::Print(StringUtil::Format("MySQL federation: falling back to simple pushdown (%s)", e.what()));
#endif
			fed = FederationState();
			filter_string = MySQLFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
		}
	} else {
		filter_string = MySQLFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
	}

	if (bind_data.use_predicate_analyzer && !fed.partition_clause.empty()) {
		select += " " + fed.partition_clause;
	}
	if (bind_data.use_predicate_analyzer && !fed.filter_analysis.recommended_index.empty()) {
		string index_identifier = MySQLUtils::WriteIdentifier(fed.filter_analysis.recommended_index);
		if (fed.filter_analysis.suggest_force_index) {
			select += " FORCE INDEX (" + index_identifier + ")";
		} else {
			select += " USE INDEX (" + index_identifier + ")";
		}
	}
	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}
	if (!bind_data.order_by_clause.empty()) {
		select += bind_data.order_by_clause;
	}
	if (!bind_data.limit.empty()) {
		select += bind_data.limit;
	}

	if (bind_data.use_predicate_analyzer) {
		InjectQueryHints(context, select, fed, bind_data, con, mysql_catalog.GetStatsCache());
	}

	auto query_result = con.Query(select, MySQLResultStreaming::FORCE_MATERIALIZATION);
	auto result = make_uniq<MySQLGlobalState>(std::move(query_result));

	if (bind_data.use_predicate_analyzer) {
		ConfigureAdaptiveFeedback(context, *result, fed, bind_data);
	}

	if (fed.local_filter_expression) {
		result->owned_filter_expression = std::move(fed.local_filter_expression);
		result->local_filter_executor = make_uniq<ExpressionExecutor>(context, *result->owned_filter_expression);
	}

	return result;
}

static unique_ptr<LocalTableFunctionState> MySQLInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<MySQLLocalState>();
}

static void MySQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();

	while (true) {
		if (gstate.result->Exhausted()) {
			if (gstate.adaptive_feedback_enabled && !gstate.feedback_submitted && gstate.catalog &&
			    gstate.estimated_rows > 0) {
				gstate.catalog->GetPlanCache().UpdatePlanFeedback(gstate.cache_key, gstate.total_rows_fetched,
				                                                  gstate.cache_generation);
				gstate.feedback_submitted = true;
			}
			output.SetCardinality(0);
			return;
		}

		DataChunk &res_chunk = gstate.result->NextChunk();
		gstate.total_rows_fetched += res_chunk.size();
		D_ASSERT(output.ColumnCount() == res_chunk.ColumnCount());
		string error;
		for (idx_t c = 0; c < output.ColumnCount(); c++) {
			Vector &output_vec = output.data[c];
			Vector &res_vec = res_chunk.data[c];
			switch (output_vec.GetType().id()) {
			case LogicalTypeId::BOOLEAN:
			case LogicalTypeId::TINYINT:
			case LogicalTypeId::UTINYINT:
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::USMALLINT:
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::UINTEGER:
			case LogicalTypeId::BIGINT:
			case LogicalTypeId::UBIGINT:
			case LogicalTypeId::FLOAT:
			case LogicalTypeId::DOUBLE:
			case LogicalTypeId::BLOB:
			case LogicalTypeId::DATE:
			case LogicalTypeId::TIME:
			case LogicalTypeId::TIMESTAMP: {
				if (output_vec.GetType().id() == res_vec.GetType().id() ||
				    (output_vec.GetType().id() == LogicalTypeId::BLOB &&
				     res_vec.GetType().id() == LogicalTypeId::VARCHAR)) {
					output_vec.Reinterpret(res_vec);
				} else {
					VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
				}
				break;
			}
			default: {
				VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
				break;
			}
			}
			if (!error.empty()) {
				throw BinderException(error);
			}
		}
		output.SetCardinality(res_chunk.size());

		if (gstate.local_filter_executor) {
			SelectionVector sel(output.size());
			idx_t result_count = gstate.local_filter_executor->SelectExpression(output, sel);
			if (result_count == 0) {
				output.SetCardinality(0);
				continue;
			}
			if (result_count < output.size()) {
				output.Slice(sel, result_count);
				output.SetCardinality(result_count);
			}
			return;
		} else {
			return;
		}
	}
}

static InsertionOrderPreservingMap<string> MySQLScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<MySQLBindData>();
	result["Table"] = bind_data.table.name;
	return result;
}

static void MySQLScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("MySQLScanSerialize");
}

static unique_ptr<FunctionData> MySQLScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("MySQLScanDeserialize");
}

static BindInfo MySQLGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MySQLBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table;
	return info;
}

MySQLScanFunction::MySQLScanFunction()
    : TableFunction("mysql_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLScan,
                    MySQLBind, MySQLInitGlobalState, MySQLInitLocalState) {
	to_string = MySQLScanToString;
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	get_bind_info = MySQLGetBindInfo;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// MySQL Query
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> MySQLQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to mysql_query cannot be NULL");
	}

	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in mysql_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "mysql") {
		throw BinderException("Attached database \"%s\" does not refer to a MySQL database", db_name);
	}
	auto sql = input.inputs[1].GetValue<string>();

	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to mysql_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}

	bool streaming_enabled = true;
	auto streaming_it = input.named_parameters.find("stream_results");
	if (streaming_it != input.named_parameters.end()) {
		Value &bool_val = streaming_it->second;
		if (!bool_val.IsNull()) {
			streaming_enabled = BooleanValue::Get(bool_val);
		}
	}

	if (!streaming_enabled) {
		auto &transaction = MySQLTransaction::Get(context, catalog);
		MySQLConnection &conn = transaction.GetConnection();
		auto result = conn.Query(sql, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
		for (auto &field : result->Fields()) {
			names.push_back(field.name);
			return_types.push_back(field.duckdb_type);
		}
		return make_uniq<MySQLQueryBindData>(std::move(sql), catalog, std::move(result));
	}

	auto &mysql_catalog = catalog.Cast<MySQLCatalog>();

	auto acquire_mode = MySQLConnectionPool::GetAcquireMode(context);
	std::string time_zone;
	Value mysql_session_time_zone;
	if (context.TryGetCurrentSetting("mysql_session_time_zone", mysql_session_time_zone)) {
		time_zone = mysql_session_time_zone.ToString();
	}
	auto conn = mysql_catalog.GetConnectionPool().Acquire(acquire_mode, time_zone);
	auto type_config = MySQLTypeConfig(context);
	conn->SetTypeConfig(type_config);

	auto stmt = conn->Prepare(sql);
	for (auto &field : stmt->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.duckdb_type);
	}
	return make_uniq<MySQLQueryBindData>(std::move(sql), catalog, std::move(conn), std::move(stmt), std::move(params));
}

static unique_ptr<GlobalTableFunctionState> MySQLQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLQueryBindData>();
	unique_ptr<MySQLResult> mysql_result;
	if (bind_data.result) {
		mysql_result = std::move(bind_data.result);
	}
	return make_uniq<MySQLGlobalState>(std::move(mysql_result));
}

static void MySQLQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
	if (!gstate.result) {
		auto &bind_data = data.bind_data->CastNoConst<MySQLQueryBindData>();
		D_ASSERT(bind_data.pooled_connection);
		D_ASSERT(bind_data.stmt);
		auto result = bind_data.pooled_connection->Query(*bind_data.stmt, bind_data.params,
		                                                 MySQLResultStreaming::ALLOW_STREAMING);
		gstate.result = std::move(result);
	}
	MySQLScan(context, data, output);
}

MySQLQueryFunction::MySQLQueryFunction()
    : TableFunction("mysql_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLQueryScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
	named_parameters["stream_results"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
