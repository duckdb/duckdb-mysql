#include "mysql_filter_pushdown.hpp"

#include "dbconn/table_scan/filter_pushdown.hpp"

namespace duckdb {

string MySQLFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                             const vector<string> &names) {
	if (!filters || !filters->HasFilters()) {
		// no filters
		return string();
	}
	string result;
	for (auto &entry : *filters) {
		column_t col_id = column_ids[entry.GetIndex()];
		auto column_name = names[col_id];
		auto &filter = entry.Filter();
		dbconnector::table_scan::FilterPushdownConfig config('`');
		auto new_filter = dbconnector::table_scan::FilterPushdown::TransformFilter(config, column_name, filter);
		if (new_filter.empty()) {
			if (dbconnector::table_scan::FilterPushdown::IsInternalFilter(filter)) {
				continue;
			}
			throw NotImplementedException(
			    "Unsupported filter pushdown, use 'mysql_enable_filter_pushdown=FALSE' to disable pushdowns."
			    " Problematic filter: \"%s\"",
			    dbconnector::table_scan::FilterPushdown::ToString(filter));
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += new_filter;
	}
	return result;
}

} // namespace duckdb
