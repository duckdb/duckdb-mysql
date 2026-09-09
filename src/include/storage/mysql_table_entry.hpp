//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/mysql_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "dbconnector/attached.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "mysql_utils.hpp"

namespace duckdb {

struct MySQLTableInfo {
	MySQLTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
	}
	MySQLTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(QualifiedName(Identifier(), Identifier(schema), Identifier(table)));
	}
	MySQLTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, Identifier(table));
	}

	const string &GetTableName() const {
		return create_info->GetTableName().GetIdentifierName();
	}

	unique_ptr<CreateTableInfo> create_info;
};

class MySQLTableEntry : public TableCatalogEntry {
public:
	MySQLTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	MySQLTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, MySQLTableInfo &info);

public:
	const ColumnList &GetColumns() const override;

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	static dbconnector::attached::AttachedTable Lookup(ClientContext &ctx, QualifiedName name);

protected:
	ColumnList columns;
};

} // namespace duckdb
