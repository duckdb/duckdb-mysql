#include "storage/mysql_table_set.hpp"
#include "storage/mysql_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/mysql_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

MySQLTableSet::MySQLTableSet(MySQLSchemaEntry &schema) : MySQLInSchemaSet(schema) {
}

void MySQLTableSet::AddColumn(ClientContext &context, MySQLResult &result, MySQLTableInfo &table_info,
                              idx_t column_offset) {
	MySQLTypeData type_info;
	idx_t column_index = column_offset;
	auto column_name = result.GetString(column_index);
	type_info.type_name = result.GetString(column_index + 1);
	type_info.column_type = result.GetString(column_index + 2);
	string default_value;
	auto is_nullable = result.GetString(column_index + 4);
	type_info.precision = result.IsNull(column_index + 5) ? -1 : result.GetInt64(column_index + 5);
	type_info.scale = result.IsNull(column_index + 6) ? -1 : result.GetInt64(column_index + 6);

	auto column_type = MySQLUtils::TypeToLogicalType(context, type_info);
	ColumnDefinition column(std::move(column_name), std::move(column_type));
	if (!default_value.empty()) {
		auto expressions = Parser::ParseExpressionList(default_value);
		if (expressions.empty()) {
			throw InternalException("Expression list is empty");
		}
		column.SetDefaultValue(std::move(expressions[0]));
	}
	auto &create_info = *table_info.create_info;
	if (is_nullable != "YES") {
		auto column_idx = create_info.columns.LogicalColumnCount();
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_idx)));
	}
	create_info.columns.AddColumn(std::move(column));
}

void MySQLTableSet::LoadEntries(ClientContext &context) {
	auto query = StringUtil::Replace(R"(
SELECT table_name, column_name, data_type, column_type, column_default, is_nullable, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema=${SCHEMA_NAME}
ORDER BY table_name, ordinal_position;
)",
	                                 "${SCHEMA_NAME}", MySQLUtils::WriteLiteral(schema.name));

	auto &transaction = MySQLTransaction::Get(context, catalog);
	auto result = transaction.Query(query);

	vector<unique_ptr<MySQLTableInfo>> tables;
	unique_ptr<MySQLTableInfo> info;

	while (result->Next()) {
		auto table_name = result->GetString(0);
		if (!info || info->GetTableName() != table_name) {
			if (info) {
				tables.push_back(std::move(info));
			}
			info = make_uniq<MySQLTableInfo>(schema, table_name);
		}
		AddColumn(context, *result, *info, 1);
	}
	if (info) {
		tables.push_back(std::move(info));
	}
	for (auto &tbl_info : tables) {
		auto table_entry = make_uniq<MySQLTableEntry>(catalog, schema, *tbl_info);
		CreateEntry(std::move(table_entry));
	}
}

string GetTableInfoQuery(const string &schema_name, const string &table_name) {
	return StringUtil::Replace(StringUtil::Replace(R"(
SELECT column_name, data_type, column_type, column_default, is_nullable, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema=${SCHEMA_NAME} AND table_name=${TABLE_NAME}
ORDER BY table_name, ordinal_position;
)",
	                                               "${SCHEMA_NAME}", MySQLUtils::WriteLiteral(schema_name)),
	                           "${TABLE_NAME}", MySQLUtils::WriteLiteral(table_name));
}

unique_ptr<MySQLTableInfo> MySQLTableSet::GetTableInfo(ClientContext &context, MySQLSchemaEntry &schema,
                                                       const string &table_name) {
	auto &transaction = MySQLTransaction::Get(context, schema.ParentCatalog());
	auto query = GetTableInfoQuery(schema.name, table_name);
	auto result = transaction.Query(query);
	auto table_info = make_uniq<MySQLTableInfo>(schema, table_name);
	while (result->Next()) {
		AddColumn(context, *result, *table_info, 0);
	}
	return table_info;
}

optional_ptr<CatalogEntry> MySQLTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	auto table_entry = make_uniq<MySQLTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

// FIXME - this is almost entirely copied from TableCatalogEntry::ColumnsToSQL -
// should be unified
string MySQLColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key
	// columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			vector<string> constraint_columns = pk.columns;
			if (pk.index.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after
				// all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << MySQLUtils::WriteIdentifier(column.Name()) << " ";
		ss << MySQLUtils::TypeToString(column.Type());
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.Generated()) {
			ss << " GENERATED ALWAYS AS(" << column.GeneratedExpression().ToString() << ")";
		} else if (column.HasDefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue().ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";
	return ss.str();
}

string GetMySQLCreateTable(ClientContext &context, CreateTableInfo &info) {
	for (idx_t i = 0; i < info.columns.LogicalColumnCount(); i++) {
		auto &col = info.columns.GetColumnMutable(LogicalIndex(i));
		col.SetType(MySQLUtils::ToMySQLType(context, col.GetType()));
	}

	std::stringstream ss;
	ss << "CREATE TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	if (!info.schema.empty()) {
		ss << MySQLUtils::WriteIdentifier(info.schema);
		ss << ".";
	}
	ss << MySQLUtils::WriteIdentifier(info.table);
	ss << MySQLColumnsToSQL(info.columns, info.constraints);
	ss << ";";
	return ss.str();
}

optional_ptr<CatalogEntry> MySQLTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &transaction = MySQLTransaction::Get(context, catalog);
	auto create_sql = GetMySQLCreateTable(context, info.Base());
	transaction.Query(create_sql);
	auto tbl_entry = make_uniq<MySQLTableEntry>(catalog, schema, info.Base());
	return CreateEntry(std::move(tbl_entry));
}

void MySQLTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	auto &transaction = MySQLTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += MySQLUtils::WriteIdentifier(info.name);
	sql += " RENAME TO ";
	sql += MySQLUtils::WriteIdentifier(info.new_table_name);
	transaction.Query(sql);
}

void MySQLTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	auto &transaction = MySQLTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += MySQLUtils::WriteIdentifier(info.name);
	sql += " RENAME COLUMN  ";
	sql += MySQLUtils::WriteIdentifier(info.old_name);
	sql += " TO ";
	sql += MySQLUtils::WriteIdentifier(info.new_name);

	transaction.Query(sql);
}

void MySQLTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	auto &transaction = MySQLTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += MySQLUtils::WriteIdentifier(info.name);
	sql += " ADD COLUMN  ";
	if (info.if_column_not_exists) {
		sql += "IF NOT EXISTS ";
	}
	sql += MySQLUtils::WriteIdentifier(info.new_column.Name());
	sql += " ";
	sql += info.new_column.Type().ToString();
	transaction.Query(sql);
}

void MySQLTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	auto &transaction = MySQLTransaction::Get(context, catalog);
	string sql = "ALTER TABLE ";
	sql += MySQLUtils::WriteIdentifier(info.name);
	sql += " DROP COLUMN  ";
	if (info.if_column_exists) {
		throw NotImplementedException("DROP COLUMN IF EXISTS not supported in MySQL");
	}
	sql += MySQLUtils::WriteIdentifier(info.removed_column);
	transaction.Query(sql);
}

void MySQLTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(context, alter.Cast<RenameTableInfo>());
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(context, alter.Cast<RenameColumnInfo>());
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(context, alter.Cast<AddColumnInfo>());
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(context, alter.Cast<RemoveColumnInfo>());
		break;
	default:
		throw BinderException("Unsupported ALTER TABLE type - MySQL tables only "
		                      "support RENAME TABLE, RENAME COLUMN, "
		                      "ADD COLUMN and DROP COLUMN");
	}
	ClearEntries();
}

} // namespace duckdb
