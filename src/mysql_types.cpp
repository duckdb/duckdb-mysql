#include "mysql_types.hpp"

#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/logical_type_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

static bool GetBoolOption(ClientContext &context, const std::string &opt_name) {
	Value val;
	if (context.TryGetCurrentSetting(Identifier(opt_name), val)) {
		return BooleanValue::Get(val);
	}
	return false;
}

MySQLTypeConfig::MySQLTypeConfig() {
}

MySQLTypeConfig::MySQLTypeConfig(ClientContext &context)
    : bit1_as_boolean(GetBoolOption(context, "mysql_bit1_as_boolean")),
      tinyint1_as_boolean(GetBoolOption(context, "mysql_tinyint1_as_boolean")),
      time_as_time(GetBoolOption(context, "mysql_time_as_time")),
      incomplete_dates_as_nulls(GetBoolOption(context, "mysql_incomplete_dates_as_nulls")) {
}

LogicalType MySQLTypes::TypeToLogicalType(const MySQLTypeConfig &type_config, const MySQLTypeData &type_info) {
	if (type_info.type_name == "tinyint") {
		if (type_info.column_type == "tinyint(1)" && type_config.tinyint1_as_boolean) {
			return LogicalType::BOOLEAN;
		}
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UTINYINT;
		} else {
			return LogicalType::TINYINT;
		}
	} else if (type_info.type_name == "smallint") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::USMALLINT;
		} else {
			return LogicalType::SMALLINT;
		}
	} else if (type_info.type_name == "mediumint" || type_info.type_name == "int") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UINTEGER;
		} else {
			return LogicalType::INTEGER;
		}
	} else if (type_info.type_name == "bigint") {
		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
			return LogicalType::UBIGINT;
		} else {
			return LogicalType::BIGINT;
		}
	} else if (type_info.type_name == "float") {
		return LogicalType::FLOAT;
	} else if (type_info.type_name == "double") {
		return LogicalType::DOUBLE;
	} else if (type_info.type_name == "date") {
		return LogicalType::DATE;
	} else if (type_info.type_name == "time") {
		// we convert time to VARCHAR by default because TIME in MySQL is more like an
		// interval and can store ranges between -838:00:00 to 838:00:00
		return type_config.time_as_time ? LogicalType::TIME : LogicalType::VARCHAR;
	} else if (type_info.type_name == "timestamp") {
		// in MySQL, "timestamp" columns are timezone aware while "datetime" columns
		// are not, citing the docs:
		//
		// > If a column uses the TIMESTAMP data type, then any inserted values are
		// > converted from the session's time zone to Coordinated Universal Time (UTC)
		// > when stored, and converted back to the session's time zone when retrieved.
		//
		// Thus Incoming MySQL TIMESTAMP values have time zone offset already applied, and
		// this offset is not available to us ('time_zone_displacement' field of MYSQL_TIME
		// is always zero). So we cannot obtain the UTC (+00) timestamp from it, thus
		// cannot use it to initialize the TIMESTAMP_TZ value which expects the UTC (+00)
		// timestamp, so we are treating it as a 'local' (to server session time zone)
		// timestamp.
		return LogicalType::TIMESTAMP;
	} else if (type_info.type_name == "year") {
		return LogicalType::INTEGER;
	} else if (type_info.type_name == "datetime") {
		return LogicalType::TIMESTAMP;
	} else if (type_info.type_name == "decimal") {
		if (type_info.precision > 0 && type_info.precision <= 38) {
			return LogicalType::DECIMAL(type_info.precision, type_info.scale);
		}
		return LogicalType::DOUBLE;
	} else if (type_info.type_name == "json") {
		// FIXME
		return LogicalType::VARCHAR;
	} else if (type_info.type_name == "enum") {
		// FIXME: we can actually retrieve the enum values from the column_type
		return LogicalType::VARCHAR;
	} else if (type_info.type_name == "set") {
		// FIXME: set is essentially a list of enum
		return LogicalType::VARCHAR;
	} else if (type_info.type_name == "bit") {
		if (type_info.column_type == "bit(1)" && type_config.bit1_as_boolean) {
			return LogicalType::BOOLEAN;
		}
		return LogicalType::BLOB;
	} else if (type_info.type_name == "blob" || type_info.type_name == "tinyblob" ||
	           type_info.type_name == "mediumblob" || type_info.type_name == "longblob" ||
	           type_info.type_name == "binary" || type_info.type_name == "varbinary" ||
	           type_info.type_name == "geometry" || type_info.type_name == "point" ||
	           type_info.type_name == "linestring" || type_info.type_name == "polygon" ||
	           type_info.type_name == "multipoint" || type_info.type_name == "multilinestring" ||
	           type_info.type_name == "multipolygon" || type_info.type_name == "geomcollection") {
		return LogicalType::BLOB;
	} else if (type_info.type_name == "varchar" || type_info.type_name == "mediumtext" ||
	           type_info.type_name == "longtext" || type_info.type_name == "text" || type_info.type_name == "enum" ||
	           type_info.type_name == "char") {
		return LogicalType::VARCHAR;
	}
	// fallback for unknown types
	return LogicalType::VARCHAR;
}

LogicalType MySQLTypes::FieldToLogicalType(const MySQLTypeConfig &type_config, MYSQL_FIELD *field) {
	MySQLTypeData type_data;
	switch (field->type) {
	case MYSQL_TYPE_TINY:
		type_data.type_name = "tinyint";
		break;
	case MYSQL_TYPE_SHORT:
		type_data.type_name = "smallint";
		break;
	case MYSQL_TYPE_INT24:
		type_data.type_name = "mediumint";
		break;
	case MYSQL_TYPE_LONG:
		type_data.type_name = "int";
		break;
	case MYSQL_TYPE_LONGLONG:
		type_data.type_name = "bigint";
		break;
	case MYSQL_TYPE_FLOAT:
		type_data.type_name = "float";
		break;
	case MYSQL_TYPE_DOUBLE:
		type_data.type_name = "double";
		break;
	case MYSQL_TYPE_DECIMAL:
	case MYSQL_TYPE_NEWDECIMAL:
		type_data.precision = int64_t(field->length) - 2; // -2 for minus sign and dot
		type_data.scale = field->decimals;
		type_data.type_name = "decimal";
		break;
	case MYSQL_TYPE_TIMESTAMP:
		type_data.type_name = "timestamp";
		break;
	case MYSQL_TYPE_DATE:
		type_data.type_name = "date";
		break;
	case MYSQL_TYPE_TIME:
		type_data.type_name = "time";
		break;
	case MYSQL_TYPE_DATETIME:
		type_data.type_name = "datetime";
		break;
	case MYSQL_TYPE_YEAR:
		type_data.type_name = "year";
		break;
	case MYSQL_TYPE_BIT:
		type_data.type_name = "bit";
		if (type_config.bit1_as_boolean && field->length == 1) {
			type_data.column_type = "bit(1)";
		}
		break;
	case MYSQL_TYPE_GEOMETRY:
		type_data.type_name = "geometry";
		break;
	case MYSQL_TYPE_NULL:
		type_data.type_name = "null";
		break;
	case MYSQL_TYPE_SET:
		type_data.type_name = "set";
		break;
	case MYSQL_TYPE_ENUM:
		type_data.type_name = "enum";
		break;
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_VAR_STRING:
		if (field->flags & BINARY_FLAG) {
			type_data.type_name = "blob";
		} else {
			type_data.type_name = "varchar";
		}
		break;
	default:
		type_data.type_name = "__unknown_type";
		break;
	}
	if (type_data.column_type.empty()) {
		type_data.column_type = type_data.type_name;
		if (field->length != 0) {
			type_data.column_type += "(" + std::to_string(field->length) + ")";
		}
	}
	if (field->flags & UNSIGNED_FLAG && field->flags & NUM_FLAG) {
		type_data.column_type += " unsigned";
	}
	return MySQLTypes::TypeToLogicalType(type_config, type_data);
}

LogicalType MySQLTypes::ToMySQLType(const MySQLTypeConfig &type_config, const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
		return type_config.time_as_time ? input : LogicalType::VARCHAR;
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::LIST:
		throw NotImplementedException("MySQL does not support arrays - unsupported type \"%s\"", input.ToString());
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
		throw NotImplementedException("MySQL does not support composite types - unsupported type \"%s\"",
		                              input.ToString());
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

string MySQLTypes::TypeToString(const LogicalType &input) {
	switch (input.id()) {
	case LogicalType::VARCHAR:
		return "TEXT";
	case LogicalType::UTINYINT:
		return "TINYINT UNSIGNED";
	case LogicalType::USMALLINT:
		return "SMALLINT UNSIGNED";
	case LogicalType::UINTEGER:
		return "INTEGER UNSIGNED";
	case LogicalType::UBIGINT:
		return "BIGINT UNSIGNED";
	case LogicalType::TIMESTAMP:
		return "DATETIME";
	case LogicalType::TIMESTAMP_TZ:
		return "TIMESTAMP";
	default:
		return input.ToString();
	}
}

LogicalType MySQLTypes::ToLogicalType(optional_ptr<ParsedExpression> type_expr_ptr) {
	if (!type_expr_ptr) {
		throw InvalidInputException("MySQLTypes::ToLogicalType: null type expression specified");
	}
	if (type_expr_ptr->GetExpressionType() != ExpressionType::TYPE) {
		throw InvalidInputException("MySQLTypes::ToLogicalType: unable to extract type name from expression: \"%s\"",
		                            type_expr_ptr->ToString());
	}
	const TypeExpression &type_expr = type_expr_ptr->Cast<TypeExpression>();
	return ToLogicalType(type_expr);
}

static uint8_t ExtractTinyIntValue(const ParsedExpression *parsed_expr) {
	if (!parsed_expr) {
		throw InvalidInputException("MySQLTypes::ToLogicalType: empty DECIMAL type modifier expression");
	}
	if (parsed_expr->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		throw InvalidInputException(
		    "MySQLTypes::ToLogicalType: DECIMAL type modifier must be a constant value, expression: \"%s\"",
		    parsed_expr->ToString());
	}
	const ConstantExpression &const_expr = parsed_expr->Cast<ConstantExpression>();
	Value const_val = const_expr.GetLiteral().ToValue();
	string cast_err;
	std::optional<Value> int_val = const_val.DefaultTryCastAs(LogicalType::TINYINT, &cast_err, true);
	if (!int_val.has_value()) {
		throw InvalidInputException(
		    "MySQLTypes::ToLogicalType: DECIMAL type modifier must be a TINYINT, expression: \"%s\"",
		    parsed_expr->ToString());
	}
	return TinyIntValue::Get(int_val.value());
}

static string ExtractVarcharCollation(const ParsedExpression *parsed_expr) {
	if (!parsed_expr) {
		throw InvalidInputException("MySQLTypes::ToLogicalType: empty VARCHAR collation expression");
	}
	if (parsed_expr->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		throw InvalidInputException(
		    "MySQLTypes::ToLogicalType: VARCHAR collation must be a constant value, expression: \"%s\"",
		    parsed_expr->ToString());
	}
	const ConstantExpression &const_expr = parsed_expr->Cast<ConstantExpression>();
	Value const_val = const_expr.GetLiteral().ToValue();
	if (const_val.type().id() == LogicalTypeId::VARCHAR) {
		return StringValue::Get(const_val);
	}
	return string();
}

LogicalType MySQLTypes::ToLogicalType(const TypeExpression &type_expr) {
	const Identifier &type_name = type_expr.GetTypeName();
	// pure map lookup, no other logic is inside, map can be moved to extension if needed
	LogicalTypeId type_id = DefaultTypeGenerator::GetDefaultType(type_name);
	if (type_id == LogicalTypeId::INVALID) {
		throw InvalidInputException("MySQLTypes::ToLogicalType: invalid type expression: \"%s\"", type_expr.ToString());
	}
	if (type_id == LogicalTypeId::DECIMAL) {
		const vector<unique_ptr<ParsedExpression>> &children = type_expr.GetChildren();
		unique_ptr<DecimalTypeInfo> type_info = make_uniq<DecimalTypeInfo>(18, 3);
		if (children.size() > 0) {
			type_info->width = ExtractTinyIntValue(children[0].get());
		}
		if (children.size() > 1) {
			type_info->scale = ExtractTinyIntValue(children[1].get());
		}
		return LogicalType(type_id, std::move(type_info));
	}
	if (type_id == LogicalType::VARCHAR) {
		const vector<unique_ptr<ParsedExpression>> &children = type_expr.GetChildren();
		unique_ptr<StringTypeInfo> type_info = make_uniq<StringTypeInfo>("");
		if (children.size() > 0) {
			type_info->collation = ExtractVarcharCollation(children[0].get());
		}
		return LogicalType(type_id, std::move(type_info));
	}
	return LogicalType(type_id);
}

} // namespace duckdb
