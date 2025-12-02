#include "mysql_result.hpp"

namespace duckdb {

static const size_t DEFAULT_BIND_BUFFER_SIZE = 64;

static size_t GetBufferSize(enum_field_types type, size_t max_len, const MySQLTypeConfig &type_config) {
	switch (type) {
	case MYSQL_TYPE_TINY:
		return sizeof(int8_t);
	case MYSQL_TYPE_SHORT:
		return sizeof(int16_t);
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_YEAR:
		return sizeof(int32_t);
	case MYSQL_TYPE_FLOAT:
		return sizeof(float);
	case MYSQL_TYPE_DOUBLE:
		return sizeof(double);
	case MYSQL_TYPE_LONGLONG:
		return sizeof(int64_t);
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIMESTAMP:
		return sizeof(MYSQL_TIME);
	default:
		return max_len > 0 ? max_len : DEFAULT_BIND_BUFFER_SIZE;
	}
}

MySQLField::MySQLField(MYSQL_FIELD *mf, LogicalType duckdb_type_p, const MySQLTypeConfig &type_config)
    : name(mf->name), mysql_type(mf->type), duckdb_type(std::move(duckdb_type_p)) {
	size_t buf_size = GetBufferSize(mf->type, mf->max_length, type_config);
	bind_buffer.resize(buf_size);
}

void MySQLField::ResetBind() {
	this->bind_is_null = 0;
	this->bind_length = 0;
	this->bind_error = 0;
	this->varlen_buffer.resize(0);
}

MYSQL_BIND MySQLField::CreateBindStruct() {
	MYSQL_BIND b;
	std::memset(&b, '\0', sizeof(MYSQL_BIND));
	b.buffer_type = mysql_type;
	b.buffer = bind_buffer.data();
	b.buffer_length = static_cast<unsigned long>(bind_buffer.size());
	b.is_null = &bind_is_null;
	b.length = &bind_length;
	b.error = &bind_error;
	return b;
}

vector<MySQLField> MySQLField::ReadFields(const std::string &query, MYSQL_STMT *stmt,
                                          const MySQLTypeConfig &type_config) {
	vector<MySQLField> fields;
	idx_t field_count = mysql_stmt_field_count(stmt);
	if (field_count == 0) {
		return fields;
	}

	auto rsmd = MySQLResultPtr(mysql_stmt_result_metadata(stmt), MySQLResultDelete);
	if (!rsmd) {
		throw IOException("Failed to fetch result metadata for MySQL query \"%s\": %s\n", query.c_str(),
		                  mysql_stmt_error(stmt));
	}

	MYSQL_FIELD *mfields = mysql_fetch_fields(rsmd.get());
	if (!mfields) {
		throw IOException("Failed to fetch result fields for MySQL query \"%s\": %s\n", query.c_str(),
		                  mysql_stmt_error(stmt));
	}
	fields.reserve(field_count);
	for (size_t i = 0; i < field_count; i++) {
		auto &mf = mfields[i];
		LogicalType ltype = MySQLTypes::FieldToLogicalType(type_config, &mf);
		MySQLField msf(&mf, std::move(ltype), type_config);
		fields.emplace_back(std::move(msf));
	}

	return fields;
}

} // namespace duckdb
