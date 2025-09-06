#include "mysql_result.hpp"

#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

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
	this->bind_is_null = false;
	this->bind_length = 0;
	this->bind_error = false;
}

static vector<MySQLField> ReadFields(const std::string &query, MYSQL_STMT *stmt, const MySQLTypeConfig &type_config) {
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

static vector<MYSQL_BIND> InitBinds(vector<MySQLField> &fields) {
	vector<MYSQL_BIND> binds;

	for (auto &f : fields) {
		MYSQL_BIND b;
		std::memset(&b, '\0', sizeof(MYSQL_BIND));
		b.buffer_type = f.mysql_type;
		b.buffer = f.bind_buffer.data();
		b.buffer_length = static_cast<unsigned long>(f.bind_buffer.size());
		b.is_null = &f.bind_is_null;
		b.length = &f.bind_length;
		b.error = &f.bind_error;
		binds.emplace_back(b);
	}

	return binds;
}

static vector<LogicalType> CreateChunkTypes(vector<MySQLField> &fields, const MySQLTypeConfig &type_config) {
	vector<LogicalType> ltypes;
	for (idx_t c = 0; c < fields.size(); c++) {
		MySQLField &f = fields[c];
		LogicalType &lt = f.duckdb_type;
		switch (lt.id()) {
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
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
			ltypes.push_back(lt);
			break;
		case LogicalTypeId::DOUBLE: {
			if (f.mysql_type == MYSQL_TYPE_DOUBLE) {
				ltypes.push_back(lt);
			} else {
				ltypes.push_back(LogicalType::VARCHAR);
			}
			break;
		}
		case LogicalTypeId::TIME: {
			if (type_config.time_as_time) {
				ltypes.push_back(lt);
			} else {
				ltypes.push_back(LogicalType::VARCHAR);
			}
			break;
		}
		default:
			ltypes.push_back(LogicalType::VARCHAR);
		}
	}
	return ltypes;
}

MySQLResult::MySQLResult(const std::string &query_p, MySQLStatementPtr stmt_p, MySQLTypeConfig type_config_p,
                         idx_t affected_rows_p)
    : query(query_p), stmt(std::move(stmt_p)), type_config(std::move(type_config_p)), affected_rows(affected_rows_p) {
	if (affected_rows != static_cast<idx_t>(-1)) {
		return;
	}
	this->fields = ReadFields(query, stmt.get(), type_config);
	this->binds = InitBinds(fields);

	int res_bind = mysql_stmt_bind_result(stmt.get(), binds.data());
	if (res_bind != 0) {
		throw IOException("Failed to bind result set for MySQL query \"%s\": %s\n", query.c_str(),
		                  mysql_stmt_error(stmt.get()));
	}

	auto ltypes = CreateChunkTypes(fields, type_config);
	this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);
}

void MySQLResult::HandleTruncatedData() {
	for (size_t i = 0; i < fields.size(); i++) {
		MySQLField &f = fields[i];
		MYSQL_BIND &b = binds[i];

		if (!f.bind_error || f.bind_buffer.size() >= f.bind_length) {
			continue;
		}

		size_t offset = f.bind_buffer.size();
		f.bind_buffer.resize(static_cast<size_t>(f.bind_length));
		b.buffer = f.bind_buffer.data() + offset;
		b.buffer_length = static_cast<unsigned long>(f.bind_buffer.size() - offset);

		f.ResetBind();
		int res_fetch =
		    mysql_stmt_fetch_column(stmt.get(), &b, static_cast<unsigned int>(i), static_cast<unsigned long>(offset));
		if (res_fetch != 0) {
			throw IOException("Failed to re-fetch result field \"%s\" for MySQL query \"%s\": %s\n", f.name.c_str(),
			                  query.c_str(), mysql_stmt_error(stmt.get()));
		}

		b.buffer = f.bind_buffer.data();
		b.buffer_length = static_cast<unsigned long>(f.bind_buffer.size());

		int res_bind = mysql_stmt_bind_result(stmt.get(), binds.data());
		if (res_bind != 0) {
			throw IOException("Failed to re-bind result set for MySQL query \"%s\": %s\n", query.c_str(),
			                  mysql_stmt_error(stmt.get()));
		}
	}
}

DataChunk &MySQLResult::NextChunk() {
	this->data_chunk.Reset();
	this->row_idx = 0;

	idx_t r = 0;
	for (; r < STANDARD_VECTOR_SIZE; r++) {
		if (!FetchNext()) {
			break;
		}

		WriteToChunk(r);
	}

	this->data_chunk.SetCardinality(r);
	return this->data_chunk;
}

bool MySQLResult::FetchNext() {
	for (auto &f : fields) {
#ifdef DEBUG
		std::memset(f.bind_buffer.data(), '\0', f.bind_buffer.size());
#endif
		f.ResetBind();
	}

	int res = mysql_stmt_fetch(stmt.get());

	if (res == 1) {
		throw IOException("Failed to fetch result row for MySQL query \"%s\": %s\n", query.c_str(),
		                  mysql_stmt_error(stmt.get()));
	}

	HandleTruncatedData();

	return res != MYSQL_NO_DATA;
}

bool MySQLResult::Next() {
	if (row_idx < data_chunk.size() - 1) {
		row_idx += 1;
		return true;
	}
	NextChunk();
	row_idx = 0;
	return data_chunk.size() > 0;
}

void MySQLResult::CheckColumnIdx(idx_t col) {
	if (col >= data_chunk.ColumnCount()) {
		throw IOException("Column: %zu out of range of field count: %zu, MySQL query \"%s\"\n", col,
		                  data_chunk.ColumnCount(), query.c_str());
	}
}

void MySQLResult::CheckNotNull(idx_t col) {
	if (IsNull(col)) {
		throw InternalException("Get called for a NULL value, column: %zu, MySQL query \"%s\"\n", col, query.c_str());
	}
}

string MySQLResult::GetString(idx_t col) {
	CheckNotNull(col);
	MySQLField &f = fields[col];
	if (f.duckdb_type.id() == LogicalTypeId::VARCHAR || f.duckdb_type.id() == LogicalTypeId::BLOB) {
		Vector &vec = data_chunk.data[col];
		string_t *data = FlatVector::GetData<string_t>(vec);
		string_t &st = data[row_idx];
		return string(st.GetData(), st.GetSize());
	}
	throw InternalException("Get called for a String type, actual type: \"%s\", column: %zu, MySQL query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int32_t MySQLResult::GetInt32(idx_t col) {
	CheckNotNull(col);
	MySQLField &f = fields[col];
	Vector &vec = data_chunk.data[col];
	if (f.duckdb_type.id() == LogicalTypeId::INTEGER) {
		int32_t *data = FlatVector::GetData<int32_t>(vec);
		return data[row_idx];
	} else if (f.duckdb_type.id() == LogicalTypeId::UINTEGER) {
		uint32_t *data = FlatVector::GetData<uint32_t>(vec);
		return static_cast<int32_t>(data[row_idx]);
	}
	throw InternalException("Get called for an Int32 type, actual type: \"%s\", column: %zu, MySQL query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int64_t MySQLResult::GetInt64(idx_t col) {
	CheckNotNull(col);
	MySQLField &f = fields[col];
	Vector &vec = data_chunk.data[col];
	if (f.duckdb_type.id() == LogicalTypeId::BIGINT) {
		int64_t *data = FlatVector::GetData<int64_t>(vec);
		return data[row_idx];
	} else if (f.duckdb_type.id() == LogicalTypeId::UBIGINT) {
		uint64_t *data = FlatVector::GetData<uint64_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	} else if (f.duckdb_type.id() == LogicalTypeId::DOUBLE) {
		if (vec.GetType().id() == LogicalTypeId::DOUBLE) {
			double *data = FlatVector::GetData<double>(vec);
			return static_cast<uint64_t>(data[row_idx]);
		} else if (vec.GetType().id() == LogicalTypeId::VARCHAR) {
			string_t *data = FlatVector::GetData<string_t>(vec);
			string_t st = data[row_idx];
			return atoll(st.GetData());
		}
	}
	throw InternalException("Get called for an Int64 type, actual type: \"%s\", column: %zu, MySQL query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

bool MySQLResult::IsNull(idx_t col) {
	CheckColumnIdx(col);
	Vector &vec = data_chunk.data[col];
	return FlatVector::IsNull(vec, row_idx);
}

idx_t MySQLResult::AffectedRows() {
	if (affected_rows == idx_t(-1)) {
		throw InternalException("MySQLResult::AffectedRows called for result "
		                        "that didn't affect any rows, query \"%s\"\n",
		                        query.c_str());
	}
	return affected_rows;
}

const vector<MySQLField> &MySQLResult::Fields() {
	return fields;
}

// MySQL TIME values may range from '-838:59:59' to '838:59:59'
// This conversion is slow, 'mysql_time_as_time' flag should be set to avoid it.
static void WriteTimeAsString(MySQLField &f, Vector &vec, idx_t row) {
	MYSQL_TIME *mt = reinterpret_cast<MYSQL_TIME *>(f.bind_buffer.data());
	if (mt->hour == 0 && mt->minute == 0 && mt->second == 0) {
		FlatVector::SetNull(vec, row, true);
		return;
	}
	dtime_t tm = Time::FromTime(0, mt->minute, mt->second, mt->second_part);
	string tail = Time::ToString(tm).substr(2);
	string head = std::to_string(mt->hour);
	while (head.length() < 2) {
		head = "0" + head;
	}
	if (mt->neg) {
		head = "-" + head;
	}
	string str = head + tail;
	string_t st(str.c_str(), str.length());
	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

static void WriteString(MySQLField &f, Vector &vec, idx_t row) {
	if (f.mysql_type == MYSQL_TYPE_TIME) {
		WriteTimeAsString(f, vec, row);
		return;
	}

	D_ASSERT(f.bind_buffer.size() >= f.bind_length);
	string_t st(f.bind_buffer.data(), f.bind_length);
	auto data = FlatVector::GetData<string_t>(vec);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

static void WriteBool(MySQLField &f, Vector &vec, idx_t row) {
	D_ASSERT(f.bind_buffer.size() >= sizeof(int8_t));
	auto data = FlatVector::GetData<bool>(vec);
	if (f.mysql_type == MYSQL_TYPE_TINY) {
		int8_t val = *reinterpret_cast<int8_t *>(f.bind_buffer.data());
		data[row] = val != 0;
		return;
	}
	if (f.bind_length == 0) {
		throw BinderException("Failed to cast MySQL boolean - expected 1 byte "
		                      "element but got element of size %d\n* SET "
		                      "mysql_tinyint1_as_boolean=false to disable "
		                      "loading TINYINT(1) columns as booleans\n* SET "
		                      "mysql_bit1_as_boolean=false to disable loading "
		                      "BIT(1) columns as booleans",
		                      f.bind_length);
	}
	// booleans are EITHER binary "1" or "0" (BIT(1))
	// OR a number
	// in both cases we can figure out what value it is from the first
	// character: \0 -> zero byte, false
	// - -> negative number, false
	// 0 -> zero number, false
	char first = f.bind_buffer[0];
	if (first == '\0' || first == '0' || first == '-') {
		data[row] = false;
	} else {
		data[row] = true;
	}
}

template <typename NUM_TYPE>
static void WriteNumber(MySQLField &f, Vector &vec, idx_t row) {
	D_ASSERT(f.bind_buffer.size() >= sizeof(NUM_TYPE));
	NUM_TYPE num = *reinterpret_cast<NUM_TYPE *>(f.bind_buffer.data());
	auto data = FlatVector::GetData<NUM_TYPE>(vec);
	data[row] = num;
}

static void WriteDateTime(MySQLField &f, Vector &vec, idx_t row) {
	D_ASSERT(f.bind_buffer.size() >= sizeof(MYSQL_TIME));
	MYSQL_TIME *mt = reinterpret_cast<MYSQL_TIME *>(f.bind_buffer.data());
	if ((mt->year == 0 && mt->month == 0 && mt->day == 0 && mt->hour == 0 && mt->minute == 0 && mt->second == 0 &&
	     mt->second_part == 0) ||
	    (f.mysql_type == MYSQL_TYPE_TIME && mt->hour == 0 && mt->minute == 0 && mt->second == 0) ||
	    (f.mysql_type == MYSQL_TYPE_DATE && mt->year == 0 && mt->month == 0 && mt->day == 0)) {
		FlatVector::SetNull(vec, row, true);
		return;
	}

	switch (vec.GetType().id()) {
	case LogicalTypeId::DATE: {
		date_t val = Date::FromDate(mt->year, mt->month, mt->day);
		date_t *data = FlatVector::GetData<date_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIME: {
		if (mt->hour < 0 || mt->hour > 24 ||
		    (mt->hour == 24 && (mt->minute > 0 || mt->second > 0 || mt->second_part > 0))) {
			throw BinderException("time field value out of range, hour value: " + std::to_string(mt->hour));
		}
		dtime_t val = Time::FromTime(mt->hour, mt->minute, mt->second, mt->second_part);
		dtime_t *data = FlatVector::GetData<dtime_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		date_t dt = Date::FromDate(mt->year, mt->month, mt->day);
		dtime_t tm = Time::FromTime(mt->hour, mt->minute, mt->second, mt->second_part);
		timestamp_t val = Timestamp::FromDatetime(dt, tm);
		timestamp_t *data = FlatVector::GetData<timestamp_t>(vec);
		data[row] = val;
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		date_t dt = Date::FromDate(mt->year, mt->month, mt->day);
		dtime_t tm = Time::FromTime(mt->hour, mt->minute, mt->second, mt->second_part);
		timestamp_t ts = Timestamp::FromDatetime(dt, tm);
		int64_t offset_us = static_cast<int64_t>(mt->time_zone_displacement) * 1000000;
		timestamp_tz_t val(ts.value + offset_us);
		timestamp_tz_t *data = FlatVector::GetData<timestamp_tz_t>(vec);
		data[row] = val;
		break;
	}
	default:
		throw InternalException("Unsupported date/time type: " + vec.GetType().ToString());
	}
}

void MySQLResult::WriteToChunk(idx_t row) {
	for (idx_t col = 0; col < data_chunk.ColumnCount(); col++) {
		MySQLField &f = fields[col];
		Vector &vec = data_chunk.data[col];

		if (f.bind_is_null) {
			FlatVector::SetNull(vec, row, true);
			continue;
		}

		switch (vec.GetType().id()) {
		case LogicalTypeId::BOOLEAN: {
			WriteBool(f, vec, row);
			break;
		}
		case LogicalTypeId::TINYINT: {
			WriteNumber<int8_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::UTINYINT: {
			WriteNumber<uint8_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			WriteNumber<int16_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::USMALLINT: {
			WriteNumber<uint16_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::INTEGER: {
			WriteNumber<int32_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::UINTEGER: {
			WriteNumber<uint32_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::BIGINT: {
			WriteNumber<int64_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::UBIGINT: {
			WriteNumber<uint64_t>(f, vec, row);
			break;
		}
		case LogicalTypeId::FLOAT: {
			WriteNumber<float>(f, vec, row);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			if (f.mysql_type == MYSQL_TYPE_DOUBLE) {
				WriteNumber<double>(f, vec, row);
			} else {
				WriteString(f, vec, row);
			}
			break;
		}
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			WriteDateTime(f, vec, row);
			break;
		}
		default: {
			WriteString(f, vec, row);
		}
		}
	}
}

} // namespace duckdb
