#include "mysql_result.hpp"

#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "mysql_connection.hpp"

namespace duckdb {

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
                         const string &connection_string_p, unsigned long connection_id_p,
                         MySQLResultStreaming streaming_p, idx_t affected_rows_p, vector<MySQLField> fields_p)
    : query(query_p), stmt(std::move(stmt_p)), type_config(std::move(type_config_p)),
      connection_string(connection_string_p), connection_id(connection_id_p), streaming(streaming_p),
      affected_rows(affected_rows_p), fields(std::move(fields_p)) {
	if (affected_rows != static_cast<idx_t>(-1)) {
		return;
	}
	if (this->fields.empty()) {
		this->fields = MySQLField::ReadFields(query, stmt.get(), type_config);
	}

	// Bind structs are copied onto the statetment, so they can live on stack
	std::vector<MYSQL_BIND> binds;
	for (MySQLField &f : fields) {
		MYSQL_BIND b = f.CreateBindStruct();
		binds.push_back(b);
	}

	int res_bind = mysql_stmt_bind_result(stmt.get(), binds.data());
	if (res_bind != 0) {
		throw IOException("Failed to bind result set for MySQL query \"%s\": %s\n", query.c_str(),
		                  mysql_stmt_error(stmt.get()));
	}

	auto ltypes = CreateChunkTypes(fields, type_config);
	this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);
}

MySQLResult::~MySQLResult() {
	bool stream_active =
	    streaming == MySQLResultStreaming::ALLOW_STREAMING || streaming == MySQLResultStreaming::REQUIRE_STREAMING;
	if (stream_active && !exhausted) {
		TryCancelQuery();
	}
}

void MySQLResult::HandleTruncatedData() {
	for (size_t i = 0; i < fields.size(); i++) {
		MySQLField &f = fields[i];

		if (!f.bind_error || f.bind_buffer.size() >= f.bind_length) {
			continue;
		}

		size_t full_len = f.bind_length;
		f.ResetBind();

		f.varlen_buffer.resize(static_cast<size_t>(full_len));
		memcpy(f.varlen_buffer.data(), f.bind_buffer.data(), f.bind_buffer.size());
		MYSQL_BIND b = f.CreateBindStruct();
		b.buffer = f.varlen_buffer.data() + f.bind_buffer.size();
		b.buffer_length = static_cast<unsigned long>(f.varlen_buffer.size() - f.bind_buffer.size());

		int res_fetch = mysql_stmt_fetch_column(stmt.get(), &b, static_cast<unsigned int>(i),
		                                        static_cast<unsigned long>(f.bind_buffer.size()));
		if (res_fetch != 0) {
			throw IOException("Failed to re-fetch result field \"%s\" for MySQL query \"%s\": %s\n", f.name.c_str(),
			                  query.c_str(), mysql_stmt_error(stmt.get()));
		}
	}
}

DataChunk &MySQLResult::NextChunk() {
	this->data_chunk.Reset();
	this->row_idx = 0;

	idx_t r = 0;
	for (; r < STANDARD_VECTOR_SIZE; r++) {
		if (!FetchNext()) {
			this->exhausted = true;
			break;
		}

		WriteToChunk(r);
	}

	this->data_chunk.SetCardinality(r);
	return this->data_chunk;
}

bool MySQLResult::Exhausted() {
	return this->exhausted;
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

bool MySQLResult::TryCancelQuery() {
	try {
		// open a new connection
		auto con = MySQLConnection::Open(type_config, connection_string, "");

		// execute KILL QUERY [connection_id] to kill the running query
		string kill_query = "KILL QUERY " + to_string(connection_id);
		con.Execute(kill_query);

		return true;
	} catch (...) {
		return false;
	}
}

// MySQL TIME values may range from '-838:59:59' to '838:59:59'
// This conversion is slow, 'mysql_time_as_time' flag should be set to avoid it.
static void WriteTimeAsString(MySQLField &f, Vector &vec, idx_t row) {
	MYSQL_TIME *mt = reinterpret_cast<MYSQL_TIME *>(f.bind_buffer.data());
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

	auto data = FlatVector::GetData<string_t>(vec);

	if (f.varlen_buffer.size() > 0) {
		string_t st(f.varlen_buffer.data(), f.varlen_buffer.size());
		data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
		return;
	}

	D_ASSERT(f.bind_buffer.size() >= f.bind_length);
	string_t st(f.bind_buffer.data(), f.bind_length);
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

static void WriteDateTime(MySQLTypeConfig &type_config, MySQLField &f, Vector &vec, idx_t row) {
	D_ASSERT(f.bind_buffer.size() >= sizeof(MYSQL_TIME));
	MYSQL_TIME *mt = reinterpret_cast<MYSQL_TIME *>(f.bind_buffer.data());

	bool is_zero_datetime = (f.mysql_type == MYSQL_TYPE_DATETIME || f.mysql_type == MYSQL_TYPE_TIMESTAMP) &&
	                        mt->year == 0 && mt->month == 0 && mt->day == 0 && mt->hour == 0 && mt->minute == 0 &&
	                        mt->second == 0 && mt->second_part == 0;

	bool is_zero_date = f.mysql_type == MYSQL_TYPE_DATE && mt->year == 0 && mt->month == 0 && mt->day == 0;

	bool is_incomplete_date = type_config.incomplete_dates_as_nulls && (mt->month == 0 || mt->day == 0);

	if (is_zero_datetime || is_zero_date || is_incomplete_date) {
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
			WriteDateTime(type_config, f, vec, row);
			break;
		}
		default: {
			WriteString(f, vec, row);
		}
		}
	}
}

} // namespace duckdb
