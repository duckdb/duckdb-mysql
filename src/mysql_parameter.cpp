#include "mysql_parameter.hpp"

#include "dbconnector/defer.hpp"

#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

#include "mysql_scanner.hpp"

namespace duckdb {

template <typename NUM_TYPE>
static void FillNumberBuffer(Value &value, vector<char> &bind_buffer) {
	bind_buffer.resize(sizeof(NUM_TYPE));
	NUM_TYPE val = value.GetValueUnsafe<NUM_TYPE>();
	std::memcpy(bind_buffer.data(), &val, sizeof(NUM_TYPE));
}

static void FillDateBuffer(Value &value, vector<char> &bind_buffer) {
	MYSQL_TIME mt;
	std::memset(&mt, '\0', sizeof(MYSQL_TIME));
	date_t dd = DateValue::Get(value);
	int32_t year, month, day;
	Date::Convert(dd, year, month, day);

	mt.year = static_cast<unsigned int>(std::abs(year));
	mt.month = static_cast<unsigned int>(std::abs(month));
	mt.day = static_cast<unsigned int>(std::abs(day));

	bind_buffer.resize(sizeof(MYSQL_TIME));
	std::memcpy(bind_buffer.data(), &mt, sizeof(MYSQL_TIME));
}

static void FillTimeBuffer(Value &value, vector<char> &bind_buffer) {
	MYSQL_TIME mt;
	std::memset(&mt, '\0', sizeof(MYSQL_TIME));
	dtime_t dt = TimeValue::Get(value);
	int32_t hour, minute, second, micros;
	Time::Convert(dt, hour, minute, second, micros);

	mt.hour = static_cast<unsigned int>(std::abs(hour));
	mt.minute = static_cast<unsigned int>(std::abs(minute));
	mt.second = static_cast<unsigned int>(std::abs(second));
	mt.second_part = static_cast<unsigned long>(std::abs(micros));

	bind_buffer.resize(sizeof(MYSQL_TIME));
	std::memcpy(bind_buffer.data(), &mt, sizeof(MYSQL_TIME));
}

static void FillTimestampBuffer(Value &value, vector<char> &bind_buffer) {
	MYSQL_TIME mt;
	std::memset(&mt, '\0', sizeof(MYSQL_TIME));
	timestamp_t ts = TimestampValue::Get(value);
	date_t dd;
	dtime_t dt;
	Timestamp::Convert(ts, dd, dt);
	int32_t year, month, day;
	Date::Convert(dd, year, month, day);
	int32_t hour, minute, second, micros;
	Time::Convert(dt, hour, minute, second, micros);

	mt.year = static_cast<unsigned int>(std::abs(year));
	mt.month = static_cast<unsigned int>(std::abs(month));
	mt.day = static_cast<unsigned int>(std::abs(day));
	mt.hour = static_cast<unsigned int>(std::abs(hour));
	mt.minute = static_cast<unsigned int>(std::abs(minute));
	mt.second = static_cast<unsigned int>(std::abs(second));
	mt.second_part = static_cast<unsigned long>(std::abs(micros));

	bind_buffer.resize(sizeof(MYSQL_TIME));
	std::memcpy(bind_buffer.data(), &mt, sizeof(MYSQL_TIME));
}

MySQLParameter::MySQLParameter(const string &query, Value value_p) : value(std::move(value_p)) {
	if (value.IsNull()) {
		return;
	}

	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		this->buffer_type = MYSQL_TYPE_TINY;
		FillNumberBuffer<bool>(value, bind_buffer);
		break;
	case LogicalTypeId::TINYINT:
		this->buffer_type = MYSQL_TYPE_TINY;
		FillNumberBuffer<int8_t>(value, bind_buffer);
		break;
	case LogicalTypeId::UTINYINT:
		this->buffer_type = MYSQL_TYPE_TINY;
		this->is_unsigned = true;
		FillNumberBuffer<uint8_t>(value, bind_buffer);
		break;
	case LogicalTypeId::SMALLINT:
		this->buffer_type = MYSQL_TYPE_SHORT;
		FillNumberBuffer<int16_t>(value, bind_buffer);
		break;
	case LogicalTypeId::USMALLINT:
		this->buffer_type = MYSQL_TYPE_SHORT;
		this->is_unsigned = true;
		FillNumberBuffer<uint16_t>(value, bind_buffer);
		break;
	case LogicalTypeId::INTEGER:
		this->buffer_type = MYSQL_TYPE_LONG;
		FillNumberBuffer<int32_t>(value, bind_buffer);
		break;
	case LogicalTypeId::UINTEGER:
		this->buffer_type = MYSQL_TYPE_LONG;
		this->is_unsigned = true;
		FillNumberBuffer<uint32_t>(value, bind_buffer);
		break;
	case LogicalTypeId::BIGINT:
		this->buffer_type = MYSQL_TYPE_LONGLONG;
		FillNumberBuffer<int64_t>(value, bind_buffer);
		break;
	case LogicalTypeId::UBIGINT:
		this->buffer_type = MYSQL_TYPE_LONGLONG;
		this->is_unsigned = true;
		FillNumberBuffer<uint64_t>(value, bind_buffer);
		break;
	case LogicalTypeId::FLOAT:
		this->buffer_type = MYSQL_TYPE_FLOAT;
		FillNumberBuffer<float>(value, bind_buffer);
		break;
	case LogicalTypeId::DOUBLE:
		this->buffer_type = MYSQL_TYPE_DOUBLE;
		FillNumberBuffer<double>(value, bind_buffer);
		break;
	case LogicalTypeId::DATE:
		this->buffer_type = MYSQL_TYPE_DATE;
		FillDateBuffer(value, bind_buffer);
		break;
	case LogicalTypeId::TIME:
		this->buffer_type = MYSQL_TYPE_TIME;
		FillTimeBuffer(value, bind_buffer);
		break;
	case LogicalTypeId::TIMESTAMP:
		this->buffer_type = MYSQL_TYPE_DATETIME;
		FillTimestampBuffer(value, bind_buffer);
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		this->buffer_type = MYSQL_TYPE_TIMESTAMP;
		FillTimestampBuffer(value, bind_buffer);
		break;
	case LogicalTypeId::VARCHAR:
		// use string ref from the value
		break;
	default:
		throw IOException("Unsupported parameters type: \"%s\", MySQL query \"%s\"", value.type(), query.c_str());
	}
}

MYSQL_BIND MySQLParameter::CreateBind() {
	MYSQL_BIND bind;
	std::memset(&bind, '\0', sizeof(MYSQL_BIND));

	if (value.IsNull()) {
		bind.buffer_type = MYSQL_TYPE_NULL;
		bind.length = &bind_length;
	} else if (value.type().id() == LogicalTypeId::VARCHAR) {
		const string &str = StringValue::Get(value);
		bind.buffer_type = MYSQL_TYPE_VAR_STRING;
		bind.buffer = const_cast<char *>(str.c_str());
		bind.buffer_length = str.length();
		bind_length = str.length();
		bind.length = &bind_length;
	} else {
		bind.buffer_type = buffer_type;
		bind.is_unsigned = is_unsigned;
		bind.buffer = bind_buffer.data();
		bind.buffer_length = bind_buffer.size();
		bind_length = bind_buffer.size();
		bind.length = &bind_length;
	}

	return bind;
}

int64_t MySQLParameterHandles::Add(unique_ptr<vector<Value>> params) {
	if (params.get() == nullptr) {
		throw InvalidInputException("Cannot register invalid empty params");
	}
	lock_guard<mutex> guard(lock);
	int64_t params_id = reinterpret_cast<int64_t>(params.get());
	auto res = registry.insert(params_id);
	bool inserted = res.second;
	if (!inserted) {
		throw InvalidInputException("Parameters are already registered, ID: %lld" + std::to_string(params_id));
	}
	params.release();
	return params_id;
}

unique_ptr<vector<Value>> MySQLParameterHandles::Remove(int64_t params_id) {
	lock_guard<mutex> guard(lock);
	auto removed_count = registry.erase(params_id);
	if (removed_count == 0) {
		return unique_ptr<vector<Value>>(nullptr);
	}
	vector<Value> *params_ptr = reinterpret_cast<vector<Value> *>(params_id);
	return unique_ptr<vector<Value>>(params_ptr);
}

static void MySQLCreateParams(DataChunk &args, ExpressionState &state, Vector &result) {
	auto params_ptr = make_uniq<vector<Value>>();
	auto result_data = FlatVector::GetDataMutable<int64_t>(result);
	result_data[0] = MySQLParameterHandles::Add(std::move(params_ptr));
}

MySQLCreateParamsFunction::MySQLCreateParamsFunction()
    : ScalarFunction("mysql_create_params", vector<LogicalType>(), LogicalType::BIGINT, MySQLCreateParams) {
	SetStability(FunctionStability::VOLATILE);
}

static void MySQLBindParams(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat params_id_data;
	args.data[0].ToUnifiedFormat(params_id_data);
	if (!params_id_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified query parameters ID must be not null");
	}
	auto params_id = UnifiedVectorFormat::GetData<int64_t>(params_id_data)[0];

	Vector &params_vec = args.data[1];
	LogicalTypeId params_vec_id = params_vec.GetType().id();
	if (params_vec_id != LogicalTypeId::STRUCT && params_vec_id != LogicalTypeId::TUPLE) {
		throw InvalidInputException("Specified query parameters must be a STRUCT");
	}
	UnifiedVectorFormat params_data;
	params_vec.ToUnifiedFormat(params_data);
	if (!params_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified query parameters STRUCT must be not null");
	}
	vector<Vector> &field_vectors = StructVector::GetEntries(params_vec);
	vector<Value> params;
	for (Vector &field_vec : field_vectors) {
		Value par = field_vec.GetValue(0);
		params.emplace_back(std::move(par));
	}

	auto params_ptr = MySQLParameterHandles::Remove(params_id);
	if (params_ptr.get() == nullptr) {
		throw InvalidInputException("Specified parameters handle not found, ID: %lld", params_id);
	}
	auto deferred_params = dbconnector::Defer([&params_ptr] { MySQLParameterHandles::Add(std::move(params_ptr)); });
	params_ptr->clear();

	for (Value &par : params) {
		params_ptr->emplace_back(std::move(par));
	}

	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.SetInvalid(0);
}

MySQLBindParamsFunction::MySQLBindParamsFunction()
    : ScalarFunction("mysql_bind_params", {LogicalType::BIGINT, LogicalType::ANY}, LogicalType::BOOLEAN,
                     MySQLBindParams) {
	SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	SetFallible();
	SetVolatile();
}

} // namespace duckdb
