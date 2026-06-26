#include "storage/mysql_catalog.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/storage/database_size.hpp"

#include "mysql_connection.hpp"
#include "mysql_scanner.hpp"
#include "mysql_sql_writer.hpp"
#include "mysql_types.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "storage/mysql_schema_entry.hpp"
#include "storage/mysql_transaction.hpp"

namespace duckdb {

MySQLCatalog::MySQLCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p,
                           AccessMode access_mode, shared_ptr<MySQLConnectionPool> pool_p)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      access_mode(access_mode), schemas(*this), connection_pool(std::move(pool_p)) {
	MySQLConnectionParameters connection_params;
	unordered_set<string> unused;
	std::tie(connection_params, unused) = MySQLUtils::ParseConnectionParameters(connection_string);
	default_schema = connection_params.db;

	auto pooled = connection_pool->ForceAcquire();
	auto server_info = mysql_get_server_info(pooled->GetConn());
	version = MySQLVersion::Parse(server_info ? server_info : "");
}

MySQLCatalog::~MySQLCatalog() = default;

string EscapeConnectionString(const string &input) {
	string result = "\"";
	for (auto c : input) {
		if (c == '\\') {
			result += "\\\\";
		} else if (c == '"') {
			result += "\\\"";
		} else {
			result += c;
		}
	}
	result += "\"";
	return result;
}

string AddConnectionOption(const KeyValueSecret &kv_secret, const string &name,
                           const unordered_set<string> &existing_params) {
	if (existing_params.find(name) != existing_params.end()) {
		// option already provided in connection string
		return string();
	}
	Value input_val = kv_secret.TryGetValue(Identifier(name));
	if (input_val.IsNull()) {
		// not provided
		return string();
	}
	string result;
	result += name;
	result += "=";
	result += EscapeConnectionString(input_val.ToString());
	result += " ";
	return result;
}

unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this
	// use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

struct URIToken {
	string value;
	char delimiter;
};

string UnescapePercentage(const string &input, idx_t start, idx_t end) {
	// url escapes encoded as [ESC][RESULT]
	auto url_escapes = "20 3C<3E>23#25%2B+7B{7D}7C|5C\\5E^7E~5B[5D]60`3B;2F/3F?3A:40@3D=26&24$21!2A*27'22\"28(29)2C,";

	string result;
	for (idx_t i = start; i < end; i++) {
		if (i + 2 < end && input[i] == '%') {
			// find the escape code
			char first_char = StringUtil::CharacterToUpper(input[i + 1]);
			char second_char = StringUtil::CharacterToUpper(input[i + 2]);
			char escape_result = '\0';
			for (idx_t esc_pos = 0; url_escapes[esc_pos]; esc_pos += 3) {
				if (first_char == url_escapes[esc_pos] && second_char == url_escapes[esc_pos + 1]) {
					// found the correct escape
					escape_result = url_escapes[esc_pos + 2];
					break;
				}
			}
			if (escape_result != '\0') {
				// found the escape - skip forward
				result += escape_result;
				i += 2;
				continue;
			}
			// escape not found - just put the % in as normal
		}
		result += input[i];
	}
	return result;
}

vector<URIToken> ParseURITokens(const string &dsn, idx_t start) {
	vector<URIToken> result;
	for (idx_t pos = start; pos < dsn.size(); pos++) {
		switch (dsn[pos]) {
		case ':':
		case '@':
		case '/':
		case '?':
		case '=':
		case '&': {
			// found a delimiter
			URIToken token;
			token.value = UnescapePercentage(dsn, start, pos);
			token.delimiter = dsn[pos];
			start = pos + 1;
			result.push_back(std::move(token));
			break;
		}
		default:
			// include in token
			break;
		}
	}
	URIToken token;
	token.value = UnescapePercentage(dsn, start, dsn.size());
	token.delimiter = '\0';
	result.push_back(std::move(token));
	return result;
}

struct URIValue {
	URIValue(string name_p, string value_p) : name(std::move(name_p)), value(std::move(value_p)) {
	}

	string name;
	string value;
};

vector<string> GetAttributeNames(const vector<URIToken> &tokens, idx_t token_count, ErrorData &error) {
	// [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
	vector<string> result;
	if (token_count == 1) {
		// only one token - always the host
		result.emplace_back("host");
		return result;
	}
	idx_t current_pos = 0;
	if (tokens[0].delimiter == '@') {
		// user@...
		result.emplace_back("user");
		result.emplace_back("host");
		current_pos = 1;
	} else if (tokens[1].delimiter == '@') {
		// user:password@
		if (tokens[0].delimiter != ':') {
			error = ParserException("Invalid URI string - expected user:password");
			return result;
		}
		D_ASSERT(token_count > 2);
		result.emplace_back("user");
		result.emplace_back("passwd");
		result.emplace_back("host");
		current_pos = 2;
	} else {
		// neither user nor password - this MUST be the host
		result.emplace_back("host");
		current_pos = 0;
	}
	if (current_pos + 1 == token_count) {
		// we have parsed the entire string (until the attributes)
		return result;
	}
	// we are at host_pos
	if (tokens[current_pos].delimiter == ':') {
		// host:port
		result.emplace_back("port");
		current_pos++;
		if (current_pos + 1 == token_count) {
			return result;
		}
		// we still have a "/schema"
		if (tokens[current_pos].delimiter != '/') {
			error = ParserException("Invalid URI string - expected host:port/schema");
		}
		result.emplace_back("db");
		current_pos++;
	} else if (tokens[current_pos].delimiter == '/') {
		// host/schema
		result.emplace_back("db");
		current_pos++;
	} else {
		error = ParserException("Invalid URI string - expected host:port or host/schema");
	}
	if (current_pos + 1 != token_count) {
		error = ParserException("Invalid URI string - expected ? after "
		                        "[user[:[password]]@]host[:port][/schema]");
	}
	return result;
}

void ParseMainAttributes(const vector<URIToken> &tokens, idx_t token_count, vector<URIValue> &result,
                         ErrorData &error) {
	auto attribute_names = GetAttributeNames(tokens, token_count, error);
	if (error.HasError()) {
		return;
	}
	D_ASSERT(attribute_names.size() == token_count);
	for (idx_t i = 0; i < token_count; i++) {
		result.emplace_back(attribute_names[i], tokens[i].value);
	}
}

void ParseAttributes(const vector<URIToken> &tokens, idx_t attribute_start, vector<URIValue> &result) {
	unordered_map<string, string> uri_attribute_map;
	uri_attribute_map["socket"] = "socket";
	uri_attribute_map["compression"] = "compression";
	uri_attribute_map["ssl-mode"] = "ssl_mode";
	uri_attribute_map["ssl-ca"] = "ssl_ca";
	uri_attribute_map["ssl-capath"] = "ssl_capath";
	uri_attribute_map["ssl-cert"] = "ssl_cert";
	uri_attribute_map["ssl-cipher"] = "ssl_cipher";
	uri_attribute_map["ssl-crl"] = "ssl_crl";
	uri_attribute_map["ssl-crlpath"] = "ssl_crlpath";
	uri_attribute_map["ssl-key"] = "ssl_key";

	// parse key=value attributes
	for (idx_t i = attribute_start; i < tokens.size(); i += 2) {
		// check if the format is correct
		if (i + 1 >= tokens.size() || tokens[i].delimiter != '=') {
			throw ParserException("Invalid URI string - expected attribute=value pairs after ?");
		}
		if (tokens[i + 1].delimiter != '\0' && tokens[i + 1].delimiter != '&') {
			throw ParserException("Invalid URI string - attribute=value pairs must be separated by &");
		}
		auto entry = uri_attribute_map.find(tokens[i].value);
		if (entry == uri_attribute_map.end()) {
			string supported_options;
			for (auto &entry : uri_attribute_map) {
				if (!supported_options.empty()) {
					supported_options += ", ";
				}
				supported_options += entry.first;
			}
			throw ParserException("Invalid URI string - unsupported attribute "
			                      "\"%s\"\nSupported options: %s",
			                      tokens[i].value, supported_options);
		}
		result.emplace_back(entry->second, tokens[i + 1].value);
	}
}

vector<URIValue> ExtractURIValues(const vector<URIToken> &tokens, ErrorData &error) {
	// [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
	vector<URIValue> result;

	if (tokens.empty()) {
		return result;
	}

	// If we only have one empty token with no delimiter, don't treat it as a host
	if (tokens.size() == 1 && tokens[0].value.empty() && tokens[0].delimiter == '\0') {
		return result;
	}

	// figure out how many "non-attribute" tokens we have
	idx_t attribute_start = tokens.size();
	for (idx_t i = 0; i < tokens.size(); i++) {
		if (tokens[i].delimiter == '?') {
			// found a question-mark - this is a token
			attribute_start = i + 1;
			break;
		}
	}

	// parse the main attributes in the string
	ParseMainAttributes(tokens, attribute_start, result, error);
	// parse key-value attributes
	ParseAttributes(tokens, attribute_start, result);

	return result;
}

bool TryConvertURIInternal(const string &dsn, idx_t start_pos, string &connection_string, ErrorData &error) {
	// parse tokens from the string
	auto tokens = ParseURITokens(dsn, start_pos);

	auto values = ExtractURIValues(tokens, error);
	if (error.HasError()) {
		return false;
	}

	unordered_set<string> added_params;

	for (auto &val : values) {
		// Skip duplicate parameters
		if (added_params.find(val.name) != added_params.end()) {
			continue;
		}

		added_params.insert(val.name);

		if (!connection_string.empty()) {
			connection_string += " ";
		}
		connection_string += val.name;
		connection_string += "=";
		connection_string += EscapeConnectionString(val.value);
	}

	return true;
}

void TryConvertURI(string &dsn) {
	// Skip empty strings
	if (dsn.empty()) {
		return;
	}

	// [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
	idx_t start_pos = 0;
	// skip the past the scheme (either mysql:// or mysqlx://)
	if (StringUtil::StartsWith(dsn, "mysql://")) {
		start_pos = 8;
	} else if (StringUtil::StartsWith(dsn, "mysqlx://")) {
		start_pos = 9;
	}

	// try to convert this as a URI
	string connection_string;
	ErrorData error;
	if (TryConvertURIInternal(dsn, start_pos, connection_string, error)) {
		// success! this is a URI
		dsn = std::move(connection_string);
		return;
	}

	// not a URI
	if (start_pos > 0) {
		// but it started with mysql:// or mysqlx:// - throw an error
		error.Throw();
	}
}

string MySQLCatalog::GetConnectionString(ClientContext &context, const string &attach_path, string secret_name) {
	// if no secret is specified we default to the unnamed mysql secret, if it
	// exists
	bool explicit_secret = !secret_name.empty();
	if (!explicit_secret) {
		// look up settings from the default unnamed mysql secret if none is
		// provided
		secret_name = "__default_mysql";
	}

	auto secret_entry = GetSecret(context, secret_name);
	string connection_string = attach_path;
	StringUtil::Trim(connection_string);

	// if the connection string is a URI, try and convert it
	TryConvertURI(connection_string);

	if (secret_entry) {
		// secret found - read data
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

		// Parse the original connection string to find which parameters are already
		// set
		MySQLConnectionParameters unused;
		unordered_set<string> existing_params;
		std::tie(unused, existing_params) = MySQLUtils::ParseConnectionParameters(connection_string);

		// Build a new connection string with parameters from the secret that don't
		// already exist in the original connection string
		string new_connection_info;

		new_connection_info += AddConnectionOption(kv_secret, "user", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "password", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "host", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "port", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "database", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "socket", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_mode", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_ca", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_capath", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_cert", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_cipher", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_crl", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_crlpath", existing_params);
		new_connection_info += AddConnectionOption(kv_secret, "ssl_key", existing_params);

		// Combine the parameters, putting secret parameters first
		if (!new_connection_info.empty()) {
			if (!connection_string.empty()) {
				// Only add a space if both parts are non-empty
				connection_string = new_connection_info + " " + connection_string;
			} else {
				connection_string = new_connection_info;
			}
		}
	} else if (explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	return connection_string;
}

void MySQLCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> MySQLCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropEntry(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void MySQLCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void MySQLCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<MySQLSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> MySQLCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException("Attempting to fetch the default schema - but no database was "
			                            "provided in the connection string");
		}
		schema_name = default_schema;
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool MySQLCatalog::InMemory() {
	return false;
}

string MySQLCatalog::GetDBPath() {
	return attach_path;
}

bool MySQLCatalog::IsMySQLScan(const string &name) {
	return name == "mysql_scan";
}

DatabaseSize MySQLCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	auto &postgres_transaction = MySQLTransaction::Get(context, *this);
	auto query = StringUtil::Replace(R"(
SELECT SUM(data_length + index_length)
FROM information_schema.tables
WHERE table_schema = ${SCHEMA_NAME};
)",
	                                 "${SCHEMA_NAME}", MySQLUtils::WriteLiteral(default_schema));
	auto result = postgres_transaction.Query(query);
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	if (!result->Next()) {
		throw InternalException("MySQLCatalog::GetDatabaseSize - No row returned!?");
	}
	size.bytes = result->IsNull(0) ? 0 : result->GetInt64(0);
	return size;
}

unique_ptr<TableRef> MySQLCatalog::RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) {
	// serialize the query node into MySQL-compatible SQL (identifier quoting, type / function
	// remapping, explicit NULL ordering, ...)
	return RemoteExecute(context, MySQLSQLWriter::MySQLToString(context, version, *node));
}

unique_ptr<TableRef> MySQLCatalog::RemoteExecute(ClientContext &context, const string &sql) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(GetName())));
	args.push_back(make_uniq<ConstantExpression>(Value(sql)));
	auto func_ref = make_uniq<TableFunctionRef>();
	func_ref->function = make_uniq<FunctionExpression>("mysql_query", std::move(args));
	return func_ref;
}

bool MySQLSupportsType(const LogicalType &type, bool for_cast) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: // TRUE / FALSE literals; MySQL has no boolean cast target
	case LogicalTypeId::SQLNULL: // NULL literal
	case LogicalTypeId::BLOB:    // x'..' literals; DuckDB's VARCHAR -> BLOB cast interprets
	                             // backslash escapes, MySQL's CAST AS BINARY does not
		return !for_cast;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT: // CAST(x AS SIGNED)
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:   // CAST(x AS UNSIGNED)
	case LogicalTypeId::VARCHAR:   // CAST(x AS CHAR)
	case LogicalTypeId::TIMESTAMP: // CAST(x AS DATETIME)
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::FLOAT:  // CAST(x AS FLOAT) - MySQL 8.0.17+
	case LogicalTypeId::DOUBLE: // CAST(x AS DOUBLE) - MySQL 8.0.17+
	case LogicalTypeId::DECIMAL:
		return true;
	case LogicalTypeId::UNBOUND: {
		auto try_bind = UnboundType::TryDefaultBind(type);
		if (try_bind.id() == LogicalTypeId::UNBOUND) {
			return false;
		}
		return MySQLSupportsType(try_bind, for_cast);
	}
	default:
		return false;
	}
}

//! Check whether a constant value is representable in MySQL - MySQL has no infinity / NaN
//! doubles and no infinite dates / timestamps
static bool MySQLSupportsValue(const Value &value) {
	if (value.IsNull()) {
		return true;
	}
	switch (value.type().id()) {
	case LogicalTypeId::FLOAT:
		return Value::FloatIsFinite(FloatValue::Get(value));
	case LogicalTypeId::DOUBLE:
		return Value::DoubleIsFinite(DoubleValue::Get(value));
	case LogicalTypeId::DATE:
		return Value::IsFinite(DateValue::Get(value));
	case LogicalTypeId::TIMESTAMP:
		return Value::IsFinite(TimestampValue::Get(value));
	default:
		return true;
	}
}

bool MySQLSupportsFunction(const FunctionExpression &func) {
	auto &function_name = func.FunctionName().GetIdentifierName();

	// Whitelist of functions that can be pushed into MySQL 8.0+ - either because the name and
	// semantics are identical, or because MySQLSQLWriter rewrites them during serialization:
	//   "count_star"  — serialized as count(*)
	//   "concat"      — serialized as concat_ws('', ...) (DuckDB's concat skips NULLs)
	//   "||"          — serialized as concat(...) (both propagate NULLs)
	//   "avg"         — serialized as avg(CAST(x AS DOUBLE)) (MySQL's avg over integers is DECIMAL(x,4))
	//   "log"         — log(x) is serialized as log10(x); the two-argument log(b, x) matches
	//   "length"/"len" — serialized as char_length (MySQL's length counts bytes)
	//   "stddev"/"variance" — serialized as the sample variants stddev_samp/var_samp
	//   "week"        — serialized as week(x, 3) (ISO weeks)
	//   "dayofweek"/"weekday" — serialized as (dayofweek(x) - 1) (DuckDB numbers from Sunday=0)
	//   "isodow"      — serialized as (weekday(x) + 1) (ISO numbering from Monday=1)
	//   "microsecond" — serialized as (microsecond(x) + second(x) * 1000000)
	//   "trunc"       — serialized as truncate(x, 0)
	//   "greatest"/"least" — serialized as a COALESCE rotation (MySQL propagates NULL arguments)
	//   "substring"/"substr" — only pushed with literal positive offsets (see SupportsPushdown)
	//   "lpad"/"rpad" — only pushed with a literal non-negative length (see SupportsPushdown)
	//   "~~"/"!~~"    — [NOT] LIKE, serialized with an empty ESCAPE clause (DuckDB's LIKE has no
	//                   default escape character, MySQL's default escape is backslash)
	// Excluded (verified mismatches that cannot be fixed by rewriting):
	//   "/"           — DuckDB division by zero yields inf, MySQL yields NULL
	//   "sqrt"/"ln"/"log"/"log2"/"log10"/"asin"/"acos" — MySQL returns NULL on domain errors
	//                   (sqrt(-1), ln(0), asin(2), ...) where DuckDB yields nan / -inf
	//   "exp"/"pow"/"power"/"cot" — MySQL raises an out-of-range error (exp(1000), pow(0,-1),
	//                   cot(0)) where DuckDB yields inf
	//   "round"       — MySQL rounds doubles half-to-even (round(2.5e0) = 2), DuckDB rounds
	//                   half away from zero (3); we cannot know the argument type pre-bind
	//   "upper"/"lower" — MySQL only applies simple case mapping (upper('ß') = 'ß'), DuckDB
	//                   applies full Unicode case mapping ('ẞ')
	//   "reverse"     — MySQL reverses code points, splitting combining sequences; DuckDB
	//                   reverses grapheme clusters
	//   "bin"         — MySQL coerces string arguments to numbers (bin('abc') = '0'), DuckDB
	//                   converts the characters
	//   "unhex"       — MySQL returns NULL for invalid input, DuckDB raises an error
	//   "repeat"      — MySQL returns NULL when the result exceeds @@max_allowed_packet
	//   "monthname"/"dayname" — depend on the server's lc_time_names locale setting
	//   "any_value"   — MySQL's ANY_VALUE is not an aggregate (it returns a value per row
	//                   without GROUP BY) and may return NULL for groups with non-NULL values;
	//                   DuckDB returns the first non-NULL value
	//   "random"      — pushing would ignore DuckDB's setseed()
	//   "translate"   — not available in MySQL
	//   "format"      — different semantics (MySQL: number formatting; DuckDB: printf-style)
	//   "make_date"/"make_time"/"make_timestamp" — MySQL's makedate/maketime take different arguments
	//   "left"/"right" — negative offsets behave differently
	//   "instr"       — MySQL's INSTR only matches case-sensitively with BINARY arguments, but those
	//                   make it count byte positions instead of character positions
	//   "ascii"       — MySQL returns the first byte, DuckDB the first code point
	//   "md5"/"sha1"  — removed in MySQL 9.0
	//   "regexp_replace" — different regex dialect (RE2 vs ICU)
	//   "now"         — DuckDB returns the transaction timestamp with time zone; MySQL the statement time
	//   "bit_and"/"bit_or"/"bit_xor" — MySQL operates on (and returns) unsigned 64-bit integers
	//   "corr"/"covar_pop"/"covar_samp" — not available in MySQL
	//   "group_concat" — MySQL silently truncates the result to @@group_concat_max_len (1024 by default)
	//   "if"/"lcase"/"ucase" — not available in DuckDB (pushing would mask a binder error)
	// Double-precision overflow in "+"/"-"/"*"/"degrees" (e.g. 1e308 * 10) raises an out-of-range
	// error in MySQL where DuckDB yields inf - this requires inputs near the limits of the DOUBLE
	// range and remains a documented limitation.
	static const case_insensitive_set_t MYSQL_SCALAR_FUNCTIONS = {
	    // String
	    "concat",
	    "concat_ws",
	    "char_length",
	    "character_length",
	    "length",
	    "len",
	    "bit_length",
	    "trim",
	    "ltrim",
	    "rtrim",
	    "replace",
	    "lpad",
	    "rpad",
	    "hex",
	    "substring",
	    "substr",
	    "~~",
	    "!~~",
	    // Math
	    "abs",
	    "ceil",
	    "ceiling",
	    "floor",
	    "sin",
	    "cos",
	    "tan",
	    "atan",
	    "atan2",
	    "degrees",
	    "radians",
	    "pi",
	    "sign",
	    "mod",
	    "trunc",
	    "+",
	    "-",
	    "*",
	    "%",
	    "||",
	    // Date / time
	    "year",
	    "month",
	    "day",
	    "hour",
	    "minute",
	    "second",
	    "dayofmonth",
	    "dayofweek",
	    "dayofyear",
	    "weekday",
	    "isodow",
	    "week",
	    "weekofyear",
	    "microsecond",
	    "quarter",
	    "last_day",
	    // Conditional / NULL handling
	    "coalesce",
	    "nullif",
	    "ifnull",
	    "greatest",
	    "least",
	    // Aggregate
	    "count",
	    "count_star",
	    "sum",
	    "avg",
	    "min",
	    "max",
	    "stddev",
	    "stddev_pop",
	    "stddev_samp",
	    "variance",
	    "var_pop",
	    "var_samp",
	};
	return MYSQL_SCALAR_FUNCTIONS.count(function_name) > 0;
}

//! Collect the binding names of all tables within a join tree - MySQL requires unique
//! table names/aliases within a join, while DuckDB can disambiguate duplicates
static void MySQLCollectJoinTableNames(const TableRef &ref, case_insensitive_set_t &names, bool &duplicate) {
	switch (ref.type) {
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		MySQLCollectJoinTableNames(*join.left, names, duplicate);
		MySQLCollectJoinTableNames(*join.right, names, duplicate);
		break;
	}
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		const auto &name = base.alias.empty() ? base.table_name : base.alias;
		if (!names.insert(name.GetIdentifierName()).second) {
			duplicate = true;
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		if (!ref.alias.empty() && !names.insert(ref.alias.GetIdentifierName()).second) {
			duplicate = true;
		}
		break;
	}
	default:
		break;
	}
}

static void MySQLCollectColumnRefs(const QueryNode &node, case_insensitive_set_t &names);

//! Collect the names of all unqualified column references in an expression tree, descending
//! into subqueries
static void MySQLCollectColumnRefs(const ParsedExpression &expr, case_insensitive_set_t &names) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			names.insert(colref.GetColumnName().GetIdentifierName());
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr.Cast<SubqueryExpression>();
		if (subquery.Subquery() && subquery.Subquery()->node) {
			MySQLCollectColumnRefs(*subquery.Subquery()->node, names);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { MySQLCollectColumnRefs(child, names); });
}

static void MySQLCollectColumnRefs(const TableRef &ref, case_insensitive_set_t &names) {
	switch (ref.type) {
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		MySQLCollectColumnRefs(*join.left, names);
		MySQLCollectColumnRefs(*join.right, names);
		if (join.condition) {
			MySQLCollectColumnRefs(*join.condition, names);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			MySQLCollectColumnRefs(*subquery.subquery->node, names);
		}
		break;
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &expr_list = ref.Cast<ExpressionListRef>();
		for (auto &row : expr_list.values) {
			for (auto &expr : row) {
				MySQLCollectColumnRefs(*expr, names);
			}
		}
		break;
	}
	default:
		break;
	}
}

static void MySQLCollectColumnRefs(const QueryNode &node, case_insensitive_set_t &names) {
	for (auto &cte_pair : node.cte_map.map) {
		if (cte_pair.second->query_node) {
			MySQLCollectColumnRefs(*cte_pair.second->query_node, names);
		}
	}
	for (auto &modifier : node.modifiers) {
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				MySQLCollectColumnRefs(*order.expression, names);
			}
		}
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		for (auto &expr : select.select_list) {
			MySQLCollectColumnRefs(*expr, names);
		}
		if (select.from_table) {
			MySQLCollectColumnRefs(*select.from_table, names);
		}
		if (select.where_clause) {
			MySQLCollectColumnRefs(*select.where_clause, names);
		}
		for (auto &group : select.groups.group_expressions) {
			MySQLCollectColumnRefs(*group, names);
		}
		if (select.having) {
			MySQLCollectColumnRefs(*select.having, names);
		}
		if (select.qualify) {
			MySQLCollectColumnRefs(*select.qualify, names);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			MySQLCollectColumnRefs(*child, names);
		}
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		MySQLCollectColumnRefs(*cte.left, names);
		MySQLCollectColumnRefs(*cte.right, names);
		break;
	}
	default:
		break;
	}
}

//! MySQL cannot resolve select-list aliases in the WHERE clause or within other select-list
//! expressions (error 1054), while DuckDB can. Both systems prefer a real table column over an
//! alias, so an alias reference is only a problem when no column with that name exists - which
//! cannot be known pre-bind. We approximate: a name is assumed to be a real column if it is also
//! referenced at a position where alias resolution is impossible (in a select-list item at or
//! before the item that defines the alias). Otherwise the query is conservatively not pushed down.
static bool MySQLSupportsSelectAliasUsage(const SelectNode &select) {
	// find the first select-list item that defines each alias
	case_insensitive_map_t<idx_t> alias_definitions;
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		auto &alias = select.select_list[i]->GetAlias().GetIdentifierName();
		if (!alias.empty() && alias_definitions.find(alias) == alias_definitions.end()) {
			alias_definitions[alias] = i;
		}
	}
	if (alias_definitions.empty()) {
		return true;
	}
	// collect the column references per select-list item
	vector<case_insensitive_set_t> item_refs(select.select_list.size());
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		MySQLCollectColumnRefs(*select.select_list[i], item_refs[i]);
	}
	// a reference at or before the defining item cannot be an alias reference - it must be a column
	case_insensitive_set_t column_evidence;
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		for (auto &name : item_refs[i]) {
			auto entry = alias_definitions.find(name);
			if (entry == alias_definitions.end() || entry->second >= i) {
				column_evidence.insert(name);
			}
		}
	}
	// collect the names that DuckDB might resolve as an alias where MySQL cannot:
	// references in the WHERE clause and lateral references in later select-list items
	case_insensitive_set_t suspects;
	if (select.where_clause) {
		MySQLCollectColumnRefs(*select.where_clause, suspects);
	}
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		for (auto &name : item_refs[i]) {
			auto entry = alias_definitions.find(name);
			if (entry != alias_definitions.end() && entry->second < i) {
				suspects.insert(name);
			}
		}
	}
	for (auto &name : suspects) {
		if (alias_definitions.find(name) != alias_definitions.end() && column_evidence.count(name) == 0) {
			return false;
		}
	}
	return true;
}

//! Check whether a join tree contains a NATURAL or USING join - these coalesce the join columns
//! when expanding an unqualified *, and MySQL places the coalesced columns first while DuckDB
//! keeps them in the position of the left table's column order
static bool MySQLJoinTreeHasCoalescingJoin(const TableRef &ref) {
	if (ref.type != TableReferenceType::JOIN) {
		return false;
	}
	auto &join = ref.Cast<JoinRef>();
	if (join.ref_type == JoinRefType::NATURAL || !join.using_columns.empty()) {
		return true;
	}
	return MySQLJoinTreeHasCoalescingJoin(*join.left) || MySQLJoinTreeHasCoalescingJoin(*join.right);
}

//! Check whether a list of ORDER BY entries can be rewritten for MySQL (see MySQLRewriteOrderEntries)
static bool MySQLSupportsOrderEntries(const vector<OrderByNode> &orders,
                                      optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list) {
	for (auto &order : orders) {
		// note: explicit NULLS FIRST / NULLS LAST is supported - the serializer encodes the NULL
		// placement through the injected "expr IS NULL" ordering key
		auto &expr = *order.expression;
		if (expr.GetExpressionClass() == ExpressionClass::STAR) {
			// ORDER BY ALL is DuckDB-specific
			return false;
		}
		if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &val = expr.Cast<ConstantExpression>().GetValue();
			if (!val.type().IsIntegral()) {
				// ORDER BY with a non-integer literal is a binder error in DuckDB (unless
				// order_by_non_integer_literal is set), while MySQL silently ignores it
				return false;
			}
			{
				// positional reference - it must be resolvable against the select list so that
				// the NULL ordering can be made explicit when serializing
				if (!select_list || val.IsNull()) {
					return false;
				}
				Value bigint_value;
				string error;
				if (!val.DefaultTryCastAs(LogicalType::BIGINT, bigint_value, &error) || bigint_value.IsNull()) {
					return false;
				}
				auto index = BigIntValue::Get(bigint_value);
				if (index < 1 || idx_t(index) > select_list->size()) {
					return false;
				}
				auto &target = *(*select_list)[idx_t(index) - 1];
				if (target.GetAlias().empty() && target.GetExpressionClass() != ExpressionClass::COLUMN_REF) {
					// without an alias we would have to copy the expression into the NULL ordering key,
					// which can violate MySQL's ONLY_FULL_GROUP_BY checks for positional GROUP BY
					return false;
				}
			}
		}
	}
	return true;
}

static bool MySQLIsIntegerLiteral(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::CONSTANT) {
		return false;
	}
	auto &val = expr.Cast<ConstantExpression>().GetValue();
	return val.type().IsIntegral() && !val.IsNull();
}

static bool MySQLIsIntegerLiteralAtLeast(const ParsedExpression &expr, int64_t minimum_value) {
	if (!MySQLIsIntegerLiteral(expr)) {
		return false;
	}
	auto &val = expr.Cast<ConstantExpression>().GetValue();
	Value bigint_value;
	string error;
	if (!val.DefaultTryCastAs(LogicalType::BIGINT, bigint_value, &error) || bigint_value.IsNull()) {
		return false;
	}
	return BigIntValue::Get(bigint_value) >= minimum_value;
}

static bool MySQLIsIntegerLiteralAtMost(const ParsedExpression &expr, int64_t maximum_value) {
	if (!MySQLIsIntegerLiteral(expr)) {
		return false;
	}
	auto &val = expr.Cast<ConstantExpression>().GetValue();
	Value bigint_value;
	string error;
	if (!val.DefaultTryCastAs(LogicalType::BIGINT, bigint_value, &error) || bigint_value.IsNull()) {
		return false;
	}
	return BigIntValue::Get(bigint_value) <= maximum_value;
}

static bool MySQLSupportsResultModifiers(const QueryNode &node,
                                         optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list) {
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_mod = modifier->Cast<OrderModifier>();
			if (!MySQLSupportsOrderEntries(order_mod.orders, select_list)) {
				return false;
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			if (limit_mod.limit_type != LimitValueType::ROW_COUNT) {
				// MySQL has no LIMIT n PERCENT
				return false;
			}
			if (limit_mod.offset && !limit_mod.limit) {
				// MySQL cannot parse OFFSET without LIMIT
				return false;
			}
			// MySQL only supports non-negative integer literals in LIMIT / OFFSET
			// (negative values are a binder error in DuckDB, but a syntax error in MySQL -
			// keep them local so the user gets DuckDB's error message)
			if (limit_mod.limit && !MySQLIsIntegerLiteralAtLeast(*limit_mod.limit, 0)) {
				return false;
			}
			if (limit_mod.offset && !MySQLIsIntegerLiteralAtLeast(*limit_mod.offset, 0)) {
				return false;
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			if (!distinct_mod.distinct_on_targets.empty()) {
				// MySQL has no DISTINCT ON
				return false;
			}
			if (!select_list) {
				// DISTINCT is only serialized as part of a SELECT node
				return false;
			}
			break;
		}
		default:
			// unknown result modifier
			return false;
		}
	}
	return true;
}

static bool MySQLSupportsCTEMap(const CommonTableExpressionMap &cte_map) {
	for (auto &cte_pair : cte_map.map) {
		auto &cte_info = *cte_pair.second;
		if (cte_pair.first.size() > 64) {
			// MySQL limits identifiers to 64 characters ("Identifier name is too long")
			return false;
		}
		for (auto &alias : cte_info.aliases) {
			if (alias.size() > 64) {
				// CTE column names are also subject to MySQL's identifier length limit
				return false;
			}
		}
		if (cte_info.materialized != CTEMaterialize::CTE_MATERIALIZE_DEFAULT) {
			// AS [NOT] MATERIALIZED is not supported in MySQL
			return false;
		}
		if (!cte_info.key_targets.empty()) {
			// USING KEY is DuckDB-specific
			return false;
		}
	}
	return true;
}

static bool MySQLSupportsWindow(const WindowExpression &window) {
	if (window.IgnoreNulls()) {
		// MySQL has no IGNORE NULLS
		return false;
	}
	if (window.Filter()) {
		// MySQL has no FILTER on window functions
		return false;
	}
	if (window.Distinct()) {
		// MySQL window functions do not support DISTINCT
		return false;
	}
	if (!window.ArgOrders().empty()) {
		// ORDER BY within the argument list is DuckDB-specific
		return false;
	}
	if (window.WindowExclude() != WindowExcludeMode::NO_OTHER) {
		// MySQL has no EXCLUDE clause
		return false;
	}
	for (auto boundary : {window.WindowStart(), window.WindowEnd()}) {
		switch (boundary) {
		case WindowBoundary::CURRENT_ROW_GROUPS:
		case WindowBoundary::EXPR_PRECEDING_GROUPS:
		case WindowBoundary::EXPR_FOLLOWING_GROUPS:
			// MySQL has no GROUPS frame mode
			return false;
		case WindowBoundary::EXPR_PRECEDING_RANGE:
		case WindowBoundary::EXPR_FOLLOWING_RANGE:
			// RANGE frames with an offset require a single numeric ORDER BY key in MySQL,
			// which conflicts with making the NULL ordering explicit
			return false;
		default:
			break;
		}
	}
	// frame offsets must be integer literals in MySQL
	if (window.StartExpr() && !MySQLIsIntegerLiteral(*window.StartExpr())) {
		return false;
	}
	if (window.EndExpr() && !MySQLIsIntegerLiteral(*window.EndExpr())) {
		return false;
	}
	// the NULL ordering of the window ORDER BY must be made explicit when serializing
	if (!MySQLSupportsOrderEntries(window.OrderBy(), nullptr)) {
		return false;
	}
	auto &children = window.GetArguments();
	for (auto &child : children) {
		if (child.HasName()) {
			// MySQL has no named function arguments
			return false;
		}
	}
	switch (window.GetExpressionType()) {
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		// MySQL requires a literal non-negative offset; DuckDB also accepts negative offsets
		// and arbitrary expressions
		if (children.size() >= 2 && !MySQLIsIntegerLiteralAtLeast(children[1].GetExpression(), 0)) {
			return false;
		}
		break;
	case ExpressionType::WINDOW_NTILE:
		// MySQL requires a literal positive bucket count
		if (children.size() == 1 && !MySQLIsIntegerLiteralAtLeast(children[0].GetExpression(), 1)) {
			return false;
		}
		break;
	case ExpressionType::WINDOW_NTH_VALUE:
		// MySQL requires a literal positive position
		if (children.size() == 2 && !MySQLIsIntegerLiteralAtLeast(children[1].GetExpression(), 1)) {
			return false;
		}
		break;
	default:
		break;
	}
	// whitelist of window functions supported by MySQL 8.0+ with identical semantics
	// (count_star is rewritten to count(*) during serialization)
	static const case_insensitive_set_t MYSQL_WINDOW_FUNCTIONS = {
	    "row_number", "rank",        "dense_rank", "percent_rank", "cume_dist",   "ntile",      "lead",
	    "lag",        "first_value", "last_value", "nth_value",    "count",       "count_star", "sum",
	    "avg",        "min",         "max",        "stddev_pop",   "stddev_samp", "var_pop",    "var_samp",
	};
	return MYSQL_WINDOW_FUNCTIONS.count(window.FunctionName().GetIdentifierName()) > 0;
}

bool MySQLCatalog::SupportsPushdown(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::CAST: {
		auto &cast_expr = expr.Cast<CastExpression>();
		if (cast_expr.IsTryCast()) {
			// MySQL has no TRY_CAST
			return false;
		}
		if (!MySQLSupportsType(cast_expr.TargetType(), true)) {
			return false;
		}
		// MySQL's CAST semantics differ from DuckDB's in several ways: malformed input returns
		// NULL or a partial value instead of raising an error (CAST('abc' AS SIGNED) is 0,
		// CAST('12abc' AS SIGNED) is 12), numeric strings are truncated instead of parsed
		// (CAST('1e2' AS SIGNED) is 1, CAST('5.7' AS SIGNED) is 5) and time zone offsets are
		// converted to the session time zone instead of being ignored. Casts of literals are
		// therefore evaluated locally and serialized as the resulting value (see
		// MySQLSQLWriter::WriteExpression) - verify here that this is possible. Casts of
		// non-literal expressions over malformed data remain a documented limitation.
		if (cast_expr.Child().GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &value = cast_expr.Child().Cast<ConstantExpression>().GetValue();
			auto target_type = cast_expr.TargetType();
			if (target_type.id() == LogicalTypeId::UNBOUND) {
				target_type = UnboundType::TryDefaultBind(target_type);
			}
			Value cast_result;
			string error;
			if (!value.DefaultTryCastAs(target_type, cast_result, &error)) {
				return false;
			}
			// the cast result must also be representable in MySQL
			if (!MySQLSupportsValue(cast_result)) {
				return false;
			}
			if (cast_result.type().id() == LogicalTypeId::VARCHAR && version.GetBinaryCollation().empty()) {
				// the folded result is serialized as a string literal, which requires a binary collation
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::CONSTANT: {
		auto &constant = expr.Cast<ConstantExpression>();
		auto &value = constant.GetValue();
		if (!MySQLSupportsType(value.type(), false)) {
			return false;
		}
		if (value.type().id() == LogicalTypeId::VARCHAR && version.GetBinaryCollation().empty()) {
			// string literals can only be pushed if the server has a NO PAD binary collation
			// to give comparisons byte-wise (DuckDB) semantics
			return false;
		}
		return MySQLSupportsValue(value);
	}
	case ExpressionClass::FUNCTION: {
		auto &func = expr.Cast<FunctionExpression>();
		auto &function_name = func.FunctionName().GetIdentifierName();
		if (func.Filter()) {
			// FILTER (WHERE ...) on aggregates is not supported in MySQL
			return false;
		}
		if (func.OrderBy() && !func.OrderBy()->orders.empty()) {
			// ORDER BY within an aggregate requires function-specific syntax in MySQL
			return false;
		}
		if (func.ExportState()) {
			return false;
		}
		if (func.Distinct()) {
			// DISTINCT within aggregates is only supported for a small set of functions in MySQL
			// (avg is excluded: it is serialized as avg(CAST(x AS DOUBLE)), which would deduplicate
			// the DOUBLE-cast values instead of the original ones)
			static const case_insensitive_set_t MYSQL_DISTINCT_AGGREGATES = {"count", "sum", "min", "max"};
			if (MYSQL_DISTINCT_AGGREGATES.count(function_name) == 0) {
				return false;
			}
		}
		// functions that are rewritten during serialization, with restrictions on their arguments
		auto &children = func.GetArguments();
		for (auto &child : children) {
			if (child.HasName()) {
				// MySQL has no named function arguments
				return false;
			}
		}
		if (children.size() > 1 &&
		    (StringUtil::CIEquals(function_name, "trim") || StringUtil::CIEquals(function_name, "ltrim") ||
		     StringUtil::CIEquals(function_name, "rtrim"))) {
			// the two-argument variants of trim/ltrim/rtrim are not supported in MySQL
			return false;
		}
		static const case_insensitive_set_t MYSQL_SINGLE_ARGUMENT_REWRITES = {"week",   "dayofweek",   "weekday",
		                                                                      "isodow", "microsecond", "trunc"};
		if (MYSQL_SINGLE_ARGUMENT_REWRITES.count(function_name) > 0 && children.size() != 1) {
			return false;
		}
		if (StringUtil::CIEquals(function_name, "greatest") || StringUtil::CIEquals(function_name, "least")) {
			// rewritten into a COALESCE rotation, which grows quadratically - bound the argument count
			if (children.size() < 2 || children.size() > 8) {
				return false;
			}
		}
		if (StringUtil::CIEquals(function_name, "lpad") || StringUtil::CIEquals(function_name, "rpad")) {
			// MySQL returns NULL for negative lengths where DuckDB returns an empty string, and
			// returns NULL when the result exceeds @@max_allowed_packet - only push down calls
			// with a literal length that is non-negative and small enough for any server setting
			if (children.size() != 3 || !MySQLIsIntegerLiteralAtLeast(children[1].GetExpression(), 0) ||
			    !MySQLIsIntegerLiteralAtMost(children[1].GetExpression(), 1000000)) {
				return false;
			}
		}
		if (StringUtil::CIEquals(function_name, "substring") || StringUtil::CIEquals(function_name, "substr")) {
			// MySQL handles zero and negative offsets differently - only push down calls with
			// literal positive offsets (and a literal non-negative length)
			if (children.size() < 2 || children.size() > 3) {
				return false;
			}
			if (!MySQLIsIntegerLiteralAtLeast(children[1].GetExpression(), 1)) {
				return false;
			}
			if (children.size() == 3 && !MySQLIsIntegerLiteralAtLeast(children[2].GetExpression(), 0)) {
				return false;
			}
		}
		return MySQLSupportsFunction(func);
	}
	case ExpressionClass::WINDOW: {
		auto &window = expr.Cast<WindowExpression>();
		if (!version.SupportsWindowFunctions()) {
			// window functions require MySQL 8.0 / MariaDB 10.2
			return false;
		}
		return MySQLSupportsWindow(window);
	}
	case ExpressionClass::COMPARISON: {
		switch (expr.GetExpressionType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_DISTINCT_FROM:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			// IS [NOT] DISTINCT FROM is serialized using MySQL's NULL-safe <=> operator
			break;
		default:
			// other comparison types are not supported in MySQL
			return false;
		}
		// string comparisons are safe to push: the serializer attaches an explicit binary
		// collation to string literals, which makes the comparison byte-wise like DuckDB
		return true;
	}
	case ExpressionClass::BETWEEN:
		return true;
	case ExpressionClass::OPERATOR: {
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_COALESCE:
		case ExpressionType::OPERATOR_NULLIF:
			return true;
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			return true;
		default:
			// TRY(...), array indexing and other operators are DuckDB-specific
			return false;
		}
	}
	case ExpressionClass::SUBQUERY: {
		auto &subquery = expr.Cast<SubqueryExpression>();
		if (subquery.GetSubqueryType() == SubqueryType::ANY) {
			// MySQL does not support LIMIT within IN / ANY / ALL subqueries (error 1235)
			for (auto &modifier : subquery.Subquery()->node->modifiers) {
				if (modifier->type == ResultModifierType::LIMIT_MODIFIER) {
					return false;
				}
			}
		}
		switch (subquery.GetComparisonType()) {
		case ExpressionType::INVALID: // EXISTS / scalar subqueries
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return true;
		default:
			return false;
		}
	}
	case ExpressionClass::STAR: {
		auto &star = expr.Cast<StarExpression>();
		if (!star.ExcludeList().empty() || !star.ReplaceList().empty() || !star.RenameList().empty()) {
			// EXCLUDE / REPLACE / RENAME are DuckDB-specific
			return false;
		}
		if (star.Expression() || star.IsColumns()) {
			// COLUMNS(...) expressions are DuckDB-specific
			return false;
		}
		return true;
	}
	case ExpressionClass::CASE:
	case ExpressionClass::CONJUNCTION:
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::TYPE:
		return true;
	default:
		// parameters, COLLATE, lambdas, positional references, ... - not supported
		return false;
	}
}

bool MySQLCatalog::SupportsPushdown(const QueryNode &node) {
	if (!node.cte_map.map.empty() && !version.SupportsCTEs()) {
		// common table expressions require MySQL 8.0 / MariaDB 10.2
		return false;
	}
	if (!MySQLSupportsCTEMap(node.cte_map)) {
		return false;
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.qualify) {
			// MySQL cannot execute QUALIFY
			return false;
		}
		if (select.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
			// GROUP BY ALL is DuckDB-specific and would be serialized verbatim
			return false;
		}
		if (select.groups.grouping_sets.size() > 1) {
			// ROLLUP / CUBE / GROUPING SETS are serialized as GROUP BY GROUPING SETS,
			// which MySQL only supports with a secondary engine (HeatWave)
			return false;
		}
		if (select.sample) {
			// MySQL has no USING SAMPLE
			return false;
		}
		if (select.having && select.groups.grouping_sets.empty()) {
			// HAVING without GROUP BY: MySQL resolves bare column references against the select
			// list and filters rows, while DuckDB turns the query into an aggregation and raises
			// a binder error for non-aggregated columns
			return false;
		}
		if (!MySQLSupportsSelectAliasUsage(select)) {
			// the query references select-list aliases at positions MySQL cannot resolve
			return false;
		}
		if (select.from_table && MySQLJoinTreeHasCoalescingJoin(*select.from_table)) {
			// NATURAL / USING joins coalesce the join columns when expanding an unqualified *,
			// and MySQL orders the result columns differently than DuckDB
			for (auto &expr : select.select_list) {
				if (expr->GetExpressionClass() == ExpressionClass::STAR &&
				    expr->Cast<StarExpression>().RelationName().empty()) {
					return false;
				}
			}
		}
		return MySQLSupportsResultModifiers(node, &select.select_list);
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		if (setop.setop_type == SetOperationType::UNION_BY_NAME) {
			// UNION BY NAME is DuckDB-specific
			return false;
		}
		if (setop.setop_type == SetOperationType::EXCEPT || setop.setop_type == SetOperationType::INTERSECT) {
			// EXCEPT / INTERSECT require MySQL 8.0.31 / MariaDB 10.3 (ALL variants: MariaDB 10.5)
			if (!version.SupportsExceptIntersect(setop.setop_all)) {
				return false;
			}
		}
		// there is no select list to resolve positional ORDER BY references against
		return MySQLSupportsResultModifiers(node, nullptr);
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		if (!version.SupportsCTEs()) {
			// recursive CTEs require MySQL 8.0 / MariaDB 10.2
			return false;
		}
		if (!cte.key_targets.empty()) {
			// USING KEY is DuckDB-specific
			return false;
		}
		return MySQLSupportsResultModifiers(node, nullptr);
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto &update = node.Cast<UpdateQueryNode>();
		if (!update.returning_list.empty()) {
			return false;
		}
		if (!SupportsPushdown(*update.table)) {
			return false;
		}
		if (update.from_table) {
			if (!SupportsPushdown(*update.from_table)) {
				return false;
			}
			for (auto &modifier : update.modifiers) {
				if (modifier->type == ResultModifierType::ORDER_MODIFIER ||
				    modifier->type == ResultModifierType::LIMIT_MODIFIER) {
					return false;
				}
			}
		}
		if (!update.set_info) {
			return false;
		}
		for (auto &expr : update.set_info->expressions) {
			if (!SupportsPushdown(*expr)) {
				return false;
			}
		}
		if (update.set_info->condition && !SupportsPushdown(*update.set_info->condition)) {
			return false;
		}
		return MySQLSupportsResultModifiers(node, nullptr);
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto &del = node.Cast<DeleteQueryNode>();
		if (!del.returning_list.empty()) {
			return false;
		}
		if (!SupportsPushdown(*del.table)) {
			return false;
		}
		if (!del.using_clauses.empty()) {
			for (auto &clause : del.using_clauses) {
				if (!SupportsPushdown(*clause)) {
					return false;
				}
			}
			for (auto &modifier : del.modifiers) {
				if (modifier->type == ResultModifierType::ORDER_MODIFIER ||
				    modifier->type == ResultModifierType::LIMIT_MODIFIER) {
					return false;
				}
			}
		}
		if (del.condition && !SupportsPushdown(*del.condition)) {
			return false;
		}
		return MySQLSupportsResultModifiers(node, nullptr);
	}
	case QueryNodeType::INSERT_QUERY_NODE:
		return false;
	default:
		// unknown query node type
		return false;
	}
}

bool MySQLCatalog::SupportsPushdown(const TableRef &ref) {
	if (ref.sample) {
		// MySQL has no TABLESAMPLE
		return false;
	}
	if (!ref.column_name_alias.empty()) {
		// MySQL has no column alias lists (e.g. FROM tbl AS t(a, b))
		return false;
	}
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		if (base.at_clause) {
			// AT (...) time travel clauses are DuckDB-specific
			return false;
		}
		return true;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		switch (join.ref_type) {
		case JoinRefType::REGULAR:
		case JoinRefType::NATURAL:
		case JoinRefType::CROSS:
			break;
		default:
			// ASOF / POSITIONAL joins are DuckDB-specific
			return false;
		}
		switch (join.type) {
		case JoinType::INNER:
		case JoinType::LEFT:
		case JoinType::RIGHT:
			break;
		default:
			// SEMI / ANTI / FULL OUTER joins are not supported in MySQL - worse, MySQL parses
			// "x SEMI JOIN y" as "x AS semi JOIN y" and silently computes an inner join instead
			return false;
		}
		// MySQL requires unique table names/aliases within a join (e.g. a self-join without
		// aliases fails with "Not unique table/alias")
		case_insensitive_set_t join_table_names;
		bool duplicate_table_name = false;
		MySQLCollectJoinTableNames(join, join_table_names, duplicate_table_name);
		if (duplicate_table_name) {
			return false;
		}
		return true;
	}
	case TableReferenceType::SUBQUERY:
	case TableReferenceType::EMPTY_FROM:
		return true;
	case TableReferenceType::TABLE_FUNCTION:
		// mysql doesn't support any table functions
		return false;
	case TableReferenceType::EXPRESSION_LIST:
		// VALUES lists are serialized without the ROW(...) keyword MySQL requires in a FROM clause
		return false;
	default:
		// pivot references, column data references, ... - not supported
		return false;
	}
}

void MySQLCatalog::ClearCache() {
	schemas.ClearEntries();
}

MySQLConnectionPool &MySQLCatalog::GetConnectionPool() {
	return *connection_pool;
}

} // namespace duckdb
