#include "storage/mysql_catalog.hpp"
#include "storage/mysql_schema_entry.hpp"
#include "storage/mysql_transaction.hpp"
#include "mysql_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "mysql_scanner.hpp"

namespace duckdb {

MySQLCatalog::MySQLCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p,
                           AccessMode access_mode)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      access_mode(access_mode), schemas(*this) {
	MySQLConnectionParameters connection_params;
	unordered_set<string> unused;
	std::tie(connection_params, unused) = MySQLUtils::ParseConnectionParameters(connection_string);
	default_schema = connection_params.db;
	// try to connect
	auto connection = MySQLConnection::Open(connection_string);
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
	Value input_val = kv_secret.TryGetValue(name);
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
	auto url_escapes = "20 3C<3E>23#25%2B+7B{7D}7C|5C\\5E^7E~5B[5D]60`3B;2F/3F?3A;40@3D=26&24$";

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
		error = ParserException("Invalid URI string - expected ? after [user[:[password]]@]host[:port][/schema]");
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
			throw ParserException("Invalid URI string - unsupported attribute \"%s\"\nSupported options: %s",
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

		// Parse the original connection string to find which parameters are already set
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

bool MySQLCatalog::IsMySQLQuery(const string &name) {
	return name == "mysql_query";
}

void MySQLCatalog::MaterializeMySQLScans(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = op.Cast<PhysicalTableScan>();
		if (MySQLCatalog::IsMySQLScan(table_scan.function.name)) {
			auto &bind_data = table_scan.bind_data->Cast<MySQLBindData>();
			bind_data.streaming = MySQLResultStreaming::FORCE_MATERIALIZATION;
		}
		if (MySQLCatalog::IsMySQLQuery(table_scan.function.name)) {
			auto &bind_data = table_scan.bind_data->Cast<MySQLQueryBindData>();
			bind_data.streaming = MySQLResultStreaming::FORCE_MATERIALIZATION;
		}
	}
	for (auto &child : op.children) {
		MaterializeMySQLScans(child);
	}
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

void MySQLCatalog::ClearCache() {
	schemas.ClearEntries();
}

} // namespace duckdb
