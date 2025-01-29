#include "storage/mysql_catalog.hpp"
#include "storage/mysql_schema_entry.hpp"
#include "storage/mysql_transaction.hpp"
#include "mysql_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

MySQLCatalog::MySQLCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p, AccessMode access_mode)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)), access_mode(access_mode), schemas(*this) {
	default_schema = MySQLUtils::ParseConnectionParameters(connection_string).db;
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

string AddConnectionOption(const KeyValueSecret &kv_secret, const string &name) {
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
	auto connection_string = attach_path;
	if (secret_entry) {
		// secret found - read data
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		string new_connection_info;

		new_connection_info += AddConnectionOption(kv_secret, "user");
		new_connection_info += AddConnectionOption(kv_secret, "password");
		new_connection_info += AddConnectionOption(kv_secret, "host");
		new_connection_info += AddConnectionOption(kv_secret, "port");
		new_connection_info += AddConnectionOption(kv_secret, "database");
		new_connection_info += AddConnectionOption(kv_secret, "socket");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_mode");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_ca");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_capath");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_cert");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_cipher");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_crl");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_crlpath");
		new_connection_info += AddConnectionOption(kv_secret, "ssl_key");
		connection_string = new_connection_info + connection_string;
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

optional_ptr<SchemaCatalogEntry> MySQLCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                         OnEntryNotFound if_not_found,
                                                         QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException("Attempting to fetch the default schema - but no database was "
			                            "provided in the connection string");
		}
		return GetSchema(transaction, default_schema, if_not_found, error_context);
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
