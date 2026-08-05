#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "mysql_storage.hpp"
#include "storage/mysql_connection_pool.hpp"
#include "storage/mysql_catalog.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/mysql_transaction_manager.hpp"

namespace duckdb {

static vector<string> ExtractSchemas(Value &value) {
	if (value.IsNull()) {
		throw BinderException("Value for \"SCHEMA\" option must not be null");
	}
	switch (value.type().id()) {
	case LogicalTypeId::VARCHAR: {
		vector<string> res;
		const string &name = StringValue::Get(value);
		if (name.empty()) {
			throw BinderException("Value \"SCHEMA\" option must be not empty");
		}
		res.push_back(name);
		return res;
	}
	case LogicalTypeId::LIST: {
		if (ListType::GetChildType(value.type()).id() != LogicalTypeId::VARCHAR) {
			throw BinderException(
			    "Value for \"SCHEMA\" option must be either \"VARCHAR\" or \"VARCHAR[]\", was: \"%s\"",
			    value.type().ToString());
		}
		vector<string> res;
		for (const Value &en : ListValue::GetChildren(value)) {
			if (en.IsNull()) {
				throw BinderException("Values for \"SCHEMA\" option must not be null");
			}
			const string &name = StringValue::Get(en);
			if (name.empty()) {
				throw BinderException("Values \"SCHEMA\" option must be not empty");
			}
			res.push_back(name);
		}
		return res;
	}
	default:
		throw BinderException("Value for `SCHEMA` option must be either \"VARCHAR\" or \"VARCHAR[]\", was: \"%s\"",
		                      value.type().ToString());
	}
}

static unique_ptr<Catalog> MySQLAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &attach_options) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Attaching MySQL databases is disabled through configuration");
	}
	// check if we have a secret provided
	string secret_name;
	vector<string> schemas_to_load;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else if (lower_name == "schema") {
			schemas_to_load = ExtractSchemas(entry.second);
		} else {
			throw BinderException("Unrecognized option for MySQL attach: %s", entry.first);
		}
	}

	string attach_path = info.path;
	auto connection_string = MySQLCatalog::GetConnectionString(context, attach_path, secret_name);

	auto pool = make_shared_ptr<MySQLConnectionPool>(context, connection_string, attach_path);

	return make_uniq<MySQLCatalog>(db, std::move(connection_string), std::move(attach_path), attach_options.access_mode,
	                               std::move(schemas_to_load), std::move(pool));
}

static unique_ptr<TransactionManager> MySQLCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
	auto &mysql_catalog = catalog.Cast<MySQLCatalog>();
	return make_uniq<MySQLTransactionManager>(db, mysql_catalog);
}

MySQLStorageExtension::MySQLStorageExtension() {
	attach = MySQLAttach;
	create_transaction_manager = MySQLCreateTransactionManager;
}

} // namespace duckdb
