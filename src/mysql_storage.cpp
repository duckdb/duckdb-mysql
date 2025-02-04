#include "duckdb.hpp"

#include "mysql_storage.hpp"
#include "storage/mysql_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/mysql_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<Catalog> MySQLAttach(StorageExtensionInfo *storage_info, ClientContext &context, AttachedDatabase &db,
                                       const string &name, AttachInfo &info, AccessMode access_mode) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Attaching MySQL databases is disabled through configuration");
	}
	// check if we have a secret provided
	string secret_name;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for MySQL attach: %s", entry.first);
		}
	}

	string attach_path = info.path;
	auto connection_string = MySQLCatalog::GetConnectionString(context, attach_path, secret_name);
	return make_uniq<MySQLCatalog>(db, std::move(connection_string), std::move(attach_path), access_mode);
}

static unique_ptr<TransactionManager> MySQLCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
	auto &mysql_catalog = catalog.Cast<MySQLCatalog>();
	return make_uniq<MySQLTransactionManager>(db, mysql_catalog);
}

MySQLStorageExtension::MySQLStorageExtension() {
	attach = MySQLAttach;
	create_transaction_manager = MySQLCreateTransactionManager;
}

} // namespace duckdb
