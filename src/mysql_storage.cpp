#include "duckdb.hpp"

#include "mysql_storage.hpp"
#include "mysql_connection_pool.hpp"
#include "storage/mysql_catalog.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/mysql_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<Catalog> MySQLAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &attach_options) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Attaching MySQL databases is disabled through configuration");
	}
	// check if we have a secret provided
	string secret_name;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for MySQL attach: %s", entry.first);
		}
	}

	string attach_path = info.path;
	auto connection_string = MySQLCatalog::GetConnectionString(context, attach_path, secret_name);

	Value pool_size_value;
	idx_t pool_size = MySQLConnectionPool::DEFAULT_POOL_SIZE;
	if (context.TryGetCurrentSetting("mysql_pool_size", pool_size_value)) {
		pool_size = UBigIntValue::Get(pool_size_value);
	}

	Value pool_timeout_value;
	idx_t pool_timeout_ms = MySQLConnectionPool::DEFAULT_POOL_TIMEOUT_MS;
	if (context.TryGetCurrentSetting("mysql_pool_timeout_ms", pool_timeout_value)) {
		pool_timeout_ms = UBigIntValue::Get(pool_timeout_value);
	}

	Value thread_local_cache_value;
	bool thread_local_cache_enabled = true;
	if (context.TryGetCurrentSetting("mysql_thread_local_cache", thread_local_cache_value)) {
		thread_local_cache_enabled = BooleanValue::Get(thread_local_cache_value);
	}

	return make_uniq<MySQLCatalog>(db, std::move(connection_string), std::move(attach_path), attach_options.access_mode,
	                               pool_size, pool_timeout_ms, thread_local_cache_enabled);
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
