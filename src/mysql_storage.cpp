#include "duckdb.hpp"

#include "mysql_storage.hpp"
#include "mysql_connection_pool.hpp"
#include "storage/mysql_catalog.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/mysql_transaction_manager.hpp"

namespace duckdb {

static idx_t ReadUBigIntOption(ClientContext &ctx, const std::string &name, idx_t default_val) {
	Value val;
	if (ctx.TryGetCurrentSetting(name, val)) {
		return UBigIntValue::Get(val);
	}
	return default_val;
}

static bool ReadBooleanOption(ClientContext &ctx, const std::string &name, bool default_val) {
	Value val;
	if (ctx.TryGetCurrentSetting(name, val)) {
		return BooleanValue::Get(val);
	}
	return default_val;
}

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

	idx_t pool_size = ReadUBigIntOption(context, "mysql_pool_size", MySQLConnectionPool::DefaultPoolSize());
	idx_t pool_timeout_ms =
	    ReadUBigIntOption(context, "mysql_pool_timeout_ms", MySQLConnectionPool::DEFAULT_POOL_TIMEOUT_MS);
	bool thread_local_cache_enabled = ReadBooleanOption(context, "mysql_pool_thread_local_cache", true);
	idx_t pool_connection_max_lifetime_seconds =
	    ReadUBigIntOption(context, "mysql_pool_connection_max_lifetime_seconds", 0);
	idx_t pool_connection_idle_timeout_seconds =
	    ReadUBigIntOption(context, "mysql_pool_connection_idle_timeout_seconds", 0);
	bool pool_enable_reaper_thread = ReadBooleanOption(context, "mysql_pool_enable_reaper_thread", false);

	MySQLTypeConfig type_config;
	auto pool =
	    make_shared_ptr<MySQLConnectionPool>(connection_string, attach_path, type_config, pool_size, pool_timeout_ms);
	pool->SetThreadLocalCacheEnabled(thread_local_cache_enabled);
	pool->SetMaxLifetimeSeconds(pool_connection_max_lifetime_seconds);
	pool->SetIdleTimeoutSeconds(pool_connection_idle_timeout_seconds);
	if (pool_enable_reaper_thread) {
		pool->EnsureReaperRunning();
	}

	return make_uniq<MySQLCatalog>(db, std::move(connection_string), std::move(attach_path), attach_options.access_mode,
	                               std::move(pool));
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
