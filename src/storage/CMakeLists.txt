add_library(
  mysql_ext_storage OBJECT
  mysql_catalog.cpp
  mysql_catalog_set.cpp
  mysql_clear_cache.cpp
  mysql_execute_query.cpp
  mysql_index.cpp
  mysql_index_entry.cpp
  mysql_index_set.cpp
  mysql_insert.cpp
  mysql_optimizer.cpp
  mysql_schema_entry.cpp
  mysql_schema_set.cpp
  mysql_table_entry.cpp
  mysql_table_set.cpp
  mysql_transaction.cpp
  mysql_transaction_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_ext_storage>
    PARENT_SCOPE)
