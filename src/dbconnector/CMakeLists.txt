set(MYSQL_DBCONNECTOR_PATH "${CMAKE_CURRENT_LIST_DIR}/../../database-connector")

add_library(
  mysql_ext_dbconnector OBJECT
  ${MYSQL_DBCONNECTOR_PATH}/src/functions/configure_pool.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/query/query_writer.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/table_scan/filter_pushdown.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/table_scan/filter_util.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_ext_dbconnector>
    PARENT_SCOPE)
