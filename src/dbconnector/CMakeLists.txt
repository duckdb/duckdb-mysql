set(MYSQL_DBCONNECTOR_PATH "${CMAKE_CURRENT_LIST_DIR}/../../database-connector")

add_library(
  mysql_ext_dbconnector OBJECT
  ${MYSQL_DBCONNECTOR_PATH}/src/query/query_writer.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/table_scan/filter_pushdown.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/table_scan/filter_util.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/optimizer/aggregate_optimizer.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/optimizer/optimizer_util.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/optimizer/order_by_and_limit_optimizer.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_ext_dbconnector>
    PARENT_SCOPE)
