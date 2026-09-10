set(MYSQL_DBCONNECTOR_PATH "${CMAKE_CURRENT_LIST_DIR}/../../database-connector")

add_library(
  mysql_ext_dbconnector OBJECT
  ${MYSQL_DBCONNECTOR_PATH}/src/attached/attached_catalog.cpp
  ${MYSQL_DBCONNECTOR_PATH}/src/attached/attached_table.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_ext_dbconnector>
    PARENT_SCOPE)
