include_directories(include)

add_subdirectory(storage)

add_library(
  mysql_ext_library OBJECT
  mysql_connection.cpp
  mysql_connection_pool.cpp
  mysql_execute.cpp
  mysql_extension.cpp
  mysql_field.cpp
  mysql_filter_pushdown.cpp
  mysql_parameter.cpp
  mysql_result.cpp
  mysql_scanner.cpp
  mysql_storage.cpp
  mysql_types.cpp
  mysql_utils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_ext_library>
    PARENT_SCOPE)
