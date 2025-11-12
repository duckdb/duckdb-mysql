cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME mysql_scanner)
project(${TARGET_NAME})

set(libmariadb_DIR ${CMAKE_CURRENT_LIST_DIR}/vcpkg_ports/libmariadb)
find_package(libmariadb REQUIRED)
set(EXTENSION_NAME ${TARGET_NAME}_extension)

set(MYSQL_INCLUDE_DIR
    ${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include/mysql)
include_directories(${MYSQL_INCLUDE_DIR})

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

# Loadable binary
target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE include ${MYSQL_INCLUDE_DIR})
target_link_libraries(${TARGET_NAME}_loadable_extension ${MYSQL_LIBRARIES})
