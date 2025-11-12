include(CMakeFindDependencyMacro)
find_dependency(unofficial-libmariadb CONFIG REQUIRED)
set(libmariadb_FOUND 1)
set(MYSQL_LIBRARIES unofficial::libmariadb)
