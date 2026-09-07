add_library(mysql_ext_scan OBJECT mysql_planned_table.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_ext_scan>
    PARENT_SCOPE)
