add_library(mysql_ext_federation OBJECT cost_model.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mysql_ext_federation>
    PARENT_SCOPE)
