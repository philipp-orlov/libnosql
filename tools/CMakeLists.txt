add_executable(nosql_cli nosql_tool.cpp)
target_link_libraries(nosql_cli PRIVATE nosql)
set_target_properties(nosql_cli PROPERTIES OUTPUT_NAME nosql)

install(TARGETS nosql_cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
