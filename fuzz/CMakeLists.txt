add_executable(fuzz_pages fuzz_pages.cpp)
target_link_libraries(fuzz_pages PRIVATE nosql)
target_link_options(fuzz_pages PRIVATE -fsanitize=fuzzer)

add_executable(fuzz_ops fuzz_ops.cpp)
target_link_libraries(fuzz_ops PRIVATE nosql)
target_link_options(fuzz_ops PRIVATE -fsanitize=fuzzer)

add_executable(fuzz_power fuzz_power.cpp)
target_link_libraries(fuzz_power PRIVATE nosql)
target_link_options(fuzz_power PRIVATE -fsanitize=fuzzer)

if(NOSQL_WITH_REPLICATION)
    add_executable(fuzz_replication fuzz_replication.cpp)
    target_link_libraries(fuzz_replication PRIVATE nosql)
    target_link_options(fuzz_replication PRIVATE -fsanitize=fuzzer)
endif()
