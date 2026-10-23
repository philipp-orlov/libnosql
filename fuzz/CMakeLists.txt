add_executable(fuzz_pages fuzz_pages.cpp)
target_link_libraries(fuzz_pages PRIVATE nosql)
target_link_options(fuzz_pages PRIVATE -fsanitize=fuzzer)
