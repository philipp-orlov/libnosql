# Benchmarks are executables, not tests: they print a table and optionally a
# JSON/CSV file, and are meant to be run pinned to a core in Release.
foreach(name kv checksum)
    add_executable(bench_${name} bench_${name}.cpp)
    target_link_libraries(bench_${name} PRIVATE nosql Threads::Threads)
    target_include_directories(bench_${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    if(MSVC)
        target_compile_options(bench_${name} PRIVATE /W4 /permissive- /Zc:__cplusplus)
        target_compile_definitions(bench_${name} PRIVATE _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
        target_link_libraries(bench_${name} PRIVATE psapi)
    else()
        target_compile_options(bench_${name} PRIVATE -Wall -Wextra)
    endif()
endforeach()
