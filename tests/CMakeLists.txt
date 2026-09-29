add_library(nosql_testmain STATIC test_main.cpp)
target_link_libraries(nosql_testmain PUBLIC nosql)

# name:file pairs; the ctest name stays a single lowercased word (e.g. "mqsteady")
# even where the snake_case source file needs an underscore ("test_mq_steady.cpp").
set(NOSQL_TEST_LIST Basic:basic Env:env Cursor:cursor Named:named Nested:nested
    Overflow:overflow Reclaim:reclaim Steady:steady Memory:memory
    Mvcc:mvcc Torture:torture Format:format Checksum:checksum Hardening:hardening)
if(NOSQL_WITH_MQ)
    list(APPEND NOSQL_TEST_LIST Mq:mq MqSteady:mq_steady)
endif()
if(NOSQL_WITH_REPLICATION)
    list(APPEND NOSQL_TEST_LIST Replication:replication ReplicationTorture:replication_torture
         Fault:fault)
endif()
foreach(entry ${NOSQL_TEST_LIST})
    string(REPLACE ":" ";" pair ${entry})
    list(GET pair 0 t)
    list(GET pair 1 stem)
    string(TOLOWER ${t} name)
    add_executable(test_${name} test_${stem}.cpp)
    target_link_libraries(test_${name} PRIVATE nosql_testmain)
    add_test(NAME ${name} COMMAND test_${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 900)
endforeach()
target_sources(test_checksum PRIVATE checksum_reference.cpp)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT BUILD_SHARED_LIBS)
    target_compile_definitions(test_memory PRIVATE NOSQL_TEST_WRAP_ALLOCATIONS)
    target_link_options(test_memory PRIVATE
        "LINKER:--wrap=aligned_alloc" "LINKER:--wrap=free"
        "LINKER:--wrap=mmap" "LINKER:--wrap=munmap")
endif()

if(NOSQL_WITH_BLOB)
    add_executable(test_tar test_blob_storage.cpp)
    target_link_libraries(test_tar PRIVATE nosql_testmain)
    add_test(NAME tar COMMAND test_tar)
    set_tests_properties(tar PROPERTIES TIMEOUT 900)
else()
    target_compile_definitions(test_hardening PRIVATE NOSQL_TEST_NO_BLOB=1)
endif()
