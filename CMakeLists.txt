cmake_minimum_required(VERSION 3.16)
project(libnosql VERSION 0.1.0 LANGUAGES CXX)
if(NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(FATAL_ERROR "libnosql requires a 64-bit target")
endif()

option(NOSQL_BUILD_TESTS "Build the test suite" ON)
option(NOSQL_BUILD_EXAMPLES "Build the examples" ON)
option(NOSQL_BUILD_TOOLS "Build the nosql command line tool" ON)
option(NOSQL_BUILD_BENCHMARKS "Build the benchmarks (see benchmarks/)" OFF)
option(NOSQL_ENABLE_ASAN "Build with address+undefined sanitizers" OFF)
option(NOSQL_ENABLE_TSAN "Build with the thread sanitizer" OFF)
option(NOSQL_PIPELINE_COMMITS_DEFAULT
       "Make Env::Options::pipelineCommits() default to on (the suite runs this way in CI)" OFF)
option(NOSQL_BUILD_C_API "Build libnosql_c, the C interface (include/nosql/nosql_c.h) the C# binding loads" OFF)
option(NOSQL_BUILD_FUZZERS "Build the libFuzzer targets in fuzz/ (clang only)" OFF)
option(NOSQL_PREFIX_KEYS_DEFAULT
       "Give every new byte-ordered tree, the main one included, DbFlags::PrefixKeys (the suite runs this way in CI)" OFF)
# Subsystems layered on the key-value store. A consumer that uses only the KV
# layer (libsql does) switches off what it does not use: less code to build,
# patch and audit, and no parsers of segment or archive files from disk.
option(NOSQL_WITH_BLOB "Build the tar-backed blob storage" ON)
option(NOSQL_WITH_MQ "Build the message queue (needs NOSQL_WITH_BLOB)" ON)
option(NOSQL_WITH_REPLICATION "Build change shipping and replication (ship log, bundles)" ON)
if(NOSQL_WITH_MQ AND NOT NOSQL_WITH_BLOB)
    message(FATAL_ERROR "NOSQL_WITH_MQ needs NOSQL_WITH_BLOB")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

add_library(nosql
    src/internal/os.cpp
    src/internal/buffer_pool.cpp
    src/internal/checksum.cpp
    src/env.cpp
    src/txn.cpp
    src/b_tree.cpp
    src/api.cpp
    src/optimistic.cpp
    src/verify.cpp
    src/inspect.cpp
)
if(NOSQL_WITH_BLOB)
    target_sources(nosql PRIVATE src/blob_storage.cpp)
endif()
if(NOSQL_WITH_MQ)
    target_sources(nosql PRIVATE src/message_queue.cpp src/mq_writer.cpp)
endif()
if(NOSQL_WITH_REPLICATION)
    target_sources(nosql PRIVATE src/replication.cpp src/ship_log.cpp)
else()
    # Env::Options::shipTo() then fails with ErrorCode::Unsupported.
    target_compile_definitions(nosql PUBLIC NOSQL_NO_REPLICATION=1)
endif()
add_library(nosql::nosql ALIAS nosql)

target_include_directories(nosql
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
)
target_compile_features(nosql PUBLIC cxx_std_17)

if(MSVC)
    target_compile_options(nosql PRIVATE /W4 /permissive- /Zc:__cplusplus)
    target_compile_definitions(nosql PRIVATE _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
else()
    target_compile_options(nosql PRIVATE -Wall -Wextra -Wpedantic -Wshadow)
endif()

find_package(Threads REQUIRED)
target_link_libraries(nosql PUBLIC Threads::Threads)

if(NOSQL_ENABLE_ASAN AND NOT MSVC)
    # Undefined behaviour stops the test like an ASan report would, rather than
    # printing a line a passing run can hide.
    target_compile_options(nosql PUBLIC -fsanitize=address,undefined -fno-sanitize-recover=undefined
                           -fno-omit-frame-pointer -g)
    target_link_options(nosql PUBLIC -fsanitize=address,undefined)
endif()
if(NOSQL_PIPELINE_COMMITS_DEFAULT)
    target_compile_definitions(nosql PUBLIC NOSQL_PIPELINE_COMMITS_DEFAULT=1)
endif()
if(NOSQL_PREFIX_KEYS_DEFAULT)
    target_compile_definitions(nosql PUBLIC NOSQL_PREFIX_KEYS_DEFAULT=1)
endif()
if(NOSQL_ENABLE_TSAN AND NOT MSVC)
    if(NOSQL_ENABLE_ASAN)
        message(FATAL_ERROR "NOSQL_ENABLE_ASAN and NOSQL_ENABLE_TSAN cannot be combined")
    endif()
    target_compile_options(nosql PUBLIC -fsanitize=thread -fno-omit-frame-pointer -g)
    target_link_options(nosql PUBLIC -fsanitize=thread)
endif()

if(NOSQL_BUILD_C_API)
    set_target_properties(nosql PROPERTIES POSITION_INDEPENDENT_CODE ON)
    add_library(nosql_c SHARED src/c_api.cpp)
    target_link_libraries(nosql_c PRIVATE nosql)
    set_target_properties(nosql_c PROPERTIES CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN ON)
    find_program(NOSQL_DOTNET dotnet HINTS $ENV{DOTNET_ROOT} $ENV{HOME}/.dotnet)
    if(NOSQL_DOTNET AND NOSQL_BUILD_TESTS)
        enable_testing()
        add_test(NAME csharp_binding
                 COMMAND ${NOSQL_DOTNET} run --project ${CMAKE_CURRENT_SOURCE_DIR}/bindings/csharp/Nosql.Tests -- ${CMAKE_CURRENT_BINARY_DIR}/csharp_work)
        set_tests_properties(csharp_binding PROPERTIES TIMEOUT 300
            ENVIRONMENT "NOSQL_LIBRARY=$<TARGET_FILE:nosql_c>;DOTNET_CLI_TELEMETRY_OPTOUT=1;DOTNET_NOLOGO=1;DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1")
    endif()
    install(TARGETS nosql_c LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

set_target_properties(nosql PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    OUTPUT_NAME nosql
)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
configure_package_config_file(cmake/nosqlConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/nosqlConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nosql)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/nosqlConfigVersion.cmake
    VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)

# Tests, examples, tools and benchmarks exercise every subsystem.
set(NOSQL_ALL_SUBSYSTEMS OFF)
if(NOSQL_WITH_BLOB AND NOSQL_WITH_MQ AND NOSQL_WITH_REPLICATION)
    set(NOSQL_ALL_SUBSYSTEMS ON)
endif()

if(NOSQL_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
if(NOSQL_ALL_SUBSYSTEMS)
    if(NOSQL_BUILD_EXAMPLES)
        add_subdirectory(examples)
    endif()
    if(NOSQL_BUILD_TOOLS)
        add_subdirectory(tools)
    endif()
elseif(NOSQL_BUILD_EXAMPLES OR NOSQL_BUILD_TOOLS)
    message(STATUS "libnosql: examples and tools need every NOSQL_WITH_* subsystem; skipped")
endif()
if(NOSQL_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
if(NOSQL_BUILD_FUZZERS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "NOSQL_BUILD_FUZZERS needs clang (libFuzzer)")
    endif()
    # The library is instrumented for coverage and checked by both sanitizers,
    # every report fatal, so the fuzzer sees what it reaches and stops on it.
    target_compile_options(nosql PUBLIC -fsanitize=fuzzer-no-link,address,undefined
                           -fno-sanitize-recover=all -g)
    target_link_options(nosql PUBLIC -fsanitize=address,undefined)
    add_subdirectory(fuzz)
endif()

install(TARGETS nosql EXPORT nosql-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY include/nosql DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    PATTERN "internal" EXCLUDE)
install(EXPORT nosql-targets NAMESPACE nosql:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nosql)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/nosqlConfig.cmake
          ${CMAKE_CURRENT_BINARY_DIR}/nosqlConfigVersion.cmake
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nosql)
