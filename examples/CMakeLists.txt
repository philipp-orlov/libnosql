# The targets keep the example_ prefix so they cannot collide with those of a
# project that adds this one as a subdirectory; the binaries do not.
foreach(e Quickstart Catalog Bench Mq Replication Baseline)
    string(TOLOWER ${e} name)
    add_executable(example_${name} ${name}.cpp)
    target_link_libraries(example_${name} PRIVATE nosql)
    set_target_properties(example_${name} PROPERTIES OUTPUT_NAME ${name})
endforeach()
