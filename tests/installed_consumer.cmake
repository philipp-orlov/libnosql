# Installs the build tree to a scratch prefix, then configures, builds and runs tests/InstalledConsumer
# against it. Run by CTest as: cmake -DBUILD_DIR=... -DSOURCE_DIR=... -DWORK_DIR=... -P installed_consumer.cmake
file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}")
function(run)
    execute_process(COMMAND ${ARGN} RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "failed: ${ARGN}\n${out}\n${err}")
    endif()
endfunction()
run(${CMAKE_COMMAND} --install "${BUILD_DIR}" --prefix "${WORK_DIR}/prefix")
run(${CMAKE_COMMAND} -S "${SOURCE_DIR}/tests/InstalledConsumer" -B "${WORK_DIR}/consumer"
    "-DCMAKE_PREFIX_PATH=${WORK_DIR}/prefix" "-DCMAKE_BUILD_TYPE=Release")
run(${CMAKE_COMMAND} --build "${WORK_DIR}/consumer")
run("${WORK_DIR}/consumer/consumer")
