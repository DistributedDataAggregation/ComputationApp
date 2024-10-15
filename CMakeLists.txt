cmake_minimum_required(VERSION 3.28)
project(test_reading_files C)

set(CMAKE_C_STANDARD 11)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PARQUET REQUIRED parquet-glib)

include_directories(${PARQUET_INCLUDE_DIRS})

add_executable(test_reading_files calculate.c
        threads_functionality/thread_computation/thread_computation.h
        threads_functionality/thread_computation/thread_computation.c
        threads_functionality/thread_argument.h
        file_handling/file_handling.h
        file_handling/file_handling.c
        consts/POSITIONS.h
        threads_functionality/results.h)

set(CSV_FILE ${CMAKE_SOURCE_DIR}/large_test_data.csv)
set(CSV_FILE_SMALL ${CMAKE_SOURCE_DIR}/data_set_small.csv)

# Custom command for copying the large test data CSV file
add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/large_test_data.csv
        COMMAND ${CMAKE_COMMAND} -E copy ${CSV_FILE} ${CMAKE_CURRENT_BINARY_DIR}/large_test_data.csv
        DEPENDS ${CSV_FILE}
)


# Custom command for copying the small data set CSV file
add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/data_set_small.csv
        COMMAND ${CMAKE_COMMAND} -E copy ${CSV_FILE_SMALL} ${CMAKE_CURRENT_BINARY_DIR}/data_set_small.csv
        DEPENDS ${CSV_FILE_SMALL}
)

# Add custom targets to make sure they run before the executable
add_custom_target(copy_csv_files ALL DEPENDS
        ${CMAKE_CURRENT_BINARY_DIR}/large_test_data.csv
        ${CMAKE_CURRENT_BINARY_DIR}/data_set_small.csv
)


target_link_libraries(test_reading_files ${PARQUET_LIBRARIES})
