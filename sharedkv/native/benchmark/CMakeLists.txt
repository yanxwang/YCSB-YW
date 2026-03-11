cmake_minimum_required(VERSION 3.10)
project(SharedKV_Benchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)

# 2RW Zero-Copy Architecture sources
set(TWO_RW_CORE_SOURCES
    ../src/uintr_threading.cpp
    ../src/kv_request.cpp       # needed by ycsb_benchmark.cc (execute_operation)
    ../src/2rw_context.cpp
    ../src/2rw_synchronizer.cpp
    ../src/2rw_worker.cpp
    ../src/2rw_poller.cpp
)

# 2RW YCSB benchmark executable
add_executable(sharedkv_2rw_benchmark
    2rw_benchmark.cc
    2rw_benchmark_main.cc
    ycsb_benchmark.cc       # workload loader + print_benchmark_stats
    ${TWO_RW_CORE_SOURCES}
)

# target_compile_options(sharedkv_2rw_benchmark PRIVATE -muintr)
target_compile_options(sharedkv_2rw_benchmark PRIVATE -muintr -mclflushopt -mclwb)
target_link_libraries(sharedkv_2rw_benchmark pthread numa rt)

install(TARGETS sharedkv_2rw_benchmark RUNTIME DESTINATION bin)

message(STATUS "------------------------------------")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "------------------------------------")