cmake_minimum_required(VERSION 3.10)
project(SharedKV_Benchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")

add_definitions(-pthread -muintr)
# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)

# Source files
set(BENCHMARK_SOURCES
    ycsb_benchmark.cc
    async_benchmark.cc
    benchmark_main.cc
    globals.cc
)

# Core SharedKV sources (needed for linking)
set(SHAREDKV_SOURCES
    ../src/shared_kv_bucket.cpp
    ../src/kv_request.cpp
    ../src/kv_worker.cpp
    ../src/kv_synchronizer.cpp
    ../src/kv_poller.cpp
    ../src/kv_response_thread.cpp
    ../src/kv_context.cpp
    ../src/uintr_threading.cpp
)

# Create benchmark executable
add_executable(sharedkv_benchmark
    ${BENCHMARK_SOURCES}
    ${SHAREDKV_SOURCES}
)

# Link libraries
target_link_libraries(sharedkv_benchmark pthread numa rt)

# Install target
install(TARGETS sharedkv_benchmark RUNTIME DESTINATION bin)

message(STATUS "------------------------------------")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "------------------------------------")