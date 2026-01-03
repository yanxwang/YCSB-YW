cmake_minimum_required(VERSION 3.10)
project(SharedKV VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

# Find Java and JNI
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(CORE_SOURCES
    src/shared_kv_bucket.cpp
)

set(YCSB_SOURCES
    src/SharedKV_YCSB.cpp
)

set(JNI_SOURCES
    src/sharedkv_jni.cpp
)

# Build shared library
add_library(sharedkv_jni SHARED
    ${CORE_SOURCES}
    ${YCSB_SOURCES}
    ${JNI_SOURCES}
)

# Link libraries
target_link_libraries(sharedkv_jni
    PRIVATE
        pthread
        numa
)

# Set output directory
set_target_properties(sharedkv_jni PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Installation rules
install(TARGETS sharedkv_jni
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

# Optional: Enable testing
option(BUILD_TESTS "Build test programs" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Print configuration summary
message(STATUS "")
message(STATUS "SharedKV Configuration Summary:")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  JNI include dirs: ${JNI_INCLUDE_DIRS}")
message(STATUS "  Install prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
