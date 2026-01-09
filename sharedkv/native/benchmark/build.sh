#!/bin/bash

# Build script for SharedKV native benchmark

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Building SharedKV Native Benchmark"
echo "=========================================="

# Create build directory
mkdir -p build
cd build

# Run CMake
echo "Running CMake..."
cmake ..

# Build
echo "Building..."
make -j$(nproc)

echo ""
echo "=========================================="
echo "Build complete!"
echo "Executable: $(pwd)/sharedkv_benchmark"
echo "=========================================="

# Create workloads directory
mkdir -p ../workloads
echo ""
echo "Workloads directory: $(cd .. && pwd)/workloads"
echo "Use gen_workload.py to generate workload files"
