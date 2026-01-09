#!/bin/bash

# Quick test script for native benchmark

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Quick Test: Native Benchmark"
echo "=========================================="

# Clean shared memory
echo "Cleaning shared memory..."
../scripts/clean_shm.sh > /dev/null 2>&1

# Generate small workload if not exists
if [ ! -f "workloads/workloadc_load.txt" ]; then
    echo "Generating test workload (10K records)..."
    python3 gen_workload.py -w c -r 10000 -o 50000
fi

echo ""
echo "Running benchmark (4 clients, 4 workers, 5 seconds)..."
echo ""

# Run benchmark from parent directory so paths work correctly
cd "$SCRIPT_DIR/.."

# Run benchmark (filter out debug logs)
benchmark/build/sharedkv_benchmark -w workloadc -n 3 -c 32 -W 32 -s 64 -t 10 2>&1 | \
    grep -v "^\[Latency\]" | \
    grep -v "^\[PIN\]" | \
    grep -v "^CXL:"

echo ""
echo "=========================================="
echo "Quick test complete!"
echo "=========================================="
