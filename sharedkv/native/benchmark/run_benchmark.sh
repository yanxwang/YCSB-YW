#!/bin/bash

# Convenient script to run SharedKV native benchmark

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_BIN="$SCRIPT_DIR/build/sharedkv_benchmark"

# Check if benchmark is built
if [ ! -f "$BENCHMARK_BIN" ]; then
    echo "ERROR: Benchmark not built. Run ./build.sh first"
    exit 1
fi

# Default configuration
WORKLOAD="workloadc"
NUMA_NODE=3
CLIENTS=32
WORKERS=32
DURATION=10
CPU_START=64
MODE="throughput"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -w|--workload)
            WORKLOAD="$2"
            shift 2
            ;;
        -n|--numa)
            NUMA_NODE="$2"
            shift 2
            ;;
        -c|--clients)
            CLIENTS="$2"
            shift 2
            ;;
        -W|--workers)
            WORKERS="$2"
            shift 2
            ;;
        -t|--time)
            DURATION="$2"
            shift 2
            ;;
        -s|--cpu-start)
            CPU_START="$2"
            shift 2
            ;;
        -l|--latency)
            MODE="latency"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -w, --workload <name>   Workload name (default: workloadc)"
            echo "  -n, --numa <node>       NUMA node (default: 3)"
            echo "  -c, --clients <num>     Number of clients (default: 32)"
            echo "  -W, --workers <num>     Number of workers (default: 32)"
            echo "  -t, --time <sec>        Duration in seconds (default: 10)"
            echo "  -s, --cpu-start <cpu>   Starting CPU for clients (default: 64)"
            echo "  -l, --latency           Measure latency instead of throughput"
            echo "  -h, --help              Show this help"
            echo ""
            echo "Example:"
            echo "  $0 -w workloadc -c 32 -W 32 -t 30"
            echo "  $0 -w workloadc -c 16 -W 16 --latency"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h for help"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "SharedKV Native Benchmark Runner"
echo "=========================================="
echo "Workload:       $WORKLOAD"
echo "NUMA node:      $NUMA_NODE"
echo "Clients:        $CLIENTS"
echo "Workers:        $WORKERS"
echo "CPU start:      $CPU_START"
if [ "$MODE" = "latency" ]; then
    echo "Mode:           Latency measurement"
else
    echo "Mode:           Throughput measurement"
    echo "Duration:       $DURATION seconds"
fi
echo "=========================================="
echo ""

# Clean shared memory
echo "Cleaning shared memory..."
$SCRIPT_DIR/../scripts/clean_shm.sh

# Build command
CMD="$BENCHMARK_BIN -w $WORKLOAD -n $NUMA_NODE -c $CLIENTS -W $WORKERS -s $CPU_START"

if [ "$MODE" = "latency" ]; then
    CMD="$CMD -l -o 100000"
else
    CMD="$CMD -t $DURATION"
fi

echo "Running: $CMD"
echo ""

# Run benchmark
$CMD
