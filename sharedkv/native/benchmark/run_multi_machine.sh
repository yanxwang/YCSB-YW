#!/bin/bash
# ============================================================================
# SharedKV 2RW Multi-Machine Benchmark Script
#
# Usage:
#   ./run_multi_machine.sh master    # On Host 0 (initializes CXL memory)
#   ./run_multi_machine.sh slave     # On Host 1 (attaches to existing CXL memory)
#
# The CXL physical base address can be found via:
#   cat /sys/bus/dax/devices/dax0.0/resource
# ============================================================================

set -euo pipefail

ROLE="${1:-}"
if [[ "$ROLE" != "master" && "$ROLE" != "slave" ]]; then
    echo "Usage: $0 {master|slave}"
    echo ""
    echo "  master  - Run on Host 0 (node-id=0, initializes CXL memory)"
    echo "  slave   - Run on Host 1 (node-id=1, attaches to CXL memory)"
    exit 1
fi

# ============================================================================
# Global Configuration (MUST be identical on both machines)
# ============================================================================
WORKLOAD="workloada"
WORKLOAD_DIR="/mnt/ywang/workloads"
MEM_GB=64
NUM_BUCKETS=8388608                # 8M buckets
NUM_CLIENTS=2                      # total across cluster
NUM_WORKERS=10                     # total across cluster
NUM_SYNCHRONIZERS=2                # total across cluster
QUEUE_DEPTH=1024
SLOTS=1024

# CXL memory access method (choose one):
#   Option A: system-ram mode (NUMA node, use /dev/mem)
CXL_PHYS_BASE=0x4080000000        # from: cat /sys/bus/dax/devices/dax0.0/resource
CXL_NUMA=1
#   Option B: devdax mode (uncomment and set CXL_DEVICE, comment out CXL_PHYS_BASE)
# CXL_DEVICE="/dev/dax0.0"

# ============================================================================
# Per-Node Configuration
# ============================================================================
#                           Master (node 0)     Slave (node 1)
#   Synchronizers:          SN0                  SN1
#   Workers:                W0..W4 (5)           W5..W9 (5)
#   Clients:                C0 (1)               C1 (1)

if [[ "$ROLE" == "master" ]]; then
    NODE_ID=0
    GLOBAL_SN_START=0
    GLOBAL_SN_COUNT=1
    GLOBAL_WORKER_START=0
    GLOBAL_WORKER_COUNT=5
    GLOBAL_CLIENT_START=0
    GLOBAL_CLIENT_COUNT=1
else
    NODE_ID=1
    GLOBAL_SN_START=1
    GLOBAL_SN_COUNT=1
    GLOBAL_WORKER_START=5
    GLOBAL_WORKER_COUNT=5
    GLOBAL_CLIENT_START=1
    GLOBAL_CLIENT_COUNT=1
fi

# Local CPU layout (same on both machines, since each runs its own subset)
WORKER_CPU_START=3
CLIENT_CPU_START=14

# ============================================================================
# Benchmark Mode (edit as needed)
# ============================================================================
# Throughput mode:
MODE_ARGS="--duration 30"
# Latency mode (uncomment to switch):
# MODE_ARGS="--latency --operations-per-client 1000000"

# Optional flags:
EXTRA_ARGS="--local-workerring --verbose --counters"
# Add --stats for chain depth stats:
# EXTRA_ARGS="$EXTRA_ARGS --stats"

# ============================================================================
# Build CXL memory argument
# ============================================================================
CXL_ARG=""
if [[ -n "${CXL_DEVICE:-}" ]]; then
    CXL_ARG="--cxl-device $CXL_DEVICE"
elif [[ -n "${CXL_PHYS_BASE:-}" ]]; then
    CXL_ARG="--cxl-phys-base $CXL_PHYS_BASE"
else
    echo "ERROR: Set either CXL_DEVICE or CXL_PHYS_BASE"
    exit 1
fi

# ============================================================================
# Run
# ============================================================================
BIN="$(dirname "$0")/build/sharedkv_2rw_benchmark"
if [[ ! -x "$BIN" ]]; then
    echo "ERROR: Binary not found: $BIN"
    echo "       Run: mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j\$(nproc)"
    exit 1
fi

echo "=============================================="
echo "  Role:       $ROLE (node $NODE_ID)"
echo "  Workload:   $WORKLOAD"
echo "  CXL:        $CXL_ARG"
echo "  NUMA:       $CXL_NUMA"
echo "  Global:     n=$NUM_CLIENTS  m=$NUM_WORKERS  s=$NUM_SYNCHRONIZERS"
echo "  Local SN:   [$GLOBAL_SN_START .. +$GLOBAL_SN_COUNT)"
echo "  Local W:    [$GLOBAL_WORKER_START .. +$GLOBAL_WORKER_COUNT)"
echo "  Local C:    [$GLOBAL_CLIENT_START .. +$GLOBAL_CLIENT_COUNT)"
echo "=============================================="
echo ""

CMD="$BIN \
  --workload $WORKLOAD \
  --workload-dir $WORKLOAD_DIR \
  --numa $CXL_NUMA \
  --mem-gb $MEM_GB \
  --num-nodes 2 \
  --node-id $NODE_ID \
  $CXL_ARG \
  --num-clients $NUM_CLIENTS \
  --num-workers $NUM_WORKERS \
  --num-synchronizers $NUM_SYNCHRONIZERS \
  --num-buckets $NUM_BUCKETS \
  --queue-depth $QUEUE_DEPTH \
  --slots $SLOTS \
  --global-sn-start $GLOBAL_SN_START \
  --global-sn-count $GLOBAL_SN_COUNT \
  --global-worker-start $GLOBAL_WORKER_START \
  --global-worker-count $GLOBAL_WORKER_COUNT \
  --global-client-start $GLOBAL_CLIENT_START \
  --global-client-count $GLOBAL_CLIENT_COUNT \
  --worker-cpu-start $WORKER_CPU_START \
  --client-cpu-start $CLIENT_CPU_START \
  $MODE_ARGS \
  $EXTRA_ARGS"

echo "Command:"
echo "  $CMD"
echo ""

exec $CMD
