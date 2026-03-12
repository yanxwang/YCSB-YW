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
./build.sh
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
NUM_CLIENTS=2                      # total across cluster
NUM_WORKERS=4                      # total across cluster
NUM_SYNCHRONIZERS=2                # total across cluster
DEQUEUE_BATCH=64
READ_ACK_BATCH=64

# CXL DAX device (devdax mode required for multi-machine)
CXL_DEVICE="/dev/dax0.0"
CXL_NUMA=1

# ============================================================================
# Per-Node Configuration
# ============================================================================
#                           Master (node 0)     Slave (node 1)
#   Synchronizers:          SN0                  SN1
#   Workers:                W0..W3 (4)           W4..W7 (4)
#   Clients:                C0 (1)               C1 (1)

if [[ "$ROLE" == "master" ]]; then
    NODE_ID=0
    GLOBAL_SN_START=0
    GLOBAL_SN_COUNT=1
    GLOBAL_WORKER_START=0
    GLOBAL_WORKER_COUNT=2
    GLOBAL_CLIENT_START=0
    GLOBAL_CLIENT_COUNT=1
else
    NODE_ID=1
    GLOBAL_SN_START=1
    GLOBAL_SN_COUNT=1
    GLOBAL_WORKER_START=2
    GLOBAL_WORKER_COUNT=2
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
# MODE_ARGS="--duration 30"
# Latency mode (uncomment to switch):
MODE_ARGS="--latency"
#  --operations-per-client 1000000"

# Optional flags:
EXTRA_ARGS="--local-workerring --verbose --counters"
# Add --stats for chain depth stats:
# EXTRA_ARGS="$EXTRA_ARGS --stats"

# ============================================================================
# Pre-flight check
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
echo "  CXL:        $CXL_DEVICE"
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
  --num-nodes 2 \
  --node-id $NODE_ID \
  --cxl-device $CXL_DEVICE \
  --num-clients $NUM_CLIENTS \
  --num-workers $NUM_WORKERS \
  --num-synchronizers $NUM_SYNCHRONIZERS \
  --dequeue-batch $DEQUEUE_BATCH \
  --read-ack-batch $READ_ACK_BATCH \
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
