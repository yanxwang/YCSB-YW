#!/bin/bash
# Scaling experiment: vary workload, num_synchronizers, and num_clients
# Total ops target: 10M (ops_per_client = 10000000 / num_clients)

set -euo pipefail

BENCH=./build/sharedkv_2rw_benchmark
OUTDIR=/mnt/ywang/results
TOTAL_OPS=10000000

NUM_WORKERS=12  # divisible by 1, 2, 3
BASE_ARGS="--numa 1 --num-workers ${NUM_WORKERS} \
--latency --local-workerring --stats --verbose --counters"

mkdir -p "$OUTDIR"

WORKLOADS=(workloada workloadc)

# (synchronizers, clients) pairs
EXPERIMENTS=(
    "1 4"
    "1 8"
    "1 16"
    "2 8"
    "2 16"
    "2 32"
    "3 12"
    "3 24"
)

for WL in "${WORKLOADS[@]}"; do
    for exp in "${EXPERIMENTS[@]}"; do
        read -r SN NC <<< "$exp"
        OPC=$((TOTAL_OPS / NC))
        WCPU=$((SN + 1))              # Poller=CPU0, SN=CPU1..SN, Workers start after
        CSTART=$((WCPU + NUM_WORKERS)) # ReqThreads start after last worker
        OUTFILE="${OUTDIR}/${WL}_sn${SN}_c${NC}.txt"

        echo "===== Running: ${WL} SN=${SN} clients=${NC} ops_per_client=${OPC} worker_cpu=${WCPU} clients_start=${CSTART} ====="
        $BENCH $BASE_ARGS \
            --workload "$WL" \
            --worker-cpu "$WCPU" \
            --clients-start "$CSTART" \
            --num-synchronizers "$SN" \
            --num-clients "$NC" \
            --operations-per-client "$OPC" \
            2>&1 | tee "$OUTFILE"
        echo ""
    done
done

echo "All 16 experiments complete. Results in ${OUTDIR}/"
