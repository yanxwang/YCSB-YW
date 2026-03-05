#!/bin/bash
# Sweep DEQUEUE_BATCH values and record transaction throughput.
# Usage: sudo ./run_dequeue_batch.sh [extra benchmark args...]
#
# Example:
#   sudo ./run_dequeue_batch.sh --numa 1 --num-clients 8 --num-workers 10 \
#        --clients-start 16 --worker-cpu 3 --local-workerring

set -uo pipefail

BENCHMARK="./build/sharedkv_2rw_benchmark"
WORKLOAD="workloada"
OPS_PER_CLIENT=1250000
BATCH_SIZES=(4 8 16 32)
RESULT_DIR="results/dequeue_batch_sweep"

mkdir -p "$RESULT_DIR"

EXTRA_ARGS=("$@")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="$RESULT_DIR/summary_${TIMESTAMP}.txt"

printf "%-16s %-12s %-14s %-10s\n" "dequeue_batch" "throughput" "duration(s)" "latency_p50(us)" | tee "$SUMMARY"
printf "%-16s %-12s %-14s %-10s\n" "-------------" "----------" "-----------" "---------------" | tee -a "$SUMMARY"

for BATCH in "${BATCH_SIZES[@]}"; do
    OUTFILE="$RESULT_DIR/db${BATCH}_${TIMESTAMP}.txt"

    echo ">>> Running dequeue-batch=$BATCH ..."

    "$BENCHMARK" \
        --workload "$WORKLOAD" \
        --dequeue-batch "$BATCH" \
        --latency \
        --operations-per-client "$OPS_PER_CLIENT" \
        "${EXTRA_ARGS[@]}" \
        2>&1 | tee "$OUTFILE"

    # Parse transaction phase results
    THROUGHPUT=$(grep -A8 "TRANSACTION PHASE Results" "$OUTFILE" | grep "Throughput:" | awk '{print $2}' || echo "N/A")
    DURATION=$(grep -A8 "TRANSACTION PHASE Results" "$OUTFILE" | grep "Duration:" | awk '{print $2}' || echo "N/A")
    P50=$(grep "end-to-end" "$OUTFILE" | awk '{print $3}' | tail -1 || echo "N/A")

    printf "%-16s %-12s %-14s %-10s\n" "$BATCH" "${THROUGHPUT:-N/A}" "${DURATION:-N/A}" "${P50:-N/A}" | tee -a "$SUMMARY"

    echo ">>> Done. Saved to $OUTFILE"
    echo ""

    sleep 2  # cooldown between runs
done

echo ""
echo "=== Summary ==="
cat "$SUMMARY"
echo ""
echo "Full logs in $RESULT_DIR/"
