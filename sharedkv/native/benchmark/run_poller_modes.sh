#!/bin/bash
# ============================================================================
# Benchmark all 4 poller modes and save results for plotting
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK="$SCRIPT_DIR/build/sharedkv_2rw_benchmark"
RESULTS_DIR="$SCRIPT_DIR/results/poller_modes"

# Base benchmark args (edit these as needed)
BASE_ARGS=(
    --workload workloada
    --numa 2
    --num-clients 8
    --num-workers 10
    --clients-start 16
    --worker-cpu 3
    --latency
    --operations-per-client 1250000
    --local-workerring
    --stats
    --verbose
    --counters
)

MODES=(response worker dual none)

mkdir -p "$RESULTS_DIR"

echo "============================================"
echo "  Poller Mode Benchmark Suite"
echo "  Results → $RESULTS_DIR"
echo "============================================"

for mode in "${MODES[@]}"; do
    echo ""
    echo ">>> Running poller_mode=$mode ..."
    outfile="$RESULTS_DIR/poller_mode_${mode}.txt"

    "$BENCHMARK" "${BASE_ARGS[@]}" --poller-mode "$mode" 2>&1 | tee "$outfile"

    echo ">>> Saved → $outfile"
    echo ""
done

echo "============================================"
echo "  All modes complete. Plotting..."
echo "============================================"

python3 "$SCRIPT_DIR/plot_poller_modes.py" "$RESULTS_DIR" "$RESULTS_DIR/plots"

echo "Done."
