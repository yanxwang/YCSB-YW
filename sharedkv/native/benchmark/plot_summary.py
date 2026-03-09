#!/usr/bin/env python3
"""Plot bar charts from a dequeue_batch sweep summary file.

Usage:
    python3 plot_summary.py results/dequeue_batch_sweep/summary_XXXX.txt [output.png]
"""

import sys
import matplotlib.pyplot as plt
import numpy as np

def parse_summary(path):
    batches, throughputs, durations, p50s = [], [], [], []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("dequeue") or line.startswith("---"):
                continue
            parts = line.split()
            if len(parts) < 4:
                continue
            try:
                b = int(parts[0])
            except ValueError:
                continue
            batches.append(b)
            # throughput might be N/A; estimate from duration if so
            tp = parts[1]
            dur = float(parts[2]) if parts[2] != "N/A" else 0
            p50 = float(parts[3]) if parts[3] != "N/A" else 0
            if tp == "N/A" and dur > 0:
                # 8 clients * 1250000 ops = 10M total
                total_ops = 10_000_000
                tp = total_ops / dur
            else:
                tp = float(tp)
            throughputs.append(tp)
            durations.append(dur)
            p50s.append(p50)
    return batches, throughputs, durations, p50s

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else path.replace(".txt", ".png")

    batches, throughputs, durations, p50s = parse_summary(path)
    x = np.arange(len(batches))
    labels = [str(b) for b in batches]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Throughput bar chart
    bars1 = ax1.bar(x, [t / 1e6 for t in throughputs], color="#4C72B0", width=0.6)
    ax1.set_xlabel("DEQUEUE_BATCH")
    ax1.set_ylabel("Throughput (M ops/s)")
    ax1.set_title("Transaction Throughput")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    for bar, val in zip(bars1, throughputs):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                 f"{val/1e6:.2f}", ha="center", va="bottom", fontsize=10)

    # P50 latency bar chart
    bars2 = ax2.bar(x, p50s, color="#DD8452", width=0.6)
    ax2.set_xlabel("DEQUEUE_BATCH")
    ax2.set_ylabel("P50 Latency (us)")
    ax2.set_title("End-to-End P50 Latency")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.set_yscale("log")
    for bar, val in zip(bars2, p50s):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.1,
                 f"{val:.1f}", ha="center", va="bottom", fontsize=10)

    fig.suptitle(f"DEQUEUE_BATCH Sweep", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Saved to {out}")

if __name__ == "__main__":
    main()
