#!/usr/bin/env python3
"""Plot throughput, latency, and pipeline counters across poller modes.

Usage:
    python3 plot_poller_modes.py [results_dir] [output_dir]

Expects files named poller_mode_{response,worker,dual,none}.txt in results_dir.
Each file is the full stdout+stderr of sharedkv_2rw_benchmark --latency --counters.
"""

import os
import re
import sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

matplotlib.rcParams.update({
    "font.size": 12,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
})

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(_SCRIPT_DIR, "results/poller_modes")
OUTPUT_DIR  = sys.argv[2] if len(sys.argv) > 2 else os.path.join(RESULTS_DIR, "plots")

MODES = ["response", "worker", "dual", "none"]
MODE_LABELS = {
    "response": "Response\nPoller",
    "worker":   "Worker\nPoller",
    "dual":     "Dual\nPoller",
    "none":     "No Poller\n(busy-poll)",
}
COLORS = {
    "response": "#4C72B0",
    "worker":   "#DD8452",
    "dual":     "#55A868",
    "none":     "#C44E52",
}


def parse_result(filepath):
    """Parse throughput, latency, and pipeline counters from a benchmark result file.

    Returns dict with:
        throughput: int (ops/s) — from TRANSACTION phase (second Throughput line)
        latency: dict of stage_name -> {avg, p50, p99, max} in microseconds
        resp_poller: {scan_rounds, uintrs_sent} or None
        worker_poller: {scan_rounds, uintrs_sent} or None
        resp_threads: {total_completed, total_drain_rounds, total_empty_rounds,
                       total_uintr_wakes, total_recycle_waits}
        workers: {total_ops_done, total_empty_polls, total_resp_fullwaits, total_uintr_wakes}
        req_threads: {total_submitted, total_enq_waits}
    """
    result = {
        "throughput": None,
        "latency": {},
        "resp_poller": None,
        "worker_poller": None,
        "resp_threads": {},
        "workers": {},
        "req_threads": {},
    }
    throughputs = []

    with open(filepath, errors="replace") as f:
        lines = f.readlines()

    # Track which section we're in for table parsing
    section = None
    # Whether we've seen TRANSACTION phase header (to distinguish LOAD vs TRANSACTION)
    in_transaction = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Detect TRANSACTION PHASE
        if "TRANSACTION PHASE" in line:
            in_transaction = True

        # Throughput
        m = re.search(r"Throughput:\s+([\d]+)\s+ops/s", line)
        if m:
            throughputs.append(int(m.group(1)))

        # Decomposed latency table rows (only from TRANSACTION phase)
        m = re.match(
            r"\s+(t\d→t\d\s+\S+(?:\s+\S+)?)\s+"
            r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)",
            line,
        )
        if m:
            stage = m.group(1).strip()
            result["latency"][stage] = {
                "avg": float(m.group(2)),
                "p50": float(m.group(3)),
                "p99": float(m.group(4)),
                "max": float(m.group(5)),
            }

        # Section detection
        if "--- Response Threads (dequeue" in line:
            section = "resp_threads"
            continue
        elif "--- Workers (dequeue" in line:
            section = "workers"
            continue
        elif "--- Request Threads (enqueue" in line:
            section = "req_threads"
            continue
        elif "--- Response Poller" in line:
            section = "resp_poller"
            continue
        elif "--- Worker Poller" in line:
            section = "worker_poller"
            continue
        elif stripped.startswith("---") or stripped.startswith("==="):
            section = None

        # Parse TOTAL rows in tables
        if section == "resp_threads" and stripped.startswith("TOTAL"):
            parts = stripped.split()
            # TOTAL completed failed drain_rounds empty_rounds uintr_wakes
            if len(parts) >= 6:
                result["resp_threads"] = {
                    "total_completed": int(parts[1]),
                    "total_failed": int(parts[2]),
                    "total_drain_rounds": int(parts[3]),
                    "total_empty_rounds": int(parts[4]),
                    "total_uintr_wakes": int(parts[5]),
                }
        if section == "resp_threads" and "recycle_waits" in stripped:
            m2 = re.search(r"recycle_waits.*?:\s*(\d+)", stripped)
            if m2:
                result["resp_threads"]["total_recycle_waits"] = int(m2.group(1))

        if section == "workers" and stripped.startswith("TOTAL"):
            parts = stripped.split()
            if len(parts) >= 5:
                result["workers"] = {
                    "total_ops_done": int(parts[1]),
                    "total_empty_polls": int(parts[2]),
                    "total_resp_fullwaits": int(parts[3]),
                    "total_uintr_wakes": int(parts[4]),
                }
            elif len(parts) >= 4:
                # Old format without uintr_wakes column
                result["workers"] = {
                    "total_ops_done": int(parts[1]),
                    "total_empty_polls": int(parts[2]),
                    "total_resp_fullwaits": int(parts[3]),
                    "total_uintr_wakes": 0,
                }

        if section == "req_threads" and stripped.startswith("TOTAL"):
            parts = stripped.split()
            if len(parts) >= 3:
                result["req_threads"] = {
                    "total_submitted": int(parts[1]),
                    "total_enq_waits": int(parts[2]),
                }

        # Poller counters (key: value format)
        if section == "resp_poller":
            m2 = re.match(r"\s*scan_rounds:\s+(\d+)", stripped)
            if m2:
                if result["resp_poller"] is None:
                    result["resp_poller"] = {}
                result["resp_poller"]["scan_rounds"] = int(m2.group(1))
            m2 = re.match(r"\s*uintrs_sent:\s+(\d+)", stripped)
            if m2:
                if result["resp_poller"] is None:
                    result["resp_poller"] = {}
                result["resp_poller"]["uintrs_sent"] = int(m2.group(1))

        if section == "worker_poller":
            m2 = re.match(r"\s*scan_rounds:\s+(\d+)", stripped)
            if m2:
                if result["worker_poller"] is None:
                    result["worker_poller"] = {}
                result["worker_poller"]["scan_rounds"] = int(m2.group(1))
            m2 = re.match(r"\s*uintrs_sent:\s+(\d+)", stripped)
            if m2:
                if result["worker_poller"] is None:
                    result["worker_poller"] = {}
                result["worker_poller"]["uintrs_sent"] = int(m2.group(1))

        # Also parse stderr-style poller stop messages (backup)
        # [RespPoller] Stopped. scan_rounds=15268365  uintrs_sent=16
        m2 = re.match(r"\[RespPoller\] Stopped\. scan_rounds=(\d+)\s+uintrs_sent=(\d+)", stripped)
        if m2 and result["resp_poller"] is None:
            result["resp_poller"] = {
                "scan_rounds": int(m2.group(1)),
                "uintrs_sent": int(m2.group(2)),
            }
        m2 = re.match(r"\[WorkerPoller\] Stopped\. scan_rounds=(\d+)\s+uintrs_sent=(\d+)", stripped)
        if m2 and result["worker_poller"] is None:
            result["worker_poller"] = {
                "scan_rounds": int(m2.group(1)),
                "uintrs_sent": int(m2.group(2)),
            }
        # Old format: [Poller] Stopped. ...
        m2 = re.match(r"\[Poller\] Stopped\. scan_rounds=(\d+)\s+uintrs_sent=(\d+)", stripped)
        if m2 and result["resp_poller"] is None:
            result["resp_poller"] = {
                "scan_rounds": int(m2.group(1)),
                "uintrs_sent": int(m2.group(2)),
            }

    # Second throughput = transaction phase; fall back to first
    if len(throughputs) >= 2:
        result["throughput"] = throughputs[1]
    elif throughputs:
        result["throughput"] = throughputs[0]

    return result


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load results
    data = {}
    for mode in MODES:
        path = os.path.join(RESULTS_DIR, f"poller_mode_{mode}.txt")
        if not os.path.exists(path):
            print(f"WARNING: {path} not found, skipping mode '{mode}'")
            continue
        data[mode] = parse_result(path)
        tp = data[mode]["throughput"]
        print(f"  {mode:10s}  throughput={tp:>12,} ops/s" if tp else f"  {mode:10s}  throughput=N/A")
        for stage, lat in data[mode]["latency"].items():
            print(f"              {stage:30s}  avg={lat['avg']:.2f}  p50={lat['p50']:.2f}  p99={lat['p99']:.2f}  max={lat['max']:.2f}")
        rp = data[mode].get("resp_poller")
        if rp:
            print(f"              resp_poller: scan_rounds={rp['scan_rounds']:,}  uintrs_sent={rp['uintrs_sent']:,}")
        wp = data[mode].get("worker_poller")
        if wp:
            print(f"              worker_poller: scan_rounds={wp['scan_rounds']:,}  uintrs_sent={wp['uintrs_sent']:,}")
        rt = data[mode].get("resp_threads", {})
        if rt:
            print(f"              resp_threads: uintr_wakes={rt.get('total_uintr_wakes',0):,}  "
                  f"drain_rounds={rt.get('total_drain_rounds',0):,}  empty_rounds={rt.get('total_empty_rounds',0):,}")
        wk = data[mode].get("workers", {})
        if wk:
            print(f"              workers: uintr_wakes={wk.get('total_uintr_wakes',0):,}  "
                  f"empty_polls={wk.get('total_empty_polls',0):,}  resp_fullwaits={wk.get('total_resp_fullwaits',0):,}")

    available = [m for m in MODES if m in data and data[m]["throughput"] is not None]
    if not available:
        print("ERROR: No valid results found.")
        return

    # ========================================================================
    # Figure 1: Throughput bar chart
    # ========================================================================
    fig1, ax1 = plt.subplots(figsize=(8, 5))
    x = np.arange(len(available))
    tps = [data[m]["throughput"] / 1e6 for m in available]
    bars = ax1.bar(x, tps, color=[COLORS[m] for m in available],
                   width=0.55, edgecolor="black", linewidth=0.5)

    for bar, val in zip(bars, tps):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(tps) * 0.01,
                 f"{val:.2f}", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax1.set_xticks(x)
    ax1.set_xticklabels([MODE_LABELS[m] for m in available])
    ax1.set_ylabel("Throughput (Mops/s)")
    ax1.set_title("Throughput by Poller Mode")
    ax1.set_ylim(0, max(tps) * 1.15)
    ax1.grid(axis="y", alpha=0.3)
    fig1.tight_layout()
    fig1.savefig(os.path.join(OUTPUT_DIR, "poller_mode_throughput.png"), dpi=150)
    print(f"\nSaved: poller_mode_throughput.png")

    # ========================================================================
    # Figure 2: End-to-end latency (avg, p50, p99) grouped bar chart
    # ========================================================================
    e2e_key = None
    for m in available:
        for stage in data[m]["latency"]:
            if "end-to-end" in stage:
                e2e_key = stage
                break
        if e2e_key:
            break

    if e2e_key:
        fig2, ax2 = plt.subplots(figsize=(9, 5))
        metrics = ["avg", "p50", "p99"]
        metric_labels = ["Average", "p50 (Median)", "p99"]
        n_metrics = len(metrics)
        bar_width = 0.18
        x = np.arange(len(available))

        for mi, (metric, label) in enumerate(zip(metrics, metric_labels)):
            vals = [data[m]["latency"].get(e2e_key, {}).get(metric, 0) for m in available]
            offset = (mi - n_metrics / 2 + 0.5) * bar_width
            bars = ax2.bar(x + offset, vals, bar_width, label=label,
                           edgecolor="black", linewidth=0.3)
            for bar, val in zip(bars, vals):
                if val > 0:
                    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                             f"{val:.1f}", ha="center", va="bottom", fontsize=8)

        ax2.set_xticks(x)
        ax2.set_xticklabels([MODE_LABELS[m] for m in available])
        ax2.set_ylabel("Latency (us)")
        ax2.set_title("End-to-End Latency by Poller Mode")
        ax2.legend()
        ax2.grid(axis="y", alpha=0.3)
        fig2.tight_layout()
        fig2.savefig(os.path.join(OUTPUT_DIR, "poller_mode_latency_e2e.png"), dpi=150)
        print(f"Saved: poller_mode_latency_e2e.png")

    # ========================================================================
    # Figure 3: Decomposed latency stacked bar (avg per stage)
    # ========================================================================
    stage_keys = []
    stage_labels = []
    for m in available:
        for stage in data[m]["latency"]:
            if stage not in stage_keys and "end-to-end" not in stage:
                stage_keys.append(stage)
                short = stage.split(None, 1)[-1] if " " in stage else stage
                stage_labels.append(short)
        break

    if stage_keys:
        fig3, ax3 = plt.subplots(figsize=(8, 5))
        stage_colors = ["#a6cee3", "#fb9a99", "#b2df8a", "#fdbf6f"]
        x = np.arange(len(available))
        bottoms = np.zeros(len(available))

        for si, (skey, slabel) in enumerate(zip(stage_keys, stage_labels)):
            vals = np.array([data[m]["latency"].get(skey, {}).get("avg", 0) for m in available])
            color = stage_colors[si % len(stage_colors)]
            ax3.bar(x, vals, bottom=bottoms, width=0.5, label=slabel,
                    color=color, edgecolor="black", linewidth=0.3)
            for xi, val in enumerate(vals):
                if val > 0:
                    ax3.text(x[xi], bottoms[xi] + val / 2, f"{val:.1f}",
                             ha="center", va="center", fontsize=8)
            bottoms += vals

        ax3.set_xticks(x)
        ax3.set_xticklabels([MODE_LABELS[m] for m in available])
        ax3.set_ylabel("Average Latency (us)")
        ax3.set_title("Decomposed Average Latency by Poller Mode")
        ax3.legend(loc="upper right", fontsize=9)
        ax3.grid(axis="y", alpha=0.3)
        fig3.tight_layout()
        fig3.savefig(os.path.join(OUTPUT_DIR, "poller_mode_latency_decomposed.png"), dpi=150)
        print(f"Saved: poller_mode_latency_decomposed.png")

    # ========================================================================
    # Figure 4: UINTR — Sent vs Received (combined)
    #
    # "uintrs_sent" = actual _senduipi() calls by poller (edge-triggered:
    #     only on empty→non-empty transition, so very few under heavy load)
    # "uintr_wakes" = uintr_wait() returns (includes spurious wakeups from
    #     signals/EINTR — any pending signal causes uintr_wait to return,
    #     so this count >> uintrs_sent is EXPECTED behavior)
    # ========================================================================
    fig4, (ax4a, ax4b) = plt.subplots(1, 2, figsize=(16, 5.5))
    x = np.arange(len(available))
    bar_width = 0.3

    def _label_bars(ax, bars, vals, fmt="{:,}"):
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        fmt.format(val), ha="center", va="bottom", fontsize=9)

    # 4a: Actual UINTRs sent by pollers (edge-triggered)
    resp_uintrs = []
    worker_uintrs = []
    for m in available:
        rp = data[m].get("resp_poller")
        wp = data[m].get("worker_poller")
        resp_uintrs.append(rp["uintrs_sent"] if rp else 0)
        worker_uintrs.append(wp["uintrs_sent"] if wp else 0)

    b1 = ax4a.bar(x - bar_width / 2, resp_uintrs, bar_width,
                  label="Resp Poller → Resp Threads",
                  color="#4C72B0", edgecolor="black", linewidth=0.3)
    b2 = ax4a.bar(x + bar_width / 2, worker_uintrs, bar_width,
                  label="Worker Poller → Workers",
                  color="#DD8452", edgecolor="black", linewidth=0.3)
    _label_bars(ax4a, b1, resp_uintrs)
    _label_bars(ax4a, b2, worker_uintrs)
    ax4a.set_xticks(x)
    ax4a.set_xticklabels([MODE_LABELS[m] for m in available])
    ax4a.set_ylabel("Count")
    ax4a.set_title("_senduipi() Calls by Poller\n(edge-triggered: fires only on empty→non-empty)")
    ax4a.legend(fontsize=9)
    ax4a.grid(axis="y", alpha=0.3)

    # 4b: uintr_wait() returns on thread side (includes spurious)
    resp_wakes = []
    worker_wakes = []
    for m in available:
        rt = data[m].get("resp_threads", {})
        wk = data[m].get("workers", {})
        resp_wakes.append(rt.get("total_uintr_wakes", 0))
        worker_wakes.append(wk.get("total_uintr_wakes", 0))

    b1 = ax4b.bar(x - bar_width / 2, resp_wakes, bar_width,
                  label="Response Threads",
                  color="#4C72B0", edgecolor="black", linewidth=0.3)
    b2 = ax4b.bar(x + bar_width / 2, worker_wakes, bar_width,
                  label="Worker Threads",
                  color="#DD8452", edgecolor="black", linewidth=0.3)
    _label_bars(ax4b, b1, resp_wakes)
    _label_bars(ax4b, b2, worker_wakes)
    ax4b.set_xticks(x)
    ax4b.set_xticklabels([MODE_LABELS[m] for m in available])
    ax4b.set_ylabel("Count")
    ax4b.set_title("uintr_wait() Returns on Threads\n(includes spurious wakeups from signals/EINTR)")
    ax4b.legend(fontsize=9)
    ax4b.grid(axis="y", alpha=0.3)

    fig4.suptitle("UINTR: Sent by Pollers vs Received by Threads\n"
                  "(left ≪ right is expected — uintr_wait() returns on any signal, not just UINTR IPI)",
                  fontsize=11, fontweight="bold", y=1.02)
    fig4.tight_layout()
    fig4.savefig(os.path.join(OUTPUT_DIR, "poller_mode_uintr_combined.png"), dpi=150,
                 bbox_inches="tight")
    print(f"Saved: poller_mode_uintr_combined.png")

    # ========================================================================
    # Figure 5: Poller scan rounds
    # ========================================================================
    fig5, ax5 = plt.subplots(figsize=(9, 5))

    resp_scans = []
    worker_scans = []
    for m in available:
        rp = data[m].get("resp_poller")
        wp = data[m].get("worker_poller")
        resp_scans.append(rp["scan_rounds"] / 1e6 if rp else 0)
        worker_scans.append(wp["scan_rounds"] / 1e6 if wp else 0)

    b1 = ax5.bar(x - bar_width / 2, resp_scans, bar_width, label="Resp Poller",
                 color="#4C72B0", edgecolor="black", linewidth=0.3)
    b2 = ax5.bar(x + bar_width / 2, worker_scans, bar_width, label="Worker Poller",
                 color="#DD8452", edgecolor="black", linewidth=0.3)

    def _label_bars_m(bars, vals):
        for bar, val in zip(bars, vals):
            if val > 0:
                ax5.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                         f"{val:.1f}M", ha="center", va="bottom", fontsize=9)

    _label_bars_m(b1, resp_scans)
    _label_bars_m(b2, worker_scans)

    ax5.set_xticks(x)
    ax5.set_xticklabels([MODE_LABELS[m] for m in available])
    ax5.set_ylabel("Scan Rounds (millions)")
    ax5.set_title("Poller Scan Rounds")
    ax5.legend()
    ax5.grid(axis="y", alpha=0.3)
    fig5.tight_layout()
    fig5.savefig(os.path.join(OUTPUT_DIR, "poller_mode_scan_rounds.png"), dpi=150)
    print(f"Saved: poller_mode_scan_rounds.png")

    # ========================================================================
    # Figure 6: Worker empty polls & resp thread empty/drain rounds
    # ========================================================================
    fig7, (ax7a, ax7b) = plt.subplots(1, 2, figsize=(14, 5))

    # 7a: Worker empty polls
    w_empty = [data[m].get("workers", {}).get("total_empty_polls", 0) / 1e6 for m in available]
    bars = ax7a.bar(x, w_empty, 0.5, color=[COLORS[m] for m in available],
                    edgecolor="black", linewidth=0.3)
    for bar, val in zip(bars, w_empty):
        if val > 0:
            ax7a.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                      f"{val:.1f}M", ha="center", va="bottom", fontsize=9)
    ax7a.set_xticks(x)
    ax7a.set_xticklabels([MODE_LABELS[m] for m in available])
    ax7a.set_ylabel("Empty Polls (millions)")
    ax7a.set_title("Worker Empty Polls")
    ax7a.grid(axis="y", alpha=0.3)

    # 7b: Response thread drain rounds vs empty rounds
    drain = [data[m].get("resp_threads", {}).get("total_drain_rounds", 0) / 1e3 for m in available]
    empty = [data[m].get("resp_threads", {}).get("total_empty_rounds", 0) / 1e3 for m in available]

    b1 = ax7b.bar(x - bar_width / 2, drain, bar_width, label="Drain Rounds",
                  color="#55A868", edgecolor="black", linewidth=0.3)
    b2 = ax7b.bar(x + bar_width / 2, empty, bar_width, label="Empty Rounds",
                  color="#C44E52", edgecolor="black", linewidth=0.3)

    for bar, val in zip(b1, drain):
        if val > 0:
            ax7b.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                      f"{val:.0f}K", ha="center", va="bottom", fontsize=8)
    for bar, val in zip(b2, empty):
        if val > 0:
            ax7b.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                      f"{val:.0f}K", ha="center", va="bottom", fontsize=8)

    ax7b.set_xticks(x)
    ax7b.set_xticklabels([MODE_LABELS[m] for m in available])
    ax7b.set_ylabel("Rounds (thousands)")
    ax7b.set_title("Response Thread Drain/Empty Rounds")
    ax7b.legend()
    ax7b.grid(axis="y", alpha=0.3)

    fig7.tight_layout()
    fig7.savefig(os.path.join(OUTPUT_DIR, "poller_mode_poll_activity.png"), dpi=150)
    print(f"Saved: poller_mode_poll_activity.png")

    # ========================================================================
    # Figure 7: Request thread enqueue waits & worker resp_fullwaits
    # ========================================================================
    fig8, (ax8a, ax8b) = plt.subplots(1, 2, figsize=(14, 5))

    # 8a: Request thread enqueue waits
    enq_waits = [data[m].get("req_threads", {}).get("total_enq_waits", 0) / 1e6 for m in available]
    bars = ax8a.bar(x, enq_waits, 0.5, color=[COLORS[m] for m in available],
                    edgecolor="black", linewidth=0.3)
    for bar, val in zip(bars, enq_waits):
        if val > 0:
            ax8a.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                      f"{val:.1f}M", ha="center", va="bottom", fontsize=9)
    ax8a.set_xticks(x)
    ax8a.set_xticklabels([MODE_LABELS[m] for m in available])
    ax8a.set_ylabel("Enqueue Waits (millions)")
    ax8a.set_title("Request Thread Enqueue Waits\n(RequestQueue full → back-pressure)")
    ax8a.grid(axis="y", alpha=0.3)

    # 8b: Worker resp_fullwaits
    resp_fw = [data[m].get("workers", {}).get("total_resp_fullwaits", 0) / 1e6 for m in available]
    bars = ax8b.bar(x, resp_fw, 0.5, color=[COLORS[m] for m in available],
                    edgecolor="black", linewidth=0.3)
    for bar, val in zip(bars, resp_fw):
        if val > 0:
            ax8b.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                      f"{val:.1f}M", ha="center", va="bottom", fontsize=9)
    ax8b.set_xticks(x)
    ax8b.set_xticklabels([MODE_LABELS[m] for m in available])
    ax8b.set_ylabel("Resp Fullwaits (millions)")
    ax8b.set_title("Worker ResponseQueue Fullwaits\n(RespThread too slow → back-pressure)")
    ax8b.grid(axis="y", alpha=0.3)

    fig8.tight_layout()
    fig8.savefig(os.path.join(OUTPUT_DIR, "poller_mode_backpressure.png"), dpi=150)
    print(f"Saved: poller_mode_backpressure.png")

    print(f"\nAll plots saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
