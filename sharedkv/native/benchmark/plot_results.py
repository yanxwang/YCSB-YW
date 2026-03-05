#!/usr/bin/env python3
"""Plot SharedKV 2RW YCSB benchmark results."""

import os
import re
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.size'] = 12

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plots")


def parse_transaction_throughput(filepath):
    """Extract TRANSACTION PHASE throughput (ops/s) from a result file."""
    throughputs = []
    with open(filepath, errors='replace') as f:
        for line in f:
            m = re.search(r'Throughput:\s+([\d]+)\s+ops/s', line)
            if m:
                throughputs.append(int(m.group(1)))
    # Second throughput value is the transaction phase
    if len(throughputs) >= 2:
        return throughputs[1]
    elif len(throughputs) == 1:
        return throughputs[0]
    return None


def load_all_results():
    """Load all result files into a dict keyed by (workload, sn, clients)."""
    data = {}
    for fname in os.listdir(RESULTS_DIR):
        m = re.match(r'(workload[ac])_sn(\d+)_c(\d+)\.txt', fname)
        if not m:
            continue
        workload = m.group(1)
        sn = int(m.group(2))
        clients = int(m.group(3))
        tp = parse_transaction_throughput(os.path.join(RESULTS_DIR, fname))
        if tp is not None:
            data[(workload, sn, clients)] = tp
    return data


def plot_single_sn_scaling(data, workload, ax):
    """Plot 1/3: single synchronizer (sn=1), varying clients."""
    clients_list = sorted([c for (w, s, c) in data if w == workload and s == 1])
    throughputs = [data[(workload, 1, c)] / 1e6 for c in clients_list]

    bars = ax.bar(range(len(clients_list)), throughputs, color='#4C72B0', width=0.6,
                  edgecolor='black', linewidth=0.5)

    ax.set_xticks(range(len(clients_list)))
    ax.set_xticklabels([str(c) for c in clients_list])
    ax.set_xlabel("Number of Clients")
    ax.set_ylabel("Throughput (Mops/s)")
    wl_label = "A (50% Read, 50% Update)" if workload == "workloada" else "C (100% Read)"
    ax.set_title(f"Workload {wl_label}\n1 Synchronizer, Varying Clients")
    ax.set_ylim(0, max(throughputs) * 1.2)

    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f'{val:.2f}', ha='center', va='bottom', fontsize=10)


def plot_multi_sn_comparison(data, workload, ax):
    """Plot 2/4: multi-synchronizer comparison with grouped bars."""
    # Specific data points requested by user
    groups = [
        (1, [4, 8, 16]),
        (2, [8, 16, 32]),
        (4, [16, 30]),
        (8, [28]),
    ]

    colors = ['#4C72B0', '#DD8452', '#55A868', '#C44E52']
    group_labels = []
    group_positions = []
    bar_positions = []  # (pos, client_count) for x-axis client labels
    bar_width = 0.7
    gap_between_groups = 1.0
    pos = 0

    for gi, (sn, client_list) in enumerate(groups):
        group_start = pos
        for c in client_list:
            key = (workload, sn, c)
            tp = data.get(key)
            if tp is None:
                pos += bar_width + 0.1
                continue
            tp_m = tp / 1e6
            bar = ax.bar(pos, tp_m, width=bar_width, color=colors[gi],
                         edgecolor='black', linewidth=0.5)
            ax.text(pos, tp_m + 0.05, f'{tp_m:.1f}', ha='center', va='bottom',
                    fontsize=9)
            bar_positions.append((pos, c))
            pos += bar_width + 0.1

        group_center = (group_start + pos - bar_width - 0.1) / 2
        group_labels.append(f'SN={sn}')
        group_positions.append(group_center)
        pos += gap_between_groups

    # Two-level x-axis: client counts on minor ticks, SN labels below
    ax.set_xticks([p for p, _ in bar_positions])
    ax.set_xticklabels([str(c) for _, c in bar_positions], fontsize=9)
    ax.tick_params(axis='x', which='major', pad=2, length=0)

    # Add SN group labels below via secondary x-axis
    ax2 = ax.secondary_xaxis('bottom')
    ax2.set_xticks(group_positions)
    ax2.set_xticklabels(group_labels, fontsize=11, fontweight='bold')
    ax2.tick_params(axis='x', length=0, pad=22)

    ax.set_xlabel("Number of Clients", labelpad=30)
    ax.set_ylabel("Throughput (Mops/s)")
    wl_label = "A (50% Read, 50% Update)" if workload == "workloada" else "C (100% Read)"
    ax.set_title(f"Workload {wl_label}\nScaling Across Synchronizers")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.15)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    data = load_all_results()

    print("Loaded data points:")
    for key in sorted(data.keys()):
        w, s, c = key
        print(f"  {w} sn={s} c={c}: {data[key]:,} ops/s")

    # --- Figure 1: Workload A, single SN scaling ---
    fig1, ax1 = plt.subplots(figsize=(7, 5))
    plot_single_sn_scaling(data, "workloada", ax1)
    fig1.tight_layout()
    fig1.savefig(os.path.join(OUTPUT_DIR, "fig1_workloada_sn1_scaling.jpg"), dpi=150)
    print("Saved fig1_workloada_sn1_scaling.jpg")

    # --- Figure 2: Workload A, multi-SN comparison ---
    fig2, ax2 = plt.subplots(figsize=(9, 5))
    plot_multi_sn_comparison(data, "workloada", ax2)
    fig2.tight_layout()
    fig2.savefig(os.path.join(OUTPUT_DIR, "fig2_workloada_multi_sn.jpg"), dpi=150)
    print("Saved fig2_workloada_multi_sn.jpg")

    # --- Figure 3: Workload C, single SN scaling ---
    fig3, ax3 = plt.subplots(figsize=(7, 5))
    plot_single_sn_scaling(data, "workloadc", ax3)
    fig3.tight_layout()
    fig3.savefig(os.path.join(OUTPUT_DIR, "fig3_workloadc_sn1_scaling.jpg"), dpi=150)
    print("Saved fig3_workloadc_sn1_scaling.jpg")

    # --- Figure 4: Workload C, multi-SN comparison ---
    fig4, ax4 = plt.subplots(figsize=(9, 5))
    plot_multi_sn_comparison(data, "workloadc", ax4)
    fig4.tight_layout()
    fig4.savefig(os.path.join(OUTPUT_DIR, "fig4_workloadc_multi_sn.jpg"), dpi=150)
    print("Saved fig4_workloadc_multi_sn.jpg")

    print("\nDone. Figures saved to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()