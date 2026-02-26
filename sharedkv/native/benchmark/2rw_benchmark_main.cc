// ============================================================================
// SharedKV 2RW YCSB Benchmark — Main Entry Point
//
// Usage:
//   sharedkv_2rw_benchmark -w <workload> [options]
//
// CPU Layout (must not overlap):
//   CPU 0                         : Poller
//   CPU 1 .. s                    : SN_0 .. SN_{s-1}  (s = num_synchronizers)
//   CPU worker_cpu .. +m-1        : Workers      (default: 1+s)
//   CPU cpu_start .. +n-1         : Request Threads  (one per client, dedicated)
//   CPU cpu_start+n .. +2n-1      : Response Threads (one per client, dedicated)
//
// Recommended: cpu_start >= worker_cpu + num_workers
// ============================================================================

#include "2rw_benchmark.h"
#include "ycsb_benchmark.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <unistd.h>

using namespace TwoRW;

// ============================================================================
// Global Stop Flag
// ============================================================================

static volatile bool g_should_stop = false;

static void signal_handler(int) {
    g_should_stop = true;
}

// ============================================================================
// Usage
// ============================================================================

static void print_usage(const char* prog) {
    printf(
        "Usage: %s --workload <workload> [options]\n"
        "\n"
        "Required:\n"
        "  --workload <name>           Workload name (workloada / workloadb / workloadc)\n"
        "\n"
        "2RW Architecture:\n"
        "  --num-clients       N    Request/Response thread pairs  (default: 4)\n"
        "  --num-workers       N    Worker threads                 (default: 8)\n"
        "  --num-synchronizers N    Synchronizer threads (m div s)  (default: 1)\n"
        "  --slots             N    Pool slots per client          (default: 1024)\n"
        "  --num-buckets       N    Hash table buckets (power-of-2)(default: 1048576)\n"
        "  --queue-depth       N    SPSC queue depth (power-of-2)  (default: 1024)\n"
        "  --mem-gb            N    CXL memory size in GB          (default: 16)\n"
        "  --worker-cpu        N    First CPU for Worker threads   (default: 1+s)\n"
        "  --local-workerring       WorkerRing on local DRAM instead of CXL\n"
        "\n"
        "Benchmark:\n"
        "  --numa <numa>           NUMA node for CXL memory       (default: 2)\n"
        "  --clients-start <cpu>            Starting CPU for client threads (default: 14)\n"
        "  --duration <seconds>        Throughput test duration        (default: 10)\n"
        "  --latency                  Latency mode (fixed ops, not timed)\n"
        "  --operations-per-client <ops>            Ops per client in latency mode  (default: 100000)\n"
        "\n"
        "Example:\n"
        "  %s --workload workloadc --num-clients 4 --num-workers 8 --worker-cpu 2 --clients-start 18 --duration 30\n"
        "  %s --workload workloada --num-clients 4 --num-workers 8 --worker-cpu 2 --clients-start 18 --latency --operations-per-client 100000\n",
        prog, prog, prog);
}

// ============================================================================
// Print Phase Results
// ============================================================================

static void print_phase_result(const char* phase, const PhaseResult& r,
                                uint64_t tsc_mhz, bool measure_latency) {
    double secs    = r.duration_usec / 1e6;
    double tput    = secs > 0 ? r.total_ops / secs : 0;
    uint64_t succ  = r.total_ops - r.failed_ops;

    printf("\n==============================================\n");
    printf("%s Results:\n", phase);
    printf("==============================================\n");
    printf("  Total ops:    %lu\n",  r.total_ops);
    printf("  Successful:   %lu\n",  succ);
    printf("  Failed:       %lu\n",  r.failed_ops);
    printf("  Duration:     %.3f s\n",  secs);
    printf("  Throughput:   %.0f ops/s\n", tput);

    if (r.num_synchronizers > 1 && !r.sn_ops.empty()) {
        printf("  Per-SN throughput:\n");
        for (uint32_t k = 0; k < r.num_synchronizers; k++) {
            double sn_tput = secs > 0 ? r.sn_ops[k] / secs : 0;
            printf("    SN%u: %lu ops  (%.0f ops/s)\n", k, r.sn_ops[k], sn_tput);
        }
    }

    if (measure_latency && !r.total_ticks.empty()) {
        print_latency_decomposed(r, tsc_mhz);
    }
    printf("==============================================\n\n");
}

// ============================================================================
// main
// ============================================================================

int main(int argc, char** argv) {
    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);

    // ---- Defaults ----
    const char* workload_name  = nullptr;
    int         cpu_start      = 14;
    int         duration_sec   = 10;
    bool        measure_latency = false;
    uint32_t    ops_per_client  = 100000;

    // 2RW config defaults
    TwoRWConfig cfg;
    cfg.numa_node        = 2;
    cfg.num_clients      = 4;
    cfg.num_workers      = 8;
    cfg.slots_per_client = 1024;
    cfg.num_buckets      = 1 << 20;   // 1M buckets
    cfg.queue_depth      = 1024;
    cfg.memory_size      = 16ULL << 30;
    cfg.num_synchronizers = 1;
    cfg.worker_cpu_start = -1;        // -1 = auto: 1 + num_synchronizers

    // ---- Argument Parsing ----
    for (int i = 1; i < argc; i++) {
        auto next_int = [&](const char* name) -> int {
            if (i + 1 >= argc) {
                fprintf(stderr, "ERROR: %s requires an argument\n", name);
                exit(1);
            }
            return atoi(argv[++i]);
        };
        auto next_u32 = [&](const char* name) -> uint32_t {
            return static_cast<uint32_t>(next_int(name));
        };

        if (strcmp(argv[i], "--workload") == 0 && i + 1 < argc) {
            workload_name = argv[++i];
        } else if (strcmp(argv[i], "--numa") == 0) {
            cfg.numa_node = next_int("--numa");
        } else if (strcmp(argv[i], "--clients-start") == 0) {
            cpu_start = next_int("--clients-start");
        } else if (strcmp(argv[i], "--duration") == 0) {
            duration_sec = next_int("--duration");
        } else if (strcmp(argv[i], "--latency") == 0) {
            measure_latency = true;
        } else if (strcmp(argv[i], "--operations-per-client") == 0) {
            ops_per_client = next_u32("--operations-per-client");
        } else if (strcmp(argv[i], "--num-clients") == 0) {
            cfg.num_clients = next_u32("--num-clients");
        } else if (strcmp(argv[i], "--num-workers") == 0) {
            cfg.num_workers = next_u32("--num-workers");
        } else if (strcmp(argv[i], "--num-synchronizers") == 0) {
            cfg.num_synchronizers = next_u32("--num-synchronizers");
        } else if (strcmp(argv[i], "--slots") == 0) {
            cfg.slots_per_client = next_u32("--slots");
        } else if (strcmp(argv[i], "--num-buckets") == 0) {
            cfg.num_buckets = next_u32("--num-buckets");
        } else if (strcmp(argv[i], "--queue-depth") == 0) {
            cfg.queue_depth = next_u32("--queue-depth");
        } else if (strcmp(argv[i], "--mem-gb") == 0) {
            cfg.memory_size = static_cast<uint64_t>(next_int("--mem-gb")) << 30;
        } else if (strcmp(argv[i], "--worker-cpu") == 0) {
            cfg.worker_cpu_start = next_int("--worker-cpu");
        } else if (strcmp(argv[i], "--local-workerring") == 0) {
            cfg.local_workerring = true;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "ERROR: Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    if (workload_name == nullptr) {
        fprintf(stderr, "ERROR: -w <workload> is required\n\n");
        print_usage(argv[0]);
        return 1;
    }

    const uint32_t n = cfg.num_clients;
    const uint32_t m = cfg.num_workers;
    const uint32_t s = cfg.num_synchronizers;

    // Effective worker CPU start (resolve -1 = auto: 1 + s)
    const int w_cpu = (cfg.worker_cpu_start < 0)
                      ? static_cast<int>(1 + s)
                      : cfg.worker_cpu_start;

    // ---- CPU Layout Validation ----
    int worker_max_cpu = w_cpu + static_cast<int>(m) - 1;
    int bench_min_cpu  = cpu_start;
    if (bench_min_cpu <= worker_max_cpu || w_cpu <= static_cast<int>(s)) {
        fprintf(stderr,
            "WARNING: Possible CPU overlap detected.\n"
            "  Poller:   CPU 0\n"
            "  SNs:      CPU 1..%u\n"
            "  Workers:  CPU %d..%d\n"
            "  Clients:  CPU %d..%d (ReqTh) / %d..%d (RespTh)\n"
            "  Recommended: -s %d or higher, --worker-cpu >= %u.\n",
            s,
            w_cpu, worker_max_cpu,
            cpu_start, cpu_start + (int)n - 1,
            cpu_start + (int)n, cpu_start + (int)(2*n) - 1,
            worker_max_cpu + 1, s + 1);
    }

    // ---- Print Config ----
    printf("==============================================\n");
    printf("SharedKV 2RW YCSB Benchmark\n");
    printf("==============================================\n");
    printf("  Workload:              %s\n",   workload_name);
    printf("  NUMA node:             %d\n",   cfg.numa_node);
    printf("  num_clients (n):       %u\n",   n);
    printf("  num_workers (m):       %u\n",   m);
    printf("  num_synchronizers (s): %u  (workers per SN: %u)\n", s, m / s);
    printf("  slots_per_client:      %u\n",   cfg.slots_per_client);
    printf("  num_buckets:           %u\n",   cfg.num_buckets);
    printf("  queue_depth:           %u\n",   cfg.queue_depth);
    printf("  memory_size:           %lu GB\n", cfg.memory_size >> 30);
    printf("  CPU layout:\n");
    printf("    Poller:     CPU 0\n");
    for (uint32_t k = 0; k < s; k++)
        printf("    SN%u:        CPU %u\n", k, k + 1);
    printf("    Workers:    CPU %d..%d\n", w_cpu, w_cpu + (int)m - 1);
    printf("    ReqTh:      CPU %d..%d\n", cpu_start,
           cpu_start + static_cast<int>(n) - 1);
    printf("    RespTh:     CPU %d..%d\n", cpu_start + static_cast<int>(n),
           cpu_start + static_cast<int>(2 * n) - 1);
    printf("  local_workerring:      %s\n",
           cfg.local_workerring ? "yes (DRAM)" : "no (CXL)");
    if (measure_latency) {
        printf("  Mode:         Latency  (%u ops/client, %u total)\n",
               ops_per_client, ops_per_client * n);
    } else {
        printf("  Mode:         Throughput (%d seconds)\n", duration_sec);
    }
    printf("==============================================\n\n");

    // ---- Load Workload Files ----
    WorkloadFileNames wf = get_workload_files(workload_name);
    std::vector<YCSBOperation> load_ops, trans_ops;

    printf("[Main] Loading workload files...\n");
    if (load_ycsb_workload(wf.load_file.c_str(), load_ops) != 0) {
        fprintf(stderr, "ERROR: Cannot load %s\n", wf.load_file.c_str());
        return 1;
    }
    if (load_ycsb_workload(wf.trans_file.c_str(), trans_ops) != 0) {
        fprintf(stderr, "ERROR: Cannot load %s\n", wf.trans_file.c_str());
        return 1;
    }
    printf("[Main] load_ops=%zu  trans_ops=%zu\n\n",
           load_ops.size(), trans_ops.size());

    // ---- Estimate TSC Frequency ----
    printf("[Main] Estimating TSC frequency...\n");
    uint64_t tsc_mhz = estimate_tsc_mhz();
    printf("[Main] TSC ~ %lu MHz\n\n", tsc_mhz);

    // ---- Init 2RW Context ----
    printf("[Main] Initializing 2RW context...\n");
    TwoRWContext* ctx = two_rw_init(cfg);
    if (!ctx) {
        fprintf(stderr, "ERROR: two_rw_init failed\n");
        return 1;
    }

    printf("[Main] Starting core threads (Sync, Poller, Workers)...\n");
    two_rw_start_threads(ctx);
    printf("\n");

    // ---- LOAD PHASE ----
    printf("[Main] ===== LOAD PHASE =====\n");
    {
        // Divide load ops evenly; each client handles a slice sequentially.
        // Use fixed-ops mode (not throughput), no latency collection.
        uint32_t ops_per_c = static_cast<uint32_t>(load_ops.size()) / n;
        if (ops_per_c == 0) ops_per_c = 1;

        PhaseResult lr = run_2rw_phase(
            ctx, load_ops, n, cpu_start,
            /*throughput=*/false, /*duration=*/0,
            ops_per_c, /*measure_latency=*/false);

        print_phase_result("LOAD PHASE", lr, tsc_mhz, false);
    }

    if (g_should_stop) {
        printf("[Main] Interrupted during load. Stopping.\n");
        goto cleanup;
    }

    // ---- TRANSACTION PHASE ----
    printf("[Main] ===== TRANSACTION PHASE =====\n");
    {
        PhaseResult tr = run_2rw_phase(
            ctx, trans_ops, n, cpu_start,
            /*throughput=*/!measure_latency,
            duration_sec,
            ops_per_client,
            measure_latency);

        print_phase_result("TRANSACTION PHASE", tr, tsc_mhz, measure_latency);
    }

cleanup:
    printf("[Main] Stopping core threads...\n");
    two_rw_stop(ctx);
    two_rw_destroy(ctx);
    printf("[Main] Done.\n");
    return 0;
}