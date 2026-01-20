#include "ycsb_benchmark.h"
#include "async_benchmark.h"
#include <unistd.h>
#include <signal.h>
#include <algorithm>

// Global flag for stopping benchmark
volatile bool g_should_stop = false;

void signal_handler(int signum) {
    printf("\n[Benchmark] Caught signal %d, stopping...\n", signum);
    g_should_stop = true;
}

void print_usage(const char* prog_name) {
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("\nOptions:\n");
    printf("  -w <workload>    Workload name (e.g., workloada, workloadc)\n");
    printf("  -n <numa_node>   NUMA node for CXL memory (default: 3)\n");
    printf("  -c <clients>     Number of client threads (default: 16)\n");
    printf("  -W <workers>     Number of worker threads (default: 16)\n");
    printf("  -t <seconds>     Run duration in seconds (default: 10)\n");
    printf("  -l               Measure latency (runs fixed ops instead of timed)\n");
    printf("  -o <ops>         Number of operations for latency test (default: 10000)\n");
    printf("  -s <cpu_start>   Starting CPU for client threads (default: 64)\n");
    printf("  -a, --async      Use async mode (Request + Response threads)\n");
    printf("  -h               Show this help message\n");
    printf("\nExample:\n");
    printf("  %s -w workloadc -c 32 -W 32 -t 30\n", prog_name);
    printf("  %s -w workloadc -c 16 -W 16 -l -o 100000\n", prog_name);
    printf("  %s -w workloadc -c 32 -W 32 -t 30 --async\n", prog_name);
}

int main(int argc, char** argv) {
    // Default configuration
    const char* workload_name = nullptr;
    int numa_node = 3;
    int num_clients = 16;
    int num_workers = 16;
    int duration_sec = 10;
    bool measure_latency = false;
    int num_ops_latency = 10000;
    int cpu_start = 64;  // Start CPUs for client threads after infrastructure threads
    bool async_mode = false;  // Use async Request/Response threads

    // Parse command line arguments
    int opt;
    while ((opt = getopt(argc, argv, "w:n:c:W:t:lo:s:ah")) != -1) {
        switch (opt) {
            case 'w':
                workload_name = optarg;
                break;
            case 'n':
                numa_node = atoi(optarg);
                break;
            case 'c':
                num_clients = atoi(optarg);
                break;
            case 'W':
                num_workers = atoi(optarg);
                break;
            case 't':
                duration_sec = atoi(optarg);
                break;
            case 'l':
                measure_latency = true;
                break;
            case 'o':
                num_ops_latency = atoi(optarg);
                break;
            case 's':
                cpu_start = atoi(optarg);
                break;
            case 'a':
                async_mode = true;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (workload_name == nullptr) {
        fprintf(stderr, "ERROR: Workload name (-w) is required\n\n");
        print_usage(argv[0]);
        return 1;
    }

    // Setup signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    printf("==============================================\n");
    printf("SharedKV Native YCSB Benchmark\n");
    printf("==============================================\n");
    printf("Configuration:\n");
    printf("  Workload:       %s\n", workload_name);
    printf("  NUMA node:      %d\n", numa_node);
    printf("  Client threads: %d\n", num_clients);
    printf("  Worker threads: %d\n", num_workers);
    printf("  CPU start:      %d\n", cpu_start);
    printf("  Architecture:   %s\n", async_mode ? "Async (Request+Response threads)" : "Synchronous");
    if (measure_latency) {
        printf("  Mode:           Latency measurement\n");
        printf("  Operations:     %d per thread\n", num_ops_latency);
    } else {
        printf("  Mode:           Throughput measurement\n");
        printf("  Duration:       %d seconds\n", duration_sec);
    }
    printf("==============================================\n\n");

    // Get workload files
    WorkloadFileNames workload_files = get_workload_files(workload_name);

    // Load workload operations
    printf("[Benchmark] Loading workload files...\n");
    std::vector<YCSBOperation> load_ops, trans_ops;

    int ret = load_ycsb_workload(workload_files.load_file.c_str(), load_ops);
    if (ret != 0) {
        fprintf(stderr, "ERROR: Failed to load workload file: %s\n",
                workload_files.load_file.c_str());
        return 1;
    }

    ret = load_ycsb_workload(workload_files.trans_file.c_str(), trans_ops);
    if (ret != 0) {
        fprintf(stderr, "ERROR: Failed to load workload file: %s\n",
                workload_files.trans_file.c_str());
        return 1;
    }

    printf("[Benchmark] Loaded %zu load operations, %zu transaction operations\n\n",
           load_ops.size(), trans_ops.size());

    // Initialize SharedKV context
    fprintf(stderr, "[Benchmark] Initializing SharedKV context...\n");
    fflush(stderr);
    SharedKVContext* ctx = get_or_create_context(num_clients, num_workers, numa_node);
    fprintf(stderr, "[Benchmark] Context created successfully\n");
    fflush(stderr);
    if (!ctx) {
        fprintf(stderr, "ERROR: Failed to create SharedKV context\n");
        return 1;
    }

    // Set execution mode (affects shutdown behavior)
    if (async_mode) {
        ctx->set_mode(ExecutionMode::ASYNC);
        printf("[Benchmark] SharedKV context initialized (ASYNC mode)\n\n");
    } else {
        ctx->set_mode(ExecutionMode::SYNC);
        printf("[Benchmark] SharedKV context initialized (SYNC mode)\n\n");
    }

    // ========================================================================
    // LOAD PHASE
    // ========================================================================

    printf("[Benchmark] Starting LOAD PHASE...\n");

    // NOTE: For LOAD phase, always use synchronous mode even in async benchmark
    // This is because:
    // 1. LOAD phase needs to insert each key exactly once (no circular wrap)
    // 2. LOAD phase must complete all inserts before transaction phase begins
    // 3. Async mode with circular ops would insert duplicate keys
    {
        // Sync mode: original implementation
        pthread_barrier_t load_barrier;
        pthread_barrier_init(&load_barrier, NULL, num_clients);

        std::atomic<uint64_t> load_total_ops{0};
        std::atomic<uint64_t> load_failed_ops{0};
        volatile bool load_stop = false;

        std::vector<pthread_t> load_threads(num_clients);
        std::vector<BenchmarkThreadArgs> load_args(num_clients);

        uint32_t ops_per_client = load_ops.size() / num_clients;

        for (int i = 0; i < num_clients; i++) {
            load_args[i].thread_id = i;
            load_args[i].num_threads = num_clients;
            load_args[i].cpu_id = cpu_start + i;
            load_args[i].ctx = ctx;
            load_args[i].client_id = i;
            load_args[i].operations = &load_ops;
            load_args[i].ops_start_idx = i * ops_per_client;
            load_args[i].ops_count = (i == num_clients - 1) ?
                                     (load_ops.size() - i * ops_per_client) : ops_per_client;
            load_args[i].start_barrier = &load_barrier;
            load_args[i].should_stop = &load_stop;
            load_args[i].total_ops = &load_total_ops;
            load_args[i].failed_ops = &load_failed_ops;
            load_args[i].measure_latency = false;
            load_args[i].latencies = nullptr;
        }

        uint64_t load_start = get_time_usec();

        for (int i = 0; i < num_clients; i++) {
            pthread_create(&load_threads[i], NULL, benchmark_latency_worker, &load_args[i]);
        }

        for (int i = 0; i < num_clients; i++) {
            pthread_join(load_threads[i], NULL);
        }

        uint64_t load_end = get_time_usec();
        uint64_t load_duration = load_end - load_start;

        print_benchmark_stats("LOAD PHASE", load_total_ops.load(), load_failed_ops.load(),
                             load_duration, nullptr);

        pthread_barrier_destroy(&load_barrier);
    }

    // ========================================================================
    // TRANSACTION PHASE
    // ========================================================================

    printf("[Benchmark] Starting TRANSACTION PHASE...\n");

    if (async_mode) {
        // Async mode: use Request + Response threads
        uint64_t trans_total_ops = 0;
        uint64_t trans_failed_ops = 0;
        std::vector<uint64_t> all_latencies;

        run_async_transaction_phase(
            ctx,
            trans_ops,
            num_clients,
            duration_sec,
            cpu_start,
            measure_latency,
            &trans_total_ops,
            &trans_failed_ops,
            measure_latency ? &all_latencies : nullptr
        );

        uint64_t trans_duration = duration_sec * 1000000ULL;  // Approximate
        print_benchmark_stats("TRANSACTION PHASE", trans_total_ops,
                             trans_failed_ops, trans_duration,
                             measure_latency ? &all_latencies : nullptr);

    } else {
        // Synchronous mode: original implementation
        pthread_barrier_t trans_barrier;
        pthread_barrier_init(&trans_barrier, NULL, num_clients);

        std::atomic<uint64_t> trans_total_ops{0};
        std::atomic<uint64_t> trans_failed_ops{0};

        std::vector<pthread_t> trans_threads(num_clients);
        std::vector<BenchmarkThreadArgs> trans_args(num_clients);
        std::vector<std::vector<uint64_t>> latency_per_thread(num_clients);

        uint32_t ops_per_client;

        if (measure_latency) {
        // Latency measurement mode: fixed number of operations
        ops_per_client = num_ops_latency;
        if (ops_per_client > trans_ops.size()) {
            ops_per_client = trans_ops.size();
        }

        for (int i = 0; i < num_clients; i++) {
            trans_args[i].thread_id = i;
            trans_args[i].num_threads = num_clients;
            trans_args[i].cpu_id = cpu_start + i;
            trans_args[i].ctx = ctx;
            trans_args[i].client_id = i;
            trans_args[i].operations = &trans_ops;
            trans_args[i].ops_start_idx = (i * ops_per_client) % trans_ops.size();
            trans_args[i].ops_count = ops_per_client;
            trans_args[i].start_barrier = &trans_barrier;
            trans_args[i].should_stop = &g_should_stop;
            trans_args[i].total_ops = &trans_total_ops;
            trans_args[i].failed_ops = &trans_failed_ops;
            trans_args[i].measure_latency = true;
            trans_args[i].latencies = &latency_per_thread[i];
        }

        uint64_t trans_start = get_time_usec();

        for (int i = 0; i < num_clients; i++) {
            pthread_create(&trans_threads[i], NULL, benchmark_latency_worker, &trans_args[i]);
        }

        for (int i = 0; i < num_clients; i++) {
            pthread_join(trans_threads[i], NULL);
        }

        uint64_t trans_end = get_time_usec();
        uint64_t trans_duration = trans_end - trans_start;

        // Merge all latencies
        std::vector<uint64_t> all_latencies;
        for (const auto& lats : latency_per_thread) {
            all_latencies.insert(all_latencies.end(), lats.begin(), lats.end());
        }

        print_benchmark_stats("TRANSACTION PHASE", trans_total_ops.load(),
                             trans_failed_ops.load(), trans_duration, &all_latencies);

    } else {
        // Throughput measurement mode: run for fixed duration
        ops_per_client = trans_ops.size() / num_clients;

        for (int i = 0; i < num_clients; i++) {
            trans_args[i].thread_id = i;
            trans_args[i].num_threads = num_clients;
            trans_args[i].cpu_id = cpu_start + i;
            trans_args[i].ctx = ctx;
            trans_args[i].client_id = i;
            trans_args[i].operations = &trans_ops;
            trans_args[i].ops_start_idx = i * ops_per_client;
            trans_args[i].ops_count = (i == num_clients - 1) ?
                                     (trans_ops.size() - i * ops_per_client) : ops_per_client;
            trans_args[i].start_barrier = &trans_barrier;
            trans_args[i].should_stop = &g_should_stop;
            trans_args[i].total_ops = &trans_total_ops;
            trans_args[i].failed_ops = &trans_failed_ops;
            trans_args[i].measure_latency = false;
            trans_args[i].latencies = nullptr;
        }

        uint64_t trans_start = get_time_usec();

        for (int i = 0; i < num_clients; i++) {
            pthread_create(&trans_threads[i], NULL, benchmark_throughput_worker, &trans_args[i]);
        }

        // Wait for specified duration
        sleep(duration_sec);
        g_should_stop = true;

        for (int i = 0; i < num_clients; i++) {
            pthread_join(trans_threads[i], NULL);
        }

        uint64_t trans_end = get_time_usec();
        uint64_t trans_duration = trans_end - trans_start;

        print_benchmark_stats("TRANSACTION PHASE", trans_total_ops.load(),
                             trans_failed_ops.load(), trans_duration, nullptr);
        }

        pthread_barrier_destroy(&trans_barrier);
    }

    // Cleanup
    printf("[Benchmark] Cleaning up...\n");
    destroy_context();

    printf("[Benchmark] Benchmark complete!\n");
    return 0;
}
