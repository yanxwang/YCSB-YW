#include "ycsb_benchmark.h"
#include "async_benchmark.h"
#include <unistd.h>
#include <signal.h>
#include <algorithm>
#include <getopt.h>
#include <cstring>

// Global flag for stopping benchmark
volatile bool g_should_stop = false;

void signal_handler(int signum) {
    printf("\n[Benchmark] Caught signal %d, stopping...\n", signum);
    g_should_stop = true;
}

void print_usage(const char* prog_name) {
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("\nRequired:\n");
    printf("  -w <workload>       Workload name (e.g., workloada, workloadc)\n");
    printf("\nArchitecture Options:\n");
    printf("  -req_q <n>          Number of request queues (default: 8)\n");
    printf("  -resp_q <n>         Number of response queues (default: 8)\n");
    printf("  -req_th <n>         Number of request threads (default: 8)\n");
    printf("  -resp_th <n>        Number of response threads (default: 8)\n");
    printf("  -worker_th <n>      Number of worker threads (default: 8)\n");
    printf("\nOther Options:\n");
    printf("  -n <numa_node>      NUMA node for CXL memory (default: 2)\n");
    printf("  -t <seconds>        Run duration in seconds (default: 10)\n");
    printf("  -s <cpu_start>      Starting CPU for threads (default: 64)\n");
    printf("  -a                  Use async mode (default: sync mode)\n");
    printf("  -l                  Measure latency (runs fixed ops instead of timed)\n");
    printf("  -o <ops>            Number of operations for latency test (default: 10000)\n");
    printf("  -h                  Show this help message\n");
    printf("\nExample:\n");
    printf("  %s -w workloadc -n 2 -t 5 -s 64 -a -req_q 4 -resp_q 4 -req_th 8 -resp_th 4 -worker_th 8\n", prog_name);
}

int main(int argc, char** argv) {
    // Default configuration
    const char* workload_name = nullptr;
    int numa_node = 2;
    int duration_sec = 10;
    bool measure_latency = false;
    int num_ops_latency = 10000;
    int cpu_start = 64;
    bool async_mode = false;

    // Unified async config with sensible defaults
    AsyncConfig config;
    config.num_req_queues = 8;
    config.num_resp_queues = 8;
    config.num_req_threads = 8;
    config.num_resp_threads = 8;
    config.num_workers = 8;
    config.req_queue_depth = 2048;
    config.resp_queue_depth = 2048;

    // Long options for cleaner parameter names
    static struct option long_options[] = {
        {"help",      no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    // Parse command line arguments
    // Custom parsing for -req_q, -resp_q, etc. (non-standard getopt format)
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) {
            workload_name = argv[++i];
        } else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
            numa_node = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            duration_sec = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
            cpu_start = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-a") == 0) {
            async_mode = true;
        } else if (strcmp(argv[i], "-l") == 0) {
            measure_latency = true;
        } else if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
            num_ops_latency = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-req_q") == 0 && i + 1 < argc) {
            config.num_req_queues = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-resp_q") == 0 && i + 1 < argc) {
            config.num_resp_queues = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-req_th") == 0 && i + 1 < argc) {
            config.num_req_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-resp_th") == 0 && i + 1 < argc) {
            config.num_resp_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-worker_th") == 0 && i + 1 < argc) {
            config.num_workers = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "ERROR: Unknown option: %s\n\n", argv[i]);
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
    printf("  Workload:         %s\n", workload_name);
    printf("  NUMA node:        %d\n", numa_node);
    printf("  CPU start:        %d\n", cpu_start);
    printf("  Architecture:     %s\n", async_mode ? "Async" : "Sync");
    printf("  Request queues:   %u\n", config.num_req_queues);
    printf("  Response queues:  %u\n", config.num_resp_queues);
    printf("  Request threads:  %u\n", config.num_req_threads);
    printf("  Response threads: %u\n", config.num_resp_threads);
    printf("  Worker threads:   %u\n", config.num_workers);
    printf("  Queue depth:      %zu\n", config.req_queue_depth);
    if (measure_latency) {
        printf("  Mode:             Latency measurement\n");
        printf("  Operations:       %d per thread\n", num_ops_latency);
    } else {
        printf("  Mode:             Throughput measurement\n");
        printf("  Duration:         %d seconds\n", duration_sec);
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
    SharedKVContext* ctx = get_or_create_context(numa_node, config);
    fprintf(stderr, "[Benchmark] Context created successfully\n");
    fflush(stderr);
    if (!ctx) {
        fprintf(stderr, "ERROR: Failed to create SharedKV context\n");
        return 1;
    }

    // Set execution mode
    if (async_mode) {
        ctx->set_mode(ExecutionMode::ASYNC);
        printf("[Benchmark] SharedKV context initialized (ASYNC mode)\n\n");
    } else {
        ctx->set_mode(ExecutionMode::SYNC);
        printf("[Benchmark] SharedKV context initialized (SYNC mode)\n\n");
    }

    // ========================================================================
    // LOAD PHASE (always sync mode for correctness)
    // ========================================================================

    printf("[Benchmark] Starting LOAD PHASE...\n");

    {
        uint32_t num_threads = config.num_req_threads;
        pthread_barrier_t load_barrier;
        pthread_barrier_init(&load_barrier, NULL, num_threads);

        std::atomic<uint64_t> load_total_ops{0};
        std::atomic<uint64_t> load_failed_ops{0};
        volatile bool load_stop = false;

        std::vector<pthread_t> load_threads(num_threads);
        std::vector<BenchmarkThreadArgs> load_args(num_threads);

        uint32_t ops_per_thread = load_ops.size() / num_threads;

        for (uint32_t i = 0; i < num_threads; i++) {
            load_args[i].thread_id = i;
            load_args[i].num_threads = num_threads;
            load_args[i].cpu_id = cpu_start + i;
            load_args[i].ctx = ctx;
            load_args[i].client_id = i;
            load_args[i].operations = &load_ops;
            load_args[i].ops_start_idx = i * ops_per_thread;
            load_args[i].ops_count = (i == num_threads - 1) ?
                                     (load_ops.size() - i * ops_per_thread) : ops_per_thread;
            load_args[i].start_barrier = &load_barrier;
            load_args[i].should_stop = &load_stop;
            load_args[i].total_ops = &load_total_ops;
            load_args[i].failed_ops = &load_failed_ops;
            load_args[i].measure_latency = false;
            load_args[i].latencies = nullptr;
        }

        uint64_t load_start = get_time_usec();

        for (uint32_t i = 0; i < num_threads; i++) {
            pthread_create(&load_threads[i], NULL, benchmark_latency_worker, &load_args[i]);
        }

        for (uint32_t i = 0; i < num_threads; i++) {
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
            config.num_req_threads,
            duration_sec,
            cpu_start,
            measure_latency,
            &trans_total_ops,
            &trans_failed_ops,
            measure_latency ? &all_latencies : nullptr
        );

        uint64_t trans_duration = duration_sec * 1000000ULL;
        print_benchmark_stats("TRANSACTION PHASE", trans_total_ops,
                             trans_failed_ops, trans_duration,
                             measure_latency ? &all_latencies : nullptr);

    } else {
        // Synchronous mode
        uint32_t num_threads = config.num_req_threads;
        pthread_barrier_t trans_barrier;
        pthread_barrier_init(&trans_barrier, NULL, num_threads);

        std::atomic<uint64_t> trans_total_ops{0};
        std::atomic<uint64_t> trans_failed_ops{0};

        std::vector<pthread_t> trans_threads(num_threads);
        std::vector<BenchmarkThreadArgs> trans_args(num_threads);
        std::vector<std::vector<uint64_t>> latency_per_thread(num_threads);

        uint32_t ops_per_thread;

        if (measure_latency) {
            // Latency measurement mode
            ops_per_thread = num_ops_latency;
            if (ops_per_thread > trans_ops.size()) {
                ops_per_thread = trans_ops.size();
            }

            for (uint32_t i = 0; i < num_threads; i++) {
                trans_args[i].thread_id = i;
                trans_args[i].num_threads = num_threads;
                trans_args[i].cpu_id = cpu_start + i;
                trans_args[i].ctx = ctx;
                trans_args[i].client_id = i;
                trans_args[i].operations = &trans_ops;
                trans_args[i].ops_start_idx = (i * ops_per_thread) % trans_ops.size();
                trans_args[i].ops_count = ops_per_thread;
                trans_args[i].start_barrier = &trans_barrier;
                trans_args[i].should_stop = &g_should_stop;
                trans_args[i].total_ops = &trans_total_ops;
                trans_args[i].failed_ops = &trans_failed_ops;
                trans_args[i].measure_latency = true;
                trans_args[i].latencies = &latency_per_thread[i];
            }

            uint64_t trans_start = get_time_usec();

            for (uint32_t i = 0; i < num_threads; i++) {
                pthread_create(&trans_threads[i], NULL, benchmark_latency_worker, &trans_args[i]);
            }

            for (uint32_t i = 0; i < num_threads; i++) {
                pthread_join(trans_threads[i], NULL);
            }

            uint64_t trans_end = get_time_usec();
            uint64_t trans_duration = trans_end - trans_start;

            std::vector<uint64_t> all_latencies;
            for (const auto& lats : latency_per_thread) {
                all_latencies.insert(all_latencies.end(), lats.begin(), lats.end());
            }

            print_benchmark_stats("TRANSACTION PHASE", trans_total_ops.load(),
                                 trans_failed_ops.load(), trans_duration, &all_latencies);

        } else {
            // Throughput measurement mode
            ops_per_thread = trans_ops.size() / num_threads;

            for (uint32_t i = 0; i < num_threads; i++) {
                trans_args[i].thread_id = i;
                trans_args[i].num_threads = num_threads;
                trans_args[i].cpu_id = cpu_start + i;
                trans_args[i].ctx = ctx;
                trans_args[i].client_id = i;
                trans_args[i].operations = &trans_ops;
                trans_args[i].ops_start_idx = i * ops_per_thread;
                trans_args[i].ops_count = (i == num_threads - 1) ?
                                         (trans_ops.size() - i * ops_per_thread) : ops_per_thread;
                trans_args[i].start_barrier = &trans_barrier;
                trans_args[i].should_stop = &g_should_stop;
                trans_args[i].total_ops = &trans_total_ops;
                trans_args[i].failed_ops = &trans_failed_ops;
                trans_args[i].measure_latency = false;
                trans_args[i].latencies = nullptr;
            }

            uint64_t trans_start = get_time_usec();

            for (uint32_t i = 0; i < num_threads; i++) {
                pthread_create(&trans_threads[i], NULL, benchmark_throughput_worker, &trans_args[i]);
            }

            sleep(duration_sec);
            g_should_stop = true;

            for (uint32_t i = 0; i < num_threads; i++) {
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
