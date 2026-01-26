// cxl_benchmark.cc
// CXL-aware SharedKV Benchmark with YCSB workload support
// Architecture: Request Thread -> Local Queue -> Synchronizer -> Worker Ring Buffer
// Uses the same command-line parameters as sharedkv_benchmark

#include "cxl_shared.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <functional>
#include <numa.h>

// ============================================================================
// External CXL functions
// ============================================================================

extern void cxl_init_memory(void* base, uint64_t total_size,
                            uint32_t num_workers, uint32_t num_response_rings);

extern void cxl_worker_run(void* cxl_base, const CXLMemoryLayout& layout,
                           uint32_t worker_id, uint32_t host_id,
                           uint32_t num_response_rings, std::atomic<bool>& stop_flag);

extern void cxl_synchronizer_run(void* cxl_base,
                                 const CXLMemoryLayout& layout,
                                 std::vector<LockFreeQueue<KVRequest*>*>& local_req_queues,
                                 std::atomic<uint64_t>& global_sequence,
                                 std::atomic<bool>& stop_flag);

// ============================================================================
// YCSB Operation Types (same as ycsb_benchmark.h)
// ============================================================================

enum class YCSBOpType : uint8_t {
    INSERT = 0,
    READ = 1,
    UPDATE = 2,
    SCAN = 3,
    READ_MODIFY_WRITE = 4,
    DELETE = 5
};

struct YCSBOperation {
    YCSBOpType op_type;
    std::string key;
    std::string value;
    YCSBOperation() : op_type(YCSBOpType::READ) {}
};

// ============================================================================
// Workload File Loading (same logic as ycsb_benchmark.cc)
// ============================================================================

struct WorkloadFileNames {
    std::string load_file;
    std::string trans_file;
};

WorkloadFileNames get_workload_files(const char* workload_name) {
    WorkloadFileNames files;
    std::string base_path = "workloads/";
    files.load_file = base_path + workload_name + "_load.txt";
    files.trans_file = base_path + workload_name + "_trans.txt";
    return files;
}

int load_ycsb_workload(const char* filename, std::vector<YCSBOperation>& operations) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        fprintf(stderr, "ERROR: Cannot open workload file: %s\n", filename);
        return -1;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        YCSBOperation op;
        std::istringstream iss(line);
        std::string op_str, key;

        iss >> op_str >> key;

        // Parse operation type
        if (op_str == "INSERT") {
            op.op_type = YCSBOpType::INSERT;
        } else if (op_str == "READ") {
            op.op_type = YCSBOpType::READ;
        } else if (op_str == "UPDATE") {
            op.op_type = YCSBOpType::UPDATE;
        } else if (op_str == "SCAN") {
            op.op_type = YCSBOpType::SCAN;
        } else if (op_str == "READMODIFYWRITE") {
            op.op_type = YCSBOpType::READ_MODIFY_WRITE;
        } else if (op_str == "DELETE") {
            op.op_type = YCSBOpType::DELETE;
        } else {
            continue;  // Unknown operation
        }

        op.key = key;

        // For INSERT/UPDATE, generate a value
        if (op.op_type == YCSBOpType::INSERT || op.op_type == YCSBOpType::UPDATE) {
            op.value = std::string(100, 'V');  // 100 bytes value
        }

        operations.push_back(op);
    }

    return 0;
}

// ============================================================================
// Simple Request Pool (for KVRequest allocation) - Thread-safe LIFO stack
// ============================================================================

class SimpleRequestPool {
public:
    SimpleRequestPool(uint32_t capacity) : capacity_(capacity) {
        pool_ = new KVRequest[capacity];
        free_stack_ = new std::atomic<KVRequest*>[capacity];

        // Initialize free stack
        for (uint32_t i = 0; i < capacity; i++) {
            pool_[i].recycle_func = recycle_callback;
            pool_[i].recycle_ctx = this;
            free_stack_[i].store(&pool_[i], std::memory_order_relaxed);
        }
        stack_top_.store(capacity, std::memory_order_release);
    }

    ~SimpleRequestPool() {
        delete[] pool_;
        delete[] free_stack_;
    }

    KVRequest* alloc() {
        while (true) {
            uint32_t top = stack_top_.load(std::memory_order_acquire);
            if (top == 0) {
                return nullptr;  // Pool exhausted
            }

            // Try to pop from stack
            if (stack_top_.compare_exchange_weak(top, top - 1,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                return free_stack_[top - 1].load(std::memory_order_acquire);
            }
            // CAS failed, retry
        }
    }

    void free(KVRequest* req) {
        while (true) {
            uint32_t top = stack_top_.load(std::memory_order_acquire);
            if (top >= capacity_) {
                return;  // Stack full (shouldn't happen)
            }

            // Try to push to stack
            if (stack_top_.compare_exchange_weak(top, top + 1,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                free_stack_[top].store(req, std::memory_order_release);
                return;
            }
            // CAS failed, retry
        }
    }

    static void recycle_callback(void* ctx, KVRequest* req) {
        static_cast<SimpleRequestPool*>(ctx)->free(req);
    }

    uint32_t capacity() const { return capacity_; }

private:
    KVRequest* pool_;
    std::atomic<KVRequest*>* free_stack_;
    std::atomic<uint32_t> stack_top_;
    uint32_t capacity_;
};

// ============================================================================
// Global stop flag
// ============================================================================

volatile bool g_should_stop = false;

void signal_handler(int signum) {
    printf("\n[CXL-Benchmark] Caught signal %d, stopping...\n", signum);
    g_should_stop = true;
}

// ============================================================================
// Hash function (same as workers)
// ============================================================================

static uint64_t cxl_hash_key(const char* key, uint32_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (uint32_t i = 0; i < len; i++) {
        hash ^= static_cast<uint64_t>(static_cast<unsigned char>(key[i]));
        hash *= 1099511628211ULL;
    }
    return hash;
}

// ============================================================================
// Thread Context
// ============================================================================

struct CXLBenchmarkContext {
    uint32_t client_id;
    uint32_t num_clients;

    // CXL pointers
    void* cxl_base;
    CXLMemoryLayout* layout;
    uint32_t num_workers;

    // Local request queue (for synchronizer to pick up)
    LockFreeQueue<KVRequest*>* local_req_queue;

    // Request pool
    SimpleRequestPool* request_pool;

    // Workload
    std::vector<YCSBOperation>* operations;
    uint32_t ops_start_idx;
    uint32_t ops_count;
    std::vector<uint32_t>* precomputed_worker_ids;

    // Synchronization
    pthread_barrier_t* start_barrier;
    volatile bool* should_stop;         // Controls request threads
    volatile bool* resp_can_exit;       // Controls response threads (set after pipeline drain)

    // Statistics
    std::atomic<uint64_t> requests_submitted{0};
    std::atomic<uint64_t> responses_received{0};
    std::atomic<uint64_t> failed_ops{0};

    // Latency measurement
    bool measure_latency;
    std::vector<uint64_t> latencies;
    pthread_mutex_t latency_mutex;

    // Threads
    pthread_t request_thread;
    pthread_t response_thread;
};

// ============================================================================
// Request Thread (submits requests to local queue -> synchronizer picks up)
// ============================================================================

void* cxl_request_thread_func(void* arg) {
    CXLBenchmarkContext* ctx = static_cast<CXLBenchmarkContext*>(arg);

    fprintf(stderr, "[CXL-Req-%u] Started, ops_start=%u, ops_count=%u\n",
            ctx->client_id, ctx->ops_start_idx, ctx->ops_count);

    // Wait for all threads
    pthread_barrier_wait(ctx->start_barrier);

    uint32_t op_idx = ctx->ops_start_idx;
    uint32_t ops_end = ctx->ops_start_idx + ctx->ops_count;
    uint32_t local_idx = 0;
    uint64_t pool_waits = 0;
    uint64_t enqueue_waits = 0;

    while (!*(ctx->should_stop) && !g_should_stop) {
        const YCSBOperation& op = (*ctx->operations)[op_idx];

        // Allocate request from pool
        KVRequest* req = ctx->request_pool->alloc();
        while (req == nullptr) {
            pool_waits++;
            if (*(ctx->should_stop) || g_should_stop) {
                goto done;
            }
            __builtin_ia32_pause();
            req = ctx->request_pool->alloc();
        }

        // Initialize request
        req->client_id = ctx->client_id;
        req->target_worker_id = (*ctx->precomputed_worker_ids)[local_idx];
        req->timestamp = ctx->measure_latency ?
            std::chrono::steady_clock::now().time_since_epoch().count() : 0;

        // Set operation type
        switch (op.op_type) {
            case YCSBOpType::READ:
                req->op_type = KVOpType::READ;
                break;
            case YCSBOpType::INSERT:
                req->op_type = KVOpType::INSERT;
                break;
            case YCSBOpType::UPDATE:
            case YCSBOpType::READ_MODIFY_WRITE:
                req->op_type = KVOpType::UPDATE;
                break;
            case YCSBOpType::DELETE:
                req->op_type = KVOpType::DELETE;
                break;
            default:
                req->op_type = KVOpType::READ;
                break;
        }

        // Set key
        req->key_len = op.key.length();
        memcpy(req->key_data, op.key.data(), req->key_len);

        // Set value for write operations
        // NOTE: We allocate and copy value data because recycle() will call cleanup()
        // which deletes value_data
        if (op.op_type == YCSBOpType::INSERT || op.op_type == YCSBOpType::UPDATE ||
            op.op_type == YCSBOpType::READ_MODIFY_WRITE) {
            req->value_len = op.value.length();
            req->value_data = new char[req->value_len + 1];
            memcpy(req->value_data, op.value.data(), req->value_len);
            req->value_data[req->value_len] = '\0';
        } else {
            req->value_len = 0;
            req->value_data = nullptr;
        }

        // Enqueue to local request queue (synchronizer will pick up and route)
        while (!ctx->local_req_queue->enqueue(req)) {
            enqueue_waits++;
            if (*(ctx->should_stop) || g_should_stop) {
                ctx->request_pool->free(req);
                goto done;
            }
            __builtin_ia32_pause();
        }

        ctx->requests_submitted.fetch_add(1, std::memory_order_relaxed);

        // Move to next operation (circular)
        op_idx++;
        local_idx++;
        if (op_idx >= ops_end) {
            op_idx = ctx->ops_start_idx;
            local_idx = 0;
        }
    }

done:
    fprintf(stderr, "[CXL-Req-%u] Submitted %lu requests, pool_waits=%lu, enqueue_waits=%lu\n",
            ctx->client_id, ctx->requests_submitted.load(), pool_waits, enqueue_waits);
    return nullptr;
}

// ============================================================================
// Response Thread (polls responses from CXL response ring)
// ============================================================================

void* cxl_response_thread_func(void* arg) {
    CXLBenchmarkContext* ctx = static_cast<CXLBenchmarkContext*>(arg);

    // Get my response ring
    CXLResponseRing* resp_ring = cxl_get_response_ring(
        ctx->cxl_base, *ctx->layout, ctx->client_id
    );

    fprintf(stderr, "[CXL-Resp-%u] Started, resp_ring=%p\n", ctx->client_id, (void*)resp_ring);

    // Wait for all threads
    pthread_barrier_wait(ctx->start_barrier);

    uint64_t poll_count = 0;
    uint64_t empty_count = 0;

    while (true) {
        CXLResponse resp;
        poll_count++;

        if (resp_ring->dequeue(resp)) {
            // Calculate latency
            if (ctx->measure_latency && resp.timestamp > 0) {
                uint64_t now = std::chrono::steady_clock::now().time_since_epoch().count();
                uint64_t latency_ns = now - resp.timestamp;

                pthread_mutex_lock(&ctx->latency_mutex);
                ctx->latencies.push_back(latency_ns);
                pthread_mutex_unlock(&ctx->latency_mutex);
            }

            // Update statistics
            if (resp.status == CXLStatus::SUCCESS) {
                ctx->responses_received.fetch_add(1, std::memory_order_relaxed);
            } else {
                ctx->failed_ops.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            empty_count++;
            // Check if we can exit - only when resp_can_exit is set (after pipeline drain)
            if (ctx->resp_can_exit && *(ctx->resp_can_exit)) {
                // Verify empty multiple times before exiting
                bool truly_empty = true;
                for (int check = 0; check < 5; check++) {
                    if (!resp_ring->is_empty()) {
                        truly_empty = false;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
                if (truly_empty) {
                    break;
                }
            }
            // Also check global stop for emergency shutdown
            if (g_should_stop) {
                if (resp_ring->is_empty()) {
                    break;
                }
            }
            std::this_thread::yield();
        }
    }

    fprintf(stderr, "[CXL-Resp-%u] Received %lu responses (%lu failed), polls=%lu, empty=%lu\n",
            ctx->client_id, ctx->responses_received.load(), ctx->failed_ops.load(),
            poll_count, empty_count);
    return nullptr;
}

// ============================================================================
// Utility Functions
// ============================================================================

uint64_t get_time_usec() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
}

uint64_t calculate_percentile(const std::vector<uint64_t>& latencies, double percentile) {
    if (latencies.empty()) return 0;
    size_t idx = static_cast<size_t>(latencies.size() * percentile / 100.0);
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx];
}

void print_benchmark_stats(const char* phase_name,
                          uint64_t total_ops,
                          uint64_t failed_ops,
                          uint64_t duration_usec,
                          const std::vector<uint64_t>* latencies) {
    double duration_sec = duration_usec / 1000000.0;
    double throughput = total_ops / duration_sec;

    printf("\n==============================================\n");
    printf("%s Results:\n", phase_name);
    printf("==============================================\n");
    printf("Total operations:  %lu\n", total_ops);
    printf("Failed operations: %lu\n", failed_ops);
    printf("Duration:          %.2f seconds\n", duration_sec);
    printf("Throughput:        %.2f ops/sec (%.2f Kops/sec)\n",
           throughput, throughput / 1000.0);

    if (latencies && !latencies->empty()) {
        std::vector<uint64_t> sorted_lats = *latencies;
        std::sort(sorted_lats.begin(), sorted_lats.end());

        uint64_t sum = 0;
        for (auto l : sorted_lats) sum += l;
        double avg = sum / static_cast<double>(sorted_lats.size());

        printf("\nLatency (nanoseconds):\n");
        printf("  Samples:  %zu\n", sorted_lats.size());
        printf("  Average:  %.2f ns (%.2f us)\n", avg, avg / 1000.0);
        printf("  P50:      %lu ns (%.2f us)\n",
               calculate_percentile(sorted_lats, 50), calculate_percentile(sorted_lats, 50) / 1000.0);
        printf("  P90:      %lu ns (%.2f us)\n",
               calculate_percentile(sorted_lats, 90), calculate_percentile(sorted_lats, 90) / 1000.0);
        printf("  P99:      %lu ns (%.2f us)\n",
               calculate_percentile(sorted_lats, 99), calculate_percentile(sorted_lats, 99) / 1000.0);
        printf("  P99.9:    %lu ns (%.2f us)\n",
               calculate_percentile(sorted_lats, 99.9), calculate_percentile(sorted_lats, 99.9) / 1000.0);
    }
    printf("==============================================\n\n");
}

// ============================================================================
// Print Usage
// ============================================================================

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
    printf("  -m <MB>             Memory size in MB (default: 1024)\n");
    printf("  -l                  Measure latency\n");
    printf("  -h                  Show this help message\n");
    printf("\nExample:\n");
    printf("  %s -w workloadc -n 2 -t 5 -req_q 4 -resp_q 4 -req_th 8 -resp_th 8 -worker_th 8\n", prog_name);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    // Default configuration
    const char* workload_name = nullptr;
    int numa_node = 2;
    int duration_sec = 10;
    bool measure_latency = false;
    uint64_t memory_size_mb = 1024;

    uint32_t num_req_queues = 8;
    uint32_t num_resp_queues = 8;
    uint32_t num_req_threads = 8;
    uint32_t num_resp_threads = 8;
    uint32_t num_workers = 8;

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) {
            workload_name = argv[++i];
        } else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
            numa_node = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            duration_sec = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
            memory_size_mb = atoll(argv[++i]);
        } else if (strcmp(argv[i], "-l") == 0) {
            measure_latency = true;
        } else if (strcmp(argv[i], "-req_q") == 0 && i + 1 < argc) {
            num_req_queues = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-resp_q") == 0 && i + 1 < argc) {
            num_resp_queues = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-req_th") == 0 && i + 1 < argc) {
            num_req_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-resp_th") == 0 && i + 1 < argc) {
            num_resp_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-worker_th") == 0 && i + 1 < argc) {
            num_workers = atoi(argv[++i]);
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

    uint64_t memory_size = memory_size_mb * 1024 * 1024;
    uint32_t num_threads = num_req_threads;  // Same as response threads

    printf("==============================================\n");
    printf("CXL SharedKV YCSB Benchmark (Lock-Free)\n");
    printf("==============================================\n");
    printf("Configuration:\n");
    printf("  Workload:         %s\n", workload_name);
    printf("  NUMA node:        %d\n", numa_node);
    printf("  Memory:           %lu MB\n", memory_size_mb);
    printf("  Request queues:   %u\n", num_req_queues);
    printf("  Response queues:  %u\n", num_resp_queues);
    printf("  Request threads:  %u\n", num_req_threads);
    printf("  Response threads: %u\n", num_resp_threads);
    printf("  Worker threads:   %u\n", num_workers);
    printf("  Duration:         %d seconds\n", duration_sec);
    printf("  Measure latency:  %s\n", measure_latency ? "yes" : "no");
    printf("==============================================\n\n");

    // Load workload files
    WorkloadFileNames workload_files = get_workload_files(workload_name);

    printf("[CXL-Benchmark] Loading workload files...\n");
    std::vector<YCSBOperation> load_ops, trans_ops;

    if (load_ycsb_workload(workload_files.load_file.c_str(), load_ops) != 0) {
        fprintf(stderr, "ERROR: Failed to load: %s\n", workload_files.load_file.c_str());
        return 1;
    }

    if (load_ycsb_workload(workload_files.trans_file.c_str(), trans_ops) != 0) {
        fprintf(stderr, "ERROR: Failed to load: %s\n", workload_files.trans_file.c_str());
        return 1;
    }

    printf("[CXL-Benchmark] Loaded %zu load ops, %zu transaction ops\n\n",
           load_ops.size(), trans_ops.size());

    // Allocate memory on specified NUMA node (CXL device)
    printf("[CXL-Benchmark] Allocating %lu MB memory on NUMA node %d...\n",
           memory_size_mb, numa_node);

    void* cxl_base = nullptr;
    if (numa_available() >= 0) {
        cxl_base = numa_alloc_onnode(memory_size, numa_node);
        if (cxl_base) {
            printf("[CXL-Benchmark] Successfully allocated on NUMA node %d\n", numa_node);
        }
    }

    if (!cxl_base) {
        fprintf(stderr, "[CXL-Benchmark] NUMA allocation failed, using regular memory\n");
        cxl_base = aligned_alloc(4096, memory_size);
    }

    if (!cxl_base) {
        fprintf(stderr, "ERROR: Failed to allocate memory\n");
        return 1;
    }
    memset(cxl_base, 0, memory_size);

    // Calculate layout - use resp_queues from CLI (must be >= num_threads for 1:1 mapping)
    uint32_t num_response_rings = num_resp_queues;
    if (num_response_rings < num_threads) {
        fprintf(stderr, "WARNING: num_resp_queues (%u) < num_threads (%u), "
                "setting num_response_rings = num_threads\n",
                num_resp_queues, num_threads);
        num_response_rings = num_threads;
    }
    CXLMemoryLayout layout = CXLMemoryLayout::calculate(
        num_workers, num_response_rings, memory_size
    );

    printf("[CXL-Benchmark] KV data region: %lu MB\n", layout.kv_data_size / (1024 * 1024));

    // Initialize CXL memory
    printf("[CXL-Benchmark] Initializing CXL memory structures...\n");
    cxl_init_memory(cxl_base, memory_size, num_workers, num_response_rings);

    // Debug: Verify response rings are initialized correctly
    fprintf(stderr, "[CXL-Benchmark] Verifying response ring initialization:\n");
    for (uint32_t i = 0; i < num_response_rings; i++) {
        CXLResponseRing* ring = cxl_get_response_ring(cxl_base, layout, i);
        fprintf(stderr, "  resp_ring[%u] at %p: write_idx=%lu, read_idx=%lu, size=%lu, mask=%lu\n",
                i, (void*)ring, ring->write_idx, ring->read_idx, ring->size, ring->mask);
    }

    // Create local request queues (for synchronizer)
    // Same as original architecture: threads share queues via thread_id % num_req_queues
    printf("[CXL-Benchmark] Creating %u local request queues...\n", num_req_queues);
    std::vector<LockFreeQueue<KVRequest*>*> local_req_queues;
    for (uint32_t i = 0; i < num_req_queues; i++) {
        local_req_queues.push_back(new LockFreeQueue<KVRequest*>(8192));
    }

    // Start worker threads
    printf("[CXL-Benchmark] Starting %u worker threads...\n", num_workers);
    fflush(stdout);
    std::atomic<bool> stop_flag{false};
    std::vector<std::thread> worker_threads;

    for (uint32_t i = 0; i < num_workers; i++) {
        worker_threads.emplace_back(
            cxl_worker_run, cxl_base, std::ref(layout), i, 0,
            num_response_rings, std::ref(stop_flag)
        );
    }

    // Start synchronizer thread
    printf("[CXL-Benchmark] Starting synchronizer thread...\n");
    std::atomic<uint64_t> global_sequence{0};
    std::thread synchronizer_thread(
        cxl_synchronizer_run, cxl_base, std::ref(layout),
        std::ref(local_req_queues), std::ref(global_sequence), std::ref(stop_flag)
    );

    // Wait for synchronizer to be fully ready and add memory fence
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::atomic_thread_fence(std::memory_order_seq_cst);

    // Verify synchronizer is running by checking queue state
    fprintf(stderr, "[CXL-Benchmark] Verifying synchronizer is ready...\n");
    for (uint32_t i = 0; i < num_req_queues; i++) {
        auto* q = local_req_queues[i];
        fprintf(stderr, "  req_q[%u] at %p: head=%lu, tail=%lu\n",
                i, (void*)q, q->head.load(std::memory_order_acquire),
                q->tail.load(std::memory_order_acquire));
    }
    fflush(stderr);

    // ========================================================================
    // LOAD PHASE
    // ========================================================================

    printf("\n[CXL-Benchmark] Starting LOAD PHASE...\n");
    fflush(stdout);

    {
        pthread_barrier_t load_barrier;
        pthread_barrier_init(&load_barrier, nullptr, 2 * num_threads);

        volatile bool load_stop = false;
        volatile bool load_resp_can_exit = false;  // New: controls when response threads can exit
        std::vector<CXLBenchmarkContext> load_contexts(num_threads);
        std::vector<SimpleRequestPool*> load_pools;

        uint32_t ops_per_thread = load_ops.size() / num_threads;

        // Pre-compute worker IDs
        std::vector<std::vector<uint32_t>> load_worker_ids(num_threads);
        for (uint32_t c = 0; c < num_threads; c++) {
            uint32_t start_idx = c * ops_per_thread;
            uint32_t count = (c == num_threads - 1) ?
                             (load_ops.size() - c * ops_per_thread) : ops_per_thread;

            load_worker_ids[c].reserve(count);
            for (uint32_t i = start_idx; i < start_idx + count; i++) {
                uint64_t hash = cxl_hash_key(load_ops[i].key.c_str(), load_ops[i].key.length());
                uint32_t bucket_id = hash % CXL_NUM_BUCKETS;
                load_worker_ids[c].push_back(bucket_id % num_workers);
            }
        }

        // Initialize contexts
        for (uint32_t i = 0; i < num_threads; i++) {
            load_pools.push_back(new SimpleRequestPool(4096));

            load_contexts[i].client_id = i;
            load_contexts[i].num_clients = num_threads;
            load_contexts[i].cxl_base = cxl_base;
            load_contexts[i].layout = &layout;
            load_contexts[i].num_workers = num_workers;
            // Same as original architecture: thread_id % num_req_queues
            load_contexts[i].local_req_queue = local_req_queues[i % num_req_queues];
            load_contexts[i].request_pool = load_pools[i];
            load_contexts[i].operations = &load_ops;
            load_contexts[i].ops_start_idx = i * ops_per_thread;
            load_contexts[i].ops_count = (i == num_threads - 1) ?
                                         (load_ops.size() - i * ops_per_thread) : ops_per_thread;
            load_contexts[i].precomputed_worker_ids = &load_worker_ids[i];
            load_contexts[i].start_barrier = &load_barrier;
            load_contexts[i].should_stop = &load_stop;
            load_contexts[i].resp_can_exit = &load_resp_can_exit;
            load_contexts[i].measure_latency = false;
            pthread_mutex_init(&load_contexts[i].latency_mutex, nullptr);
        }

        uint64_t load_start = get_time_usec();

        // Start threads
        for (uint32_t i = 0; i < num_threads; i++) {
            pthread_create(&load_contexts[i].response_thread, nullptr,
                          cxl_response_thread_func, &load_contexts[i]);
            pthread_create(&load_contexts[i].request_thread, nullptr,
                          cxl_request_thread_func, &load_contexts[i]);
        }

        // Wait for all load operations to complete
        uint64_t target_ops = load_ops.size();
        while (true) {
            uint64_t total = 0;
            for (uint32_t i = 0; i < num_threads; i++) {
                total += load_contexts[i].responses_received.load();
                total += load_contexts[i].failed_ops.load();
            }
            if (total >= target_ops) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Phase 1: Stop request threads from submitting new requests
        load_stop = true;

        // Phase 2: Wait for request threads to finish
        for (uint32_t i = 0; i < num_threads; i++) {
            pthread_join(load_contexts[i].request_thread, nullptr);
        }

        // Phase 3: Wait for all pending requests to be processed
        // CRITICAL: Must wait for the entire pipeline to drain:
        //   1. Request queues -> Synchronizer
        //   2. Worker rings -> Workers
        //   3. Response rings -> Response threads
        fprintf(stderr, "[LOAD] Phase 3: Waiting for pipeline to drain...\n");
        for (int wait = 0; wait < 200; wait++) {  // Increased timeout
            bool all_empty = true;
            int blocked_stage = 0;

            // Check request queues
            for (uint32_t i = 0; i < num_req_queues; i++) {
                if (!local_req_queues[i]->is_empty()) {
                    all_empty = false;
                    blocked_stage = 1;
                    break;
                }
            }

            // Check worker rings
            if (all_empty) {
                for (uint32_t i = 0; i < num_workers; i++) {
                    CXLRingBuffer* worker_ring = cxl_get_request_ring(cxl_base, layout, i);
                    if (!worker_ring->is_empty()) {
                        all_empty = false;
                        blocked_stage = 2;
                        break;
                    }
                }
            }

            // Check response rings - check ALL response rings, not just num_threads
            if (all_empty) {
                for (uint32_t i = 0; i < num_response_rings; i++) {
                    CXLResponseRing* resp_ring = cxl_get_response_ring(cxl_base, layout, i);
                    if (!resp_ring->is_empty()) {
                        all_empty = false;
                        blocked_stage = 3;
                        break;
                    }
                }
            }

            if (all_empty) {
                fprintf(stderr, "[LOAD] Pipeline drained after %d iterations\n", wait);
                break;
            }

            // Print diagnostic every 20 iterations (1 second)
            if (wait % 20 == 0 && wait > 0) {
                fprintf(stderr, "[LOAD] Drain wait %d: blocked at stage %d (1=req_q, 2=worker_ring, 3=resp_ring)\n",
                        wait, blocked_stage);
                // Print response ring sizes
                for (uint32_t i = 0; i < num_response_rings; i++) {
                    CXLResponseRing* resp_ring = cxl_get_response_ring(cxl_base, layout, i);
                    uint64_t pending = resp_ring->write_idx - resp_ring->read_idx;
                    fprintf(stderr, "  resp_ring[%u]: pending=%lu\n", i, pending);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        // Extra safety: Give workers time to write final responses
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Now signal response threads that they can exit
        fprintf(stderr, "[LOAD] Signaling response threads to exit...\n");
        load_resp_can_exit = true;
        std::atomic_thread_fence(std::memory_order_seq_cst);

        // Phase 4: Join response threads
        for (uint32_t i = 0; i < num_threads; i++) {
            pthread_join(load_contexts[i].response_thread, nullptr);
        }

        uint64_t load_end = get_time_usec();

        // Collect stats
        uint64_t total_ops = 0, failed_ops = 0;
        for (uint32_t i = 0; i < num_threads; i++) {
            total_ops += load_contexts[i].responses_received.load();
            failed_ops += load_contexts[i].failed_ops.load();
            pthread_mutex_destroy(&load_contexts[i].latency_mutex);
        }
        // Note: pools intentionally not freed (same as async_benchmark.cc)
        // to avoid use-after-free on in-flight requests

        print_benchmark_stats("LOAD PHASE", total_ops, failed_ops, load_end - load_start, nullptr);
        pthread_barrier_destroy(&load_barrier);
    }

    // ========================================================================
    // TRANSACTION PHASE
    // ========================================================================

    printf("[CXL-Benchmark] Starting TRANSACTION PHASE...\n");

    // Debug: Print queue states before TRANSACTION PHASE
    fprintf(stderr, "[CXL-Benchmark] Queue states before TRANSACTION PHASE:\n");
    for (uint32_t i = 0; i < num_req_queues; i++) {
        auto* q = local_req_queues[i];
        fprintf(stderr, "  req_q[%u]: head=%lu, tail=%lu, is_empty=%d\n",
                i, q->head.load(), q->tail.load(), q->is_empty());
    }
    fflush(stderr);

    // CRITICAL: Drain any leftover responses from LOAD PHASE (same as async_benchmark.cc)
    fprintf(stderr, "[CXL-Benchmark] Draining leftover responses from LOAD PHASE...\n");
    for (uint32_t i = 0; i < num_response_rings; i++) {
        CXLResponseRing* resp_ring = cxl_get_response_ring(cxl_base, layout, i);
        fprintf(stderr, "  resp_ring[%u] before drain: write_idx=%lu, read_idx=%lu\n",
                i, resp_ring->write_idx, resp_ring->read_idx);
        CXLResponse resp;
        int drained = 0;
        while (resp_ring->dequeue(resp)) {
            drained++;
        }
        fprintf(stderr, "  resp_ring[%u] after drain: write_idx=%lu, read_idx=%lu, drained=%d\n",
                i, resp_ring->write_idx, resp_ring->read_idx, drained);
    }

    {
        pthread_barrier_t trans_barrier;
        pthread_barrier_init(&trans_barrier, nullptr, 2 * num_threads);

        volatile bool trans_stop = false;
        volatile bool trans_resp_can_exit = false;  // Controls when response threads can exit
        std::vector<CXLBenchmarkContext> trans_contexts(num_threads);
        std::vector<SimpleRequestPool*> trans_pools;

        uint32_t ops_per_thread = trans_ops.size() / num_threads;

        // Pre-compute worker IDs
        std::vector<std::vector<uint32_t>> trans_worker_ids(num_threads);
        for (uint32_t c = 0; c < num_threads; c++) {
            uint32_t start_idx = c * ops_per_thread;
            uint32_t count = (c == num_threads - 1) ?
                             (trans_ops.size() - c * ops_per_thread) : ops_per_thread;

            trans_worker_ids[c].reserve(count);
            for (uint32_t i = start_idx; i < start_idx + count; i++) {
                uint64_t hash = cxl_hash_key(trans_ops[i].key.c_str(), trans_ops[i].key.length());
                uint32_t bucket_id = hash % CXL_NUM_BUCKETS;
                trans_worker_ids[c].push_back(bucket_id % num_workers);
            }
        }

        // Initialize contexts
        for (uint32_t i = 0; i < num_threads; i++) {
            trans_pools.push_back(new SimpleRequestPool(4096));

            trans_contexts[i].client_id = i;
            trans_contexts[i].num_clients = num_threads;
            trans_contexts[i].cxl_base = cxl_base;
            trans_contexts[i].layout = &layout;
            trans_contexts[i].num_workers = num_workers;
            // Same as original architecture: thread_id % num_req_queues
            trans_contexts[i].local_req_queue = local_req_queues[i % num_req_queues];
            trans_contexts[i].request_pool = trans_pools[i];
            trans_contexts[i].operations = &trans_ops;
            trans_contexts[i].ops_start_idx = i * ops_per_thread;
            trans_contexts[i].ops_count = (i == num_threads - 1) ?
                                          (trans_ops.size() - i * ops_per_thread) : ops_per_thread;
            trans_contexts[i].precomputed_worker_ids = &trans_worker_ids[i];
            trans_contexts[i].start_barrier = &trans_barrier;
            trans_contexts[i].should_stop = &trans_stop;
            trans_contexts[i].resp_can_exit = &trans_resp_can_exit;
            trans_contexts[i].measure_latency = measure_latency;
            pthread_mutex_init(&trans_contexts[i].latency_mutex, nullptr);

            if (measure_latency) {
                trans_contexts[i].latencies.reserve(100000);
            }
        }

        uint64_t trans_start = get_time_usec();

        // Start threads
        for (uint32_t i = 0; i < num_threads; i++) {
            pthread_create(&trans_contexts[i].response_thread, nullptr,
                          cxl_response_thread_func, &trans_contexts[i]);
            pthread_create(&trans_contexts[i].request_thread, nullptr,
                          cxl_request_thread_func, &trans_contexts[i]);
        }

        // Run for duration
        sleep(duration_sec);

        // Phase 1: Stop request threads from submitting new requests
        trans_stop = true;

        // Phase 2: Wait for request threads to finish
        for (uint32_t i = 0; i < num_threads; i++) {
            pthread_join(trans_contexts[i].request_thread, nullptr);
        }

        // Phase 3: Wait for all pending requests to be processed
        fprintf(stderr, "[TRANS] Phase 3: Waiting for pipeline to drain...\n");
        for (int wait = 0; wait < 200; wait++) {  // Increased timeout
            bool all_empty = true;
            int blocked_stage = 0;

            // Check request queues
            for (uint32_t i = 0; i < num_req_queues; i++) {
                if (!local_req_queues[i]->is_empty()) {
                    all_empty = false;
                    blocked_stage = 1;
                    break;
                }
            }

            // Check worker rings
            if (all_empty) {
                for (uint32_t i = 0; i < num_workers; i++) {
                    CXLRingBuffer* worker_ring = cxl_get_request_ring(cxl_base, layout, i);
                    if (!worker_ring->is_empty()) {
                        all_empty = false;
                        blocked_stage = 2;
                        break;
                    }
                }
            }

            // Check response rings - check ALL response rings, not just num_threads
            if (all_empty) {
                for (uint32_t i = 0; i < num_response_rings; i++) {
                    CXLResponseRing* resp_ring = cxl_get_response_ring(cxl_base, layout, i);
                    if (!resp_ring->is_empty()) {
                        all_empty = false;
                        blocked_stage = 3;
                        break;
                    }
                }
            }

            if (all_empty) {
                fprintf(stderr, "[TRANS] Pipeline drained after %d iterations\n", wait);
                break;
            }

            // Print diagnostic every 20 iterations (1 second)
            if (wait % 20 == 0 && wait > 0) {
                fprintf(stderr, "[TRANS] Drain wait %d: blocked at stage %d (1=req_q, 2=worker_ring, 3=resp_ring)\n",
                        wait, blocked_stage);
                // Print response ring sizes
                for (uint32_t i = 0; i < num_response_rings; i++) {
                    CXLResponseRing* resp_ring = cxl_get_response_ring(cxl_base, layout, i);
                    uint64_t pending = resp_ring->write_idx - resp_ring->read_idx;
                    fprintf(stderr, "  resp_ring[%u]: pending=%lu\n", i, pending);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        // Extra safety: Give workers time to write final responses
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Now signal response threads that they can exit
        fprintf(stderr, "[TRANS] Signaling response threads to exit...\n");
        trans_resp_can_exit = true;
        std::atomic_thread_fence(std::memory_order_seq_cst);

        // Phase 4: Join response threads
        for (uint32_t i = 0; i < num_threads; i++) {
            pthread_join(trans_contexts[i].response_thread, nullptr);
        }

        uint64_t trans_end = get_time_usec();

        // Collect stats
        uint64_t total_ops = 0, failed_ops = 0;
        std::vector<uint64_t> all_latencies;

        for (uint32_t i = 0; i < num_threads; i++) {
            total_ops += trans_contexts[i].responses_received.load();
            failed_ops += trans_contexts[i].failed_ops.load();

            if (measure_latency) {
                pthread_mutex_lock(&trans_contexts[i].latency_mutex);
                all_latencies.insert(all_latencies.end(),
                                    trans_contexts[i].latencies.begin(),
                                    trans_contexts[i].latencies.end());
                pthread_mutex_unlock(&trans_contexts[i].latency_mutex);
            }
            pthread_mutex_destroy(&trans_contexts[i].latency_mutex);
        }
        // Note: pools intentionally not freed (same as async_benchmark.cc)
        // to avoid use-after-free on in-flight requests

        print_benchmark_stats("TRANSACTION PHASE", total_ops, failed_ops,
                             trans_end - trans_start,
                             measure_latency ? &all_latencies : nullptr);

        pthread_barrier_destroy(&trans_barrier);
    }

    // Cleanup
    printf("[CXL-Benchmark] Stopping workers and synchronizer...\n");
    stop_flag.store(true, std::memory_order_release);

    synchronizer_thread.join();

    for (auto& t : worker_threads) {
        t.join();
    }

    // Cleanup local request queues
    for (auto* q : local_req_queues) {
        delete q;
    }

    // Free CXL memory
    if (numa_available() >= 0) {
        numa_free(cxl_base, memory_size);
    } else {
        free(cxl_base);
    }

    printf("[CXL-Benchmark] Complete!\n");
    return 0;
}
