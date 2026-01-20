#include "ycsb_benchmark.h"
#include "uintr_threading.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>

// ============================================================================
// Utility Functions
// ============================================================================

uint64_t get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

uint64_t get_time_nsec() {
    auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        now.time_since_epoch()).count();
}

std::string generate_ycsb_value(int num_fields, int field_size) {
    std::string value;
    value.reserve(num_fields * (field_size + 10));  // field{N}={data}

    for (int i = 0; i < num_fields; i++) {
        value += "field" + std::to_string(i) + "=";
        // Generate random data (use simple pattern for now)
        for (int j = 0; j < field_size; j++) {
            value += char('a' + (i + j) % 26);
        }
        if (i < num_fields - 1) {
            value += ",";
        }
    }
    return value;
}

uint64_t calculate_percentile(const std::vector<uint64_t>& latencies, double percentile) {
    if (latencies.empty()) return 0;

    std::vector<uint64_t> sorted = latencies;
    std::sort(sorted.begin(), sorted.end());

    size_t index = (size_t)(percentile / 100.0 * sorted.size());
    if (index >= sorted.size()) index = sorted.size() - 1;

    return sorted[index];
}

void print_benchmark_stats(const char* phase_name,
                          uint64_t total_ops,
                          uint64_t failed_ops,
                          uint64_t duration_usec,
                          const std::vector<uint64_t>* latencies) {
    uint64_t successful_ops = total_ops - failed_ops;
    double throughput = (double)successful_ops / (duration_usec / 1000000.0);

    printf("==============================================\n");
    printf("%s Results:\n", phase_name);
    printf("==============================================\n");
    printf("Total operations:      %lu\n", total_ops);
    printf("Successful operations: %lu\n", successful_ops);
    printf("Failed operations:     %lu\n", failed_ops);
    printf("Duration:              %.2f seconds\n", duration_usec / 1000000.0);
    printf("Throughput:            %.2f ops/sec\n", throughput);

    if (latencies && !latencies->empty()) {
        uint64_t avg_lat = 0;
        for (uint64_t lat : *latencies) {
            avg_lat += lat;
        }
        avg_lat /= latencies->size();

        uint64_t p50 = calculate_percentile(*latencies, 50);
        uint64_t p95 = calculate_percentile(*latencies, 95);
        uint64_t p99 = calculate_percentile(*latencies, 99);
        uint64_t p999 = calculate_percentile(*latencies, 99.9);

        printf("\nLatency Statistics (microseconds):\n");
        printf("  Average:  %.2f us\n", avg_lat / 1000.0);
        printf("  Median:   %.2f us\n", p50 / 1000.0);
        printf("  95th:     %.2f us\n", p95 / 1000.0);
        printf("  99th:     %.2f us\n", p99 / 1000.0);
        printf("  99.9th:   %.2f us\n", p999 / 1000.0);
    }
    printf("==============================================\n\n");
}

// ============================================================================
// Workload Loading
// ============================================================================

WorkloadFileNames get_workload_files(const char* workload_name) {
    WorkloadFileNames files;
    files.load_file = std::string("workloads/") + workload_name + "_load.txt";
    files.trans_file = std::string("workloads/") + workload_name + "_trans.txt";
    return files;
}

int load_ycsb_workload(const char* filename, std::vector<YCSBOperation>& operations) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        fprintf(stderr, "[Benchmark] ERROR: Cannot open workload file: %s\n", filename);
        return -1;
    }

    std::string line;
    int line_num = 0;

    while (std::getline(file, line)) {
        line_num++;
        if (line.empty() || line[0] == '#') continue;  // Skip empty lines and comments

        YCSBOperation op;
        std::istringstream iss(line);
        std::string op_type_str;

        // Parse: <OP_TYPE> <KEY> [<VALUE>]
        iss >> op_type_str >> op.key;

        if (op_type_str == "INSERT") {
            op.op_type = YCSBOpType::INSERT;
            // Read rest of line as value
            std::getline(iss, op.value);
            // Trim leading whitespace
            op.value.erase(0, op.value.find_first_not_of(" \t"));
            if (op.value.empty()) {
                op.value = generate_ycsb_value();  // Generate if not provided
            }
        } else if (op_type_str == "READ") {
            op.op_type = YCSBOpType::READ;
        } else if (op_type_str == "UPDATE") {
            op.op_type = YCSBOpType::UPDATE;
            std::getline(iss, op.value);
            op.value.erase(0, op.value.find_first_not_of(" \t"));
            if (op.value.empty()) {
                op.value = generate_ycsb_value();
            }
        } else if (op_type_str == "DELETE") {
            op.op_type = YCSBOpType::DELETE;
        } else {
            fprintf(stderr, "[Benchmark] WARNING: Unknown operation type '%s' at line %d\n",
                    op_type_str.c_str(), line_num);
            continue;
        }

        operations.push_back(op);
    }

    file.close();
    printf("[Benchmark] Loaded %zu operations from %s\n", operations.size(), filename);
    return 0;
}

// ============================================================================
// Operation Execution
// ============================================================================

int execute_operation(SharedKVContext* ctx, uint32_t client_id,
                     const YCSBOperation& op, uint64_t* latency_ns) {
    uint64_t start_ns = 0;
    if (latency_ns) {
        start_ns = get_time_nsec();
    }

    KVRequest* req = new KVRequest();
    req->client_id = client_id;
    req->resp_q_ptr = ctx->clients[client_id]->resp_q;
    req->timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
    req->recycle_func = nullptr;  // CRITICAL: Initialize to nullptr for heap-allocated requests
    req->recycle_ctx = nullptr;

    // Set operation type and data
    switch (op.op_type) {
        case YCSBOpType::INSERT:
            req->op_type = KVOpType::INSERT;
            req->set_key(op.key);
            req->set_value(op.value);
            break;

        case YCSBOpType::READ:
            req->op_type = KVOpType::READ;
            req->set_key(op.key);
            break;

        case YCSBOpType::UPDATE:
            req->op_type = KVOpType::UPDATE;
            req->set_key(op.key);
            req->set_value(op.value);
            break;

        case YCSBOpType::DELETE:
            req->op_type = KVOpType::DELETE;
            req->set_key(op.key);
            break;

        default:
            fprintf(stderr, "[Benchmark] ERROR: Unsupported operation type\n");
            delete req;
            return -1;
    }

    // Pre-compute target worker ID
    uint64_t hash = std::hash<std::string>{}(op.key);
    uint32_t bucket_id = hash % NUM_BUCKETS;
    req->target_worker_id = bucket_id % ctx->num_workers;

    // Record timestamp for latency measurement
    req->timestamp = get_time_nsec();

    // Submit request (non-blocking, busy-spin if queue full)
    while (!ctx->submit_request(client_id, req)) {
        std::this_thread::yield();
    }

    // Synchronous Mode: Wait for response
    // (In Async Mode, Response Thread handles this)
    KVResponse resp;
    if (!ctx->get_response(client_id, resp, 5000)) {
        return -1;
    }

    if (latency_ns) {
        uint64_t end_ns = get_time_nsec();
        *latency_ns = end_ns - start_ns;
    }

    // Check response status
    if (resp.status == KVStatus::SUCCESS) {
        return 0;
    } else {
        return -1;
    }
}

// ============================================================================
// Benchmark Worker Threads
// ============================================================================

void* benchmark_throughput_worker(void* arg) {
    BenchmarkThreadArgs* args = (BenchmarkThreadArgs*)arg;

    // Pin to CPU
    pin_current_thread_to_cpu(args->cpu_id);
    printf("[Benchmark] Thread %d pinned to CPU %d, client_id=%u\n",
           args->thread_id, args->cpu_id, args->client_id);

    // Wait for all threads to be ready
    pthread_barrier_wait(args->start_barrier);

    uint64_t local_ops = 0;
    uint64_t local_failed = 0;

    // Execute operations (Synchronous Mode: submit + wait for each response)
    uint32_t op_idx = args->ops_start_idx;
    uint32_t ops_end = args->ops_start_idx + args->ops_count;

    while (!*(args->should_stop)) {
        const YCSBOperation& op = (*args->operations)[op_idx];
        int ret = execute_operation(args->ctx, args->client_id, op, nullptr);

        if (ret == 0) {
            local_ops++;
        } else {
            local_failed++;
        }

        // Move to next operation (wrap around)
        op_idx++;
        if (op_idx >= ops_end) {
            op_idx = args->ops_start_idx;
        }
    }

    // Update global counters
    args->total_ops->fetch_add(local_ops, std::memory_order_relaxed);
    args->failed_ops->fetch_add(local_failed, std::memory_order_relaxed);

    printf("[Benchmark] Thread %d completed: %lu ops (%lu failed)\n",
           args->thread_id, local_ops, local_failed);

    return nullptr;
}

void* benchmark_latency_worker(void* arg) {
    BenchmarkThreadArgs* args = (BenchmarkThreadArgs*)arg;

    // Pin to CPU
    pin_current_thread_to_cpu(args->cpu_id);
    printf("[Benchmark] Thread %d pinned to CPU %d, client_id=%u\n",
           args->thread_id, args->cpu_id, args->client_id);

    // Wait for all threads to be ready
    pthread_barrier_wait(args->start_barrier);

    uint64_t local_ops = 0;
    uint64_t local_failed = 0;
    std::vector<uint64_t> local_latencies;
    local_latencies.reserve(args->ops_count);

    // Execute operations (fixed count for latency measurement)
    for (uint32_t i = 0; i < args->ops_count; i++) {
        uint32_t op_idx = args->ops_start_idx + i;
        const YCSBOperation& op = (*args->operations)[op_idx];

        uint64_t latency_ns = 0;
        int ret = execute_operation(args->ctx, args->client_id, op, &latency_ns);

        if (ret == 0) {
            local_ops++;
            local_latencies.push_back(latency_ns);
        } else {
            local_failed++;
        }
    }

    // Update global counters
    args->total_ops->fetch_add(local_ops, std::memory_order_relaxed);
    args->failed_ops->fetch_add(local_failed, std::memory_order_relaxed);

    // Store latencies
    if (args->latencies) {
        *(args->latencies) = std::move(local_latencies);
    }

    printf("[Benchmark] Thread %d completed: %lu ops (%lu failed)\n",
           args->thread_id, local_ops, local_failed);

    return nullptr;
}
