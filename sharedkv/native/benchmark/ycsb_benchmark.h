#ifndef SHAREDKV_YCSB_BENCHMARK_H_
#define SHAREDKV_YCSB_BENCHMARK_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <atomic>
#include <vector>
#include <string>

#include "shared_kv.h"
#include "kv_request.h"

// ============================================================================
// YCSB Operation Types
// ============================================================================

enum class YCSBOpType : uint8_t {
    INSERT = 0,
    READ = 1,
    UPDATE = 2,
    SCAN = 3,
    READ_MODIFY_WRITE = 4,
    DELETE = 5
};

// ============================================================================
// YCSB Operation Record (parsed from workload file)
// ============================================================================

struct YCSBOperation {
    YCSBOpType op_type;
    std::string key;
    std::string value;  // For INSERT/UPDATE operations

    YCSBOperation() : op_type(YCSBOpType::READ) {}
};

// ============================================================================
// Workload File Names
// ============================================================================

struct WorkloadFileNames {
    std::string load_file;   // e.g., "workloads/workloada_load.txt"
    std::string trans_file;  // e.g., "workloads/workloada_trans.txt"
};

// ============================================================================
// Benchmark Thread Arguments
// ============================================================================

struct BenchmarkThreadArgs {
    // Thread configuration
    int thread_id;
    int num_threads;
    int cpu_id;  // CPU to pin this thread to

    // SharedKV context
    SharedKVContext* ctx;
    uint32_t client_id;

    // Workload
    std::vector<YCSBOperation>* operations;
    uint32_t ops_start_idx;
    uint32_t ops_count;

    // Synchronization
    pthread_barrier_t* start_barrier;
    volatile bool* should_stop;

    // Results (output)
    std::atomic<uint64_t>* total_ops;
    std::atomic<uint64_t>* failed_ops;

    // Latency measurement
    bool measure_latency;
    std::vector<uint64_t>* latencies;  // Latency in nanoseconds
};

// ============================================================================
// Workload Loading Functions
// ============================================================================

// Load workload from file (YCSB format)
int load_ycsb_workload(const char* filename, std::vector<YCSBOperation>& operations);

// Generate YCSB-style value (10 fields, 100 bytes each = 1KB)
std::string generate_ycsb_value(int num_fields = 10, int field_size = 100);

// Parse workload file names from workload name
WorkloadFileNames get_workload_files(const char* workload_name);

// ============================================================================
// Benchmark Execution Functions
// ============================================================================

// Worker thread function for throughput test
void* benchmark_throughput_worker(void* arg);

// Worker thread function for latency test
void* benchmark_latency_worker(void* arg);

// Execute single operation
int execute_operation(SharedKVContext* ctx, uint32_t client_id,
                     const YCSBOperation& op, uint64_t* latency_ns = nullptr);

// ============================================================================
// Utility Functions
// ============================================================================

// Get current time in microseconds
uint64_t get_time_usec();

// Get current time in nanoseconds
uint64_t get_time_nsec();

// Calculate percentile from sorted latency array
uint64_t calculate_percentile(const std::vector<uint64_t>& latencies, double percentile);

// Print benchmark statistics
void print_benchmark_stats(const char* phase_name,
                          uint64_t total_ops,
                          uint64_t failed_ops,
                          uint64_t duration_usec,
                          const std::vector<uint64_t>* latencies = nullptr);

#endif  // SHAREDKV_YCSB_BENCHMARK_H_
