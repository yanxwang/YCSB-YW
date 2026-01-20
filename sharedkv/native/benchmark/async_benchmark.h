#ifndef SHAREDKV_ASYNC_BENCHMARK_H_
#define SHAREDKV_ASYNC_BENCHMARK_H_

#include <pthread.h>
#include <atomic>
#include <vector>
#include <semaphore>

#include "ycsb_benchmark.h"
#include "shared_kv.h"
#include "kv_request.h"

// ============================================================================
// Lock-free Request Object Pool (avoids heap allocation per request)
// ============================================================================

struct RequestPool {
    KVRequest* requests;        // Pre-allocated request array
    uint32_t* free_indices;     // Lock-free stack of free indices
    std::atomic<uint32_t> free_top;  // Top of free stack (atomic for lock-free)
    uint32_t capacity;

    RequestPool(uint32_t cap) : capacity(cap) {
        requests = new KVRequest[cap];
        free_indices = new uint32_t[cap];
        // Initialize all requests (critical: value_data must be nullptr)
        for (uint32_t i = 0; i < cap; i++) {
            requests[i].value_data = nullptr;
            requests[i].value_len = 0;
            requests[i].recycle_func = nullptr;
            requests[i].recycle_ctx = nullptr;
            free_indices[i] = cap - 1 - i;  // Stack: top has lower indices
        }
        free_top.store(cap, std::memory_order_relaxed);
    }

    ~RequestPool() {
        // Cleanup any value_data that might still be allocated
        for (uint32_t i = 0; i < capacity; i++) {
            if (requests[i].value_data) {
                delete[] requests[i].value_data;
                requests[i].value_data = nullptr;
            }
        }
        delete[] requests;
        delete[] free_indices;
    }

    // Allocate a request from pool (returns nullptr if pool exhausted)
    KVRequest* alloc() {
        uint32_t top = free_top.load(std::memory_order_relaxed);
        while (top > 0) {
            if (free_top.compare_exchange_weak(top, top - 1,
                                               std::memory_order_acquire,
                                               std::memory_order_relaxed)) {
                return &requests[free_indices[top - 1]];
            }
            // top is updated by compare_exchange_weak on failure
        }
        return nullptr;  // Pool exhausted
    }

    // Return request to pool
    void free(KVRequest* req) {
        uint32_t idx = req - requests;  // Calculate index
        uint32_t top = free_top.load(std::memory_order_relaxed);
        while (true) {
            free_indices[top] = idx;
            if (free_top.compare_exchange_weak(top, top + 1,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
                return;
            }
        }
    }

    // Check how many are available
    uint32_t available() const {
        return free_top.load(std::memory_order_relaxed);
    }
};

// ============================================================================
// Async Benchmark Context (per client pair)
// ============================================================================

struct AsyncBenchmarkContext {
    // Thread handles
    pthread_t request_thread;
    pthread_t response_thread;

    // Thread IDs
    uint32_t client_id;
    uint32_t num_clients;

    // CPU assignment
    int request_cpu;
    int response_cpu;

    // SharedKV context
    SharedKVContext* kv_ctx;

    // Workload
    std::vector<YCSBOperation>* operations;
    uint32_t ops_start_idx;
    uint32_t ops_count;

    // Flow control (limit in-flight requests) - now managed by ClientChannel
    uint32_t max_in_flight;  // Legacy field, actual limit is in SharedKVContext

    // Object pool for requests (shared between request and response threads)
    RequestPool* request_pool;

    // Pre-computed data for fast submission
    std::vector<uint32_t>* precomputed_worker_ids;

    // UINTR for response thread
    int uintr_fd;
    std::atomic<bool> uintr_fd_ready{false};

    // Statistics
    std::atomic<uint64_t> requests_submitted{0};
    std::atomic<uint64_t> responses_received{0};
    std::atomic<uint64_t> failed_ops{0};

    // Latency measurement
    bool measure_latency;
    std::vector<uint64_t> latencies;
    pthread_mutex_t latency_mutex;

    // Control
    volatile bool* should_stop;

    // Synchronization
    pthread_barrier_t* start_barrier;
};

// ============================================================================
// Thread Functions
// ============================================================================

// Request thread: continuously generates and submits requests
void* async_request_thread_func(void* arg);

// Response thread: waits on UINTR and processes responses
void* async_response_thread_func(void* arg);

// ============================================================================
// Async Benchmark Runner
// ============================================================================

// Run async benchmark for transaction phase
void run_async_transaction_phase(
    SharedKVContext* kv_ctx,
    std::vector<YCSBOperation>& operations,
    uint32_t num_clients,
    uint32_t duration_sec,
    int cpu_start,
    bool measure_latency,
    uint64_t* total_ops_out,
    uint64_t* failed_ops_out,
    std::vector<uint64_t>* all_latencies_out
);

#endif  // SHAREDKV_ASYNC_BENCHMARK_H_
