// cxl_synchronizer.cpp
// CXL-aware synchronizer thread implementation
// Routes requests from request queues to appropriate worker ring buffers

#include "cxl_shared.h"
#include "uintr_threading.h"
#include "kv_request.h"
#include <cstdio>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <functional>

// ============================================================================
// Hash function (same as workers use)
// ============================================================================

static uint64_t sync_hash_key(const char* key, uint32_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (uint32_t i = 0; i < len; i++) {
        hash ^= static_cast<uint64_t>(static_cast<unsigned char>(key[i]));
        hash *= 1099511628211ULL;
    }
    return hash;
}

// ============================================================================
// CXL Synchronizer Context
// ============================================================================

struct CXLSynchronizerContext {
    // CXL memory
    void* cxl_base;
    CXLMemoryLayout layout;

    // CXL structures
    CXLSharedHashTable* hash_table;
    CXLWorkerRegistry* registry;

    // Local request queues (from clients on this host)
    std::vector<LockFreeQueue<KVRequest*>*> local_req_queues;

    // Global sequence counter
    std::atomic<uint64_t>* global_sequence;

    // Stop flag
    std::atomic<bool>* stop_flag;

    // Statistics
    uint64_t total_dequeued;
    uint64_t total_forwarded;
    uint64_t total_drops;
    uint64_t empty_polls;
    uint64_t ring_full_waits;
};

// ============================================================================
// Initialize Synchronizer Context
// ============================================================================

void cxl_synchronizer_init(CXLSynchronizerContext* ctx,
                           void* cxl_base,
                           const CXLMemoryLayout& layout,
                           std::vector<LockFreeQueue<KVRequest*>*>& local_req_queues,
                           std::atomic<uint64_t>* global_sequence,
                           std::atomic<bool>* stop_flag) {
    ctx->cxl_base = cxl_base;
    ctx->layout = layout;
    ctx->local_req_queues = local_req_queues;
    ctx->global_sequence = global_sequence;
    ctx->stop_flag = stop_flag;

    ctx->hash_table = cxl_get_hash_table(cxl_base, layout);
    ctx->registry = cxl_get_worker_registry(cxl_base, layout);

    ctx->total_dequeued = 0;
    ctx->total_forwarded = 0;
    ctx->total_drops = 0;
    ctx->empty_polls = 0;
    ctx->ring_full_waits = 0;

    fprintf(stderr, "[CXL-Synchronizer] Initialized with %zu local request queues\n",
            local_req_queues.size());
    fprintf(stderr, "[CXL-Synchronizer] %u workers, %u buckets\n",
            ctx->registry->num_workers, ctx->registry->num_buckets);
}

// ============================================================================
// Synchronizer Thread Main Loop
// ============================================================================

void cxl_synchronizer_thread_func(CXLSynchronizerContext* ctx) {
    fprintf(stderr, "[CXL-Synchronizer] Started\n");

    size_t num_queues = ctx->local_req_queues.size();
    size_t queue_idx = 0;
    uint64_t last_report = 0;
    auto last_time = std::chrono::steady_clock::now();
    uint64_t poll_count = 0;

    // Same as original kv_synchronizer: print queue pointers for debugging
    for (size_t q = 0; q < num_queues; q++) {
        auto* queue = ctx->local_req_queues[q];
        fprintf(stderr, "[CXL-Synchronizer] local_req_queues[%zu]=%p, head=%lu, tail=%lu\n",
                q, (void*)queue, queue->head.load(), queue->tail.load());
    }
    fflush(stderr);

    // Ensure synchronizer is fully running before request threads start
    fprintf(stderr, "[CXL-Synchronizer] Entering main loop\n");
    fflush(stderr);

    while (!ctx->stop_flag->load(std::memory_order_relaxed)) {
        bool found_any = false;
        poll_count++;

        // Same as original kv_synchronizer: time-based status log (every 2 seconds)
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_time).count() >= 2) {
            fprintf(stderr, "[CXL-Synchronizer] alive: polls=%lu, dequeued=%lu, forwarded=%lu, drops=%lu\n",
                    poll_count, ctx->total_dequeued, ctx->total_forwarded, ctx->total_drops);
            // Print queue states
            for (size_t q = 0; q < num_queues; q++) {
                auto* queue = ctx->local_req_queues[q];
                fprintf(stderr, "[CXL-Synchronizer] req_q[%zu]: head=%lu, tail=%lu, size=%zu\n",
                        q, queue->head.load(), queue->tail.load(), queue->get_size());
            }
            fflush(stderr);
            last_time = now;
        }

        // Round-robin poll all local request queues
        for (size_t i = 0; i < num_queues; i++) {
            KVRequest* req = nullptr;
            auto* queue = ctx->local_req_queues[queue_idx];

            // Debug: check queue size early
            if (poll_count <= 5) {
                fprintf(stderr, "[CXL-Synchronizer] poll %lu: queue[%zu] head=%lu, tail=%lu, is_empty=%d\n",
                        poll_count, queue_idx, queue->head.load(), queue->tail.load(), queue->is_empty());
                fflush(stderr);
            }

            if (queue->dequeue(req)) {
                found_any = true;
                ctx->total_dequeued++;

                // Assign global sequence number
                uint64_t seq = ctx->global_sequence->fetch_add(1, std::memory_order_relaxed);

                // Compute target worker based on key hash
                uint64_t hash = sync_hash_key(req->key_data, req->key_len);
                uint32_t bucket_id = hash % ctx->registry->num_buckets;
                uint32_t worker_id = ctx->registry->get_worker_for_bucket(bucket_id);

                // Convert KVRequest to CXLRequest
                CXLRequest cxl_req;
                cxl_req.sequence = seq;
                cxl_req.op_type = static_cast<CXLOpType>(static_cast<uint8_t>(req->op_type));
                cxl_req.client_id = req->client_id;
                cxl_req.resp_ring_id = req->client_id % CXL_MAX_CLIENTS;  // Map to response ring
                cxl_req.timestamp = req->timestamp;
                cxl_req.set_key(req->key_data, req->key_len);

                if (req->op_type == KVOpType::INSERT || req->op_type == KVOpType::UPDATE) {
                    if (req->value_data && req->value_len > 0) {
                        cxl_req.set_value(req->value_data, req->value_len);
                    } else {
                        cxl_req.value_len = 0;
                    }
                } else {
                    cxl_req.value_len = 0;
                }

                // Get target worker's request ring buffer
                CXLRingBuffer* worker_ring = cxl_get_request_ring(
                    ctx->cxl_base, ctx->layout, worker_id
                );

                // Enqueue to worker's ring buffer
                // Busy-wait if ring is full (back-pressure)
                while (!worker_ring->enqueue(cxl_req)) {
                    ctx->ring_full_waits++;
                    if (ctx->stop_flag->load(std::memory_order_relaxed)) {
                        // Shutdown - drop request
                        ctx->total_drops++;
                        goto cleanup_req;
                    }
                    std::this_thread::yield();
                }

                ctx->total_forwarded++;

            cleanup_req:
                // Recycle the original request
                req->recycle();

                // Periodic status output
                if (ctx->total_dequeued - last_report >= 500000) {
                    auto now = std::chrono::steady_clock::now();
                    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - last_time
                    ).count();

                    if (elapsed > 0) {
                        double rate = (ctx->total_dequeued - last_report) * 1000.0 / elapsed;
                        fprintf(stderr, "[CXL-Synchronizer] dequeued=%lu, forwarded=%lu, "
                                "drops=%lu (%.1f Kops/s)\n",
                                ctx->total_dequeued, ctx->total_forwarded,
                                ctx->total_drops, rate / 1000.0);
                    }

                    last_report = ctx->total_dequeued;
                    last_time = now;
                }
            }

            queue_idx = (queue_idx + 1) % num_queues;
        }

        if (!found_any) {
            ctx->empty_polls++;
            // Same as original kv_synchronizer: no yield, just count empty polls
            // Yielding here can cause delays in processing
        }
    }

    fprintf(stderr, "[CXL-Synchronizer] Stopped: dequeued=%lu, forwarded=%lu, drops=%lu\n",
            ctx->total_dequeued, ctx->total_forwarded, ctx->total_drops);
    fprintf(stderr, "[CXL-Synchronizer] empty_polls=%lu, ring_full_waits=%lu\n",
            ctx->empty_polls, ctx->ring_full_waits);
}

// ============================================================================
// Wrapper for std::thread
// ============================================================================

void cxl_synchronizer_run(void* cxl_base,
                          const CXLMemoryLayout& layout,
                          std::vector<LockFreeQueue<KVRequest*>*>& local_req_queues,
                          std::atomic<uint64_t>& global_sequence,
                          std::atomic<bool>& stop_flag) {
    CXLSynchronizerContext ctx;
    cxl_synchronizer_init(&ctx, cxl_base, layout, local_req_queues,
                          &global_sequence, &stop_flag);
    cxl_synchronizer_thread_func(&ctx);
}

// ============================================================================
// Alternative: Direct CXL Request Submission (no local queue)
// For hosts that want to submit directly to worker rings
// ============================================================================

bool cxl_submit_request_direct(void* cxl_base,
                               const CXLMemoryLayout& layout,
                               const CXLRequest& req) {
    CXLWorkerRegistry* registry = cxl_get_worker_registry(cxl_base, layout);

    // Compute target worker
    uint64_t hash = sync_hash_key(req.key_data, req.key_len);
    uint32_t bucket_id = hash % registry->num_buckets;
    uint32_t worker_id = registry->get_worker_for_bucket(bucket_id);

    // Get worker's ring buffer
    CXLRingBuffer* worker_ring = cxl_get_request_ring(cxl_base, layout, worker_id);

    // Try to enqueue (non-blocking)
    return worker_ring->enqueue(req);
}

// Blocking version with timeout
bool cxl_submit_request_blocking(void* cxl_base,
                                 const CXLMemoryLayout& layout,
                                 const CXLRequest& req,
                                 uint64_t timeout_us) {
    CXLWorkerRegistry* registry = cxl_get_worker_registry(cxl_base, layout);

    uint64_t hash = sync_hash_key(req.key_data, req.key_len);
    uint32_t bucket_id = hash % registry->num_buckets;
    uint32_t worker_id = registry->get_worker_for_bucket(bucket_id);

    CXLRingBuffer* worker_ring = cxl_get_request_ring(cxl_base, layout, worker_id);

    auto start = std::chrono::steady_clock::now();

    while (!worker_ring->enqueue(req)) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            now - start
        ).count();

        if (static_cast<uint64_t>(elapsed) >= timeout_us) {
            return false;  // Timeout
        }

        std::this_thread::yield();
    }

    return true;
}
