// cxl_worker.cpp
// CXL-aware worker thread implementation
// Reads requests from CXL shared ring buffer, processes KV ops, writes responses

#include "cxl_shared.h"
#include <cstdio>
#include <thread>
#include <atomic>
#include <chrono>

// External declarations for KV operations
extern bool cxl_kv_put(void* base, CXLSharedHashTable* table, CXLMemoryRegion* region,
                       const char* key, uint32_t key_len,
                       const char* value, uint32_t value_len);

extern bool cxl_kv_get(void* base, CXLSharedHashTable* table,
                       const char* key, uint32_t key_len,
                       char* value_out, uint32_t* value_len_out, uint32_t max_value_len);

extern bool cxl_kv_delete(void* base, CXLSharedHashTable* table,
                          const char* key, uint32_t key_len);

// ============================================================================
// CXL Worker Context
// ============================================================================

struct CXLWorkerContext {
    // CXL memory base
    void* cxl_base;
    CXLMemoryLayout layout;

    // Worker identity
    uint32_t worker_id;
    uint32_t host_id;

    // Pointers to CXL structures (derived from base + layout)
    CXLSharedHashTable* hash_table;
    CXLWorkerRegistry* registry;
    CXLMemoryRegion* my_region;
    CXLRingBuffer* my_request_ring;

    // Response rings (for all clients)
    uint32_t num_response_rings;

    // Stop flag
    std::atomic<bool>* stop_flag;

    // Statistics
    uint64_t ops_processed;
    uint64_t reads;
    uint64_t inserts;
    uint64_t updates;
    uint64_t deletes;
    uint64_t empty_polls;
    uint64_t resp_waits;
};

// ============================================================================
// Initialize Worker Context
// ============================================================================

void cxl_worker_init(CXLWorkerContext* ctx,
                     void* cxl_base,
                     const CXLMemoryLayout& layout,
                     uint32_t worker_id,
                     uint32_t host_id,
                     uint32_t num_response_rings,
                     std::atomic<bool>* stop_flag) {
    ctx->cxl_base = cxl_base;
    ctx->layout = layout;
    ctx->worker_id = worker_id;
    ctx->host_id = host_id;
    ctx->num_response_rings = num_response_rings;
    ctx->stop_flag = stop_flag;

    // Derive pointers from layout
    ctx->hash_table = cxl_get_hash_table(cxl_base, layout);
    ctx->registry = cxl_get_worker_registry(cxl_base, layout);
    ctx->my_region = &cxl_get_worker_regions(cxl_base, layout)[worker_id];
    ctx->my_request_ring = cxl_get_request_ring(cxl_base, layout, worker_id);

    // Clear statistics
    ctx->ops_processed = 0;
    ctx->reads = 0;
    ctx->inserts = 0;
    ctx->updates = 0;
    ctx->deletes = 0;
    ctx->empty_polls = 0;
    ctx->resp_waits = 0;

    // Mark worker as ready in registry
    WorkerInfo* my_info = &ctx->registry->workers[worker_id];
    my_info->host_id = host_id;
    _mm_sfence();
    my_info->magic = CXL_MAGIC_WORKER_READY;

    fprintf(stderr, "[CXL-Worker-%u] Initialized on host %u, ring=%p\n",
            worker_id, host_id, (void*)ctx->my_request_ring);
}

// ============================================================================
// Worker Thread Main Loop
// ============================================================================

void cxl_worker_thread_func(CXLWorkerContext* ctx) {
    fprintf(stderr, "[CXL-Worker-%u] Started\n", ctx->worker_id);

    uint64_t last_report = 0;
    auto last_time = std::chrono::steady_clock::now();

    while (!ctx->stop_flag->load(std::memory_order_relaxed)) {
        CXLRequest req;

        // Try to dequeue a request from my ring buffer
        if (ctx->my_request_ring->dequeue(req)) {
            // Process the request
            CXLResponse resp;
            resp.sequence = req.sequence;
            resp.client_id = req.client_id;
            resp.timestamp = req.timestamp;
            resp.result_len = 0;

            switch (req.op_type) {
                case CXLOpType::READ: {
                    char value_buf[CXL_MAX_VALUE_SIZE];
                    uint32_t value_len = 0;
                    bool found = cxl_kv_get(
                        ctx->cxl_base, ctx->hash_table,
                        req.key_data, req.key_len,
                        value_buf, &value_len, CXL_MAX_VALUE_SIZE
                    );
                    if (found) {
                        resp.status = CXLStatus::SUCCESS;
                        resp.set_result(value_buf, value_len);
                    } else {
                        resp.status = CXLStatus::NOT_FOUND;
                    }
                    ctx->reads++;
                    break;
                }

                case CXLOpType::INSERT:
                case CXLOpType::UPDATE: {
                    bool success = cxl_kv_put(
                        ctx->cxl_base, ctx->hash_table, ctx->my_region,
                        req.key_data, req.key_len,
                        req.value_data, req.value_len
                    );
                    resp.status = success ? CXLStatus::SUCCESS : CXLStatus::ERROR;
                    if (req.op_type == CXLOpType::INSERT) {
                        ctx->inserts++;
                    } else {
                        ctx->updates++;
                    }
                    break;
                }

                case CXLOpType::DELETE: {
                    bool deleted = cxl_kv_delete(
                        ctx->cxl_base, ctx->hash_table,
                        req.key_data, req.key_len
                    );
                    resp.status = deleted ? CXLStatus::SUCCESS : CXLStatus::NOT_FOUND;
                    ctx->deletes++;
                    break;
                }

                default:
                    resp.status = CXLStatus::ERROR;
                    break;
            }

            // Enqueue response to client's response ring
            if (req.resp_ring_id < ctx->num_response_rings) {
                CXLResponseRing* resp_ring = cxl_get_response_ring(
                    ctx->cxl_base, ctx->layout, req.resp_ring_id
                );

                // Busy-wait if response ring is full
                while (!resp_ring->enqueue(resp)) {
                    ctx->resp_waits++;
                    if (ctx->stop_flag->load(std::memory_order_relaxed)) {
                        break;  // Drop response on shutdown
                    }
                    std::this_thread::yield();
                }
            }

            ctx->ops_processed++;

            // Update stats in registry periodically
            if (ctx->ops_processed - last_report >= 100000) {
                ctx->registry->workers[ctx->worker_id].stats_processed = ctx->ops_processed;
                _mm_sfence();

                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_time
                ).count();

                if (elapsed > 0) {
                    double rate = (ctx->ops_processed - last_report) * 1000.0 / elapsed;
                    fprintf(stderr, "[CXL-Worker-%u] processed=%lu (%.1f Kops/s), "
                            "R=%lu I=%lu U=%lu D=%lu\n",
                            ctx->worker_id, ctx->ops_processed, rate / 1000.0,
                            ctx->reads, ctx->inserts, ctx->updates, ctx->deletes);
                }

                last_report = ctx->ops_processed;
                last_time = now;
            }
        } else {
            // No work available
            ctx->empty_polls++;
            std::this_thread::yield();
        }
    }

    fprintf(stderr, "[CXL-Worker-%u] Stopped: processed=%lu, empty_polls=%lu, resp_waits=%lu\n",
            ctx->worker_id, ctx->ops_processed, ctx->empty_polls, ctx->resp_waits);
    fprintf(stderr, "[CXL-Worker-%u] Final: R=%lu I=%lu U=%lu D=%lu\n",
            ctx->worker_id, ctx->reads, ctx->inserts, ctx->updates, ctx->deletes);
}

// ============================================================================
// Wrapper for std::thread
// ============================================================================

void cxl_worker_run(void* cxl_base,
                    const CXLMemoryLayout& layout,
                    uint32_t worker_id,
                    uint32_t host_id,
                    uint32_t num_response_rings,
                    std::atomic<bool>& stop_flag) {
    CXLWorkerContext ctx;
    cxl_worker_init(&ctx, cxl_base, layout, worker_id, host_id,
                    num_response_rings, &stop_flag);
    cxl_worker_thread_func(&ctx);
}
