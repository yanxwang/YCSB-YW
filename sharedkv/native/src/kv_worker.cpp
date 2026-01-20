#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <cstdio>
#include <thread>
#include <cstdlib>
#include <map>

// ============================================================================
// KVWorker Constructor/Destructor
// ============================================================================

KVWorker::KVWorker(size_t ring_buffer_size) : buffer_size(ring_buffer_size) {
    // Allocate ring buffer with cache line alignment
    buffer = (KVRequest**)aligned_alloc(64, sizeof(KVRequest*) * buffer_size);
    if (!buffer) {
        fprintf(stderr, "[KVWorker] Failed to allocate ring buffer of size %zu\n", buffer_size);
        throw std::bad_alloc();
    }
}

KVWorker::~KVWorker() {
    if (buffer) {
        free(buffer);
        buffer = nullptr;
    }
}

// ============================================================================
// KVWorker ring buffer operations
// ============================================================================

bool KVWorker::try_handoff(KVRequest* req) {
    uint64_t w = write_idx.load(std::memory_order_relaxed);
    uint64_t r = read_idx.load(std::memory_order_acquire);

    // Check if ring buffer is full
    if ((w + 1) % buffer_size == r % buffer_size) {
        return false;  // Full, cannot accept request
    }

    buffer[w % buffer_size] = req;
    write_idx.store(w + 1, std::memory_order_release);
    return true;
}

bool KVWorker::try_get_request(KVRequest*& req) {
    uint64_t r = read_idx.load(std::memory_order_relaxed);
    uint64_t w = write_idx.load(std::memory_order_acquire);

    if (r == w) {
        return false;  // Empty, no requests
    }

    req = buffer[r % buffer_size];
    read_idx.store(r + 1, std::memory_order_release);
    return true;
}

// ============================================================================
// Worker thread function
// ============================================================================

// Hash function (same as in shared_kv_bucket.cpp)
static uint64_t hash_key(const std::string& key) {
    return std::hash<std::string>{}(key);
}

void worker_thread_func(KVWorker* worker, SharedHashTable* table,
                        void* base, std::atomic<bool>& stop_flag) {
    uint64_t processed = 0;
    uint64_t last_report = 0;
    uint64_t resp_q_waits = 0;  // How many times waiting for resp_q space
    uint64_t empty_polls = 0;   // How many times no request available
    // Track which client's resp_q we write to (for debugging)
    std::map<uint32_t, uint64_t> client_write_counts;
    std::map<uint32_t, uint64_t> client_wait_counts;
    std::map<void*, uint32_t> resp_q_to_client;  // Map queue pointer to client_id

    fprintf(stderr, "[Worker-%u] Started\n", worker->worker_id);

    bool stopping = false;
    while (!stopping) {
        // Check stop flag, but keep draining if there are pending requests
        if (stop_flag.load(std::memory_order_relaxed)) {
            stopping = true;
        }

        KVRequest* req = nullptr;

        if (worker->try_get_request(req)) {
            processed++;
            stopping = false;  // Keep processing while there are requests
            // Verify bucket ownership in debug mode
            #ifdef DEBUG_BUCKET_OWNERSHIP
            uint64_t h = hash_key(req->get_key());
            uint32_t bucket_id = h % NUM_BUCKETS;
            if (!worker->owns_bucket(bucket_id)) {
                fprintf(stderr, "ERROR: Worker %u received request for bucket %u (should be worker %u)\n",
                        worker->worker_id, bucket_id, bucket_id % worker->num_workers);
            }
            #endif

            // Prepare response
            KVResponse resp;
            resp.client_id = req->client_id;
            resp.sequence_number = req->sequence_number;
            resp.timestamp = req->timestamp;

            // Execute KV operation based on request type
            switch (req->op_type) {
                case KVOpType::READ: {
                    std::string result;
                    bool found = kv_get(table, base, req->get_key(), result, nullptr);
                    resp.status = found ? KVStatus::SUCCESS : KVStatus::NOT_FOUND;
                    if (found) {
                        resp.set_result(result);
                    }
                    worker->reads.fetch_add(1, std::memory_order_relaxed);
                    break;
                }

                case KVOpType::INSERT: {
                    kv_put(table, base, req->get_key(), req->get_value(), nullptr);
                    resp.status = KVStatus::SUCCESS;
                    worker->inserts.fetch_add(1, std::memory_order_relaxed);
                    break;
                }

                case KVOpType::UPDATE: {
                    kv_put(table, base, req->get_key(), req->get_value(), nullptr);
                    resp.status = KVStatus::SUCCESS;
                    worker->updates.fetch_add(1, std::memory_order_relaxed);
                    break;
                }

                case KVOpType::DELETE: {
                    bool deleted = kv_delete(table, base, req->get_key(), nullptr);
                    resp.status = deleted ? KVStatus::SUCCESS : KVStatus::NOT_FOUND;
                    worker->deletes.fetch_add(1, std::memory_order_relaxed);
                    break;
                }

                default:
                    resp.status = KVStatus::ERROR;
                    break;
            }

            // Track which client's resp_q we're writing to (debug)
            client_write_counts[req->client_id]++;
            // Record queue pointer to client mapping (first time only)
            if (resp_q_to_client.find((void*)req->resp_q_ptr) == resp_q_to_client.end()) {
                resp_q_to_client[(void*)req->resp_q_ptr] = req->client_id;
                fprintf(stderr, "[Worker-%u] First write to client %u resp_q=%p\n",
                        worker->worker_id, req->client_id, (void*)req->resp_q_ptr);
            }

            // Enqueue response to client's response queue
            // If queue is full and stop_flag is set, drop the response to allow graceful shutdown
            bool enqueue_success = false;
            while (!(enqueue_success = req->resp_q_ptr->enqueue(resp))) {
                resp_q_waits++;
                client_wait_counts[req->client_id]++;
                if (stop_flag.load(std::memory_order_relaxed)) {
                    break;  // Drop response on shutdown
                }
                std::this_thread::yield();
            }

            // Recycle request (returns to pool or deletes)
            req->recycle();

            worker->ops_processed.fetch_add(1, std::memory_order_relaxed);

            // Periodic debug output
            if (processed - last_report >= 500000) {
                fprintf(stderr, "[Worker-%u] processed=%lu\n", worker->worker_id, processed);
                last_report = processed;
            }
        } else {
            // No work available, yield CPU
            empty_polls++;
            std::this_thread::yield();
        }
    }

    fprintf(stderr, "[Worker-%u] Stopped: processed=%lu, resp_q_waits=%lu, empty_polls=%lu\n",
            worker->worker_id, processed, resp_q_waits, empty_polls);

    // Print per-client write/wait statistics
    fprintf(stderr, "[Worker-%u] Per-client write counts:\n", worker->worker_id);
    for (const auto& [client_id, count] : client_write_counts) {
        uint64_t waits = client_wait_counts.count(client_id) ? client_wait_counts[client_id] : 0;
        fprintf(stderr, "  client %u: writes=%lu, waits=%lu\n", client_id, count, waits);
    }
    fprintf(stderr, "[Worker-%u] Unique resp_q pointers used: %zu\n",
            worker->worker_id, resp_q_to_client.size());
}
