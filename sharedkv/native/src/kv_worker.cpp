#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <cstdio>
#include <thread>

// ============================================================================
// KVWorker ring buffer operations
// ============================================================================

bool KVWorker::try_handoff(KVRequest* req) {
    uint64_t w = write_idx.load(std::memory_order_relaxed);
    uint64_t r = read_idx.load(std::memory_order_acquire);

    // Check if ring buffer is full
    if ((w + 1) % BUFFER_SIZE == r % BUFFER_SIZE) {
        return false;  // Full, cannot accept request
    }

    buffer[w % BUFFER_SIZE] = req;
    write_idx.store(w + 1, std::memory_order_release);
    return true;
}

bool KVWorker::try_get_request(KVRequest*& req) {
    uint64_t r = read_idx.load(std::memory_order_relaxed);
    uint64_t w = write_idx.load(std::memory_order_acquire);

    if (r == w) {
        return false;  // Empty, no requests
    }

    req = buffer[r % BUFFER_SIZE];
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
    while (!stop_flag.load(std::memory_order_relaxed)) {
        KVRequest* req = nullptr;

        if (worker->try_get_request(req)) {
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

            // Enqueue response to client's response queue (blocking if full)
            while (!req->resp_q_ptr->enqueue(resp)) {
                if (stop_flag.load(std::memory_order_relaxed)) {
                    break;
                }
                // Brief yield to avoid busy-waiting
                std::this_thread::yield();
            }

            // Cleanup request
            req->cleanup();
            delete req;

            worker->ops_processed.fetch_add(1, std::memory_order_relaxed);
        } else {
            // No work available, yield CPU
            std::this_thread::yield();
        }
    }
}
