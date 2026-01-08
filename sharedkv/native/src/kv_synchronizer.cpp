#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <vector>
#include <thread>

// Hash function (same as in worker)
static uint64_t hash_key(const std::string& key) {
    return std::hash<std::string>{}(key);
}

// ============================================================================
// Synchronizer thread function
// ============================================================================

void synchronizer_thread_func(std::vector<ClientChannel*>& clients,
                              std::vector<KVWorker*>& workers,
                              std::atomic<uint64_t>& global_seq,
                              std::atomic<bool>& stop_flag) {
    size_t num_clients = clients.size();
    size_t num_workers = workers.size();
    size_t client_idx = 0;  // Round-robin index

    while (!stop_flag.load(std::memory_order_relaxed)) {
        // Round-robin poll all client request queues
        for (size_t i = 0; i < num_clients; ++i) {
            KVRequest* req = nullptr;

            if (clients[client_idx]->req_q->dequeue(req)) {
                // Assign global sequence number
                req->sequence_number = global_seq.fetch_add(1, std::memory_order_relaxed);
                req->client_id = client_idx;
                req->resp_q_ptr = clients[client_idx]->resp_q;

                // Select worker based on key hash (bucket partitioning)
                uint64_t h = hash_key(req->get_key());
                uint32_t bucket_id = h % NUM_BUCKETS;
                uint32_t worker_id = bucket_id % num_workers;

                // Try direct handoff to worker's ring buffer
                if (!workers[worker_id]->try_handoff(req)) {
                    // Worker ring buffer is full
                    // Option 1: Drop request and send error response
                    KVResponse err_resp;
                    err_resp.status = KVStatus::ERROR;
                    err_resp.client_id = req->client_id;
                    err_resp.sequence_number = req->sequence_number;
                    err_resp.timestamp = req->timestamp;

                    // Try to enqueue error response
                    while (!req->resp_q_ptr->enqueue(err_resp)) {
                        if (stop_flag.load()) break;
                        std::this_thread::yield();
                    }

                    // Cleanup dropped request
                    req->cleanup();
                    delete req;
                }
            }

            client_idx = (client_idx + 1) % num_clients;
        }

        // Brief yield to avoid 100% CPU busy-waiting
        std::this_thread::yield();
    }
}
