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
    size_t client_idx = 0;  // Round-robin index

    while (!stop_flag.load(std::memory_order_relaxed)) {
        // Round-robin poll all client request queues
        for (size_t i = 0; i < num_clients; ++i) {
            KVRequest* req = nullptr;

            if (clients[client_idx]->req_q->dequeue(req)) {
                // OPTIMIZATION: Only assign global sequence number
                // All other fields (client_id, resp_q_ptr, target_worker_id) are pre-filled by Java thread
                req->sequence_number = global_seq.fetch_add(1, std::memory_order_relaxed);

                // Use pre-computed target worker ID (no hash computation needed!)
                uint32_t worker_id = req->target_worker_id;

                // Try direct handoff to worker's ring buffer
                if (!workers[worker_id]->try_handoff(req)) {
                    // Worker ring buffer is full
                    // Send error response using pre-filled routing fields
                    KVResponse err_resp;
                    err_resp.status = KVStatus::ERROR;
                    err_resp.client_id = req->client_id;
                    err_resp.sequence_number = req->sequence_number;
                    err_resp.timestamp = req->timestamp;

                    // Try to enqueue error response (resp_q_ptr already set)
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
