#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <vector>
#include <thread>
#include <cstdio>
#include <chrono>

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
    uint64_t total_dequeued = 0;
    uint64_t total_handoffs = 0;
    uint64_t total_drops = 0;
    uint64_t last_report = 0;
    uint64_t poll_loops = 0;
    uint64_t empty_polls = 0;           // How many times all queues were empty
    uint64_t handoff_retries = 0;       // Total handoff retry count
    uint64_t yield_count = 0;           // How many yields
    auto last_time_log = std::chrono::steady_clock::now();

    // fprintf(stderr, "[Synchronizer] Started: %zu clients, %zu workers\n", num_clients, num_workers);
    // Print queue pointers for debugging
    for (size_t c = 0; c < num_clients; ++c) {
        fprintf(stderr, "[Synchronizer] clients[%zu]=%p, req_q=%p, resp_q=%p\n",
                c, (void*)clients[c], (void*)clients[c]->req_q, (void*)clients[c]->resp_q);
    }
    fflush(stderr);

    while (!stop_flag.load(std::memory_order_relaxed)) {
        // Round-robin poll all client request queues
        bool found_any = false;
        for (size_t i = 0; i < num_clients; ++i) {
            KVRequest* req = nullptr;

            if (clients[client_idx]->req_q->dequeue(req)) {
                found_any = true;
                total_dequeued++;
                // OPTIMIZATION: Only assign global sequence number
                // All other fields (client_id, resp_q_ptr, target_worker_id) are pre-filled by Java thread
                req->sequence_number = global_seq.fetch_add(1, std::memory_order_relaxed);

                // Use pre-computed target worker ID (no hash computation needed!)
                uint32_t worker_id = req->target_worker_id;

                // Try direct handoff to worker's ring buffer
                // If ring buffer is full, retry with back-off instead of dropping
                int retry_count = 0;
                while (!workers[worker_id]->try_handoff(req)) {
                    retry_count++;
                    handoff_retries++;
                    if (stop_flag.load(std::memory_order_relaxed)) {
                        // Shutting down - drop the request
                        total_drops++;
                        KVResponse err_resp;
                        err_resp.status = KVStatus::ERROR;
                        err_resp.client_id = req->client_id;
                        err_resp.sequence_number = req->sequence_number;
                        err_resp.timestamp = req->timestamp;
                        req->resp_q_ptr->enqueue(err_resp);
                        req->recycle();  // Use recycle() instead of delete - handles both pool and heap allocation
                        goto next_client;
                    }
                    // Brief yield to let worker process
                    // yield_count++;
                    // std::this_thread::yield();
                }
                total_handoffs++;

                // Periodic debug output
                if (total_dequeued - last_report >= 500000) {
                    fprintf(stderr, "[Synchronizer] dequeued=%lu, handoffs=%lu, drops=%lu\n",
                            total_dequeued, total_handoffs, total_drops);
                    fflush(stderr);
                    last_report = total_dequeued;
                }
            }

        next_client:
            client_idx = (client_idx + 1) % num_clients;
        }

        poll_loops++;
        if (!found_any) {
            empty_polls++;
        }

        // Time-based status log (every 2 seconds)
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_time_log).count() >= 2) {
            fprintf(stderr, "[Synchronizer] alive: polled=%lu, dequeued=%lu, handoffs=%lu, drops=%lu\n",
                    poll_loops, total_dequeued, total_handoffs, total_drops);
            fprintf(stderr, "[Synchronizer] PERF: empty_polls=%lu, handoff_retries=%lu, yield_count=%lu\n",
                    empty_polls, handoff_retries, yield_count);
            // Print queue states
            for (size_t c = 0; c < num_clients; ++c) {
                auto* q = clients[c]->req_q;
                fprintf(stderr, "[Synchronizer] client[%zu] req_q: head=%lu, tail=%lu, size=%zu\n",
                        c, q->head.load(), q->tail.load(), q->get_size());
            }
            fflush(stderr);
            last_time_log = now;
        }

        // Brief yield to avoid 100% CPU busy-waiting
        // yield_count++;
        // std::this_thread::yield();
    }

    fprintf(stderr, "[Synchronizer] Stopped: dequeued=%lu, handoffs=%lu, drops=%lu\n",
            total_dequeued, total_handoffs, total_drops);
    fprintf(stderr, "[Synchronizer] FINAL PERF: poll_loops=%lu, empty_polls=%lu, handoff_retries=%lu, yield_count=%lu\n",
            poll_loops, empty_polls, handoff_retries, yield_count);
}
