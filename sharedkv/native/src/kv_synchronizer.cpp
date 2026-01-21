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
// Synchronizer thread function (updated for decoupled architecture)
// ============================================================================

void synchronizer_thread_func(std::vector<RequestQueue*>& req_queues,
                              std::vector<KVWorker*>& workers,
                              std::atomic<uint64_t>& global_seq,
                              std::atomic<bool>& stop_flag) {
    size_t num_queues = req_queues.size();
    size_t num_workers = workers.size();
    size_t queue_idx = 0;  // Round-robin index
    uint64_t total_dequeued = 0;
    uint64_t total_handoffs = 0;
    uint64_t total_drops = 0;
    uint64_t last_report = 0;
    uint64_t poll_loops = 0;
    uint64_t empty_polls = 0;           // How many times all queues were empty
    uint64_t handoff_retries = 0;       // Total handoff retry count
    uint64_t yield_count = 0;           // How many yields
    auto last_time_log = std::chrono::steady_clock::now();

    // Print queue pointers for debugging
    for (size_t q = 0; q < num_queues; ++q) {
        fprintf(stderr, "[Synchronizer] req_queues[%zu]=%p, queue=%p\n",
                q, (void*)req_queues[q], (void*)req_queues[q]->queue);
    }
    fflush(stderr);

    while (!stop_flag.load(std::memory_order_relaxed)) {
        // Round-robin poll all request queues
        bool found_any = false;
        for (size_t i = 0; i < num_queues; ++i) {
            KVRequest* req = nullptr;

            if (req_queues[queue_idx]->queue->dequeue(req)) {
                found_any = true;
                total_dequeued++;
                req_queues[queue_idx]->dequeued.fetch_add(1, std::memory_order_relaxed);

                // OPTIMIZATION: Only assign global sequence number
                // All other fields (client_id, resp_q_ptr, target_worker_id) are pre-filled by request thread
                req->sequence_number = global_seq.fetch_add(1, std::memory_order_relaxed);

                // Use pre-computed target worker ID (no hash computation needed!)
                uint32_t worker_id = req->target_worker_id;

                // Try direct handoff to worker's ring buffer
                // Pure busy-spin for max throughput
                while (!workers[worker_id]->try_handoff(req)) {
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
                        req->recycle();
                        goto next_queue;
                    }
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

        next_queue:
            queue_idx = (queue_idx + 1) % num_queues;
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
            for (size_t q = 0; q < num_queues; ++q) {
                auto* queue = req_queues[q]->queue;
                fprintf(stderr, "[Synchronizer] req_q[%zu]: head=%lu, tail=%lu, size=%zu\n",
                        q, queue->head.load(), queue->tail.load(), queue->get_size());
            }
            fflush(stderr);
            last_time_log = now;
        }
    }

    fprintf(stderr, "[Synchronizer] Stopped: dequeued=%lu, handoffs=%lu, drops=%lu\n",
            total_dequeued, total_handoffs, total_drops);
    fprintf(stderr, "[Synchronizer] FINAL PERF: poll_loops=%lu, empty_polls=%lu, handoff_retries=%lu, yield_count=%lu\n",
            poll_loops, empty_polls, handoff_retries, yield_count);
}
