#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <chrono>

// ============================================================================
// Client Response Thread
// ============================================================================
// Each client has a dedicated Response Thread that:
// 1. Waits on UINTR signal from Poller
// 2. Dequeues responses from resp_q
// 3. Processes responses (currently just updates statistics)
// ============================================================================

void client_response_thread_func(ClientChannel* channel, std::atomic<bool>* stop_flag) {
    printf("[RespThread-%u] Starting response thread\n", channel->client_id);

    // Register UINTR handler (empty handler, just for wakeup)
    auto handler = [](struct __uintr_frame*, unsigned long long) {};
    void (*handler_ptr)(struct __uintr_frame*, unsigned long long) = handler;
    uintr_register_handler((void*)handler_ptr, 0);

    // Create UINTR fd for Poller to send signals
    channel->uintr_fd = uintr_create_fd(0, 0);
    if (channel->uintr_fd < 0) {
        fprintf(stderr, "[RespThread-%u] ERROR: Failed to create UINTR fd\n", channel->client_id);
        return;
    }

    channel->uintr_fd_ready.store(true, std::memory_order_release);
    printf("[RespThread-%u] UINTR fd created: %d\n", channel->client_id, channel->uintr_fd);

    // Mark as running
    channel->response_thread_running.store(true, std::memory_order_release);

    // Main loop
    while (!stop_flag->load(std::memory_order_acquire)) {
        // Wait for UINTR signal from Poller
        uintr_wait(0);

        // Drain response queue by calling get_response
        KVResponse resp;

        // Dequeue all available responses (non-blocking)
        while (channel->resp_q->dequeue(resp)) {
            // Process response - complete the KV operation
            if (resp.status == KVStatus::SUCCESS) {
                channel->responses_received.fetch_add(1, std::memory_order_relaxed);
            } else {
                channel->responses_failed.fetch_add(1, std::memory_order_relaxed);
            }

            // TODO: For latency measurement, calculate latency here
            // using resp.timestamp
        }

        // Re-check queue to avoid race condition
        // (new responses might have arrived while we were processing)
        if (!channel->resp_q->is_empty()) {
            // Don't go to uintr_wait, drain again immediately
            continue;
        }

        // Queue is empty, go back to sleep (uintr_wait)
    }

    printf("[RespThread-%u] Received %lu responses (%lu failed)\n",
           channel->client_id,
           channel->responses_received.load(),
           channel->responses_failed.load());

    // Cleanup
    uintr_unregister_handler(0);
    channel->response_thread_running.store(false, std::memory_order_release);
}
