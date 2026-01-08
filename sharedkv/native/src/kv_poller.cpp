#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <vector>
#include <thread>
#include <chrono>
#include <cstdio>
#include <x86gprintrin.h>  // For _senduipi

// ============================================================================
// Poller state (per-client tracking)
// ============================================================================

struct PollerState {
    bool was_empty;   // Was the response queue empty in the last check?
    int uipi_index;   // UINTR sender index for this client
};

// ============================================================================
// Poller thread function (edge-triggered UINTR notification)
// ============================================================================

void poller_thread_func(std::vector<ClientChannel*>& clients,
                       std::atomic<bool>& stop_flag) {
    size_t num_clients = clients.size();
    std::vector<PollerState> states(num_clients);

    // Register as UINTR sender for each client
    for (size_t i = 0; i < num_clients; ++i) {
        states[i].was_empty = true;
        states[i].uipi_index = -1;

        // Wait for client response thread to create uintr_fd
        // (Response threads are not created in this simplified version,
        //  so we skip UINTR registration for now)
        // In full implementation, this would wait for uintr_fd to be ready

        // For now, we'll skip UINTR and just use the response_ready flag
        // Full UINTR implementation would be added here:
        /*
        while (clients[i]->uintr_fd < 0 && !stop_flag.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        if (stop_flag.load()) return;

        states[i].uipi_index = uintr_register_sender(clients[i]->uintr_fd, 0);
        if (states[i].uipi_index < 0) {
            fprintf(stderr, "[Poller] Failed to register sender for client %zu\n", i);
            return;
        }
        */
    }

    // Main polling loop (edge-triggered detection)
    while (!stop_flag.load(std::memory_order_relaxed)) {
        for (size_t i = 0; i < num_clients; ++i) {
            bool is_empty_now = clients[i]->resp_q->is_empty();

            // Detect empty → non-empty transition
            if (states[i].was_empty && !is_empty_now) {
                // Signal that response is ready
                clients[i]->response_ready.store(true, std::memory_order_release);

                // If UINTR is registered, send interrupt
                if (states[i].uipi_index >= 0) {
                    _senduipi(states[i].uipi_index);
                }
            }

            states[i].was_empty = is_empty_now;
        }

        // Brief yield to avoid 100% CPU usage
        std::this_thread::yield();
    }

    // Cleanup: unregister all UINTR senders
    for (size_t i = 0; i < num_clients; ++i) {
        if (states[i].uipi_index >= 0) {
            uintr_unregister_sender(states[i].uipi_index, 0);
        }
    }
}
