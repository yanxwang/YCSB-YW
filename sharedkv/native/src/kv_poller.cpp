#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <vector>
#include <thread>
#include <chrono>
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <x86gprintrin.h>  // For _senduipi

// ============================================================================
// Poller state (per-client tracking)
// ============================================================================

struct PollerState {
    bool was_empty;     // Was the response queue empty in the last check?
    int uipi_index;     // UINTR sender index for this client
    int last_known_fd;  // Last registered uintr_fd (to detect changes)
    bool error_logged;  // Has error been logged for this client?
};

// ============================================================================
// Poller thread function (edge-triggered UINTR notification)
// ============================================================================

void poller_thread_func(std::vector<ClientChannel*>& clients,
                       std::atomic<bool>& stop_flag) {
    size_t num_clients = clients.size();
    std::vector<PollerState> states(num_clients);

    // Initialize state for each client
    // UINTR registration is done lazily when Response threads are created
    for (size_t i = 0; i < num_clients; ++i) {
        states[i].was_empty = true;
        states[i].uipi_index = -1;  // Not registered yet
        states[i].last_known_fd = -1;
        states[i].error_logged = false;
    }

    // Main polling loop (edge-triggered detection)
    while (!stop_flag.load(std::memory_order_relaxed)) {
        for (size_t i = 0; i < num_clients; ++i) {
            // Check if uintr_fd has changed (new async phase started)
            int current_fd = clients[i]->uintr_fd;
            bool fd_changed = (current_fd != states[i].last_known_fd) && (current_fd >= 0);
            bool fd_ready = clients[i]->uintr_fd_ready.load(std::memory_order_acquire);

            // Lazy UINTR sender registration (or re-registration if fd changed)
            if (fd_changed && fd_ready) {
                // Unregister old sender if exists
                if (states[i].uipi_index >= 0) {
                    uintr_unregister_sender(states[i].uipi_index, 0);
                    states[i].uipi_index = -1;
                }

                // Register new sender
                states[i].uipi_index = uintr_register_sender(current_fd, 0);
                if (states[i].uipi_index >= 0) {
                    fprintf(stderr, "[Poller] Registered sender for client %zu (fd=%d, uipi_index=%d)\n",
                            i, current_fd, states[i].uipi_index);
                    states[i].last_known_fd = current_fd;
                    states[i].error_logged = false;  // Reset error state for new fd
                } else if (!states[i].error_logged) {
                    // Log error once per fd
                    fprintf(stderr, "[Poller] ERROR: Failed to register sender for client %zu "
                                    "(fd=%d, errno=%d: %s) - will not use UINTR for this client\n",
                            i, current_fd, errno, strerror(errno));
                    states[i].error_logged = true;
                    states[i].last_known_fd = current_fd;  // Mark as seen to avoid repeated attempts
                }
            }

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
