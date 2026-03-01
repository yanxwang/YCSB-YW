// ============================================================================
// SharedKV 2RW — Poller Thread (CPU 1)
//
// Monitors ResponseQueue[j][*] for empty→non-empty transitions.
// Sends UINTR IPI to Response Thread j when any of its m queues becomes
// non-empty (edge-triggered, not level-triggered).
//
// Per-client polling state is maintained to detect fd changes (Response
// Thread may re-register UINTR across benchmark runs).
// ============================================================================

#include "2rw_context.h"
#include "uintr_threading.h"
#include <cstdio>
#include <cstring>
#include <thread>

namespace TwoRW {

struct ClientPollerState {
    bool was_any_empty;       // Previous "any queue empty" state per client
    int  uipi_index;          // UINTR sender index (-1 = not registered)
    int  last_known_fd;       // fd we last registered with (-1 = none)
};

void two_rw_poller_run(PollerThreadState* s) {
    const uint32_t n = s->num_clients;
    const uint32_t m = s->num_workers;

    std::vector<ClientPollerState> states(n);
    for (uint32_t j = 0; j < n; j++) {
        states[j] = {true, -1, -1};
    }

    uint64_t scan_rounds = 0;
    uint64_t uintrs_sent = 0;

    fprintf(stderr, "[Poller] Started. Monitoring %u × %u ResponseQueues\n", n, m);

    while (!s->stop_flag->load(std::memory_order_relaxed)) {
        for (uint32_t j = 0; j < n; j++) {
            auto& st = states[j];

            // Lazily register UINTR sender when Response Thread sets its fd
            int cur_fd = s->resp_uintr_fds[j];
            if (cur_fd != st.last_known_fd && cur_fd >= 0 &&
                s->resp_fd_ready[j].load(std::memory_order_acquire)) {

                if (st.uipi_index >= 0) {
                    uintr_unregister_sender(st.uipi_index, 0);
                    st.uipi_index = -1;
                }
                long idx = uintr_register_sender(cur_fd, 0);
                if (idx >= 0) {
                    st.uipi_index = static_cast<int>(idx);
                    st.last_known_fd = cur_fd;
                }
            }

            // Check if ANY ResponseQueue[j][i] is non-empty
            bool any_nonempty = false;
            for (uint32_t i = 0; i < m; i++) {
                auto* rq = s->layout->response_queue(s->cxl_base, j, i);
                if (!rq->is_empty()) {
                    any_nonempty = true;
                    break;
                }
            }

            // Edge: empty → non-empty → send UINTR
            if (st.was_any_empty && any_nonempty && st.uipi_index >= 0) {
                _senduipi(static_cast<unsigned long long>(st.uipi_index));
                uintrs_sent++;
            }
            st.was_any_empty = !any_nonempty;
        }
        scan_rounds++;
        std::this_thread::yield();
    }

    // Cleanup: unregister all UINTR senders
    for (uint32_t j = 0; j < n; j++) {
        if (states[j].uipi_index >= 0) {
            uintr_unregister_sender(states[j].uipi_index, 0);
        }
    }

    s->exit_scan_rounds = scan_rounds;
    s->exit_uintrs_sent = uintrs_sent;

    fprintf(stderr, "[Poller] Stopped. scan_rounds=%lu  uintrs_sent=%lu\n",
            scan_rounds, uintrs_sent);
}

} // namespace TwoRW