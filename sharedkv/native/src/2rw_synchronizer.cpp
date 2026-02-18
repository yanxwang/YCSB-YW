// ============================================================================
// SharedKV 2RW — Synchronizer Thread (CPU 0)
//
// Instrumentation uses RDTSCP throughout:
//   - RDTSCP is serializing: waits for ALL prior instructions (incl. loads)
//     to globally complete before reading the TSC.
//   - Eliminates the need for separate _mm_lfence() + __rdtsc(), and
//     prevents compiler reordering (has a real output register constraint).
//   - Per Intel SDM: "waits until all previous instructions have been
//     executed and all previous loads are globally visible."
//
// Phase breakdown per request:
//   A_vis: dequeue() return time (clflushopt overhead + sfence on read_idx)
//          NOTE: dequeue's "item = *slot" CXL loads are non-blocking stores
//          on the call stack — dequeue returns before slot loads complete.
//   A_hid: CXL slot read completion (t_b0 - t_a1).
//          RDTSCP at t_b0 waits for in-flight slot loads → this captures
//          the actual CXL round-trip latency for KVRequest data.
//   B:     gsn increment — local_gsn++ (register-only, ~1 cycle).
//          Previously used fetch_add (lock xadd), which as a full memory
//          barrier was forced to wait for the prior CXL read_idx write
//          (from dequeue's clwb+sfence) to be acknowledged by the CXL
//          device (~1250 ns ≈ 1887 ticks), even though that write had
//          already left the CPU's store buffer.  rdtscp does NOT wait for
//          stores (only loads), so it did not catch this; lock xadd did.
//          Fix: gsn is only written by Sync → no lock needed.
//   C:     ring enqueue (NT stores + 2×sfence to CXL WorkerRing)
// ============================================================================

#include "2rw_context.h"
#include "cxl_ptr.h"
#include <cstdio>
#include <cstdint>
#include <immintrin.h>   // _mm_pause, __rdtsc, __rdtscp
#include <numaif.h>      // get_mempolicy, MPOL_F_NODE, MPOL_F_ADDR

namespace TwoRW {

static constexpr uint64_t REPORT_INTERVAL = 200000;

void two_rw_synchronizer_run(SyncThreadState* s) {
    const uint32_t n = s->num_clients;
    const uint32_t m = s->num_workers;

    // ========================================================================
    // Standalone GSN Microbenchmark (runs before request threads start)
    // ========================================================================
    {
        constexpr uint32_t BENCH_N = 500000;
        uint32_t aux;
        s->gsn.store(0, std::memory_order_relaxed);

        uint64_t mb_t0 = __rdtscp(&aux);
        for (uint32_t k = 0; k < BENCH_N; k++) {
            s->gsn.fetch_add(1, std::memory_order_relaxed);
            asm volatile("" ::: "memory");
        }
        uint64_t mb_t1 = __rdtscp(&aux);

        s->gsn.store(0, std::memory_order_relaxed);

        fprintf(stderr,
            "[Sync] GSN Microbenchmark (isolated, %u iters, rdtscp): %.2f ticks/fetch_add\n"
            "[Sync] sync_state->gsn address: %p\n",
            BENCH_N,
            static_cast<double>(mb_t1 - mb_t0) / BENCH_N,
            static_cast<void*>(&s->gsn));
    }

    // ── Local GSN counter (replaces s->gsn.fetch_add in the hot path) ────────
    // gsn is written ONLY by the Sync thread.  Using fetch_add (lock xadd)
    // triggered a full memory barrier that was forced to wait for the prior
    // CXL write (read_idx clwb in dequeue) to be acknowledged by the CXL
    // device (~1250 ns ≈ 1887 ticks per op).  A plain register increment
    // avoids the lock prefix entirely.  s->gsn is updated after the loop.
    uint64_t local_gsn = s->gsn.load(std::memory_order_relaxed);

    uint32_t queue_idx    = 0;
    uint64_t total_routed = 0;
    uint64_t empty_polls  = 0;

    uint64_t* queue_dequeued   = new uint64_t[n]();
    uint64_t* worker_enqueued  = new uint64_t[m]();
    uint64_t* worker_fullwaits = new uint64_t[m]();

    uint64_t a_visible_total = 0;  // dequeue() return time (excl. slot load wait)
    uint64_t a_hidden_total  = 0;  // CXL slot read wait (t_b0 - t_a1)
    uint64_t b_total         = 0;  // gsn fetch_add (t_b1 - t_b0)
    uint64_t c_total         = 0;  // ring enqueue  (t_c1 - t_c0)
    uint64_t body_total      = 0;  // t_c1 - t_a0

    uint64_t tsc_start       = __rdtsc();
    uint64_t last_report_tsc = tsc_start;
    uint64_t last_report_ops = 0;

    fprintf(stderr,
        "[Sync] Started. Polling %u RequestQueues → %u WorkerRings\n"
        "[Sync]   Timing: rdtscp (serialized). "
        "A_vis=deq_return  A_hid=CXL_slot_read  B=gsn  C=enq\n",
        n, m);

    uint32_t aux;  // rdtscp TSC_AUX (processor ID)

    while (true) {
        bool found_any = false;

        for (uint32_t j = 0; j < n; j++) {
            KVRequest req;

            // ── A_visible: time for dequeue() to return ───────────────────
            // dequeue() ends with _mm_sfence() (drains stores only).
            // "item = *slot" CXL loads are still in-flight on return.
            uint64_t t_a0 = __rdtscp(&aux);
            bool got = s->req_consumers[queue_idx].dequeue(req);
            uint64_t t_a1 = __rdtscp(&aux);

            if (got) {
                found_any = true;
                queue_dequeued[queue_idx]++;
                a_visible_total += t_a1 - t_a0;

                // ── A_hidden: CXL slot read completion ───────────────────
                // rdtscp at t_b0 serializes on ALL prior loads, including
                // the "item = *slot" CXL loads still in-flight from dequeue.
                // t_b0 - t_a1 = true CXL round-trip latency for KVRequest.
                uint64_t t_b0 = __rdtscp(&aux);
                a_hidden_total += t_b0 - t_a1;

                // ── B: gsn increment (register-only, ~1 cycle) ───────────
                // local_gsn++ avoids lock xadd and the CXL-write-drain stall.
                req.gsn = local_gsn++;
                uint64_t t_b1 = __rdtscp(&aux);
                b_total += t_b1 - t_b0;

                // ── C: enqueue to CXL WorkerRing ─────────────────────────
                uint32_t wid = req.worker_id % m;
                uint64_t wait_rounds = 0;
                uint64_t t_c0 = __rdtscp(&aux);
                while (!s->ring_producers[wid].enqueue(req)) {
                    wait_rounds++;
                    _mm_pause();
                    if (s->stop_flag->load(std::memory_order_relaxed)) {
                        uint64_t t_c1 = __rdtscp(&aux);
                        c_total    += t_c1 - t_c0;
                        body_total += t_c1 - t_a0;
                        total_routed++;
                        worker_enqueued[wid]++;
                        worker_fullwaits[wid] += wait_rounds;
                        goto drain_done;
                    }
                }
                uint64_t t_c1 = __rdtscp(&aux);
                c_total    += t_c1 - t_c0;
                body_total += t_c1 - t_a0;

                worker_enqueued[wid]++;
                worker_fullwaits[wid] += wait_rounds;
                total_routed++;

                if (total_routed % REPORT_INTERVAL == 0) {
                    uint64_t now = __rdtsc();
                    uint64_t delta_tsc = now - last_report_tsc;
                    uint64_t delta_ops = total_routed - last_report_ops;

                    double wall = delta_tsc > 0
                        ? static_cast<double>(delta_tsc) / delta_ops : 0.0;
                    double av = static_cast<double>(a_visible_total) / total_routed;
                    double ah = static_cast<double>(a_hidden_total)  / total_routed;
                    double b  = static_cast<double>(b_total)         / total_routed;
                    double c  = static_cast<double>(c_total)         / total_routed;

                    uint64_t total_fw = 0;
                    for (uint32_t i = 0; i < m; i++) total_fw += worker_fullwaits[i];

                    fprintf(stderr,
                        "[Sync] ops=%7lu  wall=%.1f  "
                        "A_vis=%.1f  A_hid(CXL)=%.1f  B(gsn)=%.1f  C(enq)=%.1f  "
                        "fw=%lu  empty=%lu\n",
                        total_routed, wall, av, ah, b, c,
                        total_fw, empty_polls);

                    last_report_tsc = now;
                    last_report_ops = total_routed;

                    // ── Sampled NUMA check for gsn (syscall, ~1µs, done once) ──
                    // Verifies the physical page backing gsn is on the expected node.
                    // get_mempolicy with MPOL_F_NODE|MPOL_F_ADDR queries the
                    // *actual* physical node (requires page to be faulted in).
                    // Page is guaranteed faulted in after REPORT_INTERVAL ops.
                    if (total_routed == REPORT_INTERVAL) {
                        int gsn_phys_node = -1;
                        get_mempolicy(&gsn_phys_node, nullptr, 0,
                                      static_cast<void*>(
                                          const_cast<std::atomic<uint64_t>*>(&s->gsn)),
                                      MPOL_F_NODE | MPOL_F_ADDR);
                        fprintf(stderr,
                            "[Sync] ── gsn NUMA sampling ──\n"
                            "[Sync]   &s->gsn           = %p\n"
                            "[Sync]   s->cxl_base       = %p\n"
                            "[Sync]   gsn physical node = %d\n"
                            "[Sync]   gsn on CXL?       = %s\n"
                            "[Sync]   (B=%.1f ticks — if gsn_node==CXL_node, "
                            "this is CXL latency per fetch_add)\n",
                            static_cast<const void*>(&s->gsn),
                            s->cxl_base,
                            gsn_phys_node,
                            // CXL slab spans [cxl_base, cxl_base+total_size).
                            // Must check BOTH bounds to avoid false positive.
                            ((uintptr_t)&s->gsn >= (uintptr_t)s->cxl_base &&
                             (uintptr_t)&s->gsn <  (uintptr_t)s->cxl_base
                                                    + s->layout->total_size)
                                ? "YES — gsn is inside CXL slab!"
                                : "no (gsn is outside CXL slab, as expected)",
                            b);
                    }
                }
            }
            queue_idx = (queue_idx + 1) % n;
        }

        if (!found_any) {
            empty_polls++;
            if (s->stop_flag->load(std::memory_order_acquire)) {
                bool all_empty = true;
                for (uint32_t j2 = 0; j2 < n; j2++) {
                    s->req_consumers[j2].refresh_write_idx();
                    if (!s->req_consumers[j2].is_empty()) {
                        all_empty = false;
                        break;
                    }
                }
                if (all_empty) break;
            }
            _mm_pause();
        }
    }
drain_done:;
    // Sync local_gsn back to s->gsn so two_rw_stop() can read the final value.
    // Use relaxed — the thread join in two_rw_stop provides the happens-before.
    s->gsn.store(local_gsn, std::memory_order_relaxed);

    uint64_t tsc_total       = __rdtsc() - tsc_start;
    uint64_t total_fullwaits = 0;
    for (uint32_t i = 0; i < m; i++) total_fullwaits += worker_fullwaits[i];

    auto avg = [&](uint64_t sum) -> double {
        return total_routed > 0 ? static_cast<double>(sum) / total_routed : 0.0;
    };
    double avg_wall = total_routed > 0
        ? static_cast<double>(tsc_total) / total_routed : 0.0;

    fprintf(stderr,
        "[Sync] ===== Final Statistics =====\n"
        "[Sync]   total_routed  : %lu\n"
        "[Sync]   empty_polls   : %lu\n"
        "[Sync]   ring_fullwaits: %lu\n"
        "[Sync]   wall ticks/op : %.1f\n"
        "[Sync]\n"
        "[Sync]   Sub-operation breakdown (rdtscp-serialized, avg ticks/op):\n"
        "[Sync]     A_vis  dequeue return     : %6.1f  (clflushopt+sfence overhead)\n"
        "[Sync]     A_hid  CXL slot read wait : %6.1f  ← bottleneck (full CXL RTT)\n"
        "[Sync]     B      gsn fetch_add      : %6.1f  (lock xadd, node-0 L1)\n"
        "[Sync]     C      ring enqueue       : %6.1f  (NT stores + 2×sfence)\n"
        "[Sync]     body   (A+B+C+rdtscp ovhd): %6.1f\n"
        "[Sync]     loop overhead (wall-body) : %6.1f\n",
        total_routed, empty_polls, total_fullwaits, avg_wall,
        avg(a_visible_total), avg(a_hidden_total),
        avg(b_total), avg(c_total), avg(body_total),
        avg_wall - avg(body_total));

    fprintf(stderr, "[Sync]   Per RequestQueue:\n");
    for (uint32_t j = 0; j < n; j++)
        fprintf(stderr, "[Sync]     ReqQ[%u]: %lu\n", j, queue_dequeued[j]);

    fprintf(stderr, "[Sync]   Per WorkerRing:\n");
    for (uint32_t i = 0; i < m; i++)
        fprintf(stderr, "[Sync]     Worker[%u]: enqueued=%lu  fullwaits=%lu\n",
                i, worker_enqueued[i], worker_fullwaits[i]);

    delete[] queue_dequeued;
    delete[] worker_enqueued;
    delete[] worker_fullwaits;
}

} // namespace TwoRW
