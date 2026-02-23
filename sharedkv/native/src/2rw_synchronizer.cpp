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
//   A_vis: dequeue_lazy() return time (clflushopt + lfence + CXL slot load).
//          No sfence inside dequeue_lazy, so this is dominated by the CXL
//          slot read latency (clflushopt evicts the line, then *slot fetches
//          from CXL and stalls until data arrives).
//   A_hid: time from dequeue_lazy() return until rdtscp(t_b0) serialises.
//          Should be near-zero: item=*slot already blocked in A_vis.
//   B:     gsn increment — local_gsn++ (register-only, ~1 cycle).
//          Previously used fetch_add (lock xadd), which as a full memory
//          barrier was forced to wait for the prior CXL read_idx write
//          (from dequeue's clwb+sfence) to be acknowledged by the CXL
//          device (~1250 ns ≈ 1887 ticks), even though that write had
//          already left the CPU's store buffer.  rdtscp does NOT wait for
//          stores (only loads), so it did not catch this; lock xadd did.
//          Fix: gsn is only written by Sync → no lock needed.
//   C:     ring enqueue (NT stores + 2×sfence to CXL WorkerRing).
//          With lazy dequeue, there is no pending clwb(read_idx) when the
//          enqueue sfences run, so C should drop from ~1300 to ~200 ticks.
//   F:     flush_read_idx() cost (clwb+sfence to CXL), measured every
//          READ_ACK_BATCH ops and amortised across the batch.
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

    // ── Local GSN counter ─────────────────────────────────────────────────────
    uint64_t local_gsn = s->gsn.load(std::memory_order_relaxed);

    // ── Tuning knobs ──────────────────────────────────────────────────────────
    // DEQUEUE_BATCH: items pulled from one RequestQueue per round-robin step.
    //   All clflushopt issued upfront, ONE lfence, then reads in parallel →
    //   pipelined CXL fetches.  Keep ≤ Capacity/4 and small enough to fit on
    //   the stack (KVRequest × DEQUEUE_BATCH bytes).
    // READ_ACK_BATCH: flush read_idx to CXL every this many dequeues per queue.
    //   Safe limit: Capacity / 4 (default queue_depth=1024 → 256 max).
    static constexpr uint32_t DEQUEUE_BATCH  = 8;
    static constexpr uint32_t READ_ACK_BATCH = 32;

    uint32_t queue_idx    = 0;
    uint64_t total_routed = 0;
    uint64_t empty_polls  = 0;
    uint64_t flush_count  = 0;

    uint64_t* queue_dequeued   = new uint64_t[n]();
    uint64_t* worker_enqueued  = new uint64_t[m]();
    uint64_t* worker_fullwaits = new uint64_t[m]();
    uint32_t* lazy_count       = new uint32_t[n]();

    // Timing accumulators:
    //   A: pipelined batch dequeue (amortised over DEQUEUE_BATCH items)
    //   C: per-item enqueue to CXL WorkerRing (NT stores + 2×sfence)
    //   F: flush_read_idx() cost, amortised over READ_ACK_BATCH items
    uint64_t a_total     = 0;
    uint64_t c_total     = 0;
    uint64_t f_total     = 0;

    uint64_t tsc_start       = __rdtsc();
    uint64_t last_report_tsc = tsc_start;
    uint64_t last_report_ops = 0;

    fprintf(stderr,
        "[Sync] Started. Polling %u RequestQueues → %u WorkerRings\n"
        "[Sync]   Mode: dequeue_batch_pipelined (DEQUEUE_BATCH=%u  READ_ACK_BATCH=%u)\n"
        "[Sync]   Timing: rdtscp. A=pipelined_batch_deq/op  C=enq/op  F=flush/op\n",
        n, m, DEQUEUE_BATCH, READ_ACK_BATCH);

    uint32_t aux;
    KVRequest batch[DEQUEUE_BATCH];  // stack-allocated, avoids heap per iteration

    while (true) {
        bool found_any = false;

        for (uint32_t j = 0; j < n; j++) {

            // ── A: pipelined batch dequeue ────────────────────────────────
            // Phases inside dequeue_batch_pipelined:
            //   1. clflushopt(slot[0..count-1]) — all upfront, non-blocking
            //   2. ONE lfence
            //   3. items[0..count-1] = *slot[i] — CPU pipelines CXL fetches
            // A ticks / count = amortised CXL read cost per item.
            uint64_t t_a0 = __rdtscp(&aux);
            uint64_t count = s->req_consumers[queue_idx]
                                 .dequeue_batch_pipelined(batch, DEQUEUE_BATCH);
            uint64_t t_a1 = __rdtscp(&aux);

            if (count > 0) {
                found_any = true;
                queue_dequeued[queue_idx] += count;
                a_total += t_a1 - t_a0;   // batch time; divide by count for per-item

                // ── Process each item from the batch ─────────────────────
                for (uint64_t k = 0; k < count; k++) {
                    KVRequest& req = batch[k];

                    // gsn: register-only, ~1 cycle per item
                    req.gsn = local_gsn++;

                    // ── C: enqueue to CXL WorkerRing ─────────────────────
                    uint32_t wid = req.worker_id % m;
                    uint64_t wait_rounds = 0;
                    uint64_t t_c0 = __rdtscp(&aux);
                    while (!s->ring_producers[wid].enqueue(req)) {
                        wait_rounds++;
                        _mm_pause();
                        if (s->stop_flag->load(std::memory_order_relaxed)) {
                            uint64_t t_c1 = __rdtscp(&aux);
                            c_total += t_c1 - t_c0;
                            worker_enqueued[wid]++;
                            worker_fullwaits[wid] += wait_rounds;
                            total_routed++;
                            goto drain_done;
                        }
                    }
                    uint64_t t_c1 = __rdtscp(&aux);
                    c_total += t_c1 - t_c0;

                    worker_enqueued[wid]++;
                    worker_fullwaits[wid] += wait_rounds;
                    total_routed++;
                }

                // ── F: lazy flush of read_idx to CXL ─────────────────────
                // Amortised: one clwb+sfence per READ_ACK_BATCH dequeues.
                lazy_count[queue_idx] += static_cast<uint32_t>(count);
                if (lazy_count[queue_idx] >= READ_ACK_BATCH) {
                    uint64_t t_f0 = __rdtscp(&aux);
                    s->req_consumers[queue_idx].flush_read_idx();
                    uint64_t t_f1 = __rdtscp(&aux);
                    f_total += t_f1 - t_f0;
                    flush_count++;
                    lazy_count[queue_idx] = 0;
                }

                if (total_routed % REPORT_INTERVAL == 0) {
                    uint64_t now = __rdtsc();
                    uint64_t delta_tsc = now - last_report_tsc;
                    uint64_t delta_ops = total_routed - last_report_ops;

                    double wall = delta_tsc > 0
                        ? static_cast<double>(delta_tsc) / delta_ops : 0.0;
                    double a = static_cast<double>(a_total) / total_routed;
                    double c = static_cast<double>(c_total) / total_routed;
                    double f = static_cast<double>(f_total) / total_routed;

                    uint64_t total_fw = 0;
                    for (uint32_t i = 0; i < m; i++) total_fw += worker_fullwaits[i];

                    fprintf(stderr,
                        "[Sync] ops=%7lu  wall=%.1f  "
                        "A(deq/op)=%.1f  C(enq/op)=%.1f  F(flush/op)=%.1f  "
                        "fw=%lu  empty=%lu\n",
                        total_routed, wall, a, c, f,
                        total_fw, empty_polls);

                    last_report_tsc = now;
                    last_report_ops = total_routed;
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
    // Flush all read_idxs so producers can reclaim slots before we exit.
    for (uint32_t j2 = 0; j2 < n; j2++) {
        s->req_consumers[j2].flush_read_idx();
    }

    // Sync local_gsn back; thread join provides happens-before.
    s->gsn.store(local_gsn, std::memory_order_relaxed);

    uint64_t tsc_total       = __rdtsc() - tsc_start;
    uint64_t total_fullwaits = 0;
    for (uint32_t i = 0; i < m; i++) total_fullwaits += worker_fullwaits[i];

    auto avg = [&](uint64_t sum) -> double {
        return total_routed > 0 ? static_cast<double>(sum) / total_routed : 0.0;
    };
    double avg_wall = total_routed > 0
        ? static_cast<double>(tsc_total) / total_routed : 0.0;

    double avg_flush_raw = flush_count > 0
        ? static_cast<double>(f_total) / flush_count : 0.0;

    fprintf(stderr,
        "[Sync] ===== Final Statistics =====\n"
        "[Sync]   total_routed  : %lu\n"
        "[Sync]   empty_polls   : %lu\n"
        "[Sync]   ring_fullwaits: %lu\n"
        "[Sync]   flush calls   : %lu  (batch=%u, raw/flush=%.1f ticks)\n"
        "[Sync]   wall ticks/op : %.1f\n"
        "[Sync]\n"
        "[Sync]   Sub-operation breakdown (rdtscp-serialized, avg ticks/op):\n"
        "[Sync]     A  pipelined batch deq/op  : %6.1f  (DEQUEUE_BATCH=%u)\n"
        "[Sync]     C  ring enqueue/op         : %6.1f  (NT stores + 2×sfence)\n"
        "[Sync]     F  flush amortised/op      : %6.1f  (READ_ACK_BATCH=%u)\n"
        "[Sync]     loop+rdtscp overhead/op    : %6.1f\n",
        total_routed, empty_polls, total_fullwaits,
        flush_count, READ_ACK_BATCH, avg_flush_raw,
        avg_wall,
        avg(a_total), DEQUEUE_BATCH,
        avg(c_total),
        avg(f_total), READ_ACK_BATCH,
        avg_wall - avg(a_total) - avg(c_total) - avg(f_total));

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
    delete[] lazy_count;
}

} // namespace TwoRW
