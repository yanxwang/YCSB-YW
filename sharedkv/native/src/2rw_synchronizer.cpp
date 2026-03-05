// ============================================================================
// SharedKV 2RW — Synchronizer Thread (CPU k+1 for SN_k)
//
// In a multi-synchronizer setup (s > 1), SN_k manages workers
//   [workers_base, workers_base + workers_count - 1]
// and polls n RequestQueues, one per client (the sub-queues for SN_k).
//
// GSN is interleaved across SNs:
//   SN_k generates GSNs k, k+s, k+2s, ...
// Correctness: same key always routes to same SN (via two_rw_submit),
//   so per-key order is monotonically increasing.  No MVCC needed
//   (CXLNode carries no GSN).
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
//   C:     ring enqueue (NT stores + 2×sfence to CXL WorkerRing, or
//          plain release-store to DRAM ring with --local-workerring).
//   F:     flush_read_idx() cost (clwb+sfence to CXL), amortised over
//          READ_ACK_BATCH ops per queue.
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
    const uint32_t n             = s->num_clients;
    const uint32_t sn_id         = s->sn_id;
    const uint32_t s_count       = s->num_synchronizers;  // GSN stride
    const uint32_t workers_base  = s->workers_base;
    const uint32_t workers_count = s->workers_count;

    // ========================================================================
    // Standalone GSN Microbenchmark (runs before request threads start)
    // ========================================================================
    {
        constexpr uint32_t BENCH_N = 500000;
        uint32_t aux;
        s->gsn.store(sn_id, std::memory_order_relaxed);

        uint64_t mb_t0 = __rdtscp(&aux);
        for (uint32_t k = 0; k < BENCH_N; k++) {
            s->gsn.fetch_add(s_count, std::memory_order_relaxed);
            asm volatile("" ::: "memory");
        }
        uint64_t mb_t1 = __rdtscp(&aux);

        s->gsn.store(sn_id, std::memory_order_relaxed);

        fprintf(stderr,
            "[SN%u] GSN Microbenchmark (isolated, %u iters, rdtscp): %.2f ticks/fetch_add(%u)\n"
            "[SN%u] sync_state->gsn address: %p\n",
            sn_id, BENCH_N,
            static_cast<double>(mb_t1 - mb_t0) / BENCH_N, s_count,
            sn_id, static_cast<void*>(&s->gsn));
    }

    // ── Local GSN counter ─────────────────────────────────────────────────────
    // Starts at sn_id; increments by s_count (interleaved assignment).
    uint64_t local_gsn = s->gsn.load(std::memory_order_relaxed);

    // ── Tuning knobs ──────────────────────────────────────────────────────────
    // DEQUEUE_BATCH: items pulled from one RequestQueue per round-robin step.
    //   All clflushopt issued upfront, ONE lfence, then reads in parallel →
    //   pipelined CXL fetches.  Keep ≤ Capacity/4 and small enough to fit on
    //   the stack (KVRequest × DEQUEUE_BATCH bytes).
    // READ_ACK_BATCH: flush read_idx to CXL every this many dequeues per queue.
    //   Safe limit: Capacity / 4 (default queue_depth=1024 → 256 max).
    // 8/32
    const uint32_t DEQUEUE_BATCH  = s->dequeue_batch;
    const uint32_t READ_ACK_BATCH = s->read_ack_batch;

    uint32_t queue_idx    = 0;
    uint64_t total_routed = 0;
    uint64_t empty_polls  = 0;
    uint64_t flush_count  = 0;

    uint64_t* queue_dequeued   = new uint64_t[n]();
    uint64_t* worker_enqueued  = new uint64_t[workers_count]();
    uint64_t* worker_fullwaits = new uint64_t[workers_count]();
    uint32_t* lazy_count       = new uint32_t[n]();

    // Timing accumulators:
    //   A: pipelined batch dequeue (amortised over DEQUEUE_BATCH items)
    //   C: per-item enqueue to WorkerRing (CXL NT stores + 2×sfence, or DRAM release-store)
    //   F: flush_read_idx() cost, amortised over READ_ACK_BATCH items
    uint64_t a_total     = 0;
    uint64_t c_total     = 0;
    uint64_t f_total     = 0;

    uint64_t tsc_start       = __rdtsc();
    uint64_t last_report_tsc = tsc_start;
    uint64_t last_report_ops = 0;

    fprintf(stderr,
        "[SN%u] Started. Polling %u RequestQueues → %u WorkerRings "
        "(workers %u..%u)\n"
        "[SN%u]   Mode: dequeue_batch_pipelined (DEQUEUE_BATCH=%u  READ_ACK_BATCH=%u)\n"
        "[SN%u]   Timing: rdtscp. A=pipelined_batch_deq/op  C=enq/op  F=flush/op\n",
        sn_id, n, workers_count, workers_base, workers_base + workers_count - 1,
        sn_id, DEQUEUE_BATCH, READ_ACK_BATCH,
        sn_id);

    uint32_t aux;
    KVRequest* batch = new KVRequest[DEQUEUE_BATCH];  // sized by runtime parameter

    while (true) {
        bool found_any = false;

        for (uint32_t j = 0; j < n; j++) {

            // ── A: pipelined batch dequeue ────────────────────────────────
            uint64_t t_a0 = __rdtscp(&aux);
            uint64_t count = s->req_consumers[queue_idx]
                                 .dequeue_batch_pipelined(batch, DEQUEUE_BATCH);
            uint64_t t_a1 = __rdtscp(&aux);

            if (count > 0) {
                found_any = true;
                queue_dequeued[queue_idx] += count;
                a_total += t_a1 - t_a0;

                // ── Process each item from the batch ─────────────────────
                for (uint64_t ki = 0; ki < count; ki++) {
                    KVRequest& req = batch[ki];

                    // GSN: register-only, ~1 cycle per item
                    req.gsn = local_gsn;
                    local_gsn += s_count;

                    // ── C: enqueue to WorkerRing ──────────────────────────
                    // local_wid: index into this SN's ring_producers[0..workers_count-1]
                    uint32_t local_wid = (req.worker_id - workers_base) % workers_count;
                    uint64_t wait_rounds = 0;
                    uint64_t t_c0 = __rdtscp(&aux);
                    if (s->use_local_ring) {
                        while (!s->local_ring_producers[local_wid].enqueue(req)) {
                            wait_rounds++;
                            _mm_pause();
                            if (s->stop_flag->load(std::memory_order_relaxed)) {
                                uint64_t t_c1 = __rdtscp(&aux);
                                c_total += t_c1 - t_c0;
                                worker_enqueued[local_wid]++;
                                worker_fullwaits[local_wid] += wait_rounds;
                                total_routed++;
                                goto drain_done;
                            }
                        }
                    } else {
                        while (!s->ring_producers[local_wid].enqueue(req)) {
                            wait_rounds++;
                            _mm_pause();
                            if (s->stop_flag->load(std::memory_order_relaxed)) {
                                uint64_t t_c1 = __rdtscp(&aux);
                                c_total += t_c1 - t_c0;
                                worker_enqueued[local_wid]++;
                                worker_fullwaits[local_wid] += wait_rounds;
                                total_routed++;
                                goto drain_done;
                            }
                        }
                    }
                    uint64_t t_c1 = __rdtscp(&aux);
                    c_total += t_c1 - t_c0;

                    worker_enqueued[local_wid]++;
                    worker_fullwaits[local_wid] += wait_rounds;
                    total_routed++;
                }

                // ── F: lazy flush of read_idx to CXL ─────────────────────
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
                    for (uint32_t i = 0; i < workers_count; i++)
                        total_fw += worker_fullwaits[i];

                    fprintf(stderr,
                        "[SN%u] ops=%7lu  wall=%.1f  "
                        "A(deq/op)=%.1f  C(enq/op)=%.1f  F(flush/op)=%.1f  "
                        "fw=%lu  empty=%lu\n",
                        sn_id, total_routed, wall, a, c, f,
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
    for (uint32_t i = 0; i < workers_count; i++) total_fullwaits += worker_fullwaits[i];

    auto avg = [&](uint64_t sum) -> double {
        return total_routed > 0 ? static_cast<double>(sum) / total_routed : 0.0;
    };
    double avg_wall = total_routed > 0
        ? static_cast<double>(tsc_total) / total_routed : 0.0;

    double avg_flush_raw = flush_count > 0
        ? static_cast<double>(f_total) / flush_count : 0.0;

    fprintf(stderr,
        "[SN%u] ===== Final Statistics =====\n"
        "[SN%u]   total_routed  : %lu\n"
        "[SN%u]   empty_polls   : %lu\n"
        "[SN%u]   ring_fullwaits: %lu\n"
        "[SN%u]   flush calls   : %lu  (batch=%u, raw/flush=%.1f ticks)\n"
        "[SN%u]   wall ticks/op : %.1f\n"
        "[SN%u]\n"
        "[SN%u]   Sub-operation breakdown (rdtscp-serialized, avg ticks/op):\n"
        "[SN%u]     A  pipelined batch deq/op  : %6.1f  (DEQUEUE_BATCH=%u)\n"
        "[SN%u]     C  ring enqueue/op         : %6.1f  (%s)\n"
        "[SN%u]     F  flush amortised/op      : %6.1f  (READ_ACK_BATCH=%u)\n"
        "[SN%u]     loop+rdtscp overhead/op    : %6.1f\n",
        sn_id,
        sn_id, total_routed,
        sn_id, empty_polls,
        sn_id, total_fullwaits,
        sn_id, flush_count, READ_ACK_BATCH, avg_flush_raw,
        sn_id, avg_wall,
        sn_id,
        sn_id,
        sn_id, avg(a_total), DEQUEUE_BATCH,
        sn_id, avg(c_total), s->use_local_ring ? "DRAM release-store" : "CXL NT+2×sfence",
        sn_id, avg(f_total), READ_ACK_BATCH,
        sn_id, avg_wall - avg(a_total) - avg(c_total) - avg(f_total));

    fprintf(stderr, "[SN%u]   Per RequestQueue:\n", sn_id);
    for (uint32_t j = 0; j < n; j++)
        fprintf(stderr, "[SN%u]     ReqQ[%u]: %lu\n", sn_id, j, queue_dequeued[j]);

    fprintf(stderr, "[SN%u]   Per WorkerRing (global IDs):\n", sn_id);
    for (uint32_t i = 0; i < workers_count; i++)
        fprintf(stderr, "[SN%u]     Worker[%u]: enqueued=%lu  fullwaits=%lu\n",
                sn_id, workers_base + i, worker_enqueued[i], worker_fullwaits[i]);

    delete[] batch;
    delete[] queue_dequeued;
    delete[] worker_enqueued;
    delete[] worker_fullwaits;
    delete[] lazy_count;
}

} // namespace TwoRW
