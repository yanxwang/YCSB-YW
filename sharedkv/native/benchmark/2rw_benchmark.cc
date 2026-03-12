// ============================================================================
// SharedKV 2RW YCSB Benchmark — Request/Response Threads + Phase Runner
// ============================================================================

#include "2rw_benchmark.h"
#include "uintr_threading.h"
#include "cxl_ptr.h"

#include <immintrin.h>      // _mm_sfence, _mm_pause, __rdtsc
#include <x86gprintrin.h>   // _stui, _clui (muintr extension)
#include <cstring>
#include <algorithm>
#include <unistd.h>
#include <sys/time.h>
#include <cassert>

namespace TwoRW {

// ============================================================================
// UINTR Handler (empty — interrupt itself wakes uintr_wait)
// ============================================================================

__attribute__((interrupt, target("general-regs-only")))
static void uintr_empty_handler(struct __uintr_frame*, unsigned long long) {}

// ============================================================================
// Internal Helpers
// ============================================================================

static uint64_t wall_usec() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

// FNV-1a 32-bit (for block->key_hash; same base as two_rw_route, truncated)
static inline uint32_t key_hash_fnv1a(const char* data, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (uint32_t i = 0; i < len; i++) {
        h ^= static_cast<uint8_t>(data[i]);
        h *= 1099511628211ULL;
    }
    return static_cast<uint32_t>(h);
}

static OpType ycsb_to_2rw_op(YCSBOpType op) {
    switch (op) {
        case YCSBOpType::INSERT:             return OpType::PUT;
        case YCSBOpType::READ:               return OpType::GET;
        case YCSBOpType::UPDATE:             return OpType::UPDATE;
        case YCSBOpType::READ_MODIFY_WRITE:  return OpType::UPDATE;
        case YCSBOpType::DELETE:             return OpType::DEL;
        default:                             return OpType::GET;
    }
}

// ============================================================================
// Request Thread
//
// Pinned to: cpu_start + client_id
//
// For each op:
//   1. Acquire free slot_id from FreeIDQueue (spin if pool exhausted)
//   2. Fill KVPoolSlot with key/value + t0 timestamp
//   3. Record t0 in ctrl->t0_table[local_idx] for Response Thread
//   4. Submit to RequestQueue[client_id] (spin if full)
//   5. Increment ctrl->submitted
//
// Throughput mode: loops cyclically over ops until should_stop
// Latency  mode:  runs exactly ops_count operations
// ============================================================================

static void* request_thread_fn(void* arg) {
    auto* a = static_cast<ReqThreadArgs*>(arg);
    TwoRWContext* const ctx      = a->ctx;
    const uint32_t      cid      = a->client_id;
    const uint32_t      m        = ctx->config.num_workers;
    const auto&         ops      = *a->operations;
    const uint32_t      op_count = static_cast<uint32_t>(ops.size());

    pin_current_thread_to_cpu(a->cpu_id);
    CXLBase::set(ctx->cxl_base);

    pthread_barrier_wait(a->barrier);

    uint64_t local_submitted = 0;
    uint64_t req_enq_waits  = 0;   // times two_rw_submit() returned false (RequestQueue full)
    uint32_t aux;

    for (uint32_t i = 0; ; i++) {
        // Stop condition
        if (a->throughput_mode) {
            if (*a->should_stop) break;
        } else {
            if (i >= a->ops_count) break;
        }

        const YCSBOperation& op = ops[(a->ops_start + i) % op_count];

        // Step 1: Acquire free block_id (spins — natural flow control)
        uint32_t     block_id = two_rw_acquire_block(ctx, cid);
        UnifiedBlock* block   = two_rw_get_unified_block(ctx, block_id);

        // Step 2: Fill UnifiedBlock via NT stores (bypasses LLC → CXL DRAM directly).
        // NT stores match the proven RequestQueue write path and avoid the dirty-writeback
        // race where a cached worker write to next_block_id/gsn can overwrite RT's fresh data.
        const uint32_t klen  = static_cast<uint32_t>(
            std::min(op.key.size(), size_t(127)));
        const uint32_t khash = key_hash_fnv1a(op.key.data(), klen);

        uint32_t vlen = 0;
        if (op.op_type == YCSBOpType::INSERT  ||
            op.op_type == YCSBOpType::UPDATE   ||
            op.op_type == YCSBOpType::READ_MODIFY_WRITE) {
            vlen = static_cast<uint32_t>(
                std::min(op.value.size(), size_t(1855)));
        }

        // Build header in a local 64B buffer, then NT-store to CXL block.
        // This keeps next_block_id=0 (worker will set it during hash-chain insert).
        alignas(64) char hdr_bytes[64] = {};
        UnifiedBlock* hdr    = reinterpret_cast<UnifiedBlock*>(hdr_bytes);
        hdr->key_hash        = khash;
        hdr->key_len         = static_cast<uint16_t>(klen);
        hdr->val_len         = vlen;
        hdr->is_external     = 0;
        hdr->t0              = __rdtscp(&aux);

        // NT-store header (line 0), key (line 1+), value bytes to CXL DRAM.
        // All NT stores are ordered by the single _mm_sfence() below.
        cxl_nt_memcpy(block, hdr_bytes, 64);
        cxl_nt_memcpy(block->data, op.key.data(), klen);
        if (vlen > 0)
            cxl_nt_memcpy(block->data + klen, op.value.data(), vlen);
        _mm_sfence();  // flush WC buffer to CXL DRAM before block_id flows to SN/Worker

        // Step 3: Submit (spin if RequestQueue full)
        // Readback: verify NT store reached CXL DRAM before handing block_id to worker.
        // After NT store + SFENCE, block cache line is NOT in RT's cache (NT bypasses cache).
        // A plain load here fetches from CXL DRAM directly.
        if (__builtin_expect(block->key_hash != khash, 0)) {
            fprintf(stderr,
                "[RT cid=%u] NT store readback MISMATCH: block_id=%u "
                "block@%p key_hash expected=0x%08x got=0x%08x\n",
                cid, block_id, (void*)block, khash, block->key_hash);
        }

        const uint8_t  op_type   = static_cast<uint8_t>(ycsb_to_2rw_op(op.op_type));
        const uint32_t worker_id = two_rw_route(op.key.data(), klen, m);
        while (!two_rw_submit(ctx, cid, block_id, worker_id, op_type,
                               khash, static_cast<uint16_t>(klen))) {
            two_rw_drain_freeblocks(ctx, cid);  // prevent deadlock with RespThread
            req_enq_waits++;
            _mm_pause();
            if (*a->should_stop) goto req_done;
        }

        // Step 4: Count
        local_submitted++;
        a->ctrl->submitted.fetch_add(1, std::memory_order_relaxed);
    }

req_done:
    a->out_submitted      = local_submitted;
    a->out_req_enq_waits  = req_enq_waits;
    a->ctrl->req_done.store(true, std::memory_order_release);
    return nullptr;
}

// ============================================================================
// Response Thread
//
// Pinned to: cpu_start + num_clients + client_id
//
// 1. Register UINTR handler + create fd → publish to Poller
// 2. Drain all ResponseQueue[client_id][0..m-1] in a loop
// 3. For each response: record latency (t3 - t0), recycle slot_id
// 4. Sleep via uintr_wait() when no work (Poller wakes us edge-triggered)
// 5. Stop when req_done && responded == submitted
// ============================================================================

static void* response_thread_fn(void* arg) {
    auto* a = static_cast<RespThreadArgs*>(arg);
    TwoRWContext* const ctx       = a->ctx;
    const uint32_t      cid       = a->client_id;
    const uint32_t      m         = ctx->config.num_workers;
    const uint32_t      s         = a->num_synchronizers;
    ClientControl&      ctrl      = *a->ctrl;

    pin_current_thread_to_cpu(a->cpu_id);
    CXLBase::set(ctx->cxl_base);

    // Register UINTR handler (only when response poller is active)
    bool uintr_ok = false;
    long fd = -1;
    if (a->use_uintr) {
        if (uintr_register_handler(
                reinterpret_cast<void*>(uintr_empty_handler), 0) == 0) {
            fd = uintr_create_fd(0, 0);
            if (fd >= 0) {
                ctx->resp_uintr_fds[cid] = static_cast<int>(fd);
                ctx->resp_fd_ready[cid].store(true, std::memory_order_release);
                uintr_ok = true;
                if (a->verbose)
                    fprintf(stderr, "[RespTh-%u] UINTR fd=%ld, pinned CPU %d\n",
                            cid, fd, a->cpu_id);
            }
        }
        if (!uintr_ok && a->verbose) {
            fprintf(stderr,
                "[RespTh-%u] UINTR unavailable — busy-poll fallback, CPU %d\n",
                cid, a->cpu_id);
        }
    } else {
        if (a->verbose)
            fprintf(stderr,
                "[RespTh-%u] busy-poll mode (poller_mode=%s), CPU %d\n",
                cid, "worker/none", a->cpu_id);
    }

    if (uintr_ok) _stui();

    pthread_barrier_wait(a->barrier);

    uint64_t completed      = 0;
    uint64_t failed         = 0;
    uint64_t drain_rounds   = 0;   // completed full scans through all m ResponseQueues
    uint64_t empty_rounds   = 0;   // scan rounds where zero responses were dequeued
    uint64_t uintr_wakeups  = 0;   // times woken from uintr_wait() by Poller IPI
    uint64_t recycle_waits  = 0;   // times FreeBlockQueue.push() spun (RT hasn't drained)
    if (a->measure_latency) {
        a->out_stage0_ticks.reserve(65536);
        a->out_stage1_ticks.reserve(65536);
        a->out_stage2_ticks.reserve(65536);
        a->out_total_ticks.reserve(65536);
    }
    if (s > 1) {
        a->out_sn_ops.assign(s, 0);
        if (a->measure_latency) {
            a->out_sn_stage1_ticks.resize(s);
            a->out_sn_total_ticks.resize(s);
        }
    }

    // ── Pipelined batch dequeue from ResponseQueues ───────────────────────────
    // Each dequeue_batch_pipelined(batch, K):
    //   Phase 1: K clflushopt upfront (non-blocking)
    //   Phase 2: ONE lfence
    //   Phase 3: K reads — CPU pipelines CXL fetches in parallel
    //   → replaces K × (clflushopt + lfence + read) with ~1 CXL RTT for K items
    //
    // dequeue_batch_pipelined is lazy (no flush_read_idx inside).
    // flush_read_idx() is called every RESP_READ_ACK_BATCH responses per queue,
    // amortising the clwb+sfence write-back cost across many responses.
    // Safety: RESP_READ_ACK_BATCH ≤ QUEUE_CAP / 4 = 1024 prevents worker stalls.
    static constexpr uint32_t RESP_DEQUEUE_BATCH  = 8;
    static constexpr uint32_t RESP_READ_ACK_BATCH = 32;

    KVResponse batch[RESP_DEQUEUE_BATCH];   // stack-allocated, no heap per iter
    std::vector<uint32_t> lazy_count(m, 0); // flush state per ResponseQueue

    while (true) {
        // Drain all m ResponseQueues for this client
        bool drained_any = true;
        bool drained_any_this_epoch = false;
        while (drained_any) {
            drained_any = false;
            for (uint32_t i = 0; i < m; i++) {
                auto& cons = ctx->resp_consumers[cid * m + i];
                uint64_t count = cons.dequeue_batch_pipelined(batch, RESP_DEQUEUE_BATCH);
                if (count == 0) continue;

                drained_any = true;
                drained_any_this_epoch = true;
                for (uint64_t k = 0; k < count; k++) {
                    const KVResponse& resp = batch[k];

                    // Record latency (t0 comes directly from resp.t0, no side table)
                    if (a->measure_latency) {
                        const uint64_t t0 = resp.t0;
                        const uint64_t t1 = resp.t1;
                        const uint64_t t2 = resp.t2;
                        const uint64_t t3 = resp.t3;
                        if (t0 != 0 && t1 >= t0 && t2 >= t1 && t3 >= t2) {
                            a->out_stage0_ticks.push_back(t1 - t0);
                            a->out_stage1_ticks.push_back(t2 - t1);
                            a->out_stage2_ticks.push_back(t3 - t2);
                            a->out_total_ticks.push_back(t3 - t0);
                            if (s > 1) {
                                const uint32_t sn = resp.sn_id;
                                a->out_sn_stage1_ticks[sn].push_back(t2 - t1);
                                a->out_sn_total_ticks[sn].push_back(t3 - t0);
                            }
                        }
                    }
                    // Per-SN op count (always tracked when s > 1)
                    if (s > 1) {
                        a->out_sn_ops[resp.sn_id]++;
                    }

                    // Recycle block IDs → FreeBlockQueue (unblocks Request Thread).
                    // If push fails (queue full), flush all pending ResponseQueue
                    // read_idx first to break a potential circular deadlock:
                    //   RespTh stuck in recycle(FBQ full) → can't drain ResponseQueues
                    //   → Workers blocked on ResponseQueue full → WorkerRings fill
                    //   → SN blocked → RequestQueues fill → RT blocked on submit
                    //   → RT cache full → drain_freeblocks is no-op → FBQ stays full
                    // Flushing read_idx lets Workers enqueue → drain WorkerRings →
                    // SN flows → RT submits → acquire_block drains cache →
                    // drain_freeblocks pops from FBQ → push succeeds.
                    auto recycle = [&](uint32_t id) {
                        if (id == 0) return;
                        if (ctx->free_block_queues[cid].push(id)) return;  // fast path
                        // Slow path: flush all dirty read_idx to unblock pipeline
                        for (uint32_t qi = 0; qi < m; qi++) {
                            if (lazy_count[qi] > 0) {
                                ctx->resp_consumers[cid * m + qi].flush_read_idx();
                                lazy_count[qi] = 0;
                            }
                        }
                        while (!ctx->free_block_queues[cid].push(id)) {
                            recycle_waits++;
                            if (ctrl.req_done.load(std::memory_order_relaxed)) return;
                            _mm_pause();
                        }
                    };
                    recycle(resp.block_id_a);
                    recycle(resp.block_id_b);

                    if (static_cast<Status>(resp.status) == Status::SUCCESS) {
                        completed++;
                    } else {
                        failed++;
                    }
                }

                // Lazy flush: clwb+sfence read_idx every RESP_READ_ACK_BATCH items
                lazy_count[i] += static_cast<uint32_t>(count);
                if (lazy_count[i] >= RESP_READ_ACK_BATCH) {
                    cons.flush_read_idx();
                    lazy_count[i] = 0;
                }
            }
        }

        drain_rounds++;
        if (!drained_any_this_epoch) {
            empty_rounds++;
        }
        drained_any_this_epoch = false;

        // Flush all dirty read_idx before sleeping.
        // Without this, Workers may see stale read_idx → think ResponseQueue
        // is full → block on enqueue → WorkerRing fills → SN blocks → deadlock.
        for (uint32_t i = 0; i < m; i++) {
            if (lazy_count[i] > 0) {
                ctx->resp_consumers[cid * m + i].flush_read_idx();
                lazy_count[i] = 0;
            }
        }

        // Update shared counter
        ctrl.responded.store(completed + failed, std::memory_order_release);

        // Stop when all submitted requests have been responded to
        if (ctrl.req_done.load(std::memory_order_acquire)) {
            if (ctrl.responded.load(std::memory_order_relaxed) >=
                ctrl.submitted.load(std::memory_order_acquire)) {
                break;
            }
        }

        // Wait for Poller's UINTR edge signal (or busy-poll fallback)
        if (uintr_ok) {
            uintr_wait(0);
            uintr_wakeups++;
        } else {
            _mm_pause();
        }
    }

    // Final flush: commit all pending read_idx to CXL so Workers can reclaim slots
    for (uint32_t i = 0; i < m; i++) {
        ctx->resp_consumers[cid * m + i].flush_read_idx();
    }

    if (uintr_ok) {
        _clui();
        uintr_unregister_handler(0);
    }

    a->out_completed      = completed;
    a->out_failed         = failed;
    a->out_drain_rounds   = drain_rounds;
    a->out_empty_rounds   = empty_rounds;
    a->out_uintr_wakeups  = uintr_wakeups;
    a->out_recycle_waits  = recycle_waits;
    return nullptr;
}

// ============================================================================
// Phase Runner
// ============================================================================

PhaseResult run_2rw_phase(
    TwoRWContext*                     ctx,
    const std::vector<YCSBOperation>& ops,
    uint32_t                          num_clients,
    uint32_t                          global_client_start,
    int                               client_cpu_start,
    bool                              throughput_mode,
    int                               duration_sec,
    uint32_t                          ops_per_client,
    bool                              measure_latency)
{
    assert(!ops.empty());

    const uint32_t n        = num_clients;   // LOCAL client count
    const uint32_t n_total  = ctx->config.num_clients;  // GLOBAL client count
    const uint32_t op_count = static_cast<uint32_t>(ops.size());

    // Barrier: n request threads + n response threads + main thread
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, 2 * n + 1);

    volatile bool stop_flag = false;

    // Per-client control blocks
    std::vector<ClientControl> ctrl(n);

    // Build thread argument arrays
    std::vector<ReqThreadArgs>  req_args(n);
    std::vector<RespThreadArgs> resp_args(n);

    for (uint32_t j = 0; j < n; j++) {
        // Global client_id for this local thread
        const uint32_t global_cid = global_client_start + j;
        // Slice ops evenly across ALL clients (global), each client handles its global slice
        const uint32_t slice = op_count / n_total;
        const uint32_t start = global_cid * slice;

        req_args[j].ctx             = ctx;
        req_args[j].client_id       = global_cid;
        req_args[j].cpu_id          = client_cpu_start + static_cast<int>(j);
        req_args[j].operations      = &ops;
        req_args[j].ops_start       = start;
        req_args[j].ops_count       = throughput_mode ? 0 : ops_per_client;
        req_args[j].throughput_mode = throughput_mode;
        req_args[j].should_stop     = &stop_flag;
        req_args[j].barrier         = &barrier;
        req_args[j].ctrl            = &ctrl[j];

        resp_args[j].ctx               = ctx;
        resp_args[j].client_id         = global_cid;
        resp_args[j].cpu_id            = client_cpu_start + static_cast<int>(n) + static_cast<int>(j);
        resp_args[j].measure_latency   = measure_latency;
        resp_args[j].verbose           = ctx->config.verbose;
        resp_args[j].use_uintr         = ctx->config.resp_thread_uses_uintr();
        resp_args[j].should_stop       = &stop_flag;
        resp_args[j].barrier           = &barrier;
        resp_args[j].ctrl              = &ctrl[j];
        resp_args[j].out_completed     = 0;
        resp_args[j].out_failed        = 0;
        resp_args[j].num_synchronizers = ctx->config.num_synchronizers;
    }

    // Launch all threads (response threads first so UINTR fds are ready early)
    std::vector<pthread_t> req_threads(n), resp_threads(n);
    for (uint32_t j = 0; j < n; j++) {
        pthread_create(&resp_threads[j], nullptr, response_thread_fn, &resp_args[j]);
    }
    for (uint32_t j = 0; j < n; j++) {
        pthread_create(&req_threads[j], nullptr, request_thread_fn, &req_args[j]);
    }

    // Release all threads at the same time
    uint64_t t_start = wall_usec();
    pthread_barrier_wait(&barrier);

    // Throughput mode: run for duration_sec, then signal stop
    if (throughput_mode) {
        sleep(static_cast<unsigned>(duration_sec));
        stop_flag = true;
    }

    // Join request threads (they set req_done on exit)
    for (uint32_t j = 0; j < n; j++) {
        pthread_join(req_threads[j], nullptr);
    }
    // Ensure req_done is set for all clients (handles early exit in throughput mode)
    for (uint32_t j = 0; j < n; j++) {
        ctrl[j].req_done.store(true, std::memory_order_release);
    }

    // Join response threads (they drain and exit when all responses arrive)
    for (uint32_t j = 0; j < n; j++) {
        pthread_join(resp_threads[j], nullptr);
    }
    uint64_t t_end = wall_usec();

    // Verbose: print per-client summary in order 0..n-1 (no interleaving risk here)
    if (ctx->config.verbose) {
        printf("--- Client Thread Summary ---\n");
        for (uint32_t j = 0; j < n; j++) {
            printf("  Client %-3u  submitted=%-10lu  completed=%-10lu  failed=%lu",
                   j,
                   req_args[j].out_submitted,
                   resp_args[j].out_completed,
                   resp_args[j].out_failed);
            if (!resp_args[j].out_total_ticks.empty())
                printf("  latency_samples=%zu", resp_args[j].out_total_ticks.size());
            printf("\n");
        }
        printf("-----------------------------\n");
    }

    // ---- Pipeline Counters (gated by --counters) ----
    if (ctx->config.counters_enabled) {
        printf("\n===== Pipeline Counters =====\n");

        // ── Request Threads ──
        printf("\n--- Request Threads (enqueue → RequestQueue) ---\n");
        printf("  %-8s  %12s  %12s\n", "Client", "submitted", "enq_waits");
        uint64_t total_submitted = 0, total_req_waits = 0;
        for (uint32_t j = 0; j < n; j++) {
            printf("  RT-%-5u  %12lu  %12lu\n",
                   j, req_args[j].out_submitted, req_args[j].out_req_enq_waits);
            total_submitted  += req_args[j].out_submitted;
            total_req_waits  += req_args[j].out_req_enq_waits;
        }
        printf("  %-8s  %12lu  %12lu\n", "TOTAL", total_submitted, total_req_waits);

        // ── Response Threads ──
        printf("\n--- Response Threads (dequeue ← ResponseQueue) ---\n");
        printf("  %-8s  %12s  %12s  %12s  %12s  %12s\n",
               "Client", "completed", "failed", "drain_rounds", "empty_rounds", "uintr_wakes");
        uint64_t tot_comp = 0, tot_fail = 0, tot_drain = 0, tot_empty = 0,
                 tot_uintr = 0, tot_recycle = 0;
        for (uint32_t j = 0; j < n; j++) {
            printf("  RT-%-5u  %12lu  %12lu  %12lu  %12lu  %12lu\n",
                   j,
                   resp_args[j].out_completed,
                   resp_args[j].out_failed,
                   resp_args[j].out_drain_rounds,
                   resp_args[j].out_empty_rounds,
                   resp_args[j].out_uintr_wakeups);
            tot_comp    += resp_args[j].out_completed;
            tot_fail    += resp_args[j].out_failed;
            tot_drain   += resp_args[j].out_drain_rounds;
            tot_empty   += resp_args[j].out_empty_rounds;
            tot_uintr   += resp_args[j].out_uintr_wakeups;
            tot_recycle += resp_args[j].out_recycle_waits;
        }
        printf("  %-8s  %12lu  %12lu  %12lu  %12lu  %12lu\n",
               "TOTAL", tot_comp, tot_fail, tot_drain, tot_empty, tot_uintr);
        printf("  recycle_waits (total): %lu\n", tot_recycle);

        printf("\n=============================\n");
    }

    // Aggregate results
    PhaseResult result{};
    result.duration_usec = t_end - t_start;

    const uint32_t s = ctx->config.num_synchronizers;
    result.num_synchronizers = s;
    if (s > 1) {
        result.sn_ops.assign(s, 0);
        if (measure_latency) {
            result.sn_stage1_ticks.resize(s);
            result.sn_total_ticks.resize(s);
        }
    }

    for (uint32_t j = 0; j < n; j++) {
        result.total_ops  += resp_args[j].out_completed + resp_args[j].out_failed;
        result.failed_ops += resp_args[j].out_failed;
        if (measure_latency) {
            auto append = [](std::vector<uint64_t>& dst,
                             std::vector<uint64_t>& src) {
                dst.insert(dst.end(), src.begin(), src.end());
            };
            append(result.stage0_ticks, resp_args[j].out_stage0_ticks);
            append(result.stage1_ticks, resp_args[j].out_stage1_ticks);
            append(result.stage2_ticks, resp_args[j].out_stage2_ticks);
            append(result.total_ticks,  resp_args[j].out_total_ticks);
            if (s > 1) {
                for (uint32_t k = 0; k < s; k++) {
                    append(result.sn_stage1_ticks[k], resp_args[j].out_sn_stage1_ticks[k]);
                    append(result.sn_total_ticks[k],  resp_args[j].out_sn_total_ticks[k]);
                }
            }
        }
        if (s > 1) {
            for (uint32_t k = 0; k < s; k++)
                result.sn_ops[k] += resp_args[j].out_sn_ops[k];
        }
    }

    pthread_barrier_destroy(&barrier);
    return result;
}

// ============================================================================
// Latency Utilities
// ============================================================================

uint64_t estimate_tsc_mhz() {
    struct timespec ts0, ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    uint64_t tsc0 = __rdtsc();

    usleep(100000);  // 100 ms

    clock_gettime(CLOCK_MONOTONIC, &ts1);
    uint64_t tsc1 = __rdtsc();

    uint64_t elapsed_ns = (ts1.tv_sec - ts0.tv_sec) * 1000000000ULL
                        + (ts1.tv_nsec - ts0.tv_nsec);
    if (elapsed_ns == 0) return 3000;  // fallback: assume 3 GHz

    // ticks / elapsed_ns * 1000 = MHz
    return (tsc1 - tsc0) * 1000ULL / elapsed_ns;
}

void print_latency_tsc(const std::vector<uint64_t>& ticks, uint64_t tsc_mhz) {
    if (ticks.empty() || tsc_mhz == 0) {
        printf("  (no latency samples)\n");
        return;
    }

    std::vector<uint64_t> sorted = ticks;
    std::sort(sorted.begin(), sorted.end());

    // Convert TSC ticks → microseconds: us = ticks / tsc_mhz
    auto to_us = [tsc_mhz](uint64_t t) -> double {
        return static_cast<double>(t) / static_cast<double>(tsc_mhz);
    };

    const size_t n = sorted.size();
    double sum = 0;
    for (uint64_t t : sorted) sum += to_us(t);

    auto pct = [&](double p) -> double {
        size_t idx = static_cast<size_t>(p / 100.0 * n);
        if (idx >= n) idx = n - 1;
        return to_us(sorted[idx]);
    };

    printf("  Latency Statistics (%zu samples, TSC ~%lu MHz):\n", n, tsc_mhz);
    printf("    Average : %8.2f us\n", sum / n);
    printf("    Median  : %8.2f us   (p50)\n", pct(50));
    printf("    p75     : %8.2f us\n", pct(75));
    printf("    p90     : %8.2f us\n", pct(90));
    printf("    p95     : %8.2f us\n", pct(95));
    printf("    p99     : %8.2f us\n", pct(99));
    printf("    p99.9   : %8.2f us\n", pct(99.9));
    printf("    Max     : %8.2f us\n", to_us(sorted.back()));
}

// ============================================================================
// print_latency_decomposed — one row per stage + total
// ============================================================================

void print_latency_decomposed(const PhaseResult& r, uint64_t tsc_mhz) {
    if (r.total_ticks.empty() || tsc_mhz == 0) {
        printf("  (no latency samples)\n");
        return;
    }

    // Helper: compute avg and p50/p99/max from a tick vector (us)
    struct Stats {
        double avg, p50, p99, p999, max;
    };
    auto compute = [tsc_mhz](const std::vector<uint64_t>& v) -> Stats {
        if (v.empty()) return {};
        std::vector<uint64_t> s = v;
        std::sort(s.begin(), s.end());
        const size_t n = s.size();
        auto to_us = [tsc_mhz](uint64_t t) {
            return static_cast<double>(t) / static_cast<double>(tsc_mhz);
        };
        auto pct = [&](double p) {
            size_t i = static_cast<size_t>(p / 100.0 * n);
            if (i >= n) i = n - 1;
            return to_us(s[i]);
        };
        double sum = 0;
        for (uint64_t t : s) sum += to_us(t);
        return {sum / n, pct(50), pct(99), pct(99.9), to_us(s.back())};
    };

    Stats s0 = compute(r.stage0_ticks);
    Stats s1 = compute(r.stage1_ticks);
    Stats s2 = compute(r.stage2_ticks);
    Stats st = compute(r.total_ticks);

    printf("  Decomposed Latency (%zu samples, TSC ~%lu MHz):\n",
           r.total_ticks.size(), tsc_mhz);
    printf("  %-28s  %8s  %8s  %8s  %8s\n",
           "Stage", "avg(us)", "p50", "p99", "max");
    printf("  %-28s  %8.2f  %8.2f  %8.2f  %8.2f\n",
           "t0→t1 pool→RequestQueue", s0.avg, s0.p50, s0.p99, s0.max);
    printf("  %-28s  %8.2f  %8.2f  %8.2f  %8.2f\n",
           "t1→t2 Sync dispatch",     s1.avg, s1.p50, s1.p99, s1.max);
    printf("  %-28s  %8.2f  %8.2f  %8.2f  %8.2f\n",
           "t2→t3 Worker execution",  s2.avg, s2.p50, s2.p99, s2.max);
    printf("  %-28s  %8.2f  %8.2f  %8.2f  %8.2f\n",
           "t0→t3 end-to-end",        st.avg, st.p50, st.p99, st.max);

    // Per-SN breakdown (shown only when s > 1 and latency was measured)
    if (r.num_synchronizers > 1 && !r.sn_stage1_ticks.empty()) {
        printf("\n  Per-SN Latency (s=%u):\n", r.num_synchronizers);
        printf("  %-6s  %-28s  %8s  %8s  %8s  %8s\n",
               "SN", "Stage", "avg(us)", "p50", "p99", "max");
        for (uint32_t k = 0; k < r.num_synchronizers; k++) {
            Stats s1k = compute(r.sn_stage1_ticks[k]);
            Stats stk = compute(r.sn_total_ticks[k]);
            printf("  SN%-4u  %-28s  %8.2f  %8.2f  %8.2f  %8.2f\n",
                   k, "t1→t2 Sync dispatch", s1k.avg, s1k.p50, s1k.p99, s1k.max);
            printf("  SN%-4u  %-28s  %8.2f  %8.2f  %8.2f  %8.2f\n",
                   k, "t0→t3 end-to-end", stk.avg, stk.p50, stk.p99, stk.max);
        }
    }
}

} // namespace TwoRW