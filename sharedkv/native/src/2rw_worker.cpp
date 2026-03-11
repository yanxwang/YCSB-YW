// ============================================================================
// SharedKV 2RW — Worker Thread (CPU w_base+i)
//
// Responsibilities:
//   1. Dequeue KVRequest from WorkerRing[worker_id]
//   2. Read key/val/hash from UnifiedBlock[block_id] (written by RT, sfenced)
//   3. Execute KV operation on own bucket partition (no locks)
//   4. Write KVResponse to ResponseQueue[client_id][worker_id]
//
// Block Swap Protocol (PUT/UPDATE):
//   RT fills block_new = UnifiedBlock[id_new], sfence, enqueues id_new.
//   Worker traverses bucket chain, finds id_old (or not), swaps pointers,
//   returns id_old via block_id_a for recycling. Zero memcpy.
//
// GET:   return val_addr = pointer into found block's data; recycle id_req.
// DEL:   unlink data block; recycle both id_cmd (block_id_a) and id_data (block_id_b).
//
// Memory fence protocol:
//   - sfence before updating bucket head / chain pointer (chain integrity)
//   - sfence inside CXLSpscProducer::enqueue (result visibility)
//
// UINTR support (when use_uintr=true, poller_mode=worker|dual):
//   Worker registers UINTR handler, publishes fd for WorkerPoller.
//   On empty WorkerRing, sleeps via uintr_wait(0) instead of _mm_pause().
//   WorkerPoller detects empty→non-empty transition and sends IPI to wake.
//
// Stats:
//   When stats_enabled, each kv_* function records chain traversal depth
//   into WorkerOpStats (heap-allocated, pointer stored in WorkerThreadState).
//   No output is produced from the worker thread itself — the main thread
//   prints all stats in order via two_rw_print_worker_stats() after joining.
// ============================================================================

#include "2rw_context.h"
#include "uintr_threading.h"
#include <cstdio>
#include <cstring>
#include <immintrin.h>
#include <emmintrin.h>
#include <x86gprintrin.h>

namespace TwoRW {

// ============================================================================
// UINTR Handler (empty — interrupt itself wakes uintr_wait)
// ============================================================================

__attribute__((interrupt, target("general-regs-only")))
static void worker_uintr_handler(struct __uintr_frame*, unsigned long long) {}

// ============================================================================
// Inline address helper — avoid casting noise throughout
// ============================================================================

static inline UnifiedBlock* ub(void* base, uint32_t id) {
    return reinterpret_cast<UnifiedBlock*>(
        static_cast<char*>(base) + static_cast<uint64_t>(id) * 2048ULL);
}

// ============================================================================
// KV PUT / UPDATE — Block Swap Protocol
//
// RT wrote key/val/hash into block_new and sfenced before enqueuing.
// Worker swaps id_new in, returns id_old via block_id_a (0 = new key).
// ============================================================================

static thread_local uint64_t put_call_count = 0;

static KVResponse kv_put(const KVRequest& req, void* base,
                           CXLBucket* buckets, uint32_t num_buckets,
                           WorkerOpStats* stats) {
    KVResponse resp{};
    put_call_count++;

    const uint32_t id_new    = req.block_id;
    UnifiedBlock*  block_new = ub(base, id_new);

    // Diagnostic: volatile reads with checkpoint markers
    volatile uint32_t dbg_step = 1;  // step 1: read block header
    const uint32_t key_hash = block_new->key_hash;
    const uint16_t key_len  = block_new->key_len;
    const char*    key      = block_new->data;

    dbg_step = 2;  // step 2: read bucket head
    const uint32_t bkt_id = key_hash % num_buckets;
    CXLBucket*     bucket = &buckets[bkt_id];

    uint32_t prev_id = 0;
    uint32_t cur_id  = bucket->head_block_id;
    uint32_t id_old  = 0;
    uint32_t depth   = 0;

    dbg_step = 3;  // step 3: chain traversal
    while (cur_id != 0) {
        UnifiedBlock* cur = ub(base, cur_id);
        depth++;
        dbg_step = 4;  // step 4: reading chain node fields
        uint32_t cur_hash = cur->key_hash;
        uint16_t cur_klen = cur->key_len;
        dbg_step = 5;  // step 5: comparing
        if (cur_hash == key_hash && cur_klen == key_len &&
            memcmp(cur->data, key, key_len) == 0) {
            id_old = cur_id;
            break;
        }
        prev_id = cur_id;
        dbg_step = 6;  // step 6: reading next pointer
        cur_id  = static_cast<uint32_t>(cur->next_block_id);
    }

    if (id_old != 0) {
        block_new->next_block_id = ub(base, id_old)->next_block_id;
        _mm_sfence();
        if (prev_id == 0)
            bucket->head_block_id = id_new;
        else
            ub(base, prev_id)->next_block_id = id_new;
    } else {
        block_new->next_block_id = bucket->head_block_id;
        _mm_sfence();
        bucket->head_block_id = id_new;
    }

    block_new->gsn = req.gsn;

    if (stats) stats->record(stats->put, depth, id_old != 0);

    resp.status     = static_cast<uint16_t>(Status::SUCCESS);
    resp.op_type    = req.op_type;
    resp.block_id_a = id_old;
    resp.block_id_b = 0;
    resp.t0         = block_new->t0;
    return resp;
}

// ============================================================================
// KV GET — zero-copy
// ============================================================================

static KVResponse kv_get(const KVRequest& req, void* base,
                           CXLBucket* buckets, uint32_t num_buckets,
                           WorkerOpStats* stats) {
    KVResponse resp{};

    const uint32_t id_req    = req.block_id;
    UnifiedBlock*  req_block = ub(base, id_req);

    const uint32_t key_hash = req_block->key_hash;
    const uint16_t key_len  = req_block->key_len;
    const char*    key      = req_block->data;

    resp.op_type    = req.op_type;
    resp.block_id_a = id_req;
    resp.block_id_b = 0;
    resp.t0         = req_block->t0;

    _mm_lfence();

    const uint32_t bkt_id = key_hash % num_buckets;
    uint32_t cur_id = buckets[bkt_id].head_block_id;
    uint32_t depth  = 0;

    while (cur_id != 0) {
        UnifiedBlock* cur = ub(base, cur_id);
        depth++;
        if (cur->key_hash == key_hash && cur->key_len == key_len &&
            memcmp(cur->data, key, key_len) == 0) {
            if (stats) stats->record(stats->get, depth, true);
            resp.status   = static_cast<uint16_t>(Status::SUCCESS);
            resp.val_addr = reinterpret_cast<uint64_t>(cur->data + key_len);
            resp.val_len  = cur->val_len;
            return resp;
        }
        cur_id = static_cast<uint32_t>(cur->next_block_id);
    }

    if (stats) stats->record(stats->get, depth, false);
    resp.status = static_cast<uint16_t>(Status::NOT_FOUND);
    return resp;
}

// ============================================================================
// KV DELETE
// ============================================================================

static KVResponse kv_del(const KVRequest& req, void* base,
                           CXLBucket* buckets, uint32_t num_buckets,
                           WorkerOpStats* stats) {
    KVResponse resp{};

    const uint32_t id_cmd    = req.block_id;
    UnifiedBlock*  cmd_block = ub(base, id_cmd);

    const uint32_t key_hash = cmd_block->key_hash;
    const uint16_t key_len  = cmd_block->key_len;
    const char*    key      = cmd_block->data;

    resp.op_type    = req.op_type;
    resp.block_id_a = id_cmd;
    resp.block_id_b = 0;
    resp.t0         = cmd_block->t0;

    const uint32_t bkt_id = key_hash % num_buckets;
    CXLBucket*     bucket = &buckets[bkt_id];

    uint32_t prev_id = 0;
    uint32_t cur_id  = bucket->head_block_id;
    uint32_t depth   = 0;

    while (cur_id != 0) {
        UnifiedBlock* cur = ub(base, cur_id);
        depth++;
        if (cur->key_hash == key_hash && cur->key_len == key_len &&
            memcmp(cur->data, key, key_len) == 0) {
            uint32_t next_id = static_cast<uint32_t>(cur->next_block_id);
            if (prev_id == 0)
                bucket->head_block_id = next_id;
            else
                ub(base, prev_id)->next_block_id = next_id;
            _mm_sfence();

            if (stats) stats->record(stats->del, depth, true);
            resp.status     = static_cast<uint16_t>(Status::SUCCESS);
            resp.block_id_b = cur_id;
            return resp;
        }
        prev_id = cur_id;
        cur_id  = static_cast<uint32_t>(cur->next_block_id);
    }

    if (stats) stats->record(stats->del, depth, false);
    resp.status = static_cast<uint16_t>(Status::NOT_FOUND);
    return resp;
}

// ============================================================================
// Worker Main Loop
// ============================================================================

void two_rw_worker_run(WorkerThreadState* s) {
    const uint32_t wid         = s->worker_id;
    const uint32_t num_workers = s->layout->num_workers;
    const uint32_t num_buckets = s->layout->num_buckets;

    CXLBucket* buckets   = s->layout->bucket_table(s->cxl_base);
    void* pool_base = static_cast<char*>(s->cxl_base) + s->layout->unifiedblockpool_off;

    // Allocate stats on heap only when enabled; nullptr = stats off (zero overhead).
    WorkerOpStats* stats = s->stats_enabled ? new WorkerOpStats{} : nullptr;

    // ---- UINTR setup (when poller_mode=worker|dual) ----
    bool uintr_ok = false;
    if (s->use_uintr) {
        if (uintr_register_handler(
                reinterpret_cast<void*>(worker_uintr_handler), 0) == 0) {
            long fd = uintr_create_fd(0, 0);
            if (fd >= 0) {
                s->worker_uintr_fds[wid] = static_cast<int>(fd);
                s->worker_fd_ready[wid].store(true, std::memory_order_release);
                uintr_ok = true;
            }
        }
        if (uintr_ok) _stui();
    }

    uint64_t ops_done       = 0;
    uint64_t empty_polls    = 0;
    uint64_t resp_fullwaits = 0;   // times enqueue to ResponseQueue failed (CXL ring full)
    uint64_t uintr_wakeups  = 0;
    uint32_t aux;

    while (true) {
        KVRequest req;
        bool got = s->use_local_ring
                   ? s->local_ring_consumer->dequeue(req)
                   : s->ring_consumer->dequeue(req);

        if (got) {
            req.t2 = __rdtscp(&aux);

            // Log first few ops and every 1000th to track progress
            if (ops_done < 5 || (ops_done % 1000 == 0 && ops_done <= 10000)) {
                fprintf(stderr, "[W%u] op#%lu block_id=%u op=%u\n",
                        wid, ops_done, req.block_id, req.op_type);
            }

            KVResponse resp;
            switch (static_cast<OpType>(req.op_type)) {
                case OpType::PUT:
                case OpType::UPDATE:
                    resp = kv_put(req, pool_base, buckets, num_buckets, stats);
                    break;
                case OpType::GET:
                    resp = kv_get(req, pool_base, buckets, num_buckets, stats);
                    break;
                case OpType::DEL:
                    resp = kv_del(req, pool_base, buckets, num_buckets, stats);
                    break;
                default:
                    resp = KVResponse{};
                    resp.status     = static_cast<uint16_t>(Status::ERROR);
                    resp.op_type    = req.op_type;
                    resp.block_id_a = req.block_id;
                    resp.t0         = ub(pool_base, req.block_id)->t0;
                    break;
            }

            resp.sn_id = s->sn_id_for_resp;
            resp.t1    = req.t1;
            resp.t2    = req.t2;
            resp.t3    = __rdtscp(&aux);

            const uint32_t resp_idx = req.client_id * num_workers + wid;
            auto& resp_prod = s->resp_producers[resp_idx];

            while (!resp_prod.enqueue(resp)) {
                resp_fullwaits++;
                _mm_pause();
                if (s->stop_flag->load(std::memory_order_relaxed)) break;
            }

            ops_done++;
        } else {
            empty_polls++;
            if (s->stop_flag->load(std::memory_order_acquire)) {
                bool empty;
                if (s->use_local_ring) {
                    empty = s->local_ring_consumer->is_empty_fresh();
                } else {
                    s->ring_consumer->refresh_write_idx();
                    empty = s->ring_consumer->is_empty();
                }
                if (empty) break;
            }

            // Sleep via UINTR or busy-poll
            if (uintr_ok) {
                uintr_wait(0);
                uintr_wakeups++;
            } else {
                _mm_pause();
            }
        }
    }

    // UINTR cleanup
    if (uintr_ok) {
        _clui();
        uintr_unregister_handler(0);
    }

    // Store exit stats for the main thread to collect after join.
    // No printing here — main thread prints all workers in order.
    s->exit_ops_done       = ops_done;
    s->exit_empty_polls    = empty_polls;
    s->exit_resp_fullwaits = resp_fullwaits;
    s->exit_uintr_wakeups  = uintr_wakeups;
    s->exit_stats          = stats;   // transfer ownership; main thread frees
}

} // namespace TwoRW
