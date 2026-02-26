#pragma once

// ============================================================================
// SharedKV 2RW — Context and Configuration
// ============================================================================

#include "2rw_structs.h"
#include "2rw_layout.h"
#include "cxl_spsc_queue.h"
#include "local_spsc_queue.h"
#include <atomic>
#include <thread>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace TwoRW {

// ============================================================================
// TwoRWConfig — Runtime-configurable benchmark parameters
//
// Set via:
//   TwoRWConfig cfg = TwoRWConfig::from_args(argc, argv);
//   TwoRWConfig cfg = TwoRWConfig::from_env();
//   TwoRWConfig cfg;  // defaults
//
// CLI flags:
//   --numa-node N
//   --num-clients N
//   --num-workers N
//   --num-synchronizers N  (must divide num_workers evenly, default 1)
//   --slots-per-client N   (must be ≤ QUEUE_CAP = 4096)
//   --num-buckets N        (must be power of 2)
//   --queue-depth N        (must be ≤ QUEUE_CAP = 4096, power of 2)
//   --memory-size N        (bytes, e.g. 17179869184 for 16GB)
//
// Env vars (override defaults, overridden by CLI):
//   TWO_RW_NUMA_NODE, TWO_RW_NUM_CLIENTS, TWO_RW_NUM_WORKERS,
//   TWO_RW_NUM_SYNCHRONIZERS, TWO_RW_SLOTS_PER_CLIENT, TWO_RW_NUM_BUCKETS,
//   TWO_RW_QUEUE_DEPTH, TWO_RW_MEMORY_SIZE
// ============================================================================

struct TwoRWConfig {
    int      numa_node          = 2;
    uint32_t num_clients        = 4;          // n: Request/Response threads
    uint32_t num_workers        = 8;          // m: Worker threads
    uint32_t num_synchronizers  = 1;          // s: Synchronizer threads (m % s == 0)
    uint32_t slots_per_client   = 1024;       // K: Pool slots per client
    uint32_t num_buckets        = 4096;       // Hash table buckets (power of 2)
    uint32_t queue_depth        = 1024;       // SPSC ring depth (≤ QUEUE_CAP)
    uint64_t memory_size        = 16ULL << 30; // 16 GB
    int      worker_cpu_start   = -1;         // -1 = auto: 1 + num_synchronizers
    bool     local_workerring   = false;      // WorkerRing on local DRAM (vs CXL)

    // ---- Parsing ----

    static TwoRWConfig from_env() {
        TwoRWConfig c;
        auto get = [](const char* name, auto& dst) {
            const char* v = getenv(name);
            if (v) dst = static_cast<std::remove_reference_t<decltype(dst)>>(
                             strtoull(v, nullptr, 10));
        };
        get("TWO_RW_NUMA_NODE",           c.numa_node);
        get("TWO_RW_NUM_CLIENTS",         c.num_clients);
        get("TWO_RW_NUM_WORKERS",         c.num_workers);
        get("TWO_RW_NUM_SYNCHRONIZERS",   c.num_synchronizers);
        get("TWO_RW_SLOTS_PER_CLIENT",    c.slots_per_client);
        get("TWO_RW_NUM_BUCKETS",         c.num_buckets);
        get("TWO_RW_QUEUE_DEPTH",         c.queue_depth);
        get("TWO_RW_MEMORY_SIZE",         c.memory_size);
        get("TWO_RW_WORKER_CPU_START",    c.worker_cpu_start);
        const char* v_lwr = getenv("TWO_RW_LOCAL_WORKERRING");
        if (v_lwr && v_lwr[0] != '0' && v_lwr[0] != '\0') c.local_workerring = true;
        return c;
    }

    static TwoRWConfig from_args(int argc, char** argv) {
        TwoRWConfig c = from_env();  // env is baseline; CLI overrides
        for (int i = 1; i < argc - 1; i++) {
            auto match = [&](const char* flag, auto& dst) {
                if (strcmp(argv[i], flag) == 0) {
                    dst = static_cast<std::remove_reference_t<decltype(dst)>>(
                        strtoull(argv[i + 1], nullptr, 10));
                    return true;
                }
                return false;
            };
            match("--numa-node",           c.numa_node)           ||
            match("--num-clients",         c.num_clients)         ||
            match("--num-workers",         c.num_workers)         ||
            match("--num-synchronizers",   c.num_synchronizers)   ||
            match("--slots-per-client",    c.slots_per_client)    ||
            match("--num-buckets",         c.num_buckets)         ||
            match("--queue-depth",         c.queue_depth)         ||
            match("--memory-size",         c.memory_size)         ||
            match("--worker-cpu-start",    c.worker_cpu_start);
        }
        return c;
    }

    void print() const {
        const uint32_t s = num_synchronizers;
        const uint32_t wps = (s > 0) ? num_workers / s : num_workers;
        int effective_wcs = (worker_cpu_start < 0)
                            ? (int)(1 + s) : worker_cpu_start;
        fprintf(stderr,
            "[2RW Config]\n"
            "  numa_node            = %d\n"
            "  num_clients (n)      = %u\n"
            "  num_workers (m)      = %u\n"
            "  num_synchronizers(s) = %u  (workers per SN: %u)\n"
            "  slots_per_client     = %u  (pool slots per client)\n"
            "  num_buckets          = %u\n"
            "  queue_depth          = %u\n"
            "  memory_size          = %zu MB\n"
            "  worker_cpu_start     = %d  (workers on CPU %d..%d)\n"
            "  local_workerring     = %s\n",
            numa_node, num_clients, num_workers,
            s, wps,
            slots_per_client, num_buckets, queue_depth,
            (size_t)(memory_size >> 20),
            worker_cpu_start, effective_wcs, effective_wcs + (int)num_workers - 1,
            local_workerring ? "yes (DRAM)" : "no (CXL)");
    }

    bool validate() const {
        if (num_clients == 0 || num_workers == 0) return false;
        if (num_synchronizers == 0 || num_workers % num_synchronizers != 0) return false;
        if (slots_per_client == 0 || slots_per_client > QUEUE_CAP) return false;
        if (queue_depth == 0 || queue_depth > QUEUE_CAP) return false;
        if ((queue_depth & (queue_depth - 1)) != 0) return false;  // power of 2
        if ((num_buckets & (num_buckets - 1)) != 0) return false;
        return true;
    }
};

// ============================================================================
// Thread State Structs (passed to each thread function)
// ============================================================================

struct SyncThreadState {
    // Read-only fields (set at init, never written during hot loop by any thread)
    TwoRWLayout*         layout;
    void*                cxl_base;
    std::atomic<bool>*   stop_flag;
    // CXL WorkerRing producers for this SN's workers [workers_count]
    CXLSpscProducer<KVRequest, QUEUE_CAP>*   ring_producers;
    // Request queue consumers for this SN: [num_clients] (one per client, this SN's slot)
    CXLSpscConsumer<KVRequest, QUEUE_CAP>*   req_consumers;
    // Local DRAM WorkerRing producers for this SN's workers [workers_count]
    LocalSpscProducer<KVRequest, QUEUE_CAP>* local_ring_producers;
    bool     use_local_ring = false;   // true → enqueue into local_ring_producers

    uint32_t num_clients;       // n: total client threads
    uint32_t num_workers;       // m: total worker threads (reference only)
    uint32_t sn_id;             // this SN's index (0..s-1)
    uint32_t num_synchronizers; // s: total synchronizers
    uint32_t workers_base;      // first global worker_id managed by this SN
    uint32_t workers_count;     // number of workers managed by this SN (m/s)

    // GSN — on its own cache line, written by this SN every op.
    // Embedded here (not in TwoRWContext) because TwoRWContext's cache line
    // is shared with std::thread objects that the C++ runtime may write during
    // execution, causing false sharing.
    // SyncThreadState is accessed ONLY by its own Sync thread → zero contention.
    // Initial value = sn_id (for interleaved GSN: SN_k generates k, k+s, k+2s, ...)
    alignas(64) std::atomic<uint64_t> gsn{0};
};

struct WorkerThreadState {
    uint32_t             worker_id;
    TwoRWLayout*         layout;
    void*                cxl_base;
    std::atomic<bool>*   stop_flag;
    DataRegionHeader*    region_meta;    // This worker's DataRegion metadata
    // Consumer for WorkerRing[worker_id] (CXL path)
    CXLSpscConsumer<KVRequest, QUEUE_CAP>* ring_consumer;
    // Consumer for local DRAM WorkerRing[worker_id] (--local-workerring path)
    LocalSpscConsumer<KVRequest, QUEUE_CAP>* local_ring_consumer;
    bool     use_local_ring = false;   // true → dequeue from local_ring_consumer
    // Producers for ResponseQueue[*][worker_id]: base pointer, indexed [j*num_workers+wid]
    CXLSpscProducer<KVResponse, QUEUE_CAP>* resp_producers; // [num_clients * num_workers]
    uint32_t num_clients;
};

struct PollerThreadState {
    TwoRWLayout*         layout;
    void*                cxl_base;
    std::atomic<bool>*   stop_flag;
    uint32_t             num_clients;
    uint32_t             num_workers;
    // UINTR fds for Response Threads — set by Response Threads, read by Poller
    int*                 resp_uintr_fds;   // [num_clients], -1 if not registered
    std::atomic<bool>*   resp_fd_ready;    // [num_clients]
};

// ============================================================================
// TwoRWContext — Top-level Runtime Object
// ============================================================================

struct TwoRWContext {
    TwoRWConfig  config;
    TwoRWLayout  layout;
    void*        cxl_base  = nullptr;

    // FreeIDQueues are local (heap), one per client
    FreeIDQueue<QUEUE_CAP>* free_id_queues = nullptr;  // [num_clients]

    // Request thread → SN producers: [n * s], indexed [client_id * s + sn_id]
    CXLSpscProducer<KVRequest,  QUEUE_CAP>* req_producers  = nullptr;

    // Per-SN consumer/producer arrays (NUMA-allocated on each SN's CPU node)
    // sn_req_consumers[k]  → [n] consumers for SN k's RequestQueues
    // sn_ring_producers[k] → [m/s] CXL producers for SN k's WorkerRings
    CXLSpscConsumer<KVRequest,  QUEUE_CAP>** sn_req_consumers  = nullptr;  // [s] ptrs
    CXLSpscProducer<KVRequest,  QUEUE_CAP>** sn_ring_producers = nullptr;  // [s] ptrs

    // Per-SN NUMA tracking for correct deallocation
    bool* sn_req_con_is_numa   = nullptr;  // [s]
    bool* sn_ring_prod_is_numa = nullptr;  // [s]
    bool* sn_state_is_numa     = nullptr;  // [s]

    // Worker consumers: ring_consumers[m], indexed by global worker_id
    CXLSpscConsumer<KVRequest,  QUEUE_CAP>* ring_consumers = nullptr;  // [m]

    // ResponseQueue handles: resp[client_id * m + worker_id]
    CXLSpscProducer<KVResponse, QUEUE_CAP>* resp_producers = nullptr;
    CXLSpscConsumer<KVResponse, QUEUE_CAP>* resp_consumers = nullptr;

    // Local DRAM WorkerRing (used when config.local_workerring == true)
    // Flat arrays [m], indexed by global worker_id
    LocalSpscQueue<KVRequest,    QUEUE_CAP>* local_rings          = nullptr;
    LocalSpscProducer<KVRequest, QUEUE_CAP>* local_ring_producers = nullptr;
    LocalSpscConsumer<KVRequest, QUEUE_CAP>* local_ring_consumers = nullptr;

    // UINTR: Response Threads register their fds here
    int*                resp_uintr_fds  = nullptr;  // [num_clients]
    std::atomic<bool>*  resp_fd_ready   = nullptr;  // [num_clients]

    // Thread control
    // stop_flag on its own cache line: read by Workers/Poller/SNs in hot loops.
    alignas(64) std::atomic<bool>        stop_flag{false};
    std::vector<std::thread>             synchronizer_threads;  // [s]
    std::thread                          poller_thread;
    std::vector<std::thread>             worker_threads;        // [m]

    // Thread state blobs (heap-allocated, referenced by threads)
    SyncThreadState**  sync_state_ptrs = nullptr;   // [s]
    WorkerThreadState* worker_states   = nullptr;   // [m]
    PollerThreadState* poller_state    = nullptr;
};

// ============================================================================
// Public API
// ============================================================================

// Initialize CXL memory, allocate all context structures.
// Returns non-null on success. Call two_rw_destroy() when done.
TwoRWContext* two_rw_init(const TwoRWConfig& config);

// Start Poller (CPU 0), Synchronizers (CPU 1..s), Workers (CPU s+1..s+m).
void two_rw_start_threads(TwoRWContext* ctx);

// Drain and stop all threads (pipeline drain order per spec §6).
void two_rw_stop(TwoRWContext* ctx);

// Join threads, free memory, unmap CXL.
void two_rw_destroy(TwoRWContext* ctx);

// ============================================================================
// Per-Thread Accessor Helpers (inline, used by YCSB benchmark code)
// ============================================================================

// Request Thread j: acquire a slot_id (spins until available)
inline uint32_t two_rw_acquire_slot(TwoRWContext* ctx, uint32_t client_id) {
    return ctx->free_id_queues[client_id].pop_spin();
}

// Request Thread j: get a pointer to Pool[slot_id] (for writing KV data)
inline KVPoolSlot* two_rw_get_pool_slot(TwoRWContext* ctx, uint32_t slot_id) {
    return CXLPtr<KVPoolSlot>(ctx->layout.pool_slot_offset(slot_id)).get();
}

// Request Thread j: submit request (after filling Pool slot and sfence)
// Routes to req_producers[client_id * s + sn_id] where sn_id = worker_id / (m/s)
inline bool two_rw_submit(TwoRWContext* ctx, uint32_t client_id,
                           uint32_t slot_id, uint32_t worker_id) {
    const uint32_t s             = ctx->config.num_synchronizers;
    const uint32_t workers_per_sn = ctx->config.num_workers / s;
    const uint32_t sn_id         = worker_id / workers_per_sn;
    KVRequest req{};
    req.worker_id = worker_id;
    req.slot_id   = slot_id;
    req.client_id = client_id;
    req.t1        = __rdtsc();
    return ctx->req_producers[client_id * s + sn_id].enqueue(req);
}

// Hash helper: compute worker_id from key
inline uint32_t two_rw_route(const char* key, uint32_t key_len, uint32_t num_workers) {
    uint64_t hash = 14695981039346656037ULL;
    for (uint32_t i = 0; i < key_len; i++) {
        hash ^= static_cast<uint8_t>(key[i]);
        hash *= 1099511628211ULL;
    }
    return static_cast<uint32_t>(hash % num_workers);
}

} // namespace TwoRW
