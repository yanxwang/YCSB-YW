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
//   --slots-per-client N   (must be ≤ QUEUE_CAP = 4096)
//   --num-buckets N        (must be power of 2)
//   --queue-depth N        (must be ≤ QUEUE_CAP = 4096, power of 2)
//   --memory-size N        (bytes, e.g. 17179869184 for 16GB)
//
// Env vars (override defaults, overridden by CLI):
//   TWO_RW_NUMA_NODE, TWO_RW_NUM_CLIENTS, TWO_RW_NUM_WORKERS,
//   TWO_RW_SLOTS_PER_CLIENT, TWO_RW_NUM_BUCKETS,
//   TWO_RW_QUEUE_DEPTH, TWO_RW_MEMORY_SIZE
// ============================================================================

struct TwoRWConfig {
    int      numa_node         = 2;
    uint32_t num_clients       = 4;          // n: Request/Response threads
    uint32_t num_workers       = 8;          // m: Worker threads
    uint32_t slots_per_client  = 1024;       // K: Pool slots per client
    uint32_t num_buckets       = 4096;       // Hash table buckets (power of 2)
    uint32_t queue_depth       = 1024;       // SPSC ring depth (≤ QUEUE_CAP)
    uint64_t memory_size       = 16ULL << 30; // 16 GB
    int      worker_cpu_start  = -1;         // -1 = auto: 2 + num_clients
    bool     local_workerring  = false;      // WorkerRing on local DRAM (vs CXL)

    // ---- Parsing ----

    static TwoRWConfig from_env() {
        TwoRWConfig c;
        auto get = [](const char* name, auto& dst) {
            const char* v = getenv(name);
            if (v) dst = static_cast<std::remove_reference_t<decltype(dst)>>(
                             strtoull(v, nullptr, 10));
        };
        get("TWO_RW_NUMA_NODE",          c.numa_node);
        get("TWO_RW_NUM_CLIENTS",        c.num_clients);
        get("TWO_RW_NUM_WORKERS",        c.num_workers);
        get("TWO_RW_SLOTS_PER_CLIENT",   c.slots_per_client);
        get("TWO_RW_NUM_BUCKETS",        c.num_buckets);
        get("TWO_RW_QUEUE_DEPTH",        c.queue_depth);
        get("TWO_RW_MEMORY_SIZE",        c.memory_size);
        get("TWO_RW_WORKER_CPU_START",   c.worker_cpu_start);
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
            match("--numa-node",          c.numa_node)          ||
            match("--num-clients",        c.num_clients)        ||
            match("--num-workers",        c.num_workers)        ||
            match("--slots-per-client",   c.slots_per_client)   ||
            match("--num-buckets",        c.num_buckets)        ||
            match("--queue-depth",        c.queue_depth)        ||
            match("--memory-size",        c.memory_size)        ||
            match("--worker-cpu-start",   c.worker_cpu_start);
        }
        return c;
    }

    void print() const {
        int effective_wcs = (worker_cpu_start < 0)
                            ? (int)(2 + num_clients) : worker_cpu_start;
        fprintf(stderr,
            "[2RW Config]\n"
            "  numa_node        = %d\n"
            "  num_clients (n)  = %u\n"
            "  num_workers (m)  = %u\n"
            "  slots_per_client = %u  (pool slots per client)\n"
            "  num_buckets      = %u\n"
            "  queue_depth      = %u\n"
            "  memory_size      = %zu MB\n"
            "  worker_cpu_start = %d  (workers on CPU %d..%d)\n"
            "  local_workerring = %s\n",
            numa_node, num_clients, num_workers,
            slots_per_client, num_buckets, queue_depth,
            (size_t)(memory_size >> 20),
            worker_cpu_start, effective_wcs, effective_wcs + (int)num_workers - 1,
            local_workerring ? "yes (DRAM)" : "no (CXL)");
    }

    bool validate() const {
        if (num_clients == 0 || num_workers == 0) return false;
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
    CXLSpscProducer<KVRequest, QUEUE_CAP>* ring_producers;       // [num_workers] CXL
    CXLSpscConsumer<KVRequest, QUEUE_CAP>* req_consumers;        // [num_clients] CXL
    LocalSpscProducer<KVRequest, QUEUE_CAP>* local_ring_producers; // [num_workers] DRAM
    bool     use_local_ring = false;   // true → enqueue into local_ring_producers
    uint32_t num_clients;
    uint32_t num_workers;

    // GSN — on its own cache line, written by Sync every op.
    // Embedded here (not in TwoRWContext) because TwoRWContext's cache line
    // is shared with std::thread objects that the C++ runtime may write during
    // execution, causing false sharing (~2000 tick coherence stall per op).
    // SyncThreadState is accessed ONLY by the Sync thread → zero contention.
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
    // Producers for ResponseQueue[*][worker_id]
    CXLSpscProducer<KVResponse, QUEUE_CAP>* resp_producers; // [num_clients]
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

    // SPSC handles — stored on heap, indexed by thread
    CXLSpscProducer<KVRequest,  QUEUE_CAP>* req_producers  = nullptr; // [num_clients]
    CXLSpscConsumer<KVRequest,  QUEUE_CAP>* req_consumers  = nullptr; // [num_clients]
    CXLSpscProducer<KVRequest,  QUEUE_CAP>* ring_producers = nullptr; // [num_workers] CXL
    CXLSpscConsumer<KVRequest,  QUEUE_CAP>* ring_consumers = nullptr; // [num_workers] CXL
    // resp[client_id * num_workers + worker_id]
    CXLSpscProducer<KVResponse, QUEUE_CAP>* resp_producers = nullptr;
    CXLSpscConsumer<KVResponse, QUEUE_CAP>* resp_consumers = nullptr;

    // Local DRAM WorkerRing (used when config.local_workerring == true)
    LocalSpscQueue<KVRequest,    QUEUE_CAP>* local_rings          = nullptr; // [num_workers]
    LocalSpscProducer<KVRequest, QUEUE_CAP>* local_ring_producers = nullptr; // [num_workers]
    LocalSpscConsumer<KVRequest, QUEUE_CAP>* local_ring_consumers = nullptr; // [num_workers]

    // UINTR: Response Threads register their fds here
    int*                resp_uintr_fds  = nullptr;  // [num_clients]
    std::atomic<bool>*  resp_fd_ready   = nullptr;  // [num_clients]

    // Sync-hot allocations: placed on sync_numa_node (CPU 0's NUMA node) via
    // numa_alloc_onnode + placement new when possible.
    // two_rw_destroy uses these flags to choose between delete[] and numa_free.
    bool sync_alloc_is_numa  = false;  // req_consumers + ring_producers
    bool sync_state_is_numa  = false;  // sync_state

    // Thread control
    // stop_flag on its own cache line: read by Workers/Poller/Sync in hot loops.
    // gsn has been MOVED to SyncThreadState::gsn (only Sync accesses it).
    alignas(64) std::atomic<bool>        stop_flag{false};
    std::thread              synchronizer_thread;
    std::thread              poller_thread;
    std::vector<std::thread> worker_threads;

    // Thread state blobs (heap-allocated, referenced by threads)
    SyncThreadState*         sync_state   = nullptr;
    WorkerThreadState*       worker_states = nullptr; // [num_workers]
    PollerThreadState*       poller_state = nullptr;
};

// ============================================================================
// Public API
// ============================================================================

// Initialize CXL memory, allocate all context structures.
// Returns non-null on success. Call two_rw_destroy() when done.
TwoRWContext* two_rw_init(const TwoRWConfig& config);

// Start Synchronizer (CPU 0), Poller (CPU 1), Workers (CPU 2+n..1+n+m).
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
inline bool two_rw_submit(TwoRWContext* ctx, uint32_t client_id,
                           uint32_t slot_id, uint32_t worker_id) {
    KVRequest req{};
    req.worker_id = worker_id;
    req.slot_id   = slot_id;
    req.client_id = client_id;
    req.t1        = __rdtsc();
    return ctx->req_producers[client_id].enqueue(req);
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