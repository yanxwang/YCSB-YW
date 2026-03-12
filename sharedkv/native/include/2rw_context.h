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
#include <string>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace TwoRW {

// ============================================================================
// PollerMode — controls which threads use UINTR sleep/wake vs busy-poll
//
//   response  : 1 poller on CPU 0, polls ResponseQueues → UINTR Response Threads
//   worker    : 1 poller on CPU 1, polls WorkerRings → UINTR Worker Threads
//   dual      : 2 pollers (CPU 0 + CPU 1), both of the above
//   none      : 0 pollers, all threads busy-poll (Response + Worker)
//
// CPU 0 and CPU 1 are always reserved for pollers regardless of mode.
// Synchronizers start from CPU 2.
// ============================================================================

enum class PollerMode : uint8_t {
    RESPONSE = 0,   // default: current behavior
    WORKER   = 1,
    DUAL     = 2,
    NONE     = 3,
};

static inline const char* poller_mode_name(PollerMode m) {
    switch (m) {
        case PollerMode::RESPONSE: return "response";
        case PollerMode::WORKER:   return "worker";
        case PollerMode::DUAL:     return "dual";
        case PollerMode::NONE:     return "none";
    }
    return "unknown";
}

static inline PollerMode parse_poller_mode(const char* s) {
    if (!s) return PollerMode::RESPONSE;
    if (strcmp(s, "worker")   == 0) return PollerMode::WORKER;
    if (strcmp(s, "dual")     == 0) return PollerMode::DUAL;
    if (strcmp(s, "none")     == 0) return PollerMode::NONE;
    return PollerMode::RESPONSE;
}

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
//   --slots-per-client N   (blocks pre-allocated per client, must be ≤ QUEUE_CAP)
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
    uint32_t slots_per_client   = 1024;       // K: blocks pre-allocated per client at init
    uint32_t num_buckets        = 4096;       // Hash table buckets (power of 2)
    uint32_t queue_depth        = 1024;       // SPSC ring depth (≤ QUEUE_CAP)
    uint64_t memory_size        = 64ULL << 30; // 64 GB
    int      worker_cpu_start   = -1;         // -1 = auto: 1 + num_synchronizers
    bool     local_workerring   = false;      // WorkerRing on local DRAM (vs CXL)
    bool     stats_enabled      = false;      // per-op chain traversal stats + bucket scan
    bool     verbose            = false;      // per-thread lifecycle messages + client summary
    bool     counters_enabled   = false;      // pipeline-wide enqueue/dequeue counters per role
    uint32_t dequeue_batch      = 8;          // SN: items pulled per RequestQueue per round-robin step
    uint32_t read_ack_batch     = 32;         // SN: flush read_idx every N dequeues per queue
    PollerMode poller_mode      = PollerMode::RESPONSE;  // --poller-mode={response,worker,dual,none}
    bool     worker_check       = false;                 // --worker-check: enable per-op route/block diagnostics

    // ---- Multi-machine cluster config ----
    // When cluster_config_path is empty, single-machine mode (backward compat).
    // When set, global counts come from config file; local ranges from node_id.
    std::string cluster_config_path;              // --cluster-config <path>
    std::string cxl_device_path;                  // --cxl-device <path> (DAX devdax mode)
    uint64_t    cxl_phys_base   = 0;              // --cxl-phys-base <addr> (system-ram mode via /dev/mem)
    uint32_t node_id            = 0;              // --node-id (0 = master)
    uint32_t num_nodes          = 1;              // total machines in cluster
    // Per-node ranges (global indices)
    uint32_t global_sn_start      = 0;
    uint32_t global_sn_count      = 0;            // 0 = auto (all local in single-machine)
    uint32_t global_worker_start  = 0;
    uint32_t global_worker_count  = 0;            // 0 = auto
    uint32_t global_client_start  = 0;
    uint32_t global_client_count  = 0;            // 0 = auto
    // Flexible worker-per-SN allocation (opt-in via --worker-per-sn "6,4,6,4")
    // Empty = default even division.  When set, sum must equal num_workers.
    std::vector<uint32_t> worker_per_sn;          // [s] workers per SN

    bool is_multi_node() const { return !cluster_config_path.empty() && num_nodes > 1; }
    bool is_master()     const { return node_id == 0; }

    // Derived helpers
    bool has_response_poller() const { return poller_mode == PollerMode::RESPONSE || poller_mode == PollerMode::DUAL; }
    bool has_worker_poller()   const { return poller_mode == PollerMode::WORKER   || poller_mode == PollerMode::DUAL; }
    bool resp_thread_uses_uintr() const { return has_response_poller(); }
    bool worker_thread_uses_uintr() const { return has_worker_poller(); }

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
        const char* v_pm = getenv("TWO_RW_POLLER_MODE");
        if (v_pm) c.poller_mode = parse_poller_mode(v_pm);
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
            match("--worker-cpu-start",    c.worker_cpu_start) ||
            match("--node-id",             c.node_id)          ||
            match("--global-sn-start",     c.global_sn_start)      ||
            match("--global-sn-count",     c.global_sn_count)      ||
            match("--global-worker-start", c.global_worker_start)  ||
            match("--global-worker-count", c.global_worker_count)  ||
            match("--global-client-start", c.global_client_start)  ||
            match("--global-client-count", c.global_client_count)  ||
            match("--num-nodes",           c.num_nodes);
            // String flags handled separately
            if (strcmp(argv[i], "--poller-mode") == 0 && i + 1 < argc) {
                c.poller_mode = parse_poller_mode(argv[i + 1]);
            }
            if (strcmp(argv[i], "--cluster-config") == 0 && i + 1 < argc) {
                c.cluster_config_path = argv[i + 1];
            }
            if (strcmp(argv[i], "--cxl-device") == 0 && i + 1 < argc) {
                c.cxl_device_path = argv[i + 1];
            }
            if (strcmp(argv[i], "--worker-check") == 0) {
                c.worker_check = true;
            }
            if (strcmp(argv[i], "--worker-per-sn") == 0 && i + 1 < argc) {
                c.worker_per_sn.clear();
                const char* p = argv[i + 1];
                while (*p) {
                    char* end;
                    uint32_t v = static_cast<uint32_t>(strtoul(p, &end, 10));
                    c.worker_per_sn.push_back(v);
                    if (*end == ',') end++;
                    p = end;
                }
            }
        }
        return c;
    }

    void print() const {
        const uint32_t s = num_synchronizers;
        const uint32_t wps = (s > 0) ? num_workers / s : num_workers;
        int effective_wcs = (worker_cpu_start < 0)
                            ? (int)(2 + s) : worker_cpu_start;
        fprintf(stderr,
            "[2RW Config]\n"
            "  numa_node            = %d\n"
            "  num_clients (n)      = %u\n"
            "  num_workers (m)      = %u\n"
            "  num_synchronizers(s) = %u  (workers per SN: %u)\n"
            "  slots_per_client     = %u  (blocks pre-alloc per client)\n"
            "  num_buckets          = %u\n"
            "  queue_depth          = %u\n"
            "  memory_size          = %zu MB\n"
            "  worker_cpu_start     = %d  (workers on CPU %d..%d)\n"
            "  local_workerring     = %s\n"
            "  poller_mode          = %s\n",
            numa_node, num_clients, num_workers,
            s, wps,
            slots_per_client, num_buckets, queue_depth,
            (size_t)(memory_size >> 20),
            worker_cpu_start, effective_wcs, effective_wcs + (int)num_workers - 1,
            local_workerring ? "yes (DRAM)" : "no (CXL)",
            poller_mode_name(poller_mode));
    }

    // Fill in auto-derived fields (call after parsing).
    void finalize_ranges() {
        if (!is_multi_node()) {
            // Single-machine: force starts to 0 (ignore stray --global-*-start flags)
            global_sn_start     = 0;
            global_worker_start = 0;
            global_client_start = 0;
        }
        if (global_sn_count == 0)     global_sn_count     = num_synchronizers;
        if (global_worker_count == 0) global_worker_count = num_workers;
        if (global_client_count == 0) global_client_count = num_clients;
    }

    // Build the worker_to_sn lookup table.  Returns a heap-allocated array
    // of size num_workers.  Caller owns the memory.
    uint8_t* build_worker_to_sn() const {
        uint8_t* tbl = new uint8_t[num_workers];
        if (!worker_per_sn.empty()) {
            // Custom: user-specified workers per SN
            uint32_t w = 0;
            for (uint32_t sn = 0; sn < num_synchronizers && sn < worker_per_sn.size(); sn++) {
                for (uint32_t j = 0; j < worker_per_sn[sn]; j++) {
                    if (w < num_workers) tbl[w++] = static_cast<uint8_t>(sn);
                }
            }
            // Fill remaining (shouldn't happen if validated)
            while (w < num_workers) tbl[w++] = static_cast<uint8_t>(num_synchronizers - 1);
        } else {
            // Default: even division
            uint32_t wps = num_workers / num_synchronizers;
            for (uint32_t w = 0; w < num_workers; w++)
                tbl[w] = static_cast<uint8_t>(w / wps);
        }
        return tbl;
    }

    // Compute workers_base for a given SN (first global worker_id it manages).
    uint32_t workers_base_for_sn(const uint8_t* worker_to_sn, uint32_t sn_id) const {
        for (uint32_t w = 0; w < num_workers; w++)
            if (worker_to_sn[w] == sn_id) return w;
        return 0;
    }

    // Compute workers_count for a given SN.
    uint32_t workers_count_for_sn(const uint8_t* worker_to_sn, uint32_t sn_id) const {
        uint32_t cnt = 0;
        for (uint32_t w = 0; w < num_workers; w++)
            if (worker_to_sn[w] == sn_id) cnt++;
        return cnt;
    }

    bool validate() const {
        if (num_clients == 0 || num_workers == 0) return false;
        if (num_synchronizers == 0) return false;
        if (slots_per_client == 0 || slots_per_client > QUEUE_CAP) return false;
        if (queue_depth == 0 || queue_depth > QUEUE_CAP) return false;
        if ((queue_depth & (queue_depth - 1)) != 0) return false;  // power of 2
        if ((num_buckets & (num_buckets - 1)) != 0) return false;

        if (!worker_per_sn.empty()) {
            // Flexible mode: validate sum == num_workers, size == num_synchronizers
            if (worker_per_sn.size() != num_synchronizers) return false;
            uint32_t sum = 0;
            for (auto v : worker_per_sn) sum += v;
            if (sum != num_workers) return false;
        } else {
            // Default mode: must divide evenly
            if (num_workers % num_synchronizers != 0) return false;
        }

        if (num_nodes > MAX_NODES) return false;
        return true;
    }
};

// ============================================================================
// WorkerOpStats — per-Worker operation traversal statistics
//
// Accumulated during hot loop (when stats_enabled=true), stored in
// WorkerThreadState.exit_stats at thread exit.  Ownership transfers to the
// main thread, which prints and deletes them via two_rw_print_worker_stats().
//
// depth = chain nodes dereferenced in the while loop per operation.
// CXL random reads per op = 1 (request block) + depth (chain nodes).
// ============================================================================

struct WorkerOpStats {
    struct PerOp {
        uint64_t total     = 0;
        uint64_t hit       = 0;     // key found (GET/DEL: match; PUT: update existing)
        uint64_t miss      = 0;     // key not found (GET/DEL: miss; PUT: new insert)
        uint64_t chain_sum = 0;     // total chain nodes traversed across all ops
        // hist[d] = # ops traversing exactly d nodes (d=0..15; d=16 means 16+)
        uint64_t hist[17]  = {};
    };
    PerOp get;
    PerOp put;        // PUT / UPDATE
    PerOp del;

    void record(PerOp& op, uint32_t depth, bool found) {
        op.total++;
        op.chain_sum += depth;
        if (found) op.hit++;
        else       op.miss++;
        op.hist[(depth < 16) ? depth : 16]++;
    }
};

// ============================================================================
// LocalBlockCache — per Request Thread block ID pool
//
// Three-level design (see dynamic_membership_management.txt §3):
//   ① LocalBlockCache (LBC): RT-private LIFO stack, capacity 4096, zero sync.
//   ② FreeBlockQueue (FBQ): SPSC bridge from Response Thread → RT, capacity 4096.
//   ③ CXL Bitmap (Global Reservoir): shared bitmap on CXL, atomic bit ops.
//
// Acquire:  LBC pop → FBQ drain→LBC → bitmap batch alloc(256)
// Drain:    LBC full → spill 256 to bitmap; then FBQ→LBC
// Recycle:  RespThread push → FBQ (unchanged)
// ============================================================================

static constexpr uint32_t LOCAL_CACHE_CAP  = QUEUE_CAP;      // 4096
static constexpr uint32_t BITMAP_BATCH     = 256;             // batch alloc/free size

struct alignas(64) LocalBlockCache {
    uint32_t stack[LOCAL_CACHE_CAP];
    uint32_t top = 0;        // number of valid entries in stack[]
    uint32_t scan_hint = 0;  // word index into bitmap, per-client to reduce contention
};

// ============================================================================
// CXL Bitmap atomic primitives
//
// Bitmap convention: bit=1 → FREE, bit=0 → IN-USE.
// Uses x86 LOCK BTR (alloc: test-and-reset) and LOCK BTS (free: test-and-set).
// ============================================================================

// Atomically test and reset bit. Returns true if bit was previously set (= successful alloc).
static inline bool atomic_btr(volatile uint64_t* word, uint32_t bit) {
    bool was_set;
    asm volatile("lock btrq %2, %0"
                 : "+m"(*word), "=@ccc"(was_set)
                 : "Ir"((uint64_t)bit)
                 : "memory");
    return was_set;
}

// Atomically test and set bit. Returns true if bit was already set (= double-free warning).
static inline bool atomic_bts(volatile uint64_t* word, uint32_t bit) {
    bool was_set;
    asm volatile("lock btsq %2, %0"
                 : "+m"(*word), "=@ccc"(was_set)
                 : "Ir"((uint64_t)bit)
                 : "memory");
    return was_set;
}

// bitmap_alloc_batch / bitmap_free_batch — defined after TwoRWContext (forward ref).

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
    uint32_t dequeue_batch;     // items pulled per RequestQueue per round-robin step
    uint32_t read_ack_batch;    // flush read_idx every N dequeues per queue

    // GSN — on its own cache line, written by this SN every op.
    alignas(64) std::atomic<uint64_t> gsn{0};
};

struct WorkerThreadState {
    uint32_t             worker_id;
    uint8_t              sn_id_for_resp;   // pre-computed: worker_id / (m/s), for KVResponse.sn_id
    TwoRWLayout*         layout;
    void*                cxl_base;
    std::atomic<bool>*   stop_flag;
    // Consumer for WorkerRing[worker_id] (CXL path)
    CXLSpscConsumer<KVRequest, QUEUE_CAP>* ring_consumer;
    // Consumer for local DRAM WorkerRing[worker_id] (--local-workerring path)
    LocalSpscConsumer<KVRequest, QUEUE_CAP>* local_ring_consumer;
    bool     use_local_ring = false;   // true → dequeue from local_ring_consumer
    // Producers for ResponseQueue[*][worker_id]: base pointer, indexed [j*num_workers+wid]
    CXLSpscProducer<KVResponse, QUEUE_CAP>* resp_producers; // [num_clients * num_workers]
    uint32_t num_clients;
    bool     stats_enabled  = false;   // collect per-op traversal depth stats
    bool     use_uintr      = false;   // true → register UINTR handler, sleep via uintr_wait
    bool     worker_check   = false;   // --worker-check: enable per-op route/block integrity checks

    // UINTR: pointers into TwoRWContext arrays (set during init, used by worker)
    int*               worker_uintr_fds = nullptr;  // &ctx->worker_uintr_fds[0]
    std::atomic<bool>* worker_fd_ready  = nullptr;  // &ctx->worker_fd_ready[0]

    // ---- Exit stats (written once at thread exit, read by main after join) ----
    uint64_t       exit_ops_done        = 0;   // total KV operations completed by this worker
    uint64_t       exit_empty_polls     = 0;   // times worker polled WorkerRing and found it empty
    uint64_t       exit_resp_fullwaits  = 0;   // times worker tried to enqueue a KVResponse to
                                               // ResponseQueue[client][wid] but the CXL SPSC ring
                                               // was full, causing the worker to spin-wait.
                                               // Non-zero values indicate RespThread is too slow
                                               // to drain responses, back-pressuring the worker
                                               // and ultimately causing SN ring_fullwaits.
    uint64_t       exit_uintr_wakeups   = 0;   // times woken from uintr_wait by WorkerPoller
    // Routing / data integrity diagnostics (always collected, zero in normal operation)
    uint64_t       exit_route_mismatches = 0;  // req.worker_id != this worker_id (SN routing bug)
    uint64_t       exit_block_mismatches = 0;  // recomputed FNV(key)%num_workers != worker_id
                                               // (stale/wrong block data — CXL visibility issue)
    WorkerOpStats* exit_stats           = nullptr;  // non-null if stats_enabled; main frees
};

struct PollerThreadState {
    TwoRWLayout*         layout;
    void*                cxl_base;
    std::atomic<bool>*   stop_flag;
    uint32_t             num_clients;
    uint32_t             num_workers;
    // Multi-machine: only monitor this node's clients (global indices)
    uint32_t             global_client_start = 0;    // first global client_id on this node
    uint32_t             global_client_count = 0;    // number of clients on this node (0 = all)
    // UINTR fds for Response Threads — set by Response Threads, read by Poller
    int*                 resp_uintr_fds;   // [num_clients], -1 if not registered
    std::atomic<bool>*   resp_fd_ready;    // [num_clients]
    // Sleeping flags — set by Response Threads before uintr_wait, read by Poller
    // std::atomic<bool>*   resp_sleeping;    // [num_clients]

    // ---- Exit counters (written once at thread exit, read by main after join) ----
    uint64_t exit_scan_rounds = 0;   // full scan rounds over all n clients;
                                     // each round checks n×m ResponseQueues for non-empty.
    uint64_t exit_uintrs_sent = 0;   // UINTR IPIs sent to Response Threads.
};

struct WorkerPollerThreadState {
    TwoRWLayout*         layout;
    void*                cxl_base;
    std::atomic<bool>*   stop_flag;
    uint32_t             num_workers;
    // Multi-machine: only monitor this node's workers (global indices)
    uint32_t             global_worker_start = 0;    // first global worker_id on this node
    uint32_t             global_worker_count = 0;    // number of workers on this node (0 = all)
    bool                 use_local_ring = false;  // true → check LocalSpscQueue instead of CXL WorkerRing
    // Pointers to local DRAM rings (only used when use_local_ring=true)
    LocalSpscQueue<KVRequest, QUEUE_CAP>* local_rings = nullptr;  // [num_workers]
    // UINTR fds for Worker Threads — set by Workers, read by WorkerPoller
    int*                 worker_uintr_fds;  // [num_workers], -1 if not registered
    std::atomic<bool>*   worker_fd_ready;   // [num_workers]

    // ---- Exit counters ----
    uint64_t exit_scan_rounds = 0;
    uint64_t exit_uintrs_sent = 0;
};

// ============================================================================
// TwoRWContext — Top-level Runtime Object
// ============================================================================

struct TwoRWContext {
    TwoRWConfig  config;
    TwoRWLayout  layout;
    void*        cxl_base  = nullptr;

    // Per-client block ID pools (local DRAM)
    LocalBlockCache*         local_block_caches = nullptr;  // [num_clients]
    FreeBlockQueue<QUEUE_CAP>* free_block_queues = nullptr; // [num_clients]

    // Request thread → SN producers: [n * s], indexed [client_id * s + sn_id]
    CXLSpscProducer<KVRequest,  QUEUE_CAP>* req_producers  = nullptr;

    // Per-SN consumer/producer arrays (NUMA-allocated on each SN's CPU node)
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
    LocalSpscQueue<KVRequest,    QUEUE_CAP>* local_rings          = nullptr;
    LocalSpscProducer<KVRequest, QUEUE_CAP>* local_ring_producers = nullptr;
    LocalSpscConsumer<KVRequest, QUEUE_CAP>* local_ring_consumers = nullptr;

    // UINTR: Response Threads register their fds here (used when has_response_poller)
    int*                resp_uintr_fds  = nullptr;  // [num_clients]
    std::atomic<bool>*  resp_fd_ready   = nullptr;  // [num_clients]
    // Sleeping flag: set by Response Thread before uintr_wait(), cleared after.
    // Poller checks this to avoid sending IPI to a thread that is busy processing.
    // std::atomic<bool>*  resp_sleeping   = nullptr;  // [num_clients]

    // UINTR: Worker Threads register their fds here (used when has_worker_poller)
    int*                worker_uintr_fds = nullptr;  // [num_workers]
    std::atomic<bool>*  worker_fd_ready  = nullptr;  // [num_workers]
    // Sleeping flag: set by Worker Thread before uintr_wait(), cleared after.
    // std::atomic<bool>*  worker_sleeping  = nullptr;  // [num_workers]

    // CXL Bitmap pointer (cached from layout.globalidmap(cxl_base) for fast access).
    // Treated as array of volatile uint64_t words; bit=1 means FREE.
    volatile uint64_t*  block_bitmap      = nullptr;
    uint32_t            block_bitmap_words = 0;   // total 64-bit words in bitmap

    // Worker-to-SN lookup table: worker_to_sn[global_worker_id] → sn_id.
    // Always populated (even in default even-division mode).
    // Owned by TwoRWContext, freed in two_rw_destroy().
    uint8_t*                             worker_to_sn = nullptr;  // [num_workers]

    // Pointer to CXL global_stop_flag in TwoRWHeader (multi-machine).
    // nullptr in single-machine mode (uses local stop_flag only).
    volatile uint32_t*                   cxl_global_stop_flag = nullptr;

    // Pointer to CXLNodeSync region (multi-machine barrier).
    CXLNodeSync*                         cxl_node_sync = nullptr;

    // Thread control
    alignas(64) std::atomic<bool>        stop_flag{false};
    std::vector<std::thread>             synchronizer_threads;  // [s]
    std::thread                          resp_poller_thread;
    std::thread                          worker_poller_thread;
    std::vector<std::thread>             worker_threads;        // [m]

    // Thread state blobs (heap-allocated, referenced by threads)
    SyncThreadState**         sync_state_ptrs       = nullptr;   // [s]
    WorkerThreadState*        worker_states         = nullptr;   // [m]
    PollerThreadState*        poller_state          = nullptr;   // response poller
    WorkerPollerThreadState*  worker_poller_state   = nullptr;   // worker poller
};

// ============================================================================
// Public API
// ============================================================================

// Single-machine init (backward compat) — allocates CXL, inits all structures.
TwoRWContext* two_rw_init(const TwoRWConfig& config);

// Multi-machine init: master allocates + initializes CXL, then local structures.
TwoRWContext* two_rw_master_init(const TwoRWConfig& config);

// Multi-machine init: worker node mmaps CXL, waits for master, then local structures.
TwoRWContext* two_rw_slave_attach(const TwoRWConfig& config);

void two_rw_start_threads(TwoRWContext* ctx);
void two_rw_stop(TwoRWContext* ctx);
void two_rw_destroy(TwoRWContext* ctx);

// Multi-machine distributed barrier: called by each node's main thread after
// local threads are started.  Phase 1: no-op (caller does local sync).
// Phase 2: sets this node's CXL ready flag and waits for all nodes.
// Single-machine: no-op.
void two_rw_node_barrier(TwoRWContext* ctx);

// Multi-machine slave stop: polls CXL global_stop_flag until master sets it,
// then calls two_rw_stop() to set local stop_flag and join threads.
// Single-machine: no-op (caller manages stop directly).
void two_rw_wait_global_stop(TwoRWContext* ctx);

// Print worker exit stats in order 0..m-1.
// Call after two_rw_stop() (all workers joined).
// Prints: ops/empty_polls when verbose; op breakdown when stats_enabled.
// Frees WorkerOpStats allocated by each worker thread.
void two_rw_print_worker_stats(TwoRWContext* ctx);

// Scan bucket table and print chain-length distribution.
// Call after LOAD phase (main thread, workers idle) when stats_enabled.
void two_rw_print_bucket_stats(TwoRWContext* ctx);

// ============================================================================
// Per-Thread Accessor Helpers (inline, used by benchmark code)
// ============================================================================

// ============================================================================
// CXL Bitmap batch operations
// ============================================================================

// Batch allocate up to `count` block_ids from CXL bitmap into LBC.
// Scans from scan_hint, wraps around once. Returns number allocated.
inline uint32_t bitmap_alloc_batch(TwoRWContext* ctx, uint32_t client_id,
                                    uint32_t count) {
    LocalBlockCache& cache = ctx->local_block_caches[client_id];
    volatile uint64_t* bmap = ctx->block_bitmap;
    const uint32_t total_words = ctx->block_bitmap_words;
    if (!bmap || total_words == 0) return 0;

    uint32_t acquired = 0;
    uint32_t wi = cache.scan_hint;
    uint32_t scanned = 0;

    while (acquired < count && scanned < total_words) {
        if (wi >= total_words) wi = 0;
        uint64_t word = bmap[wi];
        if (word != 0) {
            while (word != 0 && acquired < count) {
                uint32_t bit = __builtin_ctzll(word);
                if (atomic_btr(&bmap[wi], bit)) {
                    uint32_t block_id = wi * 64 + bit;
                    cache.stack[cache.top++] = block_id;
                    acquired++;
                }
                word &= word - 1;
            }
        }
        wi++;
        scanned++;
    }
    cache.scan_hint = wi < total_words ? wi : 0;
    return acquired;
}

// Batch free block_ids back to CXL bitmap.
inline void bitmap_free_batch(TwoRWContext* ctx,
                               const uint32_t* ids, uint32_t count) {
    volatile uint64_t* bmap = ctx->block_bitmap;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t id = ids[i];
        uint32_t wi = id / 64;
        uint32_t bit = id % 64;
        atomic_bts(&bmap[wi], bit);
    }
}

// ============================================================================
// Drain FreeBlockQueue — with bitmap spill to prevent deadlock
//
// Phase A: if LBC is full, spill BITMAP_BATCH entries from LBC bottom → bitmap
// Phase B: drain FBQ → LBC while LBC has room
// ============================================================================
inline void two_rw_drain_freeblocks(TwoRWContext* ctx, uint32_t client_id) {
    LocalBlockCache& cache = ctx->local_block_caches[client_id];

    // Phase A: spill overflow to bitmap
    if (cache.top >= LOCAL_CACHE_CAP) {
        uint32_t spill = (cache.top > BITMAP_BATCH) ? BITMAP_BATCH : cache.top;
        bitmap_free_batch(ctx, cache.stack, spill);
        cache.top -= spill;
        if (cache.top > 0)
            memmove(cache.stack, cache.stack + spill, cache.top * sizeof(uint32_t));
    }

    // Phase B: drain FBQ into LBC
    FreeBlockQueue<QUEUE_CAP>& q = ctx->free_block_queues[client_id];
    uint32_t id;
    while (cache.top < LOCAL_CACHE_CAP) {
        if (!q.pop(id)) break;
        cache.stack[cache.top++] = id;
    }
}

// Request Thread j: acquire a block_id from the local cache.
// Priority: LBC pop → FBQ drain → CXL bitmap batch alloc → OOM abort.
inline uint32_t two_rw_acquire_block(TwoRWContext* ctx, uint32_t client_id) {
    LocalBlockCache& cache = ctx->local_block_caches[client_id];

    // 1. Local stack: fastest path, no shared-memory access
    if (cache.top > 0) return cache.stack[--cache.top];

    // 2. Drain FreeBlockQueue into local stack (up to BITMAP_BATCH at once)
    FreeBlockQueue<QUEUE_CAP>& q = ctx->free_block_queues[client_id];
    uint32_t id;
    while (cache.top < BITMAP_BATCH) {
        if (!q.pop(id)) break;
        cache.stack[cache.top++] = id;
    }
    if (cache.top > 0) return cache.stack[--cache.top];

    // 3. Batch allocate from CXL bitmap
    bitmap_alloc_batch(ctx, client_id, BITMAP_BATCH);
    if (cache.top > 0) return cache.stack[--cache.top];

    // 4. Block pool exhausted
    fprintf(stderr, "[RT-%u] Block pool exhausted (bitmap_words=%u)\n",
            client_id, ctx->block_bitmap_words);
    abort();
}

// Request Thread j: pointer to UnifiedBlock for the given block_id
inline UnifiedBlock* two_rw_get_unified_block(TwoRWContext* ctx, uint32_t block_id) {
    return ctx->layout.unified_block_ptr(ctx->cxl_base, block_id);
}

// Request Thread j: submit request (after filling UnifiedBlock and sfence).
// Routes to req_producers[client_id * s + sn_id].
// SN determined via worker_to_sn[] lookup (supports both even and flexible allocation).
inline bool two_rw_submit(TwoRWContext* ctx, uint32_t client_id,
                           uint32_t block_id, uint32_t worker_id, uint8_t op_type,
                           uint32_t key_hash, uint16_t key_len) {
    const uint32_t s     = ctx->config.num_synchronizers;
    const uint32_t sn_id = ctx->worker_to_sn[worker_id];
    KVRequest req{};
    req.worker_id = worker_id;
    req.block_id  = block_id;
    req.client_id = client_id;
    req.op_type   = op_type;
    req.key_hash  = key_hash;
    req.key_len   = key_len;
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
