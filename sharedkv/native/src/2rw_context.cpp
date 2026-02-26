// ============================================================================
// SharedKV 2RW — Context Init / Start / Stop / Destroy
// ============================================================================

#include "2rw_context.h"
#include "cxl_ptr.h"
#include "uintr_threading.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cassert>
#include <new>
#include <numa.h>
#include <numaif.h>

// Declared in their respective .cpp files
namespace TwoRW {
void two_rw_synchronizer_run(SyncThreadState* state);
void two_rw_worker_run(WorkerThreadState* state);
void two_rw_poller_run(PollerThreadState* state);
}

namespace TwoRW {

// ============================================================================
// CXL Memory Allocation
// ============================================================================

static void* allocate_cxl_slab(int numa_node, uint64_t size) {
    char shm_name[64];
    snprintf(shm_name, sizeof(shm_name), "/sharedkv_2rw_node%d", numa_node);

    int fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        perror("shm_open");
        return nullptr;
    }

    if (ftruncate(fd, static_cast<off_t>(size)) < 0) {
        perror("ftruncate");
        close(fd);
        return nullptr;
    }

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        perror("mmap");
        return nullptr;
    }

    if (numa_node >= 0 && numa_available() >= 0) {
        struct bitmask* mask = numa_bitmask_alloc(numa_max_node() + 1);
        numa_bitmask_setbit(mask, static_cast<unsigned>(numa_node));
        mbind(ptr, size, MPOL_BIND, mask->maskp, mask->size + 1, 0);
        numa_bitmask_free(mask);
    }

    return ptr;
}

// ============================================================================
// CXL Initialization — Zero and set up all regions
// ============================================================================

static void init_cxl_memory(void* base, const TwoRWLayout& L,
                              const TwoRWConfig& cfg) {
    memset(base, 0, L.total_size);

    TwoRWHeader* hdr = L.header(base);
    hdr->magic            = HEADER_MAGIC;
    hdr->num_clients      = cfg.num_clients;
    hdr->num_workers      = cfg.num_workers;
    hdr->slots_per_client = cfg.slots_per_client;
    hdr->num_buckets      = cfg.num_buckets;
    hdr->queue_depth      = cfg.queue_depth;
    hdr->total_size       = L.total_size;

    // Init DataRegion metadata
    for (uint32_t i = 0; i < cfg.num_workers; i++) {
        DataRegionHeader* meta = L.data_region_meta(base, i);
        meta->base_offset      = L.data_region_offset(i);
        meta->total_size       = L.data_region_per_worker;
        meta->alloc_offset     = sizeof(DataRegionHeader);
        meta->free_list_offset = 0;
        meta->nodes_allocated  = 0;
        meta->nodes_recycled   = 0;
    }

    // Init Pool slots (magic = POOL_MAGIC, rest zeroed)
    for (uint32_t j = 0; j < cfg.num_clients; j++) {
        for (uint32_t k = 0; k < cfg.slots_per_client; k++) {
            uint32_t slot_id = L.global_slot_id(j, k);
            KVPoolSlot* s = CXLPtr<KVPoolSlot>(L.pool_slot_offset(slot_id)).get();
            s->magic = POOL_MAGIC;
        }
    }

    // Init RequestQueues: n_clients * n_synchronizers queues
    const uint32_t s = cfg.num_synchronizers;
    for (uint32_t j = 0; j < cfg.num_clients; j++) {
        for (uint32_t k = 0; k < s; k++) {
            L.request_sub_queue(base, j, k)->init();
        }
    }

    // Init WorkerRings
    for (uint32_t i = 0; i < cfg.num_workers; i++) {
        L.worker_ring(base, i)->init();
    }

    // Init ResponseQueues
    for (uint32_t j = 0; j < cfg.num_clients; j++) {
        for (uint32_t i = 0; i < cfg.num_workers; i++) {
            L.response_queue(base, j, i)->init();
        }
    }

    _mm_sfence();
    fprintf(stderr, "[2RW] CXL memory initialized. Layout:\n");
    L.print();
}

// ============================================================================
// NUMA helpers
// ============================================================================

// Allocate sz bytes on the given NUMA node via numa_alloc_onnode.
// Returns nullptr if NUMA is unavailable or allocation fails.
static void* try_numa_alloc(size_t sz, int numa_node) {
    if (numa_node < 0 || numa_available() < 0) return nullptr;
    void* p = numa_alloc_onnode(sz, numa_node);
    if (!p) {
        fprintf(stderr, "[2RW] WARNING: numa_alloc_onnode(%zu, %d) failed\n", sz, numa_node);
    }
    return p;
}

static void verify_numa_node(const void* ptr, int expected_node, const char* label) {
    if (!ptr || expected_node < 0) return;
    int node = -1;
    if (get_mempolicy(&node, NULL, 0, (void*)ptr, MPOL_F_NODE | MPOL_F_ADDR) == 0) {
        fprintf(stderr, "[2RW] %s=%p  NUMA node %d  (expected %d)%s\n",
                label, ptr, node, expected_node,
                (node == expected_node) ? "  OK" : "  WARNING: mismatch!");
    }
}

// ============================================================================
// two_rw_init
// ============================================================================

TwoRWContext* two_rw_init(const TwoRWConfig& cfg) {
    if (!cfg.validate()) {
        fprintf(stderr, "[2RW] Invalid config\n");
        cfg.print();
        return nullptr;
    }
    cfg.print();

    // ---- Allocate TwoRWContext on Poller's local NUMA node (CPU 0) ----
    const int cpu0_numa = (numa_available() >= 0) ? numa_node_of_cpu(0) : -1;

    fprintf(stderr,
        "[2RW] NUMA topology: CPU0_node=%d  CXL_node=%d  %s\n",
        cpu0_numa, cfg.numa_node,
        (cpu0_numa >= 0 && cpu0_numa == cfg.numa_node)
            ? "WARNING: CPU0 is ON the CXL node"
            : "OK (CPU0 local DRAM != CXL node)");

    TwoRWContext* ctx = nullptr;
    {
        void* raw = try_numa_alloc(sizeof(TwoRWContext), cpu0_numa);
        if (raw) {
            ctx = new (raw) TwoRWContext();
        } else {
            ctx = new TwoRWContext();
        }
    }

    ctx->config = cfg;
    ctx->layout = TwoRWLayout::calculate(
        cfg.num_clients, cfg.num_workers,
        cfg.slots_per_client, cfg.num_buckets,
        cfg.memory_size, cfg.num_synchronizers);

    // Allocate CXL slab
    ctx->cxl_base = allocate_cxl_slab(cfg.numa_node, cfg.memory_size);
    if (!ctx->cxl_base) {
        delete ctx;
        return nullptr;
    }

    // Set global TLS base for CXLPtr resolution
    CXLBase::set(ctx->cxl_base);

    // Initialize CXL memory
    init_cxl_memory(ctx->cxl_base, ctx->layout, cfg);

    const uint32_t n = cfg.num_clients;
    const uint32_t m = cfg.num_workers;
    const uint32_t s = cfg.num_synchronizers;
    const uint32_t wps = m / s;  // workers per synchronizer

    // ---- Allocate local (non-CXL) structures ----

    // FreeIDQueues — one per client, pre-filled with slot_ids
    ctx->free_id_queues = new FreeIDQueue<QUEUE_CAP>[n];
    for (uint32_t j = 0; j < n; j++) {
        ctx->free_id_queues[j].init(j, cfg.slots_per_client);
    }

    // ---- Request thread producers: [n * s] ----
    // Written by Request Threads (not SNs) → regular new[], no NUMA needed
    ctx->req_producers = new CXLSpscProducer<KVRequest, QUEUE_CAP>[n * s];
    for (uint32_t j = 0; j < n; j++) {
        for (uint32_t k = 0; k < s; k++) {
            ctx->req_producers[j * s + k].attach(
                ctx->layout.request_sub_queue(ctx->cxl_base, j, k));
        }
    }

    // ---- Per-SN: req_consumers[n] and ring_producers[wps] ----
    // These are written by SN_k on every op → NUMA-allocate on SN_k's CPU node.
    // CPU layout: SN_k runs on CPU (k+1).
    ctx->sn_req_consumers   = new CXLSpscConsumer<KVRequest, QUEUE_CAP>*[s]{};
    ctx->sn_ring_producers  = new CXLSpscProducer<KVRequest, QUEUE_CAP>*[s]{};
    ctx->sn_req_con_is_numa  = new bool[s]{};
    ctx->sn_ring_prod_is_numa = new bool[s]{};
    ctx->sn_state_is_numa    = new bool[s]{};
    ctx->sync_state_ptrs     = new SyncThreadState*[s]{};

    for (uint32_t k = 0; k < s; k++) {
        const int sn_cpu      = static_cast<int>(k + 1);  // SN_k on CPU k+1
        const int sn_numa     = (numa_available() >= 0) ? numa_node_of_cpu(sn_cpu) : -1;
        const uint32_t w_base = k * wps;

        // ---- req_consumers[n] for SN_k ----
        {
            size_t sz = n * sizeof(CXLSpscConsumer<KVRequest, QUEUE_CAP>);
            void* raw = try_numa_alloc(sz, sn_numa);
            if (raw) {
                ctx->sn_req_consumers[k] =
                    static_cast<CXLSpscConsumer<KVRequest, QUEUE_CAP>*>(raw);
                for (uint32_t j = 0; j < n; j++)
                    new (&ctx->sn_req_consumers[k][j])
                        CXLSpscConsumer<KVRequest, QUEUE_CAP>();
                ctx->sn_req_con_is_numa[k] = true;
            } else {
                ctx->sn_req_consumers[k] =
                    new CXLSpscConsumer<KVRequest, QUEUE_CAP>[n];
            }
            // Attach: SN_k polls queue [j * s + k] for each client j
            for (uint32_t j = 0; j < n; j++)
                ctx->sn_req_consumers[k][j].attach(
                    ctx->layout.request_sub_queue(ctx->cxl_base, j, k));
            { char lbl[32]; snprintf(lbl, sizeof(lbl), "sn_req_consumers[%u]", k);
              verify_numa_node(ctx->sn_req_consumers[k], sn_numa, lbl); }
        }

        // ---- ring_producers[wps] for SN_k's CXL WorkerRings ----
        {
            size_t sz = wps * sizeof(CXLSpscProducer<KVRequest, QUEUE_CAP>);
            void* raw = try_numa_alloc(sz, sn_numa);
            if (raw) {
                ctx->sn_ring_producers[k] =
                    static_cast<CXLSpscProducer<KVRequest, QUEUE_CAP>*>(raw);
                for (uint32_t i = 0; i < wps; i++)
                    new (&ctx->sn_ring_producers[k][i])
                        CXLSpscProducer<KVRequest, QUEUE_CAP>();
                ctx->sn_ring_prod_is_numa[k] = true;
            } else {
                ctx->sn_ring_producers[k] =
                    new CXLSpscProducer<KVRequest, QUEUE_CAP>[wps];
            }
            // Attach to WorkerRings[w_base .. w_base+wps-1]
            for (uint32_t i = 0; i < wps; i++)
                ctx->sn_ring_producers[k][i].attach(
                    ctx->layout.worker_ring(ctx->cxl_base, w_base + i));
            { char lbl[32]; snprintf(lbl, sizeof(lbl), "sn_ring_producers[%u]", k);
              verify_numa_node(ctx->sn_ring_producers[k], sn_numa, lbl); }
        }
    }

    // ---- Worker consumers: ring_consumers[m] (CXL) ----
    // Written by Workers, not SNs → regular new[]
    ctx->ring_consumers = new CXLSpscConsumer<KVRequest, QUEUE_CAP>[m];
    for (uint32_t i = 0; i < m; i++) {
        ctx->ring_consumers[i].attach(ctx->layout.worker_ring(ctx->cxl_base, i));
    }

    // ---- Optional: local DRAM WorkerRing ----
    if (cfg.local_workerring) {
        ctx->local_rings          = new LocalSpscQueue<KVRequest, QUEUE_CAP>[m];
        ctx->local_ring_producers = new LocalSpscProducer<KVRequest, QUEUE_CAP>[m];
        ctx->local_ring_consumers = new LocalSpscConsumer<KVRequest, QUEUE_CAP>[m];
        for (uint32_t i = 0; i < m; i++) {
            ctx->local_rings[i].init();
            ctx->local_ring_producers[i].attach(&ctx->local_rings[i]);
            ctx->local_ring_consumers[i].attach(&ctx->local_rings[i]);
        }
        fprintf(stderr, "[2RW] local_workerring: allocated %u DRAM rings (%zu KB each)\n",
                m, sizeof(LocalSpscQueue<KVRequest, QUEUE_CAP>) >> 10);
    }

    // ---- ResponseQueue handles ----
    ctx->resp_producers = new CXLSpscProducer<KVResponse, QUEUE_CAP>[n * m];
    ctx->resp_consumers = new CXLSpscConsumer<KVResponse, QUEUE_CAP>[n * m];
    for (uint32_t j = 0; j < n; j++) {
        for (uint32_t i = 0; i < m; i++) {
            uint32_t idx = j * m + i;
            ctx->resp_producers[idx].attach(
                ctx->layout.response_queue(ctx->cxl_base, j, i));
            ctx->resp_consumers[idx].attach(
                ctx->layout.response_queue(ctx->cxl_base, j, i));
        }
    }

    // ---- UINTR fd arrays ----
    ctx->resp_uintr_fds = new int[n];
    ctx->resp_fd_ready  = new std::atomic<bool>[n];
    for (uint32_t j = 0; j < n; j++) {
        ctx->resp_uintr_fds[j] = -1;
        ctx->resp_fd_ready[j].store(false);
    }

    // ---- SyncThreadState[s]: one per SN, NUMA-allocated on SN's CPU node ----
    for (uint32_t k = 0; k < s; k++) {
        const int sn_cpu  = static_cast<int>(k + 1);
        const int sn_numa = (numa_available() >= 0) ? numa_node_of_cpu(sn_cpu) : -1;
        const uint32_t w_base = k * wps;

        SyncThreadState* ss = nullptr;
        {
            void* raw = try_numa_alloc(sizeof(SyncThreadState), sn_numa);
            if (raw) {
                ss = new (raw) SyncThreadState{};
                ctx->sn_state_is_numa[k] = true;
            } else {
                ss = new SyncThreadState{};
            }
        }

        ss->layout               = &ctx->layout;
        ss->cxl_base             = ctx->cxl_base;
        ss->stop_flag            = &ctx->stop_flag;
        ss->ring_producers       = ctx->sn_ring_producers[k];
        ss->req_consumers        = ctx->sn_req_consumers[k];
        ss->local_ring_producers = cfg.local_workerring
                                   ? &ctx->local_ring_producers[w_base]
                                   : nullptr;
        ss->use_local_ring       = cfg.local_workerring;
        ss->num_clients          = n;
        ss->num_workers          = m;
        ss->sn_id                = k;
        ss->num_synchronizers    = s;
        ss->workers_base         = w_base;
        ss->workers_count        = wps;
        // GSN for SN_k starts at k (interleaved: k, k+s, k+2s, ...)
        ss->gsn.store(k, std::memory_order_relaxed);

        { char lbl[32]; snprintf(lbl, sizeof(lbl), "sync_state[%u].gsn", k);
          verify_numa_node((void*)&ss->gsn, sn_numa, lbl); }

        ctx->sync_state_ptrs[k] = ss;
    }

    // ---- WorkerThreadState[m] ----
    ctx->worker_states = new WorkerThreadState[m];
    for (uint32_t i = 0; i < m; i++) {
        auto& ws                 = ctx->worker_states[i];
        ws.worker_id             = i;
        ws.layout                = &ctx->layout;
        ws.cxl_base              = ctx->cxl_base;
        ws.stop_flag             = &ctx->stop_flag;
        ws.region_meta           = ctx->layout.data_region_meta(ctx->cxl_base, i);
        ws.ring_consumer         = &ctx->ring_consumers[i];
        ws.local_ring_consumer   = cfg.local_workerring
                                   ? &ctx->local_ring_consumers[i] : nullptr;
        ws.use_local_ring        = cfg.local_workerring;
        ws.resp_producers        = ctx->resp_producers;
        ws.num_clients           = n;
    }

    // ---- PollerThreadState ----
    ctx->poller_state = new PollerThreadState{};
    ctx->poller_state->layout          = &ctx->layout;
    ctx->poller_state->cxl_base        = ctx->cxl_base;
    ctx->poller_state->stop_flag       = &ctx->stop_flag;
    ctx->poller_state->num_clients     = n;
    ctx->poller_state->num_workers     = m;
    ctx->poller_state->resp_uintr_fds  = ctx->resp_uintr_fds;
    ctx->poller_state->resp_fd_ready   = ctx->resp_fd_ready;

    return ctx;
}

// ============================================================================
// two_rw_start_threads
// CPU layout: Poller=CPU0, SN_k=CPU(k+1), Workers=CPU[w_base..w_base+m-1]
// ============================================================================

void two_rw_start_threads(TwoRWContext* ctx) {
    const uint32_t n  = ctx->config.num_clients;
    const uint32_t m  = ctx->config.num_workers;
    const uint32_t s  = ctx->config.num_synchronizers;
    const int w_base  = (ctx->config.worker_cpu_start < 0)
                        ? static_cast<int>(1 + s)
                        : ctx->config.worker_cpu_start;

    // Poller on CPU 0
    ctx->poller_thread = std::thread([ctx]() {
        CXLBase::set(ctx->cxl_base);
        pin_current_thread_to_cpu(0);
        two_rw_poller_run(ctx->poller_state);
    });

    // Synchronizers: SN_k on CPU (k+1)
    ctx->synchronizer_threads.resize(s);
    for (uint32_t k = 0; k < s; k++) {
        ctx->synchronizer_threads[k] = std::thread([ctx, k]() {
            CXLBase::set(ctx->cxl_base);
            pin_current_thread_to_cpu(static_cast<int>(k + 1));
            two_rw_synchronizer_run(ctx->sync_state_ptrs[k]);
        });
    }

    // Workers on CPUs [w_base .. w_base+m-1]
    ctx->worker_threads.resize(m);
    for (uint32_t i = 0; i < m; i++) {
        ctx->worker_threads[i] = std::thread([ctx, i, w_base]() {
            CXLBase::set(ctx->cxl_base);
            pin_current_thread_to_cpu(w_base + static_cast<int>(i));
            two_rw_worker_run(&ctx->worker_states[i]);
        });
    }

    fprintf(stderr,
            "[2RW] Threads started: Poller=CPU0, "
            "SN[0..%u]=CPU[1..%u], Workers=CPU[%d..%d]\n",
            s - 1, s, w_base, w_base + static_cast<int>(m) - 1);
}

// ============================================================================
// two_rw_stop — drain sequence per spec §6
// ============================================================================

void two_rw_stop(TwoRWContext* ctx) {
    const uint32_t s = ctx->config.num_synchronizers;

    // Step 1: Signal stop (Request Threads must already be done submitting)
    ctx->stop_flag.store(true, std::memory_order_release);

    // Step 2: SNs drain their RequestQueues (handled in sync loop)
    // Step 3: Workers drain their WorkerRings (handled in worker loop)
    // Step 4: Response Threads drain ResponseQueues (caller's responsibility)

    for (auto& t : ctx->synchronizer_threads)
        if (t.joinable()) t.join();

    for (auto& t : ctx->worker_threads)
        if (t.joinable()) t.join();

    if (ctx->poller_thread.joinable())
        ctx->poller_thread.join();

    // Print final GSN per SN
    for (uint32_t k = 0; k < s; k++) {
        if (ctx->sync_state_ptrs && ctx->sync_state_ptrs[k]) {
            fprintf(stderr, "[2RW] SN%u final GSN: %lu\n",
                    k, ctx->sync_state_ptrs[k]->gsn.load());
        }
    }
}

// ============================================================================
// two_rw_destroy
// ============================================================================

void two_rw_destroy(TwoRWContext* ctx) {
    if (!ctx) return;

    const uint32_t n = ctx->config.num_clients;
    const uint32_t m = ctx->config.num_workers;
    const uint32_t s = ctx->config.num_synchronizers;
    const uint32_t wps = m / s;
    const bool numa_ok = (numa_available() >= 0);

    // Non-SN allocations → always regular delete[]
    delete[] ctx->free_id_queues;
    delete[] ctx->req_producers;
    delete[] ctx->ring_consumers;
    delete[] ctx->resp_producers;
    delete[] ctx->resp_consumers;
    delete[] ctx->resp_uintr_fds;
    delete[] ctx->resp_fd_ready;
    delete[] ctx->worker_states;
    delete   ctx->poller_state;

    // Local DRAM rings (regular new[], only allocated when local_workerring=true)
    delete[] ctx->local_ring_consumers;
    delete[] ctx->local_ring_producers;
    delete[] ctx->local_rings;

    // Per-SN allocations: req_consumers, ring_producers, sync_state
    if (ctx->sync_state_ptrs) {
        for (uint32_t k = 0; k < s; k++) {
            // req_consumers[n]
            if (ctx->sn_req_consumers && ctx->sn_req_consumers[k]) {
                if (ctx->sn_req_con_is_numa && ctx->sn_req_con_is_numa[k] && numa_ok) {
                    for (uint32_t j = 0; j < n; j++)
                        ctx->sn_req_consumers[k][j]
                            .~CXLSpscConsumer<KVRequest, QUEUE_CAP>();
                    numa_free(ctx->sn_req_consumers[k],
                              n * sizeof(CXLSpscConsumer<KVRequest, QUEUE_CAP>));
                } else {
                    delete[] ctx->sn_req_consumers[k];
                }
            }
            // ring_producers[wps]
            if (ctx->sn_ring_producers && ctx->sn_ring_producers[k]) {
                if (ctx->sn_ring_prod_is_numa && ctx->sn_ring_prod_is_numa[k] && numa_ok) {
                    for (uint32_t i = 0; i < wps; i++)
                        ctx->sn_ring_producers[k][i]
                            .~CXLSpscProducer<KVRequest, QUEUE_CAP>();
                    numa_free(ctx->sn_ring_producers[k],
                              wps * sizeof(CXLSpscProducer<KVRequest, QUEUE_CAP>));
                } else {
                    delete[] ctx->sn_ring_producers[k];
                }
            }
            // SyncThreadState
            if (ctx->sync_state_ptrs[k]) {
                if (ctx->sn_state_is_numa && ctx->sn_state_is_numa[k] && numa_ok) {
                    ctx->sync_state_ptrs[k]->~SyncThreadState();
                    numa_free(ctx->sync_state_ptrs[k], sizeof(SyncThreadState));
                } else {
                    delete ctx->sync_state_ptrs[k];
                }
            }
        }
    }

    delete[] ctx->sync_state_ptrs;
    delete[] ctx->sn_req_consumers;
    delete[] ctx->sn_ring_producers;
    delete[] ctx->sn_req_con_is_numa;
    delete[] ctx->sn_ring_prod_is_numa;
    delete[] ctx->sn_state_is_numa;

    if (ctx->cxl_base) {
        munmap(ctx->cxl_base, ctx->config.memory_size);
    }

    if (numa_available() >= 0) {
        ctx->~TwoRWContext();
        numa_free(ctx, sizeof(TwoRWContext));
    } else {
        delete ctx;
    }
}

// ============================================================================
// TwoRWLayout::print
// ============================================================================

void TwoRWLayout::print() const {
    fprintf(stderr,
        "  header          @ 0x%010lx\n"
        "  hash_table      @ 0x%010lx  (%u buckets)\n"
        "  data_region_meta@ 0x%010lx  (%u workers)\n"
        "  pool            @ 0x%010lx  (%u clients × %u slots × 2048B = %zu MB)\n"
        "  request_queues  @ 0x%010lx  (%u clients × %u SNs = %u queues)\n"
        "  worker_rings    @ 0x%010lx  (%u rings)\n"
        "  response_queues @ 0x%010lx  (%u × %u = %u queues)\n"
        "  data_region     @ 0x%010lx  (%zu MB per worker)\n",
        header_off,
        hash_table_off, num_buckets,
        data_region_meta_off, num_workers,
        pool_off, num_clients, slots_per_client,
        (size_t)((uint64_t)num_clients * slots_per_client * 2048 >> 20),
        request_queue_off, num_clients, num_synchronizers,
        num_clients * num_synchronizers,
        worker_ring_off, num_workers,
        response_queue_off, num_clients, num_workers, num_clients * num_workers,
        data_region_off, (size_t)(data_region_per_worker >> 20));
}

} // namespace TwoRW
