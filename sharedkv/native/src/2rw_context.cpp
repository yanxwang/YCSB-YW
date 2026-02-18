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
    // Try POSIX shared memory first (same semantics as existing codebase)
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

    // Bind to NUMA node if available
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
    // Zero entire memory
    memset(base, 0, L.total_size);

    // Write header
    TwoRWHeader* hdr = L.header(base);
    hdr->magic            = HEADER_MAGIC;
    hdr->num_clients      = cfg.num_clients;
    hdr->num_workers      = cfg.num_workers;
    hdr->slots_per_client = cfg.slots_per_client;
    hdr->num_buckets      = cfg.num_buckets;
    hdr->queue_depth      = cfg.queue_depth;
    hdr->total_size       = L.total_size;

    // Init hash table buckets (already zeroed, head_offset=0 means empty)

    // Init DataRegion metadata
    for (uint32_t i = 0; i < cfg.num_workers; i++) {
        DataRegionHeader* meta = L.data_region_meta(base, i);
        meta->base_offset      = L.data_region_offset(i);
        meta->total_size       = L.data_region_per_worker;
        meta->alloc_offset     = sizeof(DataRegionHeader); // skip own header
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

    // Init RequestQueues
    for (uint32_t j = 0; j < cfg.num_clients; j++) {
        L.request_queue(base, j)->init();
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
// two_rw_init
// ============================================================================

TwoRWContext* two_rw_init(const TwoRWConfig& cfg) {
    if (!cfg.validate()) {
        fprintf(stderr, "[2RW] Invalid config\n");
        cfg.print();
        return nullptr;
    }
    cfg.print();

    // ---- Allocate TwoRWContext on Sync's local NUMA node (CPU 0) ----
    // ctx->gsn is written by Sync every op via lock xadd.
    // If it lands on a different NUMA node (e.g. node 1 from default heap),
    // each fetch_add pays remote-NUMA latency (~1000-2000 ticks) instead of
    // L1-cache latency (~5 ticks). Force allocation on node 0.
    const int sync_numa_node = (numa_available() >= 0) ? numa_node_of_cpu(0) : -1;

    // ---- Critical topology check ----
    // If CPU 0's NUMA node == cfg.numa_node (the CXL device node), every
    // gsn.fetch_add will be a CXL round-trip (~1000 ns ≈ 1500–2000 ticks).
    // The isolated microbenchmark still shows ~18 ticks (gsn stays in L1),
    // but in the hot loop CXL dequeue ops evict gsn → CXL miss on fetch_add.
    fprintf(stderr,
        "[2RW] NUMA topology: CPU0_node=%d  CXL_node=%d  %s\n",
        sync_numa_node, cfg.numa_node,
        (sync_numa_node >= 0 && sync_numa_node == cfg.numa_node)
            ? "!!! WARNING: CPU0 is ON the CXL node — gsn will have CXL latency !!!"
            : "OK (CPU0 local DRAM != CXL node)");

    TwoRWContext* ctx = nullptr;
    if (sync_numa_node >= 0) {
        void* raw = numa_alloc_onnode(sizeof(TwoRWContext), sync_numa_node);
        if (!raw) {
            fprintf(stderr, "[2RW] numa_alloc_onnode failed, falling back to new\n");
            ctx = new TwoRWContext();
        } else {
            ctx = new (raw) TwoRWContext();  // placement new: construct in-place
        }
    } else {
        ctx = new TwoRWContext();
    }

    // gsn has been moved to SyncThreadState — checked after sync_state is allocated below

    ctx->config = cfg;
    ctx->layout = TwoRWLayout::calculate(
        cfg.num_clients, cfg.num_workers,
        cfg.slots_per_client, cfg.num_buckets,
        cfg.memory_size);

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

    // ---- Allocate local (non-CXL) structures ----

    // FreeIDQueues — one per client, pre-filled with slot_ids
    ctx->free_id_queues = new FreeIDQueue<QUEUE_CAP>[n];
    for (uint32_t j = 0; j < n; j++) {
        ctx->free_id_queues[j].init(j, cfg.slots_per_client);
    }

    // -----------------------------------------------------------------------
    // Sync-hot SPSC handles: req_consumers[n] and ring_producers[m].
    //
    // The Sync thread WRITES to these handles on every op:
    //   - req_consumers[j].cached_read_   (updated in dequeue)
    //   - ring_producers[i].cached_write_ (updated in enqueue)
    //
    // If these structs are on a DIFFERENT NUMA node from Sync (CPU 0), every
    // write is a remote-NUMA store.  The sfence at the end of each
    // dequeue/enqueue must drain the store buffer to that remote node, adding
    // latency AND L1 cache-set pressure that can evict gsn — turning each
    // gsn.fetch_add into a local-DRAM miss (~200 ticks) or worse.
    //
    // Fix: allocate them on sync_numa_node via numa_alloc_onnode + placement new.
    // -----------------------------------------------------------------------

    // For non-Sync consumers/producers (req_producers, ring_consumers,
    // resp_producers, resp_consumers) the Writer is NOT Sync → regular new[] OK.

    // req_producers[n] — written by Request Threads, not Sync → regular new[]
    ctx->req_producers = new CXLSpscProducer<KVRequest, QUEUE_CAP>[n];

    // req_consumers[n] — written by Sync on every dequeue → NUMA node 0
    // ring_producers[m] — written by Sync on every enqueue → NUMA node 0
    bool req_con_numa  = false;
    bool ring_prod_numa = false;

    if (sync_numa_node >= 0 && numa_available() >= 0) {
        // req_consumers
        {
            size_t sz = n * sizeof(CXLSpscConsumer<KVRequest, QUEUE_CAP>);
            void* raw = numa_alloc_onnode(sz, sync_numa_node);
            if (raw) {
                ctx->req_consumers = static_cast<CXLSpscConsumer<KVRequest, QUEUE_CAP>*>(raw);
                for (uint32_t j = 0; j < n; j++)
                    new (&ctx->req_consumers[j]) CXLSpscConsumer<KVRequest, QUEUE_CAP>();
                req_con_numa = true;
            } else {
                fprintf(stderr, "[2RW] WARNING: numa_alloc_onnode failed for req_consumers, "
                                "falling back to new[]\n");
                ctx->req_consumers = new CXLSpscConsumer<KVRequest, QUEUE_CAP>[n];
            }
        }
        // ring_producers
        {
            size_t sz = m * sizeof(CXLSpscProducer<KVRequest, QUEUE_CAP>);
            void* raw = numa_alloc_onnode(sz, sync_numa_node);
            if (raw) {
                ctx->ring_producers = static_cast<CXLSpscProducer<KVRequest, QUEUE_CAP>*>(raw);
                for (uint32_t i = 0; i < m; i++)
                    new (&ctx->ring_producers[i]) CXLSpscProducer<KVRequest, QUEUE_CAP>();
                ring_prod_numa = true;
            } else {
                fprintf(stderr, "[2RW] WARNING: numa_alloc_onnode failed for ring_producers, "
                                "falling back to new[]\n");
                ctx->ring_producers = new CXLSpscProducer<KVRequest, QUEUE_CAP>[m];
            }
        }
    } else {
        ctx->req_consumers  = new CXLSpscConsumer<KVRequest, QUEUE_CAP>[n];
        ctx->ring_producers = new CXLSpscProducer<KVRequest, QUEUE_CAP>[m];
    }

    // Both must succeed for sync_alloc_is_numa to be true (destroys must match inits)
    ctx->sync_alloc_is_numa = (req_con_numa && ring_prod_numa);

    // Attach req_producers and req_consumers to RequestQueues
    for (uint32_t j = 0; j < n; j++) {
        ctx->req_producers[j].attach(ctx->layout.request_queue(ctx->cxl_base, j));
        ctx->req_consumers[j].attach(ctx->layout.request_queue(ctx->cxl_base, j));
    }

    // ring_consumers[m] — written by Workers, not Sync → regular new[]
    ctx->ring_consumers = new CXLSpscConsumer<KVRequest, QUEUE_CAP>[m];
    for (uint32_t i = 0; i < m; i++) {
        ctx->ring_producers[i].attach(ctx->layout.worker_ring(ctx->cxl_base, i));
        ctx->ring_consumers[i].attach(ctx->layout.worker_ring(ctx->cxl_base, i));
    }

    // Verify req_consumers and ring_producers are on the right NUMA node
    if (ctx->sync_alloc_is_numa && n > 0) {
        int node = -1;
        if (get_mempolicy(&node, NULL, 0, (void*)ctx->req_consumers,
                          MPOL_F_NODE | MPOL_F_ADDR) == 0) {
            fprintf(stderr,
                "[2RW] req_consumers[0]=%p  on NUMA node %d  (Sync node %d)%s\n",
                (void*)ctx->req_consumers, node, sync_numa_node,
                (node == sync_numa_node) ? "  OK" : "  WARNING: mismatch!");
        }
    }
    if (ctx->sync_alloc_is_numa && m > 0) {
        int node = -1;
        if (get_mempolicy(&node, NULL, 0, (void*)ctx->ring_producers,
                          MPOL_F_NODE | MPOL_F_ADDR) == 0) {
            fprintf(stderr,
                "[2RW] ring_producers[0]=%p  on NUMA node %d  (Sync node %d)%s\n",
                (void*)ctx->ring_producers, node, sync_numa_node,
                (node == sync_numa_node) ? "  OK" : "  WARNING: mismatch!");}
    }

    // SPSC handles for ResponseQueue[n][m]
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

    // UINTR fd arrays (written by Response Threads at runtime)
    ctx->resp_uintr_fds = new int[n];
    ctx->resp_fd_ready  = new std::atomic<bool>[n];
    for (uint32_t j = 0; j < n; j++) {
        ctx->resp_uintr_fds[j] = -1;
        ctx->resp_fd_ready[j].store(false);
    }

    // ---- Thread state structs ----

    // Allocate SyncThreadState on Sync's local NUMA node (node 0).
    // SyncThreadState::gsn is the hot-path atomic (written every op).
    // Keeping it on node 0 ensures L1-cache latency (~5 ticks) not remote-NUMA.
    {
        void* raw = (sync_numa_node >= 0 && numa_available() >= 0)
                    ? numa_alloc_onnode(sizeof(SyncThreadState), sync_numa_node)
                    : nullptr;
        if (raw) {
            ctx->sync_state = new (raw) SyncThreadState{};
            ctx->sync_state_is_numa = true;
        } else {
            ctx->sync_state = new SyncThreadState{};
        }
    }
    ctx->sync_state->layout         = &ctx->layout;
    ctx->sync_state->cxl_base       = ctx->cxl_base;
    ctx->sync_state->stop_flag      = &ctx->stop_flag;
    // gsn is now embedded in SyncThreadState (no pointer setup needed)
    ctx->sync_state->ring_producers = ctx->ring_producers;
    ctx->sync_state->req_consumers  = ctx->req_consumers;
    ctx->sync_state->num_clients    = n;
    ctx->sync_state->num_workers    = m;

    // Verify gsn is on the right NUMA node
    {
        int gsn_node = -1;
        if (get_mempolicy(&gsn_node, NULL, 0, (void*)&ctx->sync_state->gsn,
                          MPOL_F_NODE | MPOL_F_ADDR) == 0) {
            fprintf(stderr,
                "[2RW] sync_state->gsn=%p  on NUMA node %d  (Sync node %d)%s\n",
                (void*)&ctx->sync_state->gsn, gsn_node, sync_numa_node,
                (gsn_node == sync_numa_node) ? "  OK" : "  WARNING: mismatch!");
        }
    }

    ctx->worker_states = new WorkerThreadState[m];
    for (uint32_t i = 0; i < m; i++) {
        auto& ws          = ctx->worker_states[i];
        ws.worker_id      = i;
        ws.layout         = &ctx->layout;
        ws.cxl_base       = ctx->cxl_base;
        ws.stop_flag      = &ctx->stop_flag;
        ws.region_meta    = ctx->layout.data_region_meta(ctx->cxl_base, i);
        ws.ring_consumer  = &ctx->ring_consumers[i];
        ws.resp_producers = &ctx->resp_producers[i];  // stride = m
        ws.num_clients    = n;
    }
    // Fix resp_producers indexing: worker i needs resp_producers[j*m + i]
    // We give each worker a pointer to the base, worker uses [j * m + worker_id]
    for (uint32_t i = 0; i < m; i++) {
        ctx->worker_states[i].resp_producers = ctx->resp_producers;
    }

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
// CPU pinning: Sync=0, Poller=1, Workers=[2+n .. 1+n+m]
// ============================================================================

void two_rw_start_threads(TwoRWContext* ctx) {
    const uint32_t n  = ctx->config.num_clients;
    const uint32_t m  = ctx->config.num_workers;
    const int w_base  = (ctx->config.worker_cpu_start < 0)
                        ? static_cast<int>(2 + n)
                        : ctx->config.worker_cpu_start;

    // Synchronizer on CPU 0
    ctx->synchronizer_thread = std::thread([ctx]() {
        CXLBase::set(ctx->cxl_base);  // Each thread must set its own TLS base
        pin_current_thread_to_cpu(0);
        two_rw_synchronizer_run(ctx->sync_state);
    });

    // Workers on CPUs [w_base .. w_base+m-1]
    ctx->worker_threads.resize(m);
    for (uint32_t i = 0; i < m; i++) {
        ctx->worker_threads[i] = std::thread([ctx, i, w_base]() {
            CXLBase::set(ctx->cxl_base);
            pin_current_thread_to_cpu(w_base + static_cast<int>(i));
            two_rw_worker_run(&ctx->worker_states[i]);
        });
    }

    // Poller on CPU 1
    ctx->poller_thread = std::thread([ctx]() {
        CXLBase::set(ctx->cxl_base);
        pin_current_thread_to_cpu(1);
        two_rw_poller_run(ctx->poller_state);
    });

    fprintf(stderr, "[2RW] Threads started: Sync=CPU0, Poller=CPU1, "
            "Workers=CPU[%d..%d]\n", w_base, w_base + static_cast<int>(m) - 1);
}

// ============================================================================
// two_rw_stop — drain sequence per spec §6
// ============================================================================

void two_rw_stop(TwoRWContext* ctx) {
    // Step 1: Signal stop (Request Threads must already be done submitting)
    ctx->stop_flag.store(true, std::memory_order_release);

    // Step 2: Sync drains its RequestQueues (handled in sync loop)
    // Step 3: Workers drain their WorkerRings (handled in worker loop)
    // Step 4: Response Threads drain ResponseQueues (caller's responsibility)

    // Join threads
    if (ctx->synchronizer_thread.joinable())
        ctx->synchronizer_thread.join();

    for (auto& t : ctx->worker_threads)
        if (t.joinable()) t.join();

    if (ctx->poller_thread.joinable())
        ctx->poller_thread.join();

    fprintf(stderr, "[2RW] All threads stopped. GSN reached: %lu\n",
            ctx->sync_state ? ctx->sync_state->gsn.load() : 0UL);
}

// ============================================================================
// two_rw_destroy
// ============================================================================

void two_rw_destroy(TwoRWContext* ctx) {
    if (!ctx) return;

    const uint32_t n = ctx->config.num_clients;
    const uint32_t m = ctx->config.num_workers;
    const bool numa_ok = (numa_available() >= 0);
    const bool use_numa = ctx->sync_alloc_is_numa && numa_ok;

    // Non-Sync allocations → always regular delete[]
    delete[] ctx->free_id_queues;
    delete[] ctx->req_producers;    // written by Request Threads, regular new[]
    delete[] ctx->ring_consumers;   // written by Workers, regular new[]
    delete[] ctx->resp_producers;
    delete[] ctx->resp_consumers;
    delete[] ctx->resp_uintr_fds;
    delete[] ctx->resp_fd_ready;
    delete[] ctx->worker_states;
    delete   ctx->poller_state;

    // Sync-hot allocations: req_consumers[n], ring_producers[m], sync_state
    // These were allocated via numa_alloc_onnode+placement-new when sync_alloc_is_numa.
    if (use_numa) {
        // req_consumers: explicit destructor loop + numa_free
        if (ctx->req_consumers) {
            for (uint32_t j = 0; j < n; j++)
                ctx->req_consumers[j].~CXLSpscConsumer<KVRequest, QUEUE_CAP>();
            numa_free(ctx->req_consumers,
                      n * sizeof(CXLSpscConsumer<KVRequest, QUEUE_CAP>));
        }
        // ring_producers: explicit destructor loop + numa_free
        if (ctx->ring_producers) {
            for (uint32_t i = 0; i < m; i++)
                ctx->ring_producers[i].~CXLSpscProducer<KVRequest, QUEUE_CAP>();
            numa_free(ctx->ring_producers,
                      m * sizeof(CXLSpscProducer<KVRequest, QUEUE_CAP>));
        }
    } else {
        delete[] ctx->req_consumers;
        delete[] ctx->ring_producers;
    }

    // sync_state has its own tracking flag (may be numa even if arrays are not)
    if (ctx->sync_state) {
        if (ctx->sync_state_is_numa && numa_ok) {
            ctx->sync_state->~SyncThreadState();
            numa_free(ctx->sync_state, sizeof(SyncThreadState));
        } else {
            delete ctx->sync_state;
        }
    }

    if (ctx->cxl_base) {
        munmap(ctx->cxl_base, ctx->config.memory_size);
    }

    // ctx was allocated with numa_alloc_onnode + placement new (or plain new).
    // Either way, call destructor explicitly then free the raw memory.
    // numa_free is safe to call on regular malloc memory too on most systems,
    // but to be safe we use the same path: explicit dtor + numa_free if
    // numa is available, otherwise plain delete.
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
        "  request_queues  @ 0x%010lx  (%u queues)\n"
        "  worker_rings    @ 0x%010lx  (%u rings)\n"
        "  response_queues @ 0x%010lx  (%u × %u = %u queues)\n"
        "  data_region     @ 0x%010lx  (%zu MB per worker)\n",
        header_off,
        hash_table_off, num_buckets,
        data_region_meta_off, num_workers,
        pool_off, num_clients, slots_per_client,
        (size_t)((uint64_t)num_clients * slots_per_client * 2048 >> 20),
        request_queue_off, num_clients,
        worker_ring_off, num_workers,
        response_queue_off, num_clients, num_workers, num_clients * num_workers,
        data_region_off, (size_t)(data_region_per_worker >> 20));
}

} // namespace TwoRW