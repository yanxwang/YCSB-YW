// ============================================================================
// SharedKV 2RW — Context Init / Start / Stop / Destroy
// ============================================================================

#include "2rw_context.h"
#include "cxl_ptr.h"
#include "uintr_threading.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
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
void two_rw_worker_poller_run(WorkerPollerThreadState* state);
}

namespace TwoRW {

// ============================================================================
// CXL Memory Allocation
// ============================================================================

static void* allocate_cxl_slab(int numa_node, uint64_t size) {
    // ── NUMA capacity check ───────────────────────────────────────────────────
    // mbind(MPOL_BIND) strictly requires all pages to come from numa_node.
    // If the node's physical capacity is smaller than `size`, every page fault
    // beyond its capacity raises SIGBUS (Bus Error) during memset.
    // Check total node memory before committing to the allocation.
    if (numa_node >= 0 && numa_available() >= 0) {
        long long free_mem  = 0;
        long long total_mem = numa_node_size64(numa_node, &free_mem);
        if (total_mem < 0) {
            fprintf(stderr, "[2RW] WARNING: Cannot query NUMA node %d memory size\n",
                    numa_node);
        } else {
            fprintf(stderr,
                "[2RW] NUMA node %d: total=%lld MB, free=%lld MB, requested=%llu MB\n",
                numa_node, total_mem >> 20, free_mem >> 20,
                (unsigned long long)size >> 20);
            if ((unsigned long long)size > (unsigned long long)total_mem) {
                fprintf(stderr,
                    "[2RW] ERROR: requested %llu MB exceeds NUMA node %d physical "
                    "capacity (%lld MB).\n"
                    "       Reduce --mem-gb to %lld or less.\n",
                    (unsigned long long)size >> 20, numa_node,
                    total_mem >> 20, total_mem >> 20);
                return nullptr;
            }
        }
    }

    // Use MAP_ANONYMOUS | MAP_PRIVATE so that physical pages come directly
    // from the CXL NUMA node (via mbind) without going through /dev/shm tmpfs.
    // shm_open + MAP_SHARED was subject to the /dev/shm tmpfs mount-size limit
    // (typically ~half of DRAM, far smaller than CXL capacity), which caused
    // Bus Errors on large allocations that exceed that limit.
    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

    if (ptr == MAP_FAILED) {
        perror("mmap(MAP_ANONYMOUS)");
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
    hdr->num_synchronizers = cfg.num_synchronizers;
    hdr->total_size       = L.total_size;
    hdr->num_nodes        = cfg.num_nodes;
    hdr->ready_flag       = 0;          // set to HEADER_READY after init completes
    hdr->global_stop_flag = 0;

    // Initialize CXL Bitmap: bit=1 → FREE, bit=0 → IN-USE.
    // Start with all blocks free, then mark pre-allocated and sentinel as in-use.
    {
        uint64_t* bmap = reinterpret_cast<uint64_t*>(L.globalidmap(base));
        uint32_t total_words = (L.total_blocks + 63) / 64;

        // All blocks free
        memset(bmap, 0xFF, total_words * sizeof(uint64_t));

        // Block 0 reserved (sentinel)
        bmap[0] &= ~1ULL;

        // Pre-allocated blocks [1 .. n*slots_per_client] → in-use
        uint32_t pre_alloc_end = cfg.num_clients * cfg.slots_per_client;
        for (uint32_t id = 1; id <= pre_alloc_end; id++) {
            bmap[id / 64] &= ~(1ULL << (id % 64));
        }

        // Bits beyond total_blocks in last word → mark invalid (in-use)
        uint32_t tail_bits = L.total_blocks % 64;
        if (tail_bits != 0) {
            bmap[total_words - 1] &= (1ULL << tail_bits) - 1;
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

    // Signal CXL memory is fully initialized (multi-machine: worker nodes spin on this).
    // Flush to CXL DRAM so remote nodes see it via CLFLUSHOPT.
    hdr->ready_flag = HEADER_READY;
    _mm_sfence();
    _mm_clflushopt(&hdr->ready_flag);
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
// CXL DAX Device Mmap (multi-machine)
// ============================================================================

static void* mmap_cxl_device(const char* device_path, uint64_t size) {
    int fd = open(device_path, O_RDWR);
    if (fd < 0) {
        fprintf(stderr, "[2RW] ERROR: cannot open CXL device '%s': %s\n",
                device_path, strerror(errno));
        return nullptr;
    }

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_POPULATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[2RW] ERROR: mmap CXL device '%s' (%lu MB) failed: %s\n",
                device_path, (unsigned long)(size >> 20), strerror(errno));
        return nullptr;
    }

    fprintf(stderr, "[2RW] CXL device '%s' mapped at %p (%lu MB)\n",
            device_path, ptr, (unsigned long)(size >> 20));
    return ptr;
}

// ============================================================================
// CXL Physical Address Mmap (system-ram mode, multi-machine via /dev/mem)
// ============================================================================

static void* mmap_cxl_phys(uint64_t phys_base, uint64_t size) {
    int fd = open("/dev/mem", O_RDWR | O_SYNC);
    if (fd < 0) {
        fprintf(stderr, "[2RW] ERROR: cannot open /dev/mem: %s\n"
                "       (requires root and CONFIG_DEVMEM=y, nopat kernel param may be needed)\n",
                strerror(errno));
        return nullptr;
    }

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_POPULATE, fd, (off_t)phys_base);
    close(fd);

    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[2RW] ERROR: mmap /dev/mem phys=0x%lx size=%lu MB failed: %s\n",
                (unsigned long)phys_base, (unsigned long)(size >> 20), strerror(errno));
        return nullptr;
    }

    fprintf(stderr, "[2RW] CXL physical memory mapped: phys=0x%lx virt=%p (%lu MB)\n",
            (unsigned long)phys_base, ptr, (unsigned long)(size >> 20));
    return ptr;
}

// Helper: resolve CXL memory mapping for multi-machine mode.
// Priority: --cxl-device (devdax) > --cxl-phys-base (/dev/mem) > NUMA slab (master only).
static void* resolve_cxl_mmap(const TwoRWConfig& cfg) {
    if (!cfg.cxl_device_path.empty()) {
        return mmap_cxl_device(cfg.cxl_device_path.c_str(), cfg.memory_size);
    }
    if (cfg.cxl_phys_base != 0) {
        return mmap_cxl_phys(cfg.cxl_phys_base, cfg.memory_size);
    }
    return nullptr;  // caller handles fallback
}

// ============================================================================
// Allocate TwoRWContext — common to all init paths
// ============================================================================

static TwoRWContext* allocate_context(const TwoRWConfig& cfg) {
    if (!cfg.validate()) {
        fprintf(stderr, "[2RW] Invalid config\n");
        cfg.print();
        return nullptr;
    }
    cfg.print();

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
    ctx->config.finalize_ranges();   // fill sn_count/worker_count/client_count if 0
    ctx->layout = TwoRWLayout::calculate(
        cfg.num_clients, cfg.num_workers,
        cfg.slots_per_client, cfg.num_buckets,
        cfg.memory_size, cfg.num_synchronizers);

    return ctx;
}

// ============================================================================
// init_local_structures — common to all init paths
//
// Called after cxl_base and layout are set.  Allocates all local DRAM
// structures: worker_to_sn, block caches, queue handles, thread states.
// ============================================================================

static void init_local_structures(TwoRWContext* ctx) {
    const TwoRWConfig& cfg = ctx->config;
    const uint32_t n = cfg.num_clients;
    const uint32_t m = cfg.num_workers;
    const uint32_t s = cfg.num_synchronizers;

    // Build worker_to_sn lookup table (supports both even and flexible allocation)
    ctx->worker_to_sn = cfg.build_worker_to_sn();

    // ---- Allocate local (non-CXL) structures ----

    // FreeBlockQueues — one per client (no pre-fill; LocalBlockCache handles init stock)
    ctx->free_block_queues = new FreeBlockQueue<QUEUE_CAP>[n];
    for (uint32_t j = 0; j < n; j++) {
        ctx->free_block_queues[j].init();
    }

    // LocalBlockCaches — pre-fill client j with block IDs [j*K+1 .. (j+1)*K]
    // (block 0 is reserved sentinel, never allocated)
    ctx->local_block_caches = new LocalBlockCache[n];
    for (uint32_t j = 0; j < n; j++) {
        auto& cache = ctx->local_block_caches[j];
        const uint32_t base_id = j * cfg.slots_per_client + 1;
        uint32_t k = 0;
        // Fill local stack (capacity LOCAL_CACHE_CAP)
        for (; k < cfg.slots_per_client && cache.top < LOCAL_CACHE_CAP; k++)
            cache.stack[cache.top++] = base_id + k;
        // Overflow: push remaining into FreeBlockQueue
        for (; k < cfg.slots_per_client; k++)
            ctx->free_block_queues[j].push(base_id + k);
    }

    // Initialize bitmap pointer and scan hints
    {
        ctx->block_bitmap = reinterpret_cast<volatile uint64_t*>(
            ctx->layout.globalidmap(ctx->cxl_base));
        ctx->block_bitmap_words = (ctx->layout.total_blocks + 63) / 64;

        // Spread scan_hints across bitmap to reduce contention
        uint32_t total_words = ctx->block_bitmap_words;
        for (uint32_t j = 0; j < n; j++) {
            ctx->local_block_caches[j].scan_hint = (j * total_words) / n;
        }
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

    // ---- Per-SN: req_consumers[n] and ring_producers[w_cnt] ----
    // These are written by SN_k on every op → NUMA-allocate on SN_k's CPU node.
    // CPU layout: SN_k runs on CPU (k+2).
    ctx->sn_req_consumers   = new CXLSpscConsumer<KVRequest, QUEUE_CAP>*[s]{};
    ctx->sn_ring_producers  = new CXLSpscProducer<KVRequest, QUEUE_CAP>*[s]{};
    ctx->sn_req_con_is_numa  = new bool[s]{};
    ctx->sn_ring_prod_is_numa = new bool[s]{};
    ctx->sn_state_is_numa    = new bool[s]{};
    ctx->sync_state_ptrs     = new SyncThreadState*[s]{};

    for (uint32_t k = 0; k < s; k++) {
        const int sn_cpu      = static_cast<int>(k + 2);  // SN_k on CPU k+2 (CPU 0-1 reserved for pollers)
        const int sn_numa     = (numa_available() >= 0) ? numa_node_of_cpu(sn_cpu) : -1;
        const uint32_t w_base = cfg.workers_base_for_sn(ctx->worker_to_sn, k);
        const uint32_t w_cnt  = cfg.workers_count_for_sn(ctx->worker_to_sn, k);

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

        // ---- ring_producers[w_cnt] for SN_k's CXL WorkerRings ----
        {
            size_t sz = w_cnt * sizeof(CXLSpscProducer<KVRequest, QUEUE_CAP>);
            void* raw = try_numa_alloc(sz, sn_numa);
            if (raw) {
                ctx->sn_ring_producers[k] =
                    static_cast<CXLSpscProducer<KVRequest, QUEUE_CAP>*>(raw);
                for (uint32_t i = 0; i < w_cnt; i++)
                    new (&ctx->sn_ring_producers[k][i])
                        CXLSpscProducer<KVRequest, QUEUE_CAP>();
                ctx->sn_ring_prod_is_numa[k] = true;
            } else {
                ctx->sn_ring_producers[k] =
                    new CXLSpscProducer<KVRequest, QUEUE_CAP>[w_cnt];
            }
            // Attach to WorkerRings[w_base .. w_base+w_cnt-1]
            for (uint32_t i = 0; i < w_cnt; i++)
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

    // ---- UINTR fd arrays (Response Threads) ----
    ctx->resp_uintr_fds = new int[n];
    ctx->resp_fd_ready  = new std::atomic<bool>[n];
    for (uint32_t j = 0; j < n; j++) {
        ctx->resp_uintr_fds[j] = -1;
        ctx->resp_fd_ready[j].store(false);
    }

    // ---- UINTR fd arrays (Worker Threads) ----
    ctx->worker_uintr_fds = new int[m];
    ctx->worker_fd_ready  = new std::atomic<bool>[m];
    for (uint32_t i = 0; i < m; i++) {
        ctx->worker_uintr_fds[i] = -1;
        ctx->worker_fd_ready[i].store(false);
    }

    // ---- SyncThreadState[s]: one per SN, NUMA-allocated on SN's CPU node ----
    for (uint32_t k = 0; k < s; k++) {
        const int sn_cpu  = static_cast<int>(k + 2);
        const int sn_numa = (numa_available() >= 0) ? numa_node_of_cpu(sn_cpu) : -1;
        const uint32_t w_base = cfg.workers_base_for_sn(ctx->worker_to_sn, k);
        const uint32_t w_cnt  = cfg.workers_count_for_sn(ctx->worker_to_sn, k);

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
        ss->workers_count        = w_cnt;
        ss->dequeue_batch        = cfg.dequeue_batch;
        ss->read_ack_batch       = cfg.read_ack_batch;
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
        ws.sn_id_for_resp        = ctx->worker_to_sn[i];
        ws.ring_consumer         = &ctx->ring_consumers[i];
        ws.local_ring_consumer   = cfg.local_workerring
                                   ? &ctx->local_ring_consumers[i] : nullptr;
        ws.use_local_ring        = cfg.local_workerring;
        ws.resp_producers        = ctx->resp_producers;
        ws.num_clients           = n;
        ws.stats_enabled         = cfg.stats_enabled;
        ws.use_uintr             = cfg.worker_thread_uses_uintr();
        ws.worker_check          = cfg.worker_check;
        ws.worker_uintr_fds      = ctx->worker_uintr_fds;
        ws.worker_fd_ready       = ctx->worker_fd_ready;
    }

    // ---- PollerThreadState (response poller, when has_response_poller) ----
    if (cfg.has_response_poller()) {
        ctx->poller_state = new PollerThreadState{};
        ctx->poller_state->layout          = &ctx->layout;
        ctx->poller_state->cxl_base        = ctx->cxl_base;
        ctx->poller_state->stop_flag       = &ctx->stop_flag;
        ctx->poller_state->num_clients     = n;
        ctx->poller_state->num_workers     = m;
        ctx->poller_state->global_client_start    = cfg.global_client_start;
        ctx->poller_state->global_client_count    = cfg.global_client_count;
        ctx->poller_state->resp_uintr_fds  = ctx->resp_uintr_fds;
        ctx->poller_state->resp_fd_ready   = ctx->resp_fd_ready;
    }

    // ---- WorkerPollerThreadState (worker poller, when has_worker_poller) ----
    if (cfg.has_worker_poller()) {
        ctx->worker_poller_state = new WorkerPollerThreadState{};
        ctx->worker_poller_state->layout            = &ctx->layout;
        ctx->worker_poller_state->cxl_base          = ctx->cxl_base;
        ctx->worker_poller_state->stop_flag         = &ctx->stop_flag;
        ctx->worker_poller_state->num_workers       = m;
        ctx->worker_poller_state->global_worker_start      = cfg.global_worker_start;
        ctx->worker_poller_state->global_worker_count      = cfg.global_worker_count;
        ctx->worker_poller_state->use_local_ring    = cfg.local_workerring;
        ctx->worker_poller_state->local_rings       = ctx->local_rings;
        ctx->worker_poller_state->worker_uintr_fds  = ctx->worker_uintr_fds;
        ctx->worker_poller_state->worker_fd_ready   = ctx->worker_fd_ready;
    }
}

// ============================================================================
// two_rw_init — Single-machine init (backward compatible)
// ============================================================================

TwoRWContext* two_rw_init(const TwoRWConfig& cfg) {
    TwoRWContext* ctx = allocate_context(cfg);
    if (!ctx) return nullptr;

    // Allocate CXL slab via mmap anonymous + mbind
    ctx->cxl_base = allocate_cxl_slab(cfg.numa_node, cfg.memory_size);
    if (!ctx->cxl_base) {
        delete ctx;
        return nullptr;
    }

    CXLBase::set(ctx->cxl_base);
    init_cxl_memory(ctx->cxl_base, ctx->layout, cfg);
    init_local_structures(ctx);
    return ctx;
}

// ============================================================================
// two_rw_master_init — Multi-machine master (node 0)
//
// Allocates CXL memory (via DAX device or anonymous mmap), initializes all
// CXL queues and header, then sets up local structures for this node's roles.
// ============================================================================

TwoRWContext* two_rw_master_init(const TwoRWConfig& cfg) {
    TwoRWContext* ctx = allocate_context(cfg);
    if (!ctx) return nullptr;

    // Allocate CXL memory: devdax > /dev/mem > NUMA slab (single-machine fallback)
    ctx->cxl_base = resolve_cxl_mmap(cfg);
    if (!ctx->cxl_base) {
        ctx->cxl_base = allocate_cxl_slab(cfg.numa_node, cfg.memory_size);
    }
    if (!ctx->cxl_base) {
        delete ctx;
        return nullptr;
    }

    CXLBase::set(ctx->cxl_base);
    init_cxl_memory(ctx->cxl_base, ctx->layout, cfg);

    // Set up multi-machine pointers
    TwoRWHeader* hdr = ctx->layout.header(ctx->cxl_base);
    ctx->cxl_global_stop_flag = &hdr->global_stop_flag;
    ctx->cxl_node_sync = reinterpret_cast<CXLNodeSync*>(
        static_cast<char*>(ctx->cxl_base) + sizeof(TwoRWHeader));

    fprintf(stderr, "[2RW] Master init: node_id=%u, num_nodes=%u\n",
            cfg.node_id, cfg.num_nodes);

    init_local_structures(ctx);
    return ctx;
}

// ============================================================================
// two_rw_slave_attach — Multi-machine worker node (node > 0)
//
// Mmaps the same CXL device, waits for master's ready_flag, validates header,
// then sets up local structures.  Does NOT memset or init CXL queues.
// ============================================================================

TwoRWContext* two_rw_slave_attach(const TwoRWConfig& cfg) {
    TwoRWContext* ctx = allocate_context(cfg);
    if (!ctx) return nullptr;

    // Slave must map the same physical CXL memory as master (devdax or /dev/mem)
    ctx->cxl_base = resolve_cxl_mmap(cfg);
    if (!ctx->cxl_base) {
        fprintf(stderr, "[2RW] ERROR: slave_attach requires --cxl-device or --cxl-phys-base\n");
        delete ctx;
        return nullptr;
    }

    CXLBase::set(ctx->cxl_base);

    // Wait for master to finish initializing CXL memory
    TwoRWHeader* hdr = ctx->layout.header(ctx->cxl_base);
    fprintf(stderr, "[2RW] Node %u: waiting for master ready_flag...\n", cfg.node_id);

    while (hdr->ready_flag != HEADER_READY) {
        _mm_clflushopt(&hdr->ready_flag);
        _mm_lfence();
        _mm_pause();
    }

    // Validate header matches our expected global config
    if (hdr->magic != HEADER_MAGIC) {
        fprintf(stderr, "[2RW] ERROR: header magic mismatch (got 0x%lx, expected 0x%lx)\n",
                hdr->magic, HEADER_MAGIC);
        munmap(ctx->cxl_base, cfg.memory_size);
        delete ctx;
        return nullptr;
    }
    if (hdr->num_clients != cfg.num_clients || hdr->num_workers != cfg.num_workers ||
        hdr->num_synchronizers != cfg.num_synchronizers) {
        fprintf(stderr, "[2RW] ERROR: header config mismatch "
                "(n=%u/%u, m=%u/%u, s=%u/%u)\n",
                hdr->num_clients, cfg.num_clients,
                hdr->num_workers, cfg.num_workers,
                hdr->num_synchronizers, cfg.num_synchronizers);
        munmap(ctx->cxl_base, cfg.memory_size);
        delete ctx;
        return nullptr;
    }

    fprintf(stderr, "[2RW] Node %u: attached to CXL memory, header validated.\n",
            cfg.node_id);

    // Set up multi-machine pointers
    ctx->cxl_global_stop_flag = &hdr->global_stop_flag;
    ctx->cxl_node_sync = reinterpret_cast<CXLNodeSync*>(
        static_cast<char*>(ctx->cxl_base) + sizeof(TwoRWHeader));

    init_local_structures(ctx);
    return ctx;
}

// ============================================================================
// two_rw_start_threads
// CPU layout:
//   CPU 0          : Response Poller (if has_response_poller)
//   CPU 1          : Worker Poller   (if has_worker_poller)
//   CPU 2..2+s-1   : Synchronizers
//   CPU w_base..   : Workers (w_base default = 2+s)
// CPU 0 and 1 are always reserved regardless of poller_mode.
// ============================================================================

void two_rw_start_threads(TwoRWContext* ctx) {
    const auto& cfg = ctx->config;
    // Local ranges (finalize_ranges() already called during init)
    const uint32_t local_sn_count     = cfg.global_sn_count;
    const uint32_t local_worker_count = cfg.global_worker_count;
    const uint32_t sn_start           = cfg.global_sn_start;
    const uint32_t worker_start       = cfg.global_worker_start;
    // CPU layout uses local indices: SNs on CPU [2 .. 2+local_sn_count-1],
    // Workers on CPU [w_base .. w_base+local_worker_count-1]
    const int w_base = (cfg.worker_cpu_start < 0)
                       ? static_cast<int>(2 + local_sn_count)
                       : cfg.worker_cpu_start;

    // Response Poller on CPU 0 (when active and this node has clients)
    if (cfg.has_response_poller() && cfg.global_client_count > 0) {
        ctx->resp_poller_thread = std::thread([ctx]() {
            CXLBase::set(ctx->cxl_base);
            pin_current_thread_to_cpu(0);
            two_rw_poller_run(ctx->poller_state);
        });
    }

    // Worker Poller on CPU 1 (when active and this node has workers)
    if (cfg.has_worker_poller() && local_worker_count > 0) {
        ctx->worker_poller_thread = std::thread([ctx]() {
            CXLBase::set(ctx->cxl_base);
            pin_current_thread_to_cpu(1);
            two_rw_worker_poller_run(ctx->worker_poller_state);
        });
    }

    // Synchronizers: local SN k_local on CPU (k_local + 2)
    ctx->synchronizer_threads.resize(local_sn_count);
    for (uint32_t k_local = 0; k_local < local_sn_count; k_local++) {
        uint32_t k_global = sn_start + k_local;
        ctx->synchronizer_threads[k_local] = std::thread([ctx, k_global, k_local]() {
            CXLBase::set(ctx->cxl_base);
            pin_current_thread_to_cpu(static_cast<int>(k_local + 2));
            two_rw_synchronizer_run(ctx->sync_state_ptrs[k_global]);
        });
    }

    // Workers on CPUs [w_base .. w_base + local_worker_count - 1]
    ctx->worker_threads.resize(local_worker_count);
    for (uint32_t i_local = 0; i_local < local_worker_count; i_local++) {
        uint32_t i_global = worker_start + i_local;
        ctx->worker_threads[i_local] = std::thread([ctx, i_global, i_local, w_base]() {
            CXLBase::set(ctx->cxl_base);
            pin_current_thread_to_cpu(w_base + static_cast<int>(i_local));
            two_rw_worker_run(&ctx->worker_states[i_global]);
        });
    }

    const char* pm = poller_mode_name(cfg.poller_mode);
    fprintf(stderr,
            "[2RW] Threads started: poller_mode=%s, "
            "SN[%u..%u]=CPU[2..%u], Workers[%u..%u]=CPU[%d..%d]\n",
            pm,
            sn_start, sn_start + local_sn_count - 1,
            1 + local_sn_count,
            worker_start, worker_start + local_worker_count - 1,
            w_base, w_base + static_cast<int>(local_worker_count) - 1);
}

// ============================================================================
// two_rw_stop — drain sequence per spec §6
// ============================================================================

void two_rw_stop(TwoRWContext* ctx) {
    const uint32_t s = ctx->config.num_synchronizers;

    // Multi-node master: signal all nodes via CXL global_stop_flag
    if (ctx->config.is_multi_node() && ctx->config.is_master() &&
        ctx->cxl_global_stop_flag) {
        *ctx->cxl_global_stop_flag = 1;
        _mm_sfence();
        _mm_clflushopt(const_cast<uint32_t*>(ctx->cxl_global_stop_flag));
        _mm_sfence();
        fprintf(stderr, "[2RW] Master: CXL global_stop_flag set\n");
    }

    // Step 1: Signal local stop (Request Threads must already be done submitting)
    ctx->stop_flag.store(true, std::memory_order_release);

    // Step 2: SNs drain their RequestQueues (handled in sync loop)
    // Step 3: Workers drain their WorkerRings (handled in worker loop)
    // Step 4: Response Threads drain ResponseQueues (caller's responsibility)

    for (auto& t : ctx->synchronizer_threads)
        if (t.joinable()) t.join();

    for (auto& t : ctx->worker_threads)
        if (t.joinable()) t.join();

    if (ctx->resp_poller_thread.joinable())
        ctx->resp_poller_thread.join();

    if (ctx->worker_poller_thread.joinable())
        ctx->worker_poller_thread.join();

    // Print final GSN per SN (--verbose only)
    if (ctx->config.verbose) {
        for (uint32_t k = 0; k < s; k++) {
            if (ctx->sync_state_ptrs && ctx->sync_state_ptrs[k]) {
                fprintf(stderr, "[2RW] SN%u final GSN: %lu\n",
                        k, ctx->sync_state_ptrs[k]->gsn.load());
            }
        }
    }
}

// ============================================================================
// two_rw_node_barrier — Distributed barrier via CXL per-node ready flags
//
// Two-phase approach (simpler than CXL CAS):
//   Phase 1: Caller does local synchronization (e.g., pthread_barrier for
//            local RT + RespTh threads). Not our concern here.
//   Phase 2: Each node's main thread sets its CXL ready flag, then waits
//            until all nodes' flags are set.
//
// Single-machine: no-op (only one node).
// ============================================================================

void two_rw_node_barrier(TwoRWContext* ctx) {
    if (!ctx->config.is_multi_node() || !ctx->cxl_node_sync)
        return;

    const uint32_t node_id   = ctx->config.node_id;
    const uint32_t num_nodes = ctx->config.num_nodes;

    // Set this node's ready flag and flush to CXL DRAM.
    // Regular store + SFENCE (drain store buffer → dirty in cache) +
    // CLFLUSHOPT (flush dirty → CXL DRAM) + SFENCE (ensure complete).
    // Without the CLFLUSHOPT, the flag stays in local cache and remote
    // nodes polling via CLFLUSHOPT never see it.
    ctx->cxl_node_sync->node_ready[node_id] = 1;
    _mm_sfence();
    _mm_clflushopt(const_cast<uint32_t*>(&ctx->cxl_node_sync->node_ready[node_id]));
    _mm_sfence();

    fprintf(stderr, "[2RW] Node %u: barrier — waiting for %u nodes...\n",
            node_id, num_nodes);

    // Wait until all nodes are ready
    while (true) {
        bool all_ready = true;
        for (uint32_t k = 0; k < num_nodes; k++) {
            _mm_clflushopt(const_cast<uint32_t*>(&ctx->cxl_node_sync->node_ready[k]));
            _mm_lfence();
            if (!ctx->cxl_node_sync->node_ready[k]) {
                all_ready = false;
                break;
            }
        }
        if (all_ready) break;
        _mm_pause();
    }

    fprintf(stderr, "[2RW] Node %u: all %u nodes ready.\n", node_id, num_nodes);
}

// ============================================================================
// two_rw_wait_global_stop — Slave node waits for master's stop signal
//
// Polls CXL global_stop_flag until master sets it, then calls two_rw_stop()
// to set local stop_flag and join threads.
// Single-machine: no-op.
// ============================================================================

void two_rw_wait_global_stop(TwoRWContext* ctx) {
    if (!ctx->config.is_multi_node() || !ctx->cxl_global_stop_flag)
        return;

    fprintf(stderr, "[2RW] Node %u: waiting for global stop...\n",
            ctx->config.node_id);

    while (true) {
        _mm_clflushopt(const_cast<uint32_t*>(ctx->cxl_global_stop_flag));
        _mm_lfence();
        if (*ctx->cxl_global_stop_flag) break;
        _mm_pause();
    }

    fprintf(stderr, "[2RW] Node %u: global stop detected, stopping local threads.\n",
            ctx->config.node_id);

    two_rw_stop(ctx);
}

// ============================================================================
// two_rw_destroy
// ============================================================================

void two_rw_destroy(TwoRWContext* ctx) {
    if (!ctx) return;

    const uint32_t n = ctx->config.num_clients;
    const uint32_t m = ctx->config.num_workers;
    const uint32_t s = ctx->config.num_synchronizers;
    const bool numa_ok = (numa_available() >= 0);

    // Non-SN allocations → always regular delete[]
    delete[] ctx->free_block_queues;
    delete[] ctx->local_block_caches;
    delete[] ctx->req_producers;
    delete[] ctx->ring_consumers;
    delete[] ctx->resp_producers;
    delete[] ctx->resp_consumers;
    delete[] ctx->resp_uintr_fds;
    delete[] ctx->resp_fd_ready;
    delete[] ctx->worker_uintr_fds;
    delete[] ctx->worker_fd_ready;
    delete[] ctx->worker_states;
    delete   ctx->poller_state;
    delete   ctx->worker_poller_state;

    // Local DRAM rings (regular new[], only allocated when local_workerring=true)
    delete[] ctx->local_ring_consumers;
    delete[] ctx->local_ring_producers;
    delete[] ctx->local_rings;

    // Per-SN allocations: req_consumers, ring_producers, sync_state
    if (ctx->sync_state_ptrs) {
        for (uint32_t k = 0; k < s; k++) {
            const uint32_t w_cnt = ctx->worker_to_sn
                ? ctx->config.workers_count_for_sn(ctx->worker_to_sn, k)
                : (m / s);

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
            // ring_producers[w_cnt]
            if (ctx->sn_ring_producers && ctx->sn_ring_producers[k]) {
                if (ctx->sn_ring_prod_is_numa && ctx->sn_ring_prod_is_numa[k] && numa_ok) {
                    for (uint32_t i = 0; i < w_cnt; i++)
                        ctx->sn_ring_producers[k][i]
                            .~CXLSpscProducer<KVRequest, QUEUE_CAP>();
                    numa_free(ctx->sn_ring_producers[k],
                              w_cnt * sizeof(CXLSpscProducer<KVRequest, QUEUE_CAP>));
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
    delete[] ctx->worker_to_sn;

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
        "  header            @ 0x%010lx  (64B)\n"
        "  hash_table        @ 0x%010lx  (%u buckets × 8B = %zu KB)\n"
        "  request_queues    @ 0x%010lx  (%u clients × %u SNs = %u queues)\n"
        "  worker_rings      @ 0x%010lx  (%u rings)\n"
        "  response_queues   @ 0x%010lx  (%u × %u = %u queues)\n"
        "  globalidmap       @ 0x%010lx  (%u blocks → %zu B bitmap)\n"
        "  unifiedblockpool  @ 0x%010lx  (%u blocks × 2048B = %zu MB)\n"
        "  spillover         @ 0x%010lx  (%u clients × %zu KB = %zu KB)\n",
        header_off,
        hash_table_off, num_buckets,
        (size_t)((uint64_t)num_buckets * 8 >> 10),
        request_queue_off, num_clients, num_synchronizers,
        num_clients * num_synchronizers,
        worker_ring_off, num_workers,
        response_queue_off, num_clients, num_workers, num_clients * num_workers,
        globalidmap_off, total_blocks,
        (size_t)((total_blocks + 7) / 8),
        unifiedblockpool_off, total_blocks,
        (size_t)((uint64_t)total_blocks * 2048 >> 20),
        spillover_off, num_clients,
        (size_t)(spillover_per_client >> 10),
        (size_t)((uint64_t)num_clients * spillover_per_client >> 10));
}

// ============================================================================
// two_rw_print_worker_stats
//
// Called by the main thread after two_rw_stop() (all workers joined).
// Prints per-worker exit stats in order 0..m-1 — no interleaving possible.
//
//   --verbose:  ops=X  empty_polls=Y per worker
//   --stats:    GET/PUT/DEL chain-depth breakdown per worker
//
// Also frees the WorkerOpStats heap objects transferred from worker threads.
// ============================================================================

void two_rw_print_worker_stats(TwoRWContext* ctx) {
    const uint32_t m       = ctx->config.num_workers;
    const bool verbose     = ctx->config.verbose;
    const bool stats       = ctx->config.stats_enabled;
    const bool counters    = ctx->config.counters_enabled;

    if (!verbose && !stats && !counters) return;

    printf("\n===== Worker Summary (0..%u) =====\n", m - 1);

    // ── Worker counters (--counters) ──
    if (counters) {
        printf("--- Workers (dequeue ← WorkerRing, enqueue → ResponseQueue) ---\n");
        printf("  %-8s  %12s  %12s  %12s  %12s\n",
               "Worker", "ops_done", "empty_polls", "resp_fullwaits", "uintr_wakes");
        for (uint32_t i = 0; i < m; i++) {
            WorkerThreadState& ws = ctx->worker_states[i];
            printf("  W-%-6u  %12lu  %12lu  %12lu  %12lu\n",
                   i, ws.exit_ops_done, ws.exit_empty_polls,
                   ws.exit_resp_fullwaits, ws.exit_uintr_wakeups);
        }
        printf("\n");
    }

    // Mismatch summary — always printed when any mismatch was detected
    {
        uint64_t total_route = 0, total_block = 0;
        for (uint32_t i = 0; i < m; i++) {
            total_route += ctx->worker_states[i].exit_route_mismatches;
            total_block += ctx->worker_states[i].exit_block_mismatches;
        }
        if (total_route > 0 || total_block > 0) {
            printf("--- Mismatch Diagnostics ---\n");
            printf("  %-8s  %16s  %16s\n",
                   "Worker", "route_mismatches", "block_mismatches");
            for (uint32_t i = 0; i < m; i++) {
                WorkerThreadState& ws = ctx->worker_states[i];
                if (ws.exit_route_mismatches == 0 && ws.exit_block_mismatches == 0)
                    continue;
                printf("  W-%-6u  %16lu  %16lu\n",
                       i, ws.exit_route_mismatches, ws.exit_block_mismatches);
            }
            printf("  TOTAL     %16lu  %16lu\n", total_route, total_block);
            printf("  route_mismatches: req.worker_id != worker_id (SN routing bug)\n");
            printf("  block_mismatches: recomputed FNV(key)%%m != worker_id"
                   " (stale/wrong CXL block data)\n");
            printf("\n");
        }
    }

    for (uint32_t i = 0; i < m; i++) {
        WorkerThreadState& ws = ctx->worker_states[i];

        if (verbose && !counters) {
            printf("  Worker %-3u  ops=%-12lu  empty_polls=%-12lu  resp_fullwaits=%lu\n",
                   i, ws.exit_ops_done, ws.exit_empty_polls, ws.exit_resp_fullwaits);
        }

        if (stats && ws.exit_stats) {
            WorkerOpStats& st = *ws.exit_stats;

            // Print header only when verbose/counters didn't already print worker line
            if (!verbose && !counters) printf("  Worker %u:\n", i);

            auto print_op = [](const char* name,
                                const WorkerOpStats::PerOp& op) {
                if (op.total == 0) return;
                double avg = (double)op.chain_sum / (double)op.total;
                printf("    %-10s  total=%8lu  hit=%8lu  miss=%8lu"
                       "  chain_avg=%5.2f\n",
                       name, op.total, op.hit, op.miss, avg);
                // Print non-zero histogram buckets
                bool hdr = false;
                for (int d = 0; d <= 16; d++) {
                    if (op.hist[d] == 0) continue;
                    if (!hdr) { printf("               depth:"); hdr = true; }
                    if (d < 16) printf("  [%2d]=%lu", d, op.hist[d]);
                    else        printf("  [16+]=%lu", op.hist[d]);
                }
                if (hdr) printf("\n");
            };

            print_op("GET",        st.get);
            print_op("PUT/UPDATE", st.put);
            print_op("DEL",        st.del);

            delete ws.exit_stats;
            ws.exit_stats = nullptr;
        }
    }

    // ── Response Poller counters (--counters) ──
    if (counters && ctx->poller_state) {
        printf("--- Response Poller (scan ResponseQueues, send UINTR) ---\n");
        printf("  scan_rounds:  %lu\n", ctx->poller_state->exit_scan_rounds);
        printf("  uintrs_sent:  %lu\n", ctx->poller_state->exit_uintrs_sent);
        if (ctx->poller_state->exit_uintrs_sent > 0)
            printf("  rounds/uintr: %.1f\n",
                   (double)ctx->poller_state->exit_scan_rounds /
                   ctx->poller_state->exit_uintrs_sent);
        printf("\n");
    }

    // ── Worker Poller counters (--counters) ──
    if (counters && ctx->worker_poller_state) {
        printf("--- Worker Poller (scan WorkerRings, send UINTR) ---\n");
        printf("  scan_rounds:  %lu\n", ctx->worker_poller_state->exit_scan_rounds);
        printf("  uintrs_sent:  %lu\n", ctx->worker_poller_state->exit_uintrs_sent);
        if (ctx->worker_poller_state->exit_uintrs_sent > 0)
            printf("  rounds/uintr: %.1f\n",
                   (double)ctx->worker_poller_state->exit_scan_rounds /
                   ctx->worker_poller_state->exit_uintrs_sent);
        printf("\n");
    }

    printf("==================================\n\n");
}

// ============================================================================
// two_rw_print_bucket_stats
//
// Scans the entire hash table bucket array, computes chain-length histogram,
// and prints a distribution report.  Call from the main thread after the
// LOAD phase (Workers idle) when --stats is enabled.
//
// CXL reads: one per chain node (random-access pattern — same as GET).
// ============================================================================

void two_rw_print_bucket_stats(TwoRWContext* ctx) {
    const uint32_t num_buckets = ctx->layout.num_buckets;
    CXLBucket* buckets  = ctx->layout.bucket_table(ctx->cxl_base);
    void*      pool_base = static_cast<char*>(ctx->cxl_base)
                           + ctx->layout.unifiedblockpool_off;

    // hist[d] = # buckets with chain length exactly d (d=0..15, d=16 means 16+)
    uint64_t hist[17]    = {};
    uint64_t total_nodes = 0;
    uint64_t non_empty   = 0;
    uint64_t max_chain   = 0;

    for (uint32_t b = 0; b < num_buckets; b++) {
        uint32_t cur   = buckets[b].head_block_id;
        uint32_t depth = 0;
        while (cur != 0) {
            depth++;
            UnifiedBlock* blk = reinterpret_cast<UnifiedBlock*>(
                static_cast<char*>(pool_base) + static_cast<uint64_t>(cur) * 2048ULL);
            cur = static_cast<uint32_t>(blk->next_block_id);
        }
        hist[(depth < 16) ? depth : 16]++;
        total_nodes += depth;
        if (depth > 0) non_empty++;
        if (depth > max_chain) max_chain = depth;
    }

    printf("\n=== Hash Table Bucket Chain Length Distribution ===\n");
    printf("  Total buckets : %u\n",    num_buckets);
    printf("  Non-empty     : %lu  (%.1f%%)\n",
           non_empty, 100.0 * non_empty / num_buckets);
    printf("  Total nodes   : %lu\n",   total_nodes);
    printf("  Max chain     : %lu\n",   max_chain);
    printf("  Avg chain len (all buckets)       : %.4f\n",
           (double)total_nodes / num_buckets);
    if (non_empty > 0)
        printf("  Avg chain len (non-empty buckets) : %.4f\n",
               (double)total_nodes / non_empty);

    printf("\n  %-8s  %10s  %6s\n", "Depth", "# Buckets", "%");
    for (int d = 0; d <= 16; d++) {
        if (hist[d] == 0) continue;
        if (d < 16)
            printf("  %-8d  %10lu  %5.2f%%\n",
                   d, hist[d], 100.0 * hist[d] / num_buckets);
        else
            printf("  %-8s  %10lu  %5.2f%%\n",
                   "16+", hist[d], 100.0 * hist[d] / num_buckets);
    }
    printf("===================================================\n\n");
}

} // namespace TwoRW
