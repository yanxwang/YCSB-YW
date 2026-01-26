// cxl_context.cpp
// CXL SharedKV Context - manages CXL memory, workers, and synchronizer

#include "cxl_shared.h"
#include "uintr_threading.h"
#include "kv_request.h"
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <numa.h>
#include <numaif.h>

// External thread functions
extern void cxl_worker_run(void* cxl_base, const CXLMemoryLayout& layout,
                           uint32_t worker_id, uint32_t host_id,
                           uint32_t num_response_rings, std::atomic<bool>& stop_flag);

extern void cxl_synchronizer_run(void* cxl_base, const CXLMemoryLayout& layout,
                                 std::vector<LockFreeQueue<KVRequest*>*>& local_req_queues,
                                 std::atomic<uint64_t>& global_sequence,
                                 std::atomic<bool>& stop_flag);

extern void cxl_init_memory(void* base, uint64_t total_size,
                            uint32_t num_workers, uint32_t num_response_rings);

extern bool cxl_is_initialized(void* base, const CXLMemoryLayout& layout);

// ============================================================================
// CXL Context Configuration
// ============================================================================

struct CXLConfig {
    // CXL memory settings
    int numa_node{2};                      // CXL typically appears as a high NUMA node
    uint64_t memory_size{CXL_MEMORY_SIZE}; // 16GB default

    // Thread configuration
    uint32_t num_workers{8};
    uint32_t num_req_queues{8};
    uint32_t num_req_threads{8};
    uint32_t num_resp_threads{8};

    // Host identity
    uint32_t host_id{0};

    // Role flags
    bool run_synchronizer{true};           // This host runs the synchronizer
    bool run_workers{true};                // This host runs workers
    uint32_t worker_start_id{0};           // First worker ID for this host
    uint32_t worker_count{8};              // Number of workers on this host

    // Ring buffer settings
    size_t local_req_queue_depth{2048};    // Local request queue depth
};

// ============================================================================
// CXL Context
// ============================================================================

struct CXLContext {
    // Configuration
    CXLConfig config;
    CXLMemoryLayout layout;

    // CXL shared memory
    void* cxl_base{nullptr};
    bool is_owner{false};                  // Did we create/initialize the memory?

    // CXL structures (pointers into cxl_base)
    CXLSharedHashTable* hash_table{nullptr};
    CXLWorkerRegistry* registry{nullptr};

    // Local request queues (for clients on this host)
    std::vector<LockFreeQueue<KVRequest*>*> local_req_queues;

    // Global sequence counter (in local memory, only used by synchronizer)
    std::atomic<uint64_t> global_sequence{0};

    // Threads
    std::vector<std::thread> worker_threads;
    std::thread synchronizer_thread;

    // Control flags
    std::atomic<bool> stop_flag{false};
    std::atomic<bool> initialized{false};

    // Statistics
    std::atomic<uint64_t> total_requests{0};

    // Get request queue for a thread
    LockFreeQueue<KVRequest*>* get_req_queue(uint32_t thread_id) {
        return local_req_queues[thread_id % local_req_queues.size()];
    }

    // Get response ring for a client
    CXLResponseRing* get_response_ring(uint32_t client_id) {
        return cxl_get_response_ring(cxl_base, layout, client_id % CXL_MAX_CLIENTS);
    }
};

// ============================================================================
// CXL Memory Allocation
// ============================================================================

static void* allocate_cxl_shared_memory(int numa_node, uint64_t size, bool& is_new) {
    // Use POSIX shared memory for cross-process (and potentially cross-host) sharing
    char shm_name[64];
    snprintf(shm_name, sizeof(shm_name), "/sharedkv_cxl_v2_node%d", numa_node);

    // Try to open existing shared memory
    int shm_fd = shm_open(shm_name, O_RDWR, 0666);
    is_new = false;

    if (shm_fd < 0) {
        // Create new shared memory
        fprintf(stderr, "[CXL] Creating new shared memory: %s (size=%lu)\n", shm_name, size);
        shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
        if (shm_fd < 0) {
            perror("shm_open failed");
            return nullptr;
        }

        if (ftruncate(shm_fd, size) != 0) {
            perror("ftruncate failed");
            close(shm_fd);
            shm_unlink(shm_name);
            return nullptr;
        }
        is_new = true;
    } else {
        fprintf(stderr, "[CXL] Reusing existing shared memory: %s\n", shm_name);
    }

    // Map to address space
    void* addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);

    if (addr == MAP_FAILED) {
        perror("mmap failed");
        if (is_new) {
            shm_unlink(shm_name);
        }
        return nullptr;
    }

    // Bind to NUMA node if new
    if (is_new && numa_available() >= 0) {
        unsigned long nodemask = 1UL << numa_node;
        unsigned long maxnode = sizeof(nodemask) * 8;

        if (mbind(addr, size, MPOL_BIND, &nodemask, maxnode, MPOL_MF_STRICT) != 0) {
            perror("mbind failed (continuing anyway)");
        }

        // Touch first page
        *(volatile char*)addr = 0;
    }

    return addr;
}

// ============================================================================
// Create and Initialize CXL Context
// ============================================================================

CXLContext* cxl_context_create(const CXLConfig& config) {
    CXLContext* ctx = new CXLContext();
    ctx->config = config;

    // Calculate memory layout
    uint32_t num_response_rings = config.num_resp_threads * 32;  // Allow for many clients
    ctx->layout = CXLMemoryLayout::calculate(
        config.num_workers, num_response_rings, config.memory_size
    );

    fprintf(stderr, "[CXL] Memory layout:\n");
    fprintf(stderr, "  hash_table: 0x%lx\n", ctx->layout.hash_table_offset);
    fprintf(stderr, "  worker_registry: 0x%lx\n", ctx->layout.worker_registry_offset);
    fprintf(stderr, "  worker_regions: 0x%lx\n", ctx->layout.worker_regions_offset);
    fprintf(stderr, "  request_rings: 0x%lx\n", ctx->layout.request_rings_offset);
    fprintf(stderr, "  response_rings: 0x%lx\n", ctx->layout.response_rings_offset);
    fprintf(stderr, "  kv_data: 0x%lx (size=%lu MB)\n",
            ctx->layout.kv_data_offset, ctx->layout.kv_data_size / (1024 * 1024));

    // Allocate CXL shared memory
    bool is_new = false;
    ctx->cxl_base = allocate_cxl_shared_memory(
        config.numa_node, config.memory_size, is_new
    );

    if (!ctx->cxl_base) {
        delete ctx;
        return nullptr;
    }

    ctx->is_owner = is_new;

    // Initialize CXL memory if we created it
    if (is_new) {
        fprintf(stderr, "[CXL] Initializing new CXL memory...\n");
        cxl_init_memory(ctx->cxl_base, config.memory_size,
                        config.num_workers, num_response_rings);
    } else {
        // Wait for initialization
        fprintf(stderr, "[CXL] Waiting for CXL memory initialization...\n");
        while (!cxl_is_initialized(ctx->cxl_base, ctx->layout)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        fprintf(stderr, "[CXL] CXL memory is initialized\n");
    }

    // Get pointers to CXL structures
    ctx->hash_table = cxl_get_hash_table(ctx->cxl_base, ctx->layout);
    ctx->registry = cxl_get_worker_registry(ctx->cxl_base, ctx->layout);

    // Create local request queues (for clients on this host)
    for (uint32_t i = 0; i < config.num_req_queues; i++) {
        auto* queue = new LockFreeQueue<KVRequest*>(config.local_req_queue_depth);
        ctx->local_req_queues.push_back(queue);
    }

    fprintf(stderr, "[CXL] Context created: %u workers, %u local req queues\n",
            config.num_workers, config.num_req_queues);

    return ctx;
}

// ============================================================================
// Start Threads
// ============================================================================

void cxl_context_start_threads(CXLContext* ctx) {
    if (ctx->initialized.exchange(true)) {
        fprintf(stderr, "[CXL] Threads already started\n");
        return;
    }

    const CXLConfig& config = ctx->config;

    // Start workers if configured
    if (config.run_workers) {
        fprintf(stderr, "[CXL] Starting %u workers (IDs %u-%u) on host %u\n",
                config.worker_count, config.worker_start_id,
                config.worker_start_id + config.worker_count - 1, config.host_id);

        for (uint32_t i = 0; i < config.worker_count; i++) {
            uint32_t worker_id = config.worker_start_id + i;
            ctx->worker_threads.emplace_back(
                cxl_worker_run,
                ctx->cxl_base,
                std::ref(ctx->layout),
                worker_id,
                config.host_id,
                config.num_resp_threads * 32,
                std::ref(ctx->stop_flag)
            );
        }
    }

    // Start synchronizer if configured
    if (config.run_synchronizer) {
        fprintf(stderr, "[CXL] Starting synchronizer on host %u\n", config.host_id);

        // Collect queue pointers
        std::vector<LockFreeQueue<KVRequest*>*> queue_ptrs;
        for (auto* q : ctx->local_req_queues) {
            queue_ptrs.push_back(q);
        }

        ctx->synchronizer_thread = std::thread(
            cxl_synchronizer_run,
            ctx->cxl_base,
            std::ref(ctx->layout),
            std::ref(queue_ptrs),
            std::ref(ctx->global_sequence),
            std::ref(ctx->stop_flag)
        );
    }

    fprintf(stderr, "[CXL] All threads started\n");
}

// ============================================================================
// Stop and Cleanup
// ============================================================================

void cxl_context_stop(CXLContext* ctx) {
    fprintf(stderr, "[CXL] Stopping...\n");

    ctx->stop_flag.store(true, std::memory_order_release);

    // Wait for synchronizer
    if (ctx->synchronizer_thread.joinable()) {
        ctx->synchronizer_thread.join();
    }

    // Wait for workers
    for (auto& t : ctx->worker_threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    fprintf(stderr, "[CXL] All threads stopped\n");
}

void cxl_context_destroy(CXLContext* ctx) {
    if (!ctx) return;

    cxl_context_stop(ctx);

    // Cleanup local request queues
    for (auto* q : ctx->local_req_queues) {
        delete q;
    }
    ctx->local_req_queues.clear();

    // Unmap CXL memory (but don't unlink - other processes may be using it)
    if (ctx->cxl_base) {
        munmap(ctx->cxl_base, ctx->config.memory_size);
    }

    delete ctx;
}

// ============================================================================
// Singleton Access (for backward compatibility)
// ============================================================================

static CXLContext* g_cxl_context = nullptr;
static std::mutex g_cxl_mutex;

CXLContext* cxl_get_or_create_context(const CXLConfig& config) {
    std::lock_guard<std::mutex> lock(g_cxl_mutex);

    if (!g_cxl_context) {
        g_cxl_context = cxl_context_create(config);
        if (g_cxl_context) {
            cxl_context_start_threads(g_cxl_context);
        }
    }

    return g_cxl_context;
}

void cxl_destroy_global_context() {
    std::lock_guard<std::mutex> lock(g_cxl_mutex);

    if (g_cxl_context) {
        cxl_context_destroy(g_cxl_context);
        g_cxl_context = nullptr;
    }
}
