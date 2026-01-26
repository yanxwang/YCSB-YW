#include "shared_kv.h"
#include "kv_request.h"
#include "uintr_threading.h"
#include <mutex>
#include <chrono>
#include <cstdio>
#include <sys/mman.h>
#include <atomic>

// Global CPU allocation tracking (shared with JNI layer)
// CPU 0: Synchronizer (fixed)
// CPU 1: Poller (fixed)
// CPU 2+: Dynamically allocated to Java threads and Worker threads
extern std::atomic<int> next_available_cpu;

// Helper function to allocate next available CPU
static int allocate_cpu() {
    return next_available_cpu.fetch_add(1, std::memory_order_relaxed);
}

// Forward declarations of thread functions
extern void worker_thread_func(KVWorker* worker, SharedHashTable* table,
                               void* base, std::atomic<bool>& stop_flag);
extern void synchronizer_thread_func(std::vector<RequestQueue*>& req_queues,
                                    std::vector<KVWorker*>& workers,
                                    std::atomic<uint64_t>& global_seq,
                                    std::atomic<bool>& stop_flag);
extern void poller_thread_func(std::vector<ResponseQueue*>& resp_queues,
                              std::atomic<bool>& stop_flag);

// ============================================================================
// RequestQueue implementation
// ============================================================================

RequestQueue::RequestQueue(uint32_t id, size_t queue_size) : queue_id(id) {
    queue = new LockFreeQueue<KVRequest*>(queue_size);
}

RequestQueue::~RequestQueue() {
    delete queue;
}

// ============================================================================
// ResponseQueue implementation
// ============================================================================

ResponseQueue::ResponseQueue(uint32_t id, size_t queue_size) : queue_id(id) {
    queue = new LockFreeQueue<KVResponse>(queue_size);
}

ResponseQueue::~ResponseQueue() {
    delete queue;
}

// ============================================================================
// SharedKVContext implementation (simplified - no ClientChannel)
// ============================================================================

SharedKVContext::SharedKVContext(int numa_node, const AsyncConfig& cfg, size_t worker_ring_buffer_size)
    : numa_node(numa_node), config(cfg) {

    fprintf(stderr, "[SharedKVContext] Initializing on NUMA node %d\n", numa_node);
    fprintf(stderr, "[SharedKVContext] Config: req_q=%u, resp_q=%u, req_th=%u, resp_th=%u, workers=%u\n",
            config.num_req_queues, config.num_resp_queues,
            config.num_req_threads, config.num_resp_threads, config.num_workers);
    fprintf(stderr, "[SharedKVContext] Queue depths: req=%zu, resp=%zu, Worker ring buffer: %zu\n",
            config.req_queue_depth, config.resp_queue_depth, worker_ring_buffer_size);

    // Allocate CXL memory via NUMA
    fprintf(stderr, "[SharedKVContext] Allocating CXL memory...\n");
    fflush(stderr);
    base = allocate_cxl_memory(numa_node, SHM_SIZE);
    table = (SharedHashTable*)base;
    fprintf(stderr, "[SharedKVContext] CXL memory allocated\n");
    fflush(stderr);

    // Initialize SharedHashTable if not already initialized
    if (table->magic != MAGIC_INIT) {
        fprintf(stderr, "[SharedKVContext] Initializing SharedHashTable (magic not found)\n");
        fflush(stderr);
        table->magic = MAGIC_INIT;
        table->.store(sizeof(SharedHashTable), std::memory_order_relaxed);
        table->reserved = 0;
 
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            table->buckets[i].head_offset = 0;
            table->buckets[i].lock.flag.clear(std::memory_order_relaxed);
        }

        __sync_synchronize();  // Full memory barrier
        fprintf(stderr, "[SharedKVContext] SharedHashTable initialized\n");
        fflush(stderr);
    } else {
        fprintf(stderr, "[SharedKVContext] SharedHashTable already initialized (magic found)\n");
        fflush(stderr);
    }

    // Create workers
    for (uint32_t i = 0; i < config.num_workers; ++i) {
        KVWorker* w = new KVWorker(worker_ring_buffer_size);
        w->worker_id = i;
        w->num_workers = config.num_workers;
        workers.push_back(w);
    }
    fprintf(stderr, "[SharedKVContext] Created %u workers (ring_buffer_size=%zu)\n",
            config.num_workers, worker_ring_buffer_size);

    // Create request queues
    fprintf(stderr, "[SharedKVContext] Creating %u request queues...\n", config.num_req_queues);
    for (uint32_t i = 0; i < config.num_req_queues; ++i) {
        req_queues.push_back(new RequestQueue(i, config.req_queue_depth));
    }

    // Create response queues
    fprintf(stderr, "[SharedKVContext] Creating %u response queues...\n", config.num_resp_queues);
    for (uint32_t i = 0; i < config.num_resp_queues; ++i) {
        resp_queues.push_back(new ResponseQueue(i, config.resp_queue_depth));
    }

    fprintf(stderr, "[SharedKVContext] Constructor complete\n");
    fflush(stderr);
}

SharedKVContext::~SharedKVContext() {
    fprintf(stderr, "[SharedKVContext] Destructor called\n");

    stop_threads();

    // Cleanup workers
    for (auto* w : workers) {
        delete w;
    }
    workers.clear();

    // Cleanup request queues
    for (auto* q : req_queues) {
        delete q;
    }
    req_queues.clear();

    // Cleanup response queues
    for (auto* q : resp_queues) {
        delete q;
    }
    resp_queues.clear();

    // Unmap memory
    if (base) {
        munmap(base, SHM_SIZE);
        base = nullptr;
        table = nullptr;
    }

    fprintf(stderr, "[SharedKVContext] Cleanup complete\n");
}

void SharedKVContext::start_threads() {
    if (initialized.exchange(true)) {
        fprintf(stderr, "[SharedKVContext] Threads already started\n");
        return;
    }

    fprintf(stderr, "[SharedKVContext] Starting threads...\n");

    // Start worker threads with dynamic CPU allocation
    for (uint32_t i = 0; i < config.num_workers; ++i) {
        worker_threads.emplace_back(worker_thread_func, workers[i], table, base,
                                     std::ref(stop_flag));
        int worker_cpu = allocate_cpu();
        pin_thread_to_cpu(worker_threads.back(), worker_cpu);
        fprintf(stderr, "[SharedKVContext] Worker %u pinned to CPU %d\n", i, worker_cpu);
    }
    fprintf(stderr, "[SharedKVContext] Started %u worker threads\n", config.num_workers);

    // Start synchronizer thread (fixed CPU 0)
    synchronizer_thread = std::thread(synchronizer_thread_func,
                                     std::ref(req_queues),
                                     std::ref(workers),
                                     std::ref(global_sequence),
                                     std::ref(stop_flag));
    pin_thread_to_cpu(synchronizer_thread, 0);
    fprintf(stderr, "[SharedKVContext] Synchronizer thread pinned to CPU 0 (fixed)\n");

    // Start poller thread (fixed CPU 1)
    poller_thread = std::thread(poller_thread_func,
                               std::ref(resp_queues),
                               std::ref(stop_flag));
    pin_thread_to_cpu(poller_thread, 1);
    fprintf(stderr, "[SharedKVContext] Poller thread pinned to CPU 1 (fixed)\n");

    fprintf(stderr, "[SharedKVContext] All threads started successfully\n");
}

void SharedKVContext::stop_threads() {
    fprintf(stderr, "[SharedKVContext] Stopping threads...\n");

    stop_flag.store(true, std::memory_order_release);

    // Join synchronizer first
    if (synchronizer_thread.joinable()) {
        synchronizer_thread.join();
        fprintf(stderr, "[SharedKVContext] Synchronizer thread joined\n");
    }

    // Join worker threads
    for (auto& t : worker_threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    fprintf(stderr, "[SharedKVContext] All %zu worker threads joined\n", worker_threads.size());

    // Join poller
    if (poller_thread.joinable()) {
        poller_thread.join();
        fprintf(stderr, "[SharedKVContext] Poller thread joined\n");
    }

    worker_threads.clear();
    fprintf(stderr, "[SharedKVContext] All threads stopped\n");
}

// ============================================================================
// Singleton management
// ============================================================================

static SharedKVContext* g_context = nullptr;
static std::mutex g_context_mutex;

SharedKVContext* get_or_create_context(int numa_node, const AsyncConfig& cfg,
                                       size_t worker_ring_buffer_size) {
    std::lock_guard<std::mutex> lock(g_context_mutex);

    if (!g_context) {
        fprintf(stderr, "[get_or_create_context] Creating new context\n");
        g_context = new SharedKVContext(numa_node, cfg, worker_ring_buffer_size);
        fprintf(stderr, "[get_or_create_context] Context created, calling start_threads...\n");
        g_context->start_threads();
        fprintf(stderr, "[get_or_create_context] start_threads returned\n");
    } else {
        fprintf(stderr, "[get_or_create_context] Returning existing context\n");
    }

    return g_context;
}

void destroy_context() {
    std::lock_guard<std::mutex> lock(g_context_mutex);

    if (g_context) {
        fprintf(stderr, "[destroy_context] Destroying context\n");
        delete g_context;
        g_context = nullptr;
    }
}
