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
extern void synchronizer_thread_func(std::vector<ClientChannel*>& clients,
                                    std::vector<KVWorker*>& workers,
                                    std::atomic<uint64_t>& global_seq,
                                    std::atomic<bool>& stop_flag);
extern void poller_thread_func(std::vector<ClientChannel*>& clients,
                              std::atomic<bool>& stop_flag);

// ============================================================================
// ClientChannel implementation
// ============================================================================

ClientChannel::ClientChannel(size_t queue_size) {
    req_q = new LockFreeQueue<KVRequest*>(queue_size);
    resp_q = new LockFreeQueue<KVResponse>(queue_size);
    uintr_fd = -1;
}

ClientChannel::~ClientChannel() {
    delete req_q;
    delete resp_q;
}

// ============================================================================
// SharedKVContext implementation
// ============================================================================

SharedKVContext::SharedKVContext(uint32_t num_clients, uint32_t num_workers, int numa_node,
                                 size_t client_queue_depth, size_t worker_ring_buffer_size)
    : num_clients(num_clients), num_workers(num_workers), numa_node(numa_node) {

    fprintf(stderr, "[SharedKVContext] Initializing with %u clients, %u workers on NUMA node %d\n",
            num_clients, num_workers, numa_node);
    fprintf(stderr, "[SharedKVContext] Queue depth: %zu, Worker ring buffer: %zu\n",
            client_queue_depth, worker_ring_buffer_size);

    // Allocate CXL memory via NUMA
    base = allocate_cxl_memory(numa_node, SHM_SIZE);
    table = (SharedHashTable*)base;

    // Initialize SharedHashTable if not already initialized
    if (table->magic != MAGIC_INIT) {
        fprintf(stderr, "[SharedKVContext] Initializing SharedHashTable (magic not found)\n");
        table->magic = MAGIC_INIT;
        table->free_offset.store(sizeof(SharedHashTable), std::memory_order_relaxed);
        table->reserved = 0;

        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            table->buckets[i].head_offset = 0;
            table->buckets[i].lock.flag.clear(std::memory_order_relaxed);
        }

        __sync_synchronize();  // Full memory barrier
        fprintf(stderr, "[SharedKVContext] SharedHashTable initialized\n");
    } else {
        fprintf(stderr, "[SharedKVContext] SharedHashTable already initialized (magic found)\n");
    }

    // Create client channels with configurable queue depth
    for (uint32_t i = 0; i < num_clients; ++i) {
        clients.push_back(new ClientChannel(client_queue_depth));
    }
    fprintf(stderr, "[SharedKVContext] Created %u client channels (queue_depth=%zu)\n",
            num_clients, client_queue_depth);

    // Create workers with configurable ring buffer size
    for (uint32_t i = 0; i < num_workers; ++i) {
        KVWorker* w = new KVWorker(worker_ring_buffer_size);
        w->worker_id = i;
        w->num_workers = num_workers;
        workers.push_back(w);
    }
    fprintf(stderr, "[SharedKVContext] Created %u workers (ring_buffer_size=%zu)\n",
            num_workers, worker_ring_buffer_size);
}

SharedKVContext::~SharedKVContext() {
    fprintf(stderr, "[SharedKVContext] Destructor called\n");

    stop_threads();

    // Cleanup workers
    for (auto* w : workers) {
        delete w;
    }
    workers.clear();

    // Cleanup clients
    for (auto* ch : clients) {
        delete ch;
    }
    clients.clear();

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
        return;  // Already started
    }

    fprintf(stderr, "[SharedKVContext] Starting threads...\n");

    // Start worker threads with dynamic CPU allocation
    for (uint32_t i = 0; i < num_workers; ++i) {
        worker_threads.emplace_back(worker_thread_func, workers[i], table, base,
                                     std::ref(stop_flag));
        // Dynamically allocate CPU for worker (from global pool starting at CPU 2)
        int worker_cpu = allocate_cpu();
        pin_thread_to_cpu(worker_threads.back(), worker_cpu);
        fprintf(stderr, "[SharedKVContext] Worker %u pinned to CPU %d\n", i, worker_cpu);
    }
    fprintf(stderr, "[SharedKVContext] Started %u worker threads\n", num_workers);

    // Start synchronizer thread (fixed CPU 0)
    synchronizer_thread = std::thread(synchronizer_thread_func,
                                     std::ref(clients),
                                     std::ref(workers),
                                     std::ref(global_sequence),
                                     std::ref(stop_flag));
    pin_thread_to_cpu(synchronizer_thread, 0);
    fprintf(stderr, "[SharedKVContext] Synchronizer thread pinned to CPU 0 (fixed)\n");

    // Start poller thread (fixed CPU 1)
    poller_thread = std::thread(poller_thread_func,
                               std::ref(clients),
                               std::ref(stop_flag));
    pin_thread_to_cpu(poller_thread, 1);
    fprintf(stderr, "[SharedKVContext] Poller thread pinned to CPU 1 (fixed)\n");

    fprintf(stderr, "[SharedKVContext] All threads started successfully\n");
}

void SharedKVContext::stop_threads() {
    fprintf(stderr, "[SharedKVContext] Stopping threads...\n");

    stop_flag.store(true, std::memory_order_relaxed);

    // Join synchronizer
    if (synchronizer_thread.joinable()) {
        synchronizer_thread.join();
        fprintf(stderr, "[SharedKVContext] Synchronizer thread joined\n");
    }

    // Join poller
    if (poller_thread.joinable()) {
        poller_thread.join();
        fprintf(stderr, "[SharedKVContext] Poller thread joined\n");
    }

    // Join worker threads
    for (auto& t : worker_threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    fprintf(stderr, "[SharedKVContext] All %zu worker threads joined\n", worker_threads.size());

    worker_threads.clear();
    fprintf(stderr, "[SharedKVContext] All threads stopped\n");
}

KVResponse SharedKVContext::submit_request(uint32_t client_id, KVRequest* req,
                                          uint64_t timeout_ms) {
    if (client_id >= num_clients) {
        fprintf(stderr, "[SharedKVContext] Invalid client_id: %u (max: %u)\n",
                client_id, num_clients);
        KVResponse err;
        err.status = KVStatus::ERROR;
        return err;
    }

    ClientChannel* ch = clients[client_id];

    auto t0 = std::chrono::high_resolution_clock::now();

    // Enqueue request (blocking if queue is full)
    while (!ch->req_q->enqueue(req)) {
        if (stop_flag.load(std::memory_order_relaxed)) {
            KVResponse err;
            err.status = KVStatus::ERROR;
            return err;
        }
        std::this_thread::yield();
    }

    auto t1 = std::chrono::high_resolution_clock::now();

    ch->requests_sent.fetch_add(1, std::memory_order_relaxed);

    // Wait for response with timeout
    auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(timeout_ms);

    KVResponse resp;
    uint64_t spin_count = 0;
    while (true) {
        if (ch->resp_q->dequeue(resp)) {
            ch->responses_received.fetch_add(1, std::memory_order_relaxed);

            auto t2 = std::chrono::high_resolution_clock::now();
            auto enqueue_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            auto wait_us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

            // Sample 1 in 10000 requests for latency breakdown
            if ((ch->responses_received.load() % 10000) == 0) {
                fprintf(stderr, "[Latency] client=%u enqueue=%ld us, wait=%ld us, spins=%lu\n",
                        client_id, enqueue_us, wait_us, spin_count);
            }

            return resp;
        }

        spin_count++;

        // Adaptive spinning: yield after some iterations to reduce CPU waste
        if (spin_count > 100) {
            std::this_thread::yield();
        }

        // Check timeout
        if (std::chrono::steady_clock::now() >= deadline) {
            fprintf(stderr, "[SharedKVContext] Request timeout for client %u after %lu ms (spins=%lu)\n",
                    client_id, timeout_ms, spin_count);
            resp.status = KVStatus::ERROR;
            return resp;
        }
    }
}

// ============================================================================
// Singleton management
// ============================================================================

static SharedKVContext* g_context = nullptr;
static std::mutex g_context_mutex;

SharedKVContext* get_or_create_context(uint32_t num_clients, uint32_t num_workers,
                                       int numa_node, size_t client_queue_depth,
                                       size_t worker_ring_buffer_size) {
    std::lock_guard<std::mutex> lock(g_context_mutex);

    if (!g_context) {
        fprintf(stderr, "[get_or_create_context] Creating new context\n");
        g_context = new SharedKVContext(num_clients, num_workers, numa_node,
                                        client_queue_depth, worker_ring_buffer_size);
        g_context->start_threads();
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
