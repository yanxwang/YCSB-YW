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
        table->free_offset.store(sizeof(SharedHashTable), std::memory_order_relaxed);
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

    // Create client channels with configurable queue depth
    fprintf(stderr, "[SharedKVContext] Creating %u client channels...\n", num_clients);
    for (uint32_t i = 0; i < num_clients; ++i) {
        fprintf(stderr, "[SharedKVContext] Creating client channel %u\n", i);
        fflush(stderr);
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

    stop_flag.store(true, std::memory_order_release);

    // Join synchronizer first (it dispatches to workers)
    if (synchronizer_thread.joinable()) {
        synchronizer_thread.join();
        fprintf(stderr, "[SharedKVContext] Synchronizer thread joined\n");
    }

    // Join worker threads (they may still have requests to process)
    // Workers will drain their ring buffers before exiting
    for (auto& t : worker_threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    fprintf(stderr, "[SharedKVContext] All %zu worker threads joined\n", worker_threads.size());

    // Join poller (it needs workers to finish producing responses)
    if (poller_thread.joinable()) {
        poller_thread.join();
        fprintf(stderr, "[SharedKVContext] Poller thread joined\n");
    }

    // Join client response threads last (they consume responses from workers)
    for (auto* ch : clients) {
        if (ch->response_thread.joinable()) {
            ch->response_thread.join();
        }
    }
    fprintf(stderr, "[SharedKVContext] All %zu response threads joined\n", clients.size());

    worker_threads.clear();
    fprintf(stderr, "[SharedKVContext] All threads stopped\n");
}

// Submit request (non-blocking, returns true on success)
bool SharedKVContext::submit_request(uint32_t client_id, KVRequest* req) {
    if (client_id >= num_clients) {
        fprintf(stderr, "[SharedKVContext] Invalid client_id: %u (max: %u)\n",
                client_id, num_clients);
        return false;
    }

    ClientChannel* ch = clients[client_id];

    // Try to enqueue (non-blocking)
    // Note: Response Thread creation is handled by async_benchmark.cc in Async Mode
    // In Sync Mode, no Response Thread is created
    if (ch->req_q->enqueue(req)) {
        ch->requests_sent.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    return false;  // Queue full, caller should retry
}

// Get response (blocking with timeout, for JNI use)
bool SharedKVContext::get_response(uint32_t client_id, KVResponse& resp,
                                   uint64_t timeout_ms) {
    if (client_id >= num_clients) {
        fprintf(stderr, "[SharedKVContext] Invalid client_id: %u (max: %u)\n",
                client_id, num_clients);
        return false;
    }

    ClientChannel* ch = clients[client_id];

    // Wait for response with timeout
    auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(timeout_ms);

    while (true) {
        if (ch->resp_q->dequeue(resp)) {
            return true;
        }

        // Check timeout
        if (std::chrono::steady_clock::now() >= deadline) {
            fprintf(stderr, "[SharedKVContext] Response timeout for client %u after %lu ms\n",
                    client_id, timeout_ms);
            return false;
        }

        // Yield to avoid busy-spinning
        std::this_thread::yield();
    }
}

// Async mode: Submit with flow control
bool SharedKVContext::submit_request_async(uint32_t client_id, KVRequest* req) {
    if (client_id >= num_clients) {
        return false;
    }

    ClientChannel* ch = clients[client_id];

    // Flow control: wait if too many in-flight requests
    while (ch->in_flight_count.load(std::memory_order_acquire) >= max_in_flight) {
        if (stop_flag.load(std::memory_order_relaxed) || draining.load(std::memory_order_relaxed)) {
            return false;  // Shutting down, don't accept new requests
        }
        std::this_thread::yield();
    }

    // Increment in-flight before enqueue
    ch->in_flight_count.fetch_add(1, std::memory_order_release);

    // Try to enqueue
    if (ch->req_q->enqueue(req)) {
        ch->requests_sent.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // Enqueue failed, decrement in-flight
    ch->in_flight_count.fetch_sub(1, std::memory_order_release);
    return false;
}

// Get total in-flight count
uint64_t SharedKVContext::get_total_in_flight() const {
    uint64_t total = 0;
    for (const auto* ch : clients) {
        total += ch->in_flight_count.load(std::memory_order_acquire);
    }
    return total;
}

// Wait for all in-flight requests to drain
void SharedKVContext::wait_for_drain(uint64_t timeout_ms) {
    draining.store(true, std::memory_order_release);

    auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(timeout_ms);

    while (true) {
        uint64_t in_flight = get_total_in_flight();
        if (in_flight == 0) {
            fprintf(stderr, "[SharedKVContext] All requests drained\n");
            break;
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            fprintf(stderr, "[SharedKVContext] Drain timeout with %lu requests still in-flight\n",
                    in_flight);
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    draining.store(false, std::memory_order_release);
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
