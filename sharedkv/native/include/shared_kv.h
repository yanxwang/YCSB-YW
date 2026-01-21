#pragma once
#include <string>
#include <cstdint>
#include <atomic>
#include <vector>
#include <thread>

#define SHM_SIZE (16ULL * 1024 * 1024 * 1024)  // same as /dev/pmem0
#define NUM_BUCKETS 4096                        // match your bucket count
#define MAGIC_INIT 0xC0DEBEEFDEADF00DULL


struct SpinLock {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
    inline void acquire() {
        while (flag.test_and_set(std::memory_order_acquire)) {
            // busy-wait; optionally use pause/relax
            asm volatile("pause" ::: "memory");
        }
    }
    inline void release() {
        flag.clear(std::memory_order_release);
    }
};
struct KVEntry;         // opaque, only used inside CPP
struct Bucket {
    SpinLock lock;
    uint64_t head_offset; // offset from base; 0 == empty
};
struct SharedHashTable {
    uint64_t magic;                 // magic to detect initialization
    std::atomic<uint64_t> free_offset;
    uint64_t reserved;              // padding / future use
    Bucket buckets[NUM_BUCKETS];
    // KV entries allocated after sizeof(SharedHashTable)
};

struct ThreadStats {
    uint64_t ops_completed = 0;
    uint64_t successful_puts = 0;
    uint64_t successful_gets = 0;
    uint64_t lock_wait_ns = 0;
    uint64_t thread_time_ns = 0;
};

// map the shared memory (for PMem/file-based)
void* map_shared_memory(const char* path, size_t size);

// allocate CXL memory via NUMA
void* allocate_cxl_memory(int numa_node, size_t size);

// KV store operations
void kv_put(SharedHashTable* table, void* base,
            const std::string& key, const std::string& value,
            uint64_t* wait_ns_out = nullptr);

bool kv_get(SharedHashTable* table, void* base,
            const std::string& key, std::string& out_value,
            uint64_t* wait_ns_out = nullptr);

// optional: delete key
bool kv_delete(SharedHashTable* table, void* base,
               const std::string& key,
               uint64_t* wait_ns_out = nullptr);

void worker_thread(SharedHashTable* table, void* base, int tid, int ops, ThreadStats* stat);
void print_stats(SharedHashTable* table);

// ============================================================================
// UINTR-based Multi-threaded Architecture
// ============================================================================

// Forward declarations for UINTR threading
struct KVRequest;
struct KVResponse;
struct SharedKVContext;
template<typename T> struct LockFreeQueue;

// Worker with direct ring buffer handoff
struct KVWorker {
    // Dynamic ring buffer size (configurable at runtime)
    size_t buffer_size;
    KVRequest** buffer;  // Dynamically allocated pointer array

    alignas(64) std::atomic<uint64_t> write_idx{0};
    alignas(64) std::atomic<uint64_t> read_idx{0};

    // Assigned bucket range for this worker
    uint32_t worker_id;
    uint32_t num_workers;

    // Statistics
    std::atomic<uint64_t> ops_processed{0};
    std::atomic<uint64_t> reads{0};
    std::atomic<uint64_t> inserts{0};
    std::atomic<uint64_t> updates{0};
    std::atomic<uint64_t> deletes{0};

    // Constructor/Destructor
    KVWorker(size_t ring_buffer_size = 1024);
    ~KVWorker();

    // Ring buffer operations
    bool try_handoff(KVRequest* req);
    bool try_get_request(KVRequest*& req);

    // Verify bucket ownership (debug mode)
    inline bool owns_bucket(uint32_t bucket_id) const {
        return (bucket_id % num_workers) == worker_id;
    }
};

// Execution mode for the context
enum class ExecutionMode {
    SYNC,   // Synchronous mode: submit_request + get_response (blocking)
    ASYNC   // Asynchronous mode: Request threads + Response threads (non-blocking)
};

// ============================================================================
// Simplified Async Architecture Components
// ============================================================================

// Request Queue: receives requests from Request Threads, consumed by Synchronizer
struct RequestQueue {
    uint32_t queue_id;
    LockFreeQueue<KVRequest*>* queue;

    // Statistics
    std::atomic<uint64_t> enqueued{0};
    std::atomic<uint64_t> dequeued{0};

    RequestQueue(uint32_t id, size_t queue_size);
    ~RequestQueue();
};

// Response Queue: receives responses from Workers, consumed by Response Threads
struct ResponseQueue {
    uint32_t queue_id;
    LockFreeQueue<KVResponse>* queue;

    // UINTR infrastructure (one per response queue)
    int uintr_fd{-1};
    std::atomic<bool> uintr_fd_ready{false};

    // Statistics
    std::atomic<uint64_t> enqueued{0};
    std::atomic<uint64_t> dequeued{0};

    ResponseQueue(uint32_t id, size_t queue_size);
    ~ResponseQueue();
};

// Async architecture configuration (simplified - no ClientChannel)
struct AsyncConfig {
    uint32_t num_req_queues{1};       // Number of request queues
    uint32_t num_resp_queues{1};      // Number of response queues
    uint32_t num_req_threads{1};      // Number of request threads
    uint32_t num_resp_threads{1};     // Number of response threads
    uint32_t num_workers{1};          // Number of worker threads
    size_t req_queue_depth{2048};     // Depth of each request queue
    size_t resp_queue_depth{2048};    // Depth of each response queue
};

// Main threading context (simplified - no ClientChannel)
struct SharedKVContext {
    // Shared resources
    SharedHashTable* table;
    void* base;

    // Configuration
    int numa_node;
    ExecutionMode mode{ExecutionMode::SYNC};
    AsyncConfig config;  // Unified configuration

    // Queue infrastructure (direct access, no ClientChannel indirection)
    std::vector<RequestQueue*> req_queues;
    std::vector<ResponseQueue*> resp_queues;

    // Worker infrastructure
    std::vector<KVWorker*> workers;
    std::vector<std::thread> worker_threads;
    std::thread synchronizer_thread;
    std::thread poller_thread;

    // Lifecycle
    std::atomic<bool> stop_flag{false};
    std::atomic<bool> draining{false};
    std::atomic<bool> initialized{false};
    std::atomic<uint64_t> global_sequence{0};

    // Constructor/Destructor
    SharedKVContext(int numa_node, const AsyncConfig& cfg, size_t worker_ring_buffer_size = 4096);
    ~SharedKVContext();

    // Thread management
    void start_threads();
    void stop_threads();

    // Set execution mode
    void set_mode(ExecutionMode m) { mode = m; }
    ExecutionMode get_mode() const { return mode; }

    // Direct queue access (thread_id % num_queues for mapping)
    LockFreeQueue<KVResponse>* get_resp_queue(uint32_t thread_id) const {
        return resp_queues[thread_id % config.num_resp_queues]->queue;
    }
    LockFreeQueue<KVRequest*>* get_req_queue(uint32_t thread_id) const {
        return req_queues[thread_id % config.num_req_queues]->queue;
    }
    ResponseQueue* get_resp_queue_obj(uint32_t thread_id) const {
        return resp_queues[thread_id % config.num_resp_queues];
    }
    RequestQueue* get_req_queue_obj(uint32_t thread_id) const {
        return req_queues[thread_id % config.num_req_queues];
    }
};

// Thread-safe initialization (simplified interface)
SharedKVContext* get_or_create_context(int numa_node, const AsyncConfig& cfg,
                                       size_t worker_ring_buffer_size = 4096);

void destroy_context();
