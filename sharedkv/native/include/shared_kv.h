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
    static constexpr size_t BUFFER_SIZE = 2048;

    alignas(64) KVRequest* buffer[BUFFER_SIZE];  // Pointer array for requests
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

    // Ring buffer operations
    bool try_handoff(KVRequest* req);
    bool try_get_request(KVRequest*& req);

    // Verify bucket ownership (debug mode)
    inline bool owns_bucket(uint32_t bucket_id) const {
        return (bucket_id % num_workers) == worker_id;
    }
};

// Client channel (per client - Solution A: No dedicated threads)
// Application threads directly enqueue to req_q and dequeue from resp_q
struct ClientChannel {
    uint32_t client_id;

    // Queues for application thread and worker communication
    LockFreeQueue<KVRequest*>* req_q;   // App threads → synchronizer
    LockFreeQueue<KVResponse>* resp_q;  // Workers → app threads

    // UINTR infrastructure (for future optimization)
    int uintr_fd;
    std::atomic<bool> response_ready{false};

    // Statistics
    std::atomic<uint64_t> requests_sent{0};
    std::atomic<uint64_t> responses_received{0};

    ClientChannel(size_t queue_size);
    ~ClientChannel();
};

// Main threading context (singleton per JVM)
struct SharedKVContext {
    // Shared resources
    SharedHashTable* table;
    void* base;

    // Configuration
    uint32_t num_clients;
    uint32_t num_workers;
    int numa_node;

    // Thread infrastructure
    std::vector<ClientChannel*> clients;
    std::vector<KVWorker*> workers;
    std::vector<std::thread> worker_threads;
    std::thread synchronizer_thread;
    std::thread poller_thread;

    // Lifecycle
    std::atomic<bool> stop_flag{false};
    std::atomic<bool> initialized{false};
    std::atomic<uint64_t> global_sequence{0};

    // Constructor/Destructor
    SharedKVContext(uint32_t num_clients, uint32_t num_workers, int numa_node);
    ~SharedKVContext();

    // Thread management
    void start_threads();
    void stop_threads();

    // Request submission (called from JNI, blocking)
    KVResponse submit_request(uint32_t client_id, KVRequest* req, uint64_t timeout_ms);
};

// Thread-safe initialization
SharedKVContext* get_or_create_context(uint32_t num_clients, uint32_t num_workers, int numa_node);
void destroy_context();
