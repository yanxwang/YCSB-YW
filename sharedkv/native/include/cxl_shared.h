#pragma once

#include <cstdint>
#include <cstring>
#include <atomic>
#include <emmintrin.h>  // _mm_sfence, _mm_lfence

// ============================================================================
// CXL Shared Memory Data Structures
// Designed for multi-host access without atomic operations
// Uses single-writer protocol + memory fences for correctness
// ============================================================================

// Configuration constants
#define CXL_MAX_WORKERS         64
#define CXL_MAX_CLIENTS         256
#define CXL_RING_BUFFER_SIZE    4096    // Must be power of 2
#define CXL_RING_BUFFER_MASK    (CXL_RING_BUFFER_SIZE - 1)
#define CXL_MAX_KEY_SIZE        128
#define CXL_MAX_VALUE_SIZE      1024
#define CXL_NUM_BUCKETS         4096
#define CXL_MEMORY_SIZE         (16ULL * 1024 * 1024 * 1024)  // 16GB

// Magic numbers for validation (valid hex values)
#define CXL_MAGIC_INITIALIZED   0xC0DE1A1D0000AULL
#define CXL_MAGIC_WORKER_READY  0xBEEF00000EA01ULL

// ============================================================================
// Request/Response structures for CXL shared memory (fixed-size, no pointers)
// ============================================================================

// KV operation types (same as kv_request.h but for CXL)
enum class CXLOpType : uint8_t {
    READ = 0,
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    NOP = 255       // Empty slot marker
};

// KV status (same as kv_request.h)
enum class CXLStatus : uint8_t {
    SUCCESS = 0,
    NOT_FOUND = 1,
    ERROR = 2,
    PENDING = 255   // Not yet processed
};

// Fixed-size request structure for CXL shared memory (no pointers!)
struct alignas(64) CXLRequest {
    // Slot metadata
    uint64_t sequence;              // Slot sequence number (for ordering)

    // Request data
    CXLOpType op_type;
    uint8_t padding1[3];
    uint32_t client_id;
    uint32_t resp_ring_id;          // Which response ring to write to
    uint64_t timestamp;

    // Key (inline, fixed size)
    uint32_t key_len;
    char key_data[CXL_MAX_KEY_SIZE];

    // Value (inline, fixed size)
    uint32_t value_len;
    char value_data[CXL_MAX_VALUE_SIZE];

    // Helper methods
    void set_key(const char* key, uint32_t len) {
        key_len = (len > CXL_MAX_KEY_SIZE) ? CXL_MAX_KEY_SIZE : len;
        memcpy(key_data, key, key_len);
    }

    void set_value(const char* value, uint32_t len) {
        value_len = (len > CXL_MAX_VALUE_SIZE) ? CXL_MAX_VALUE_SIZE : len;
        memcpy(value_data, value, value_len);
    }
};

// Fixed-size response structure for CXL shared memory
struct alignas(64) CXLResponse {
    // Slot metadata
    uint64_t sequence;              // Slot sequence number

    // Response data
    CXLStatus status;
    uint8_t padding1[3];
    uint32_t client_id;
    uint64_t timestamp;

    // Result (inline, fixed size)
    uint32_t result_len;
    char result_data[CXL_MAX_VALUE_SIZE];

    void set_result(const char* result, uint32_t len) {
        result_len = (len > CXL_MAX_VALUE_SIZE) ? CXL_MAX_VALUE_SIZE : len;
        memcpy(result_data, result, result_len);
    }
};

// ============================================================================
// Single-Writer Ring Buffer (SPSC - Single Producer Single Consumer)
// No atomic operations needed - uses sequence numbers + memory fences
// ============================================================================

struct alignas(64) CXLRingBuffer {
    // Cache-line separated indices to avoid false sharing
    // Use volatile to prevent compiler caching across function calls
    alignas(64) volatile uint64_t write_idx;     // Only producer writes
    alignas(64) volatile uint64_t read_idx;      // Only consumer writes
    alignas(64) uint64_t size;                   // Ring buffer size (power of 2)
    alignas(64) uint64_t mask;                   // size - 1 for fast modulo

    // Slots follow this header in memory
    // CXLRequest slots[CXL_RING_BUFFER_SIZE];

    // Get pointer to slots (they follow this struct in memory)
    CXLRequest* get_slots() {
        return reinterpret_cast<CXLRequest*>(
            reinterpret_cast<char*>(this) + sizeof(CXLRingBuffer)
        );
    }

    const CXLRequest* get_slots() const {
        return reinterpret_cast<const CXLRequest*>(
            reinterpret_cast<const char*>(this) + sizeof(CXLRingBuffer)
        );
    }

    // Producer: enqueue (single writer - Request Thread)
    bool enqueue(const CXLRequest& req) {
        // Read our position (we own it)
        uint64_t w = write_idx;

        // Memory fence: ensure we see the latest read_idx from consumer
        _mm_lfence();
        uint64_t r = read_idx;

        // Check if full
        if ((w - r) >= size) {
            return false;
        }

        // Write data to slot
        CXLRequest* slots = get_slots();
        slots[w & mask] = req;

        // Memory fence: ensure data is written before updating index
        _mm_sfence();

        // Update write index (makes data visible to consumer)
        write_idx = w + 1;

        // Another fence to ensure write_idx update is visible
        _mm_sfence();

        return true;
    }

    // Consumer: dequeue (single reader - Worker)
    bool dequeue(CXLRequest& req) {
        // Read our position (we own it)
        uint64_t r = read_idx;

        // Memory fence: ensure we see the latest write_idx from producer
        _mm_lfence();
        uint64_t w = write_idx;

        // Check if empty
        if (r >= w) {
            return false;
        }

        // Memory fence: ensure we read the latest data from slot
        _mm_lfence();

        // Read data from slot
        const CXLRequest* slots = get_slots();
        req = slots[r & mask];

        // Memory fence: ensure data is read before updating index
        _mm_lfence();

        // Update read index (frees slot for producer)
        read_idx = r + 1;

        // Fence to ensure read_idx update is visible to producer
        _mm_sfence();

        return true;
    }

    // Check if empty (for polling)
    bool is_empty() const {
        _mm_lfence();
        return read_idx >= write_idx;
    }

    // Get current size
    uint64_t get_size() const {
        _mm_lfence();
        uint64_t w = write_idx;
        uint64_t r = read_idx;
        return (w >= r) ? (w - r) : 0;
    }

    // Initialize ring buffer
    void init(uint64_t buffer_size) {
        write_idx = 0;
        read_idx = 0;
        size = buffer_size;
        mask = buffer_size - 1;
        _mm_sfence();

        // Clear all slots
        CXLRequest* slots = get_slots();
        memset(slots, 0, sizeof(CXLRequest) * buffer_size);
        _mm_sfence();
    }
};

// Response ring buffer (SPSC - Single Producer Single Consumer)
// Uses volatile + memory fences for CXL memory compatibility
struct alignas(64) CXLResponseRing {
    alignas(64) volatile uint64_t write_idx;     // Only worker writes
    alignas(64) volatile uint64_t read_idx;      // Only client writes
    alignas(64) uint64_t size;
    alignas(64) uint64_t mask;

    CXLResponse* get_slots() {
        return reinterpret_cast<CXLResponse*>(
            reinterpret_cast<char*>(this) + sizeof(CXLResponseRing)
        );
    }

    const CXLResponse* get_slots() const {
        return reinterpret_cast<const CXLResponse*>(
            reinterpret_cast<const char*>(this) + sizeof(CXLResponseRing)
        );
    }

    bool enqueue(const CXLResponse& resp) {
        uint64_t w = write_idx;
        _mm_lfence();
        uint64_t r = read_idx;

        if ((w - r) >= size) {
            return false;
        }

        CXLResponse* slots = get_slots();
        slots[w & mask] = resp;

        _mm_sfence();
        write_idx = w + 1;
        _mm_sfence();

        return true;
    }

    bool dequeue(CXLResponse& resp) {
        // Use mfence for full memory barrier to ensure visibility
        _mm_mfence();
        uint64_t r = read_idx;
        _mm_mfence();
        uint64_t w = write_idx;

        if (r >= w) {
            return false;
        }

        _mm_lfence();
        const CXLResponse* slots = get_slots();
        resp = slots[r & mask];

        _mm_mfence();
        read_idx = r + 1;
        _mm_mfence();

        return true;
    }

    bool is_empty() const {
        _mm_lfence();
        return read_idx >= write_idx;
    }

    void init(uint64_t buffer_size) {
        write_idx = 0;
        read_idx = 0;
        size = buffer_size;
        mask = buffer_size - 1;
        _mm_sfence();
        memset(get_slots(), 0, sizeof(CXLResponse) * buffer_size);
        _mm_sfence();
    }
};

// ============================================================================
// Worker Registry (for multi-host worker discovery)
// ============================================================================

struct WorkerInfo {
    uint64_t magic;                 // CXL_MAGIC_WORKER_READY when ready
    uint32_t worker_id;
    uint32_t host_id;               // Which host this worker runs on
    uint32_t bucket_start;          // First bucket this worker owns
    uint32_t bucket_count;          // Number of buckets this worker owns
    uint64_t ring_buffer_offset;    // Offset to this worker's request ring buffer
    uint64_t stats_processed;       // Operations processed (updated by worker)
    uint64_t padding[4];            // Padding to cache line
};

struct alignas(64) CXLWorkerRegistry {
    uint64_t magic;                 // CXL_MAGIC_INITIALIZED when ready
    uint32_t num_workers;
    uint32_t num_buckets;
    uint64_t padding[6];

    WorkerInfo workers[CXL_MAX_WORKERS];

    void init(uint32_t n_workers, uint32_t n_buckets) {
        magic = 0;
        num_workers = n_workers;
        num_buckets = n_buckets;

        // Assign buckets to workers using modulo
        for (uint32_t i = 0; i < n_workers; i++) {
            workers[i].magic = 0;
            workers[i].worker_id = i;
            workers[i].host_id = 0;  // Set by worker when it starts
            workers[i].bucket_start = i;  // For modulo assignment
            workers[i].bucket_count = (n_buckets + n_workers - 1) / n_workers;
            workers[i].ring_buffer_offset = 0;  // Set during memory layout
            workers[i].stats_processed = 0;
        }

        _mm_sfence();
        magic = CXL_MAGIC_INITIALIZED;
    }

    // Check if worker owns a bucket (modulo assignment)
    bool worker_owns_bucket(uint32_t worker_id, uint32_t bucket_id) const {
        return (bucket_id % num_workers) == worker_id;
    }

    // Get worker ID for a bucket
    uint32_t get_worker_for_bucket(uint32_t bucket_id) const {
        return bucket_id % num_workers;
    }
};

// ============================================================================
// Per-Worker Memory Allocator Region
// Each worker has its own memory region to avoid contention
// ============================================================================

struct alignas(64) CXLMemoryRegion {
    uint64_t base_offset;           // Offset from CXL memory base
    uint64_t size;                  // Total size of this region
    uint64_t free_offset;           // Current allocation offset (single writer)
    uint64_t padding[5];

    // Allocate memory from this region (single writer - the owning worker)
    uint64_t allocate(uint64_t alloc_size, uint64_t alignment = 8) {
        uint64_t aligned_offset = (free_offset + alignment - 1) & ~(alignment - 1);
        uint64_t new_offset = aligned_offset + alloc_size;

        if (new_offset > size) {
            return 0;  // Out of memory
        }

        uint64_t result = base_offset + aligned_offset;
        free_offset = new_offset;

        return result;
    }

    void init(uint64_t base, uint64_t region_size) {
        base_offset = base;
        size = region_size;
        free_offset = 0;
    }

    uint64_t get_used() const { return free_offset; }
    uint64_t get_free() const { return size - free_offset; }
};

// ============================================================================
// KV Entry (lock-free, stored in CXL memory)
// ============================================================================

struct CXLKVEntry {
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t next_offset;           // 0 = end of chain
    // char data[0];                // Key bytes followed by value bytes

    char* get_data() {
        return reinterpret_cast<char*>(this) + sizeof(CXLKVEntry);
    }

    const char* get_data() const {
        return reinterpret_cast<const char*>(this) + sizeof(CXLKVEntry);
    }
};

// ============================================================================
// Bucket (no lock needed - single worker ownership)
// ============================================================================

struct CXLBucket {
    uint64_t head_offset;           // Offset to first entry, 0 = empty
    uint64_t padding[7];            // Pad to cache line
};

// ============================================================================
// Main CXL Shared Hash Table Layout
// ============================================================================

struct CXLSharedHashTable {
    // Header
    uint64_t magic;                 // CXL_MAGIC_INITIALIZED
    uint64_t version;               // For compatibility checking
    uint32_t num_buckets;
    uint32_t num_workers;
    uint64_t total_size;            // Total CXL memory size
    uint64_t padding[4];

    // Buckets array (no locks!)
    CXLBucket buckets[CXL_NUM_BUCKETS];

    // Worker registry follows
    // CXLWorkerRegistry registry;

    // Per-worker memory regions follow
    // CXLMemoryRegion worker_regions[num_workers];

    // Ring buffers follow
    // CXLRingBuffer worker_ring_buffers[num_workers];

    // Response ring buffers follow
    // CXLResponseRing response_rings[num_clients];

    // KV data region follows (bulk of the memory)
};

// ============================================================================
// CXL Memory Layout Calculator
// ============================================================================

struct CXLMemoryLayout {
    uint64_t hash_table_offset;
    uint64_t worker_registry_offset;
    uint64_t worker_regions_offset;
    uint64_t request_rings_offset;
    uint64_t response_rings_offset;
    uint64_t kv_data_offset;
    uint64_t kv_data_size;
    uint64_t total_size;

    static CXLMemoryLayout calculate(uint32_t num_workers,
                                     uint32_t num_response_rings,
                                     uint64_t total_memory) {
        CXLMemoryLayout layout;
        uint64_t offset = 0;

        // Hash table (header + buckets)
        layout.hash_table_offset = offset;
        offset += sizeof(CXLSharedHashTable);
        offset = (offset + 63) & ~63ULL;  // Align to cache line

        // Worker registry
        layout.worker_registry_offset = offset;
        offset += sizeof(CXLWorkerRegistry);
        offset = (offset + 63) & ~63ULL;

        // Per-worker memory regions metadata
        layout.worker_regions_offset = offset;
        offset += sizeof(CXLMemoryRegion) * num_workers;
        offset = (offset + 63) & ~63ULL;

        // Request ring buffers (one per worker)
        layout.request_rings_offset = offset;
        uint64_t ring_size = sizeof(CXLRingBuffer) +
                            sizeof(CXLRequest) * CXL_RING_BUFFER_SIZE;
        ring_size = (ring_size + 63) & ~63ULL;
        offset += ring_size * num_workers;

        // Response ring buffers (one per client/response thread)
        layout.response_rings_offset = offset;
        uint64_t resp_ring_size = sizeof(CXLResponseRing) +
                                  sizeof(CXLResponse) * CXL_RING_BUFFER_SIZE;
        resp_ring_size = (resp_ring_size + 63) & ~63ULL;
        offset += resp_ring_size * num_response_rings;

        // KV data region (rest of memory, divided among workers)
        layout.kv_data_offset = offset;
        layout.kv_data_size = total_memory - offset;

        layout.total_size = total_memory;

        return layout;
    }
};

// ============================================================================
// Helper: Get pointers to various regions
// ============================================================================

inline CXLSharedHashTable* cxl_get_hash_table(void* base, const CXLMemoryLayout& layout) {
    return reinterpret_cast<CXLSharedHashTable*>(
        static_cast<char*>(base) + layout.hash_table_offset
    );
}

inline CXLWorkerRegistry* cxl_get_worker_registry(void* base, const CXLMemoryLayout& layout) {
    return reinterpret_cast<CXLWorkerRegistry*>(
        static_cast<char*>(base) + layout.worker_registry_offset
    );
}

inline CXLMemoryRegion* cxl_get_worker_regions(void* base, const CXLMemoryLayout& layout) {
    return reinterpret_cast<CXLMemoryRegion*>(
        static_cast<char*>(base) + layout.worker_regions_offset
    );
}

inline CXLRingBuffer* cxl_get_request_ring(void* base, const CXLMemoryLayout& layout,
                                           uint32_t worker_id) {
    uint64_t ring_size = sizeof(CXLRingBuffer) +
                        sizeof(CXLRequest) * CXL_RING_BUFFER_SIZE;
    ring_size = (ring_size + 63) & ~63ULL;

    return reinterpret_cast<CXLRingBuffer*>(
        static_cast<char*>(base) + layout.request_rings_offset + ring_size * worker_id
    );
}

inline CXLResponseRing* cxl_get_response_ring(void* base, const CXLMemoryLayout& layout,
                                               uint32_t ring_id) {
    uint64_t ring_size = sizeof(CXLResponseRing) +
                        sizeof(CXLResponse) * CXL_RING_BUFFER_SIZE;
    ring_size = (ring_size + 63) & ~63ULL;

    return reinterpret_cast<CXLResponseRing*>(
        static_cast<char*>(base) + layout.response_rings_offset + ring_size * ring_id
    );
}
