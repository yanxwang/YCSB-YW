#pragma once

#include <cstdint>
#include <string>

// Forward declarations
template<typename T> struct LockFreeQueue;
struct KVResponse;

// KV operation types
enum class KVOpType : uint8_t {
    READ = 0,
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3
};

// KV operation status
enum class KVStatus : uint8_t {
    SUCCESS = 0,
    NOT_FOUND = 1,
    ERROR = 2
};

// KV Request structure (cache-line aligned for performance)
struct alignas(64) KVRequest {
    KVOpType op_type;
    uint32_t client_id;              // Set by Java thread (JNI)
    uint64_t sequence_number;        // Set by synchronizer
    uint64_t timestamp;              // Set by Java thread (JNI)

    // Key storage (inline for cache efficiency)
    uint32_t key_len;
    char key_data[128];  // Most YCSB keys fit here

    // Value storage (for INSERT/UPDATE operations)
    uint32_t value_len;
    char* value_data;  // Heap-allocated if needed

    // Response queue pointer (set by Java thread in JNI)
    LockFreeQueue<KVResponse>* resp_q_ptr;

    // Pre-computed routing information (set by Java thread in JNI)
    uint32_t target_worker_id;       // Pre-computed worker ID based on key hash

    // Object pool support (for async mode without heap allocation)
    // recycle_func: function pointer to return this request to pool
    // If non-null, calls recycle_func(recycle_ctx, this) instead of delete
    void (*recycle_func)(void* ctx, KVRequest* req);
    void* recycle_ctx;               // Context for recycle_func (e.g., RequestPool*)

    // Helper methods
    void set_key(const std::string& key);
    void set_value(const std::string& value);
    std::string get_key() const;
    std::string get_value() const;
    void cleanup();  // Free heap-allocated data
    void recycle();  // Return to pool or delete
};

// KV Response structure (smaller, cache-friendly)
struct alignas(64) KVResponse {
    KVStatus status;
    uint32_t client_id;
    uint64_t sequence_number;
    uint64_t timestamp;

    // Result storage (for READ operations)
    uint32_t result_len;
    char result_data[256];  // Inline storage for most results

    // Helper methods
    void set_result(const std::string& result);
    std::string get_result() const;
};
