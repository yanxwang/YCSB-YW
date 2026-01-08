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
    uint32_t client_id;
    uint64_t sequence_number;  // Global sequence number assigned by synchronizer
    uint64_t timestamp;        // For latency tracking (optional, 0 = disabled)

    // Key storage (inline for cache efficiency)
    uint32_t key_len;
    char key_data[128];  // Most YCSB keys fit here

    // Value storage (for INSERT/UPDATE operations)
    uint32_t value_len;
    char* value_data;  // Heap-allocated if needed

    // Response queue pointer (set by synchronizer)
    LockFreeQueue<KVResponse>* resp_q_ptr;

    // Helper methods
    void set_key(const std::string& key);
    void set_value(const std::string& value);
    std::string get_key() const;
    std::string get_value() const;
    void cleanup();  // Free heap-allocated data
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
