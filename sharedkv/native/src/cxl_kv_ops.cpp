// cxl_kv_ops.cpp
// Lock-free KV operations for CXL shared memory
// Each bucket is owned by a single worker - no locks needed

#include "cxl_shared.h"
#include <cstring>
#include <functional>

// ============================================================================
// Hash function
// ============================================================================

static uint64_t cxl_hash_key(const char* key, uint32_t len) {
    // Simple FNV-1a hash
    uint64_t hash = 14695981039346656037ULL;
    for (uint32_t i = 0; i < len; i++) {
        hash ^= static_cast<uint64_t>(static_cast<unsigned char>(key[i]));
        hash *= 1099511628211ULL;
    }
    return hash;
}

// ============================================================================
// Offset-based pointer conversion
// ============================================================================

static inline void* offset_to_ptr(void* base, uint64_t offset) {
    if (offset == 0) return nullptr;
    return static_cast<char*>(base) + offset;
}

static inline uint64_t ptr_to_offset(void* base, void* ptr) {
    if (ptr == nullptr) return 0;
    return static_cast<char*>(ptr) - static_cast<char*>(base);
}

// ============================================================================
// KV PUT (INSERT/UPDATE) - No lock, single worker ownership
// ============================================================================

bool cxl_kv_put(void* base,
                CXLSharedHashTable* table,
                CXLMemoryRegion* region,
                const char* key, uint32_t key_len,
                const char* value, uint32_t value_len) {
    uint64_t hash = cxl_hash_key(key, key_len);
    uint32_t bucket_id = hash % table->num_buckets;
    CXLBucket* bucket = &table->buckets[bucket_id];

    // Search for existing key (update case)
    uint64_t cur_offset = bucket->head_offset;
    while (cur_offset != 0) {
        CXLKVEntry* entry = static_cast<CXLKVEntry*>(offset_to_ptr(base, cur_offset));

        if (entry->key_hash == hash && entry->key_len == key_len) {
            const char* entry_key = entry->get_data();
            if (memcmp(entry_key, key, key_len) == 0) {
                // Found existing key - update value in place if it fits
                if (value_len <= entry->value_len) {
                    char* entry_value = const_cast<char*>(entry_key) + entry->key_len;
                    memcpy(entry_value, value, value_len);
                    entry->value_len = value_len;
                    _mm_sfence();  // Ensure update is visible
                    return true;
                }
                // Value doesn't fit - allocate new entry and update pointer
                // (old entry becomes garbage, GC would reclaim later)
                break;
            }
        }
        cur_offset = entry->next_offset;
    }

    // Allocate new entry from worker's memory region
    uint64_t entry_size = sizeof(CXLKVEntry) + key_len + value_len;
    uint64_t entry_offset = region->allocate(entry_size, 8);

    if (entry_offset == 0) {
        return false;  // Out of memory
    }

    // Initialize new entry
    CXLKVEntry* new_entry = static_cast<CXLKVEntry*>(offset_to_ptr(base, entry_offset));
    new_entry->key_hash = hash;
    new_entry->key_len = key_len;
    new_entry->value_len = value_len;

    // Copy key and value
    char* data = new_entry->get_data();
    memcpy(data, key, key_len);
    memcpy(data + key_len, value, value_len);

    // Insert at head of bucket chain
    new_entry->next_offset = bucket->head_offset;

    // Memory fence before updating head (ensures entry data is visible)
    _mm_sfence();

    bucket->head_offset = entry_offset;

    return true;
}

// ============================================================================
// KV GET (READ) - No lock needed (read-only traversal)
// ============================================================================

bool cxl_kv_get(void* base,
                CXLSharedHashTable* table,
                const char* key, uint32_t key_len,
                char* value_out, uint32_t* value_len_out,
                uint32_t max_value_len) {
    uint64_t hash = cxl_hash_key(key, key_len);
    uint32_t bucket_id = hash % table->num_buckets;
    CXLBucket* bucket = &table->buckets[bucket_id];

    // Memory fence to ensure we see latest writes
    _mm_lfence();

    uint64_t cur_offset = bucket->head_offset;
    while (cur_offset != 0) {
        CXLKVEntry* entry = static_cast<CXLKVEntry*>(offset_to_ptr(base, cur_offset));

        if (entry->key_hash == hash && entry->key_len == key_len) {
            const char* entry_key = entry->get_data();
            if (memcmp(entry_key, key, key_len) == 0) {
                // Found!
                const char* entry_value = entry_key + entry->key_len;
                uint32_t copy_len = (entry->value_len < max_value_len)
                                    ? entry->value_len : max_value_len;
                memcpy(value_out, entry_value, copy_len);
                *value_len_out = entry->value_len;
                return true;
            }
        }
        cur_offset = entry->next_offset;
    }

    return false;  // Not found
}

// ============================================================================
// KV DELETE - No lock, single worker ownership
// ============================================================================

bool cxl_kv_delete(void* base,
                   CXLSharedHashTable* table,
                   const char* key, uint32_t key_len) {
    uint64_t hash = cxl_hash_key(key, key_len);
    uint32_t bucket_id = hash % table->num_buckets;
    CXLBucket* bucket = &table->buckets[bucket_id];

    uint64_t prev_offset = 0;
    uint64_t cur_offset = bucket->head_offset;

    while (cur_offset != 0) {
        CXLKVEntry* entry = static_cast<CXLKVEntry*>(offset_to_ptr(base, cur_offset));

        if (entry->key_hash == hash && entry->key_len == key_len) {
            const char* entry_key = entry->get_data();
            if (memcmp(entry_key, key, key_len) == 0) {
                // Found - unlink from chain
                if (prev_offset == 0) {
                    // Removing head
                    bucket->head_offset = entry->next_offset;
                } else {
                    // Removing from middle/end
                    CXLKVEntry* prev_entry = static_cast<CXLKVEntry*>(
                        offset_to_ptr(base, prev_offset)
                    );
                    prev_entry->next_offset = entry->next_offset;
                }

                _mm_sfence();  // Ensure unlink is visible
                return true;
            }
        }

        prev_offset = cur_offset;
        cur_offset = entry->next_offset;
    }

    return false;  // Not found
}

// ============================================================================
// Initialize CXL Memory
// ============================================================================

void cxl_init_memory(void* base, uint64_t total_size,
                     uint32_t num_workers, uint32_t num_response_rings) {
    CXLMemoryLayout layout = CXLMemoryLayout::calculate(
        num_workers, num_response_rings, total_size
    );

    // Initialize hash table
    CXLSharedHashTable* table = cxl_get_hash_table(base, layout);
    table->magic = 0;
    table->version = 1;
    table->num_buckets = CXL_NUM_BUCKETS;
    table->num_workers = num_workers;
    table->total_size = total_size;

    // Clear all buckets
    for (uint32_t i = 0; i < CXL_NUM_BUCKETS; i++) {
        table->buckets[i].head_offset = 0;
    }

    // Initialize worker registry
    CXLWorkerRegistry* registry = cxl_get_worker_registry(base, layout);
    registry->init(num_workers, CXL_NUM_BUCKETS);

    // Initialize per-worker memory regions
    CXLMemoryRegion* regions = cxl_get_worker_regions(base, layout);
    uint64_t kv_region_per_worker = layout.kv_data_size / num_workers;

    for (uint32_t i = 0; i < num_workers; i++) {
        uint64_t region_base = layout.kv_data_offset + (kv_region_per_worker * i);
        regions[i].init(region_base, kv_region_per_worker);

        // Update worker registry with ring buffer offset
        uint64_t ring_size = sizeof(CXLRingBuffer) +
                            sizeof(CXLRequest) * CXL_RING_BUFFER_SIZE;
        ring_size = (ring_size + 63) & ~63ULL;
        registry->workers[i].ring_buffer_offset =
            layout.request_rings_offset + (ring_size * i);
    }

    // Initialize request ring buffers
    for (uint32_t i = 0; i < num_workers; i++) {
        CXLRingBuffer* ring = cxl_get_request_ring(base, layout, i);
        ring->init(CXL_RING_BUFFER_SIZE);
    }

    // Initialize response ring buffers
    for (uint32_t i = 0; i < num_response_rings; i++) {
        CXLResponseRing* ring = cxl_get_response_ring(base, layout, i);
        ring->init(CXL_RING_BUFFER_SIZE);
    }

    // Memory fence to ensure all initialization is visible
    _mm_sfence();

    // Mark as initialized
    table->magic = CXL_MAGIC_INITIALIZED;
}

// ============================================================================
// Check if CXL memory is initialized
// ============================================================================

bool cxl_is_initialized(void* base, const CXLMemoryLayout& layout) {
    CXLSharedHashTable* table = cxl_get_hash_table(base, layout);
    _mm_lfence();
    return table->magic == CXL_MAGIC_INITIALIZED;
}
