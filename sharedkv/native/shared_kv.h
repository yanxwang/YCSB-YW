#pragma once
#include <string>
#include <cstdint>
#include <atomic>

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

// map the shared memory
void* map_shared_memory(const char* path, size_t size);

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
