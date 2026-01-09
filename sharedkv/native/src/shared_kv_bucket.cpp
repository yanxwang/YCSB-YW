// shared_kv_buckets.cpp
#include "shared_kv.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <functional>
#include <chrono>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <random>
#include <numa.h>
#include <numaif.h>

using namespace std::chrono;

// -------- low-level mapping --------
void* map_shared_memory(const char* dev_path, size_t size) {
    int fd = open(dev_path, O_RDWR);
    if (fd < 0) { perror("open"); exit(1); }
    void* addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) { perror("mmap"); close(fd); exit(1); }
    close(fd);
    return addr;
}

// -------- CXL/NUMA allocation --------
void* allocate_cxl_memory(int numa_node, size_t size) {
    if (numa_available() < 0) {
        fprintf(stderr, "NUMA not available\n");
        exit(1);
    }

    fprintf(stderr, "CXL: Starting allocation of %zu bytes on NUMA node %d...\n", size, numa_node);
    fflush(stderr);
    auto t_start = std::chrono::high_resolution_clock::now();

    // Set NUMA policy BEFORE mmap
    unsigned long nodemask = 1UL << numa_node;
    unsigned long maxnode = sizeof(nodemask) * 8;

    // Create shared memory object that persists across processes
    char shm_name[64];
    snprintf(shm_name, sizeof(shm_name), "/sharedkv_cxl_node%d", numa_node);

    // Try to open existing shared memory first
    int shm_fd = shm_open(shm_name, O_RDWR, 0666);
    bool is_new = false;

    if (shm_fd < 0) {
        // Shared memory doesn't exist, create it
        fprintf(stderr, "CXL: Creating new shared memory segment: %s\n", shm_name);
        shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
        if (shm_fd < 0) {
            perror("shm_open failed");
            exit(1);
        }

        // Set size for new shared memory
        if (ftruncate(shm_fd, size) != 0) {
            perror("ftruncate failed");
            close(shm_fd);
            shm_unlink(shm_name);
            exit(1);
        }
        is_new = true;
    } else {
        fprintf(stderr, "CXL: Reusing existing shared memory segment: %s\n", shm_name);
    }

    // Map shared memory to process address space
    void* addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                      MAP_SHARED, shm_fd, 0);
    close(shm_fd);  // Can close fd after mmap

    if (addr == MAP_FAILED) {
        perror("mmap failed");
        if (is_new) {
            shm_unlink(shm_name);
        }
        exit(1);
    }

    auto t_mmap = std::chrono::high_resolution_clock::now();
    auto mmap_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_mmap - t_start).count();
    fprintf(stderr, "CXL: mmap took %ld ms (is_new=%d)\n", mmap_ms, is_new);
    fflush(stderr);

    // Bind memory policy to specific NUMA node
    // Only use MPOL_MF_STRICT for NEW memory to avoid clearing existing data
    if (is_new) {
        // For new memory, bind strictly to the NUMA node
        if (mbind(addr, size, MPOL_BIND, &nodemask, maxnode, MPOL_MF_STRICT) != 0) {
            perror("mbind failed");
            fprintf(stderr, "Failed to bind %zu bytes to NUMA node %d\n", size, numa_node);
            munmap(addr, size);
            shm_unlink(shm_name);
            exit(1);
        }
        fprintf(stderr, "CXL: mbind completed with MPOL_MF_STRICT for new memory\n");
        fflush(stderr);

        // Touch first page to trigger allocation
        *(volatile char*)addr = 0;
    } else {
        // For existing memory, verify but don't move pages
        fprintf(stderr, "CXL: Skipping mbind for existing shared memory (preserving data)\n");
        fflush(stderr);
    }

    auto t_mbind = std::chrono::high_resolution_clock::now();
    auto mbind_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_mbind - t_mmap).count();
    fprintf(stderr, "CXL: Memory binding took %ld ms\n", mbind_ms);
    fflush(stderr);

    int actual_node = -1;
    if (get_mempolicy(&actual_node, nullptr, 0, addr, MPOL_F_NODE | MPOL_F_ADDR) == 0) {
        fprintf(stderr, "CXL: Allocated %zu bytes on NUMA node %d (requested: %d)\n",
               size, actual_node, numa_node);
        fflush(stderr);
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();
    fprintf(stderr, "CXL: Total allocation time: %ld ms\n", total_ms);
    fflush(stderr);

    return addr;
}

// -------- shared data layout (offset-based) --------
// KVEntry implementation (opaque in header)
struct KVEntry {
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t next_offset; // 0 == null
    char data[0]; // follows (key bytes then value bytes)
};

// -------- helpers --------
inline void* offset_to_ptr(void* base, uint64_t offset) {
    return (char*)base + offset;
}
inline uint64_t ptr_to_offset(void* base, void* ptr) {
    return (char*)ptr - (char*)base;
}
uint64_t hash_key(const std::string& key) {
    return std::hash<std::string>{}(key);
}

// -------- allocator & KV ops --------
KVEntry* alloc_entry(SharedHashTable* table, void* base,
                     const std::string& key, const std::string& value) {
    uint64_t total_size = sizeof(KVEntry) + key.size() + value.size();
    // Align allocations to 8 bytes for safety
    uint64_t aligned = (total_size + 7) & ~((uint64_t)7);
    uint64_t offset = table->free_offset.fetch_add(aligned, std::memory_order_relaxed);

    if (offset + aligned >= SHM_SIZE) return nullptr; // out of space

    KVEntry* e = (KVEntry*)((char*)base + offset);
    e->key_hash = hash_key(key);
    e->key_len = (uint32_t)key.size();
    e->value_len = (uint32_t)value.size();
    e->next_offset = 0;
    memcpy(e->data, key.data(), key.size());
    memcpy(e->data + key.size(), value.data(), value.size());
    return e;
}

void kv_put(SharedHashTable* table, void* base,
            const std::string& key, const std::string& value,
            uint64_t* wait_ns_out) {
    uint64_t h = hash_key(key);
    uint64_t idx = h % NUM_BUCKETS;
    Bucket* b = &table->buckets[idx];

    auto t0 = high_resolution_clock::now();
    b->lock.acquire();
    auto t1 = high_resolution_clock::now();
    if (wait_ns_out) *wait_ns_out += (uint64_t)duration_cast<nanoseconds>(t1 - t0).count();

    KVEntry* entry = alloc_entry(table, base, key, value);
    if (!entry) {
        b->lock.release();
        return;
    }
    // insert at head
    entry->next_offset = b->head_offset;
    b->head_offset = ptr_to_offset(base, entry);

    b->lock.release();
}

bool kv_get(SharedHashTable* table, void* base,
            const std::string& key, std::string& out_value,
            uint64_t* wait_ns_out) {
    uint64_t h = hash_key(key);
    uint64_t idx = h % NUM_BUCKETS;
    Bucket* b = &table->buckets[idx];

    auto t0 = high_resolution_clock::now();
    b->lock.acquire();
    auto t1 = high_resolution_clock::now();
    if (wait_ns_out) *wait_ns_out += (uint64_t)duration_cast<nanoseconds>(t1 - t0).count();

    uint64_t cur = b->head_offset;
    uint64_t target_hash = h;
    while (cur) {
        KVEntry* e = (KVEntry*)offset_to_ptr(base, cur);
        // key comparison
        if (e->key_hash == target_hash) {
            std::string k((char*)e->data, e->key_len);
            if (k == key) {
                out_value.assign((char*)e->data + e->key_len, e->value_len);
                b->lock.release();
                return true;
            }
        }
        cur = e->next_offset;
    }

    b->lock.release();
    return false;
}

bool kv_delete(SharedHashTable* table, void* base,
               const std::string& key,
               uint64_t* wait_ns_out) {
    uint64_t h = hash_key(key);
    uint64_t idx = h % NUM_BUCKETS;
    Bucket* b = &table->buckets[idx];

    auto t0 = std::chrono::high_resolution_clock::now();
    b->lock.acquire();
    auto t1 = std::chrono::high_resolution_clock::now();
    if (wait_ns_out) *wait_ns_out +=
        (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

    uint64_t prev_offset = 0;
    uint64_t cur_offset = b->head_offset;

    while (cur_offset) {
        KVEntry* e = (KVEntry*)offset_to_ptr(base, cur_offset);
        if (e->key_hash == h &&
            std::string((char*)e->data, e->key_len) == key) {
            // Remove from linked list
            if (prev_offset == 0) {
                b->head_offset = e->next_offset;
            } else {
                KVEntry* prev = (KVEntry*)offset_to_ptr(base, prev_offset);
                prev->next_offset = e->next_offset;
            }
            b->lock.release();
            return true;
        }
        prev_offset = cur_offset;
        cur_offset = e->next_offset;
    }

    b->lock.release();
    return false;
}


// -------- benchmarking & stats --------
// ThreadStats struct is defined in shared_kv.h

void worker_thread(SharedHashTable* table, void* base, int tid, int ops, ThreadStats* stat) {
    using namespace std::chrono;
    std::mt19937_64 rng((uint64_t)tid ^ (uint64_t)time(nullptr));
    std::uniform_int_distribution<int> dist(0, 1000000);

    auto t_start = high_resolution_clock::now();
    for (int i = 0; i < ops; ++i) {
        int r = dist(rng);
        std::string key = "k_" + std::to_string(r);
        std::string val = "v_" + std::to_string(r);

        // PUT
        kv_put(table, base, key, val, &stat->lock_wait_ns);
        stat->successful_puts++;
        stat->ops_completed++;

        // GET (read back)
        std::string out;
        if (kv_get(table, base, key, out, &stat->lock_wait_ns)) {
            stat->successful_gets++;
        }
        stat->ops_completed++;
    }
    auto t_end = high_resolution_clock::now();
    stat->thread_time_ns = (uint64_t)duration_cast<nanoseconds>(t_end - t_start).count();
}

