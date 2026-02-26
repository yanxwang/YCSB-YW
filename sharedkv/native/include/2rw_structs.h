#pragma once

// ============================================================================
// SharedKV 2RW Zero-Copy Architecture — Data Structures
// Spec: SharedKV_2RW_Arch.txt v1.3/v1.4
//
// Design:
//   - Only slot_id (32 bits) flows through the control plane
//   - KV data stays in CXL Global Request Pool (KVPoolSlot)
//   - All CXL memory structures use offset-based addressing (CXLPtr<T>)
//   - No spinlocks on buckets (single-writer per Worker partition)
// ============================================================================

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <type_traits>
#include <immintrin.h>

// ============================================================================
// Op / Status Constants
// ============================================================================

namespace TwoRW {

constexpr uint64_t POOL_MAGIC    = 0x534B;       // "SK"
constexpr uint64_t HEADER_MAGIC  = 0xC0DE2BC0ULL; // 2RW architecture magic

enum class OpType : uint8_t {
    GET    = 1,
    PUT    = 2,
    UPDATE = 3,
    DEL    = 4,
};

enum class Status : uint16_t {
    SUCCESS   = 0,
    NOT_FOUND = 1,
    ERROR     = 2,
    PENDING   = 0xFF,
};

// ============================================================================
// Global Request Pool Slot (2048B = 32 cache lines)
// Spec v1.3: KVPoolSlot
//
// Lives in CXL shared memory. Partitioned per client: Client j uses
// slot_ids in [j * slots_per_client, (j+1) * slots_per_client).
//
// Writer: Request Thread j
// Reader: Worker i (reads key + value for the operation)
// ============================================================================

struct alignas(64) KVPoolSlot {
    // --- Cache Line 0: Header (64B) ---
    uint64_t magic;       // POOL_MAGIC = 0x534B
    uint8_t  op_type;     // OpType enum
    uint16_t key_len;     // Length of key in key[]
    uint32_t val_len;     // Length of value in value[]
    uint64_t t0;          // rdtsc: Request Thread writes to pool
    char     reserved[40]; // pads cache line 0 to 64B (1B implicit pad before key_len)

    // --- Cache Lines 1-2: Key (128B) ---
    char     key[128];

    // --- Cache Lines 3-31: Value (1856B) ---
    char     value[1856];
};
static_assert(sizeof(KVPoolSlot) == 2048, "KVPoolSlot must be 2048B");
static_assert(alignof(KVPoolSlot) == 64);

// ============================================================================
// Control Plane Request (64B = 1 cache line)
// Spec: KVRequest
//
// Flows: RequestQueue[j] → Synchronizer → WorkerRing[i]
// Only carries slot_id (index into Pool), never KV data itself.
// ============================================================================

struct alignas(64) KVRequest {
    uint32_t worker_id;   // Target Worker (pre-computed by Request Thread)
    uint32_t slot_id;     // Index into the Global Request Pool
    uint64_t gsn;         // Global Sequence Number (assigned by Synchronizer)
    uint32_t client_id;   // Which client/response thread to route back to
    uint32_t _pad0;
    uint64_t t1;          // rdtsc: enters RequestQueue
    uint64_t t2;          // rdtsc: dequeued by Worker
    char     padding[24];
};
static_assert(sizeof(KVRequest) == 64, "KVRequest must be 64B");

// ============================================================================
// Control Plane Response (64B = 1 cache line)
// Spec: KVResponse
//
// Flows: Worker i → ResponseQueue[client_id][i] → Response Thread j
// For GET: val_addr is a CXL offset into Worker's DataRegion (zero-copy).
// For PUT/UPDATE/DELETE: val_addr = 0.
// ============================================================================

struct alignas(64) KVResponse {
    uint16_t status;      // Status enum
    uint16_t _pad0;
    uint32_t slot_id;     // For recycling via FreeIDQueue
    uint64_t val_addr;    // CXL offset: CXLNode.data + key_len (GET only, else 0)
    uint32_t val_len;     // Value length (GET only)
    uint32_t _pad1;
    uint64_t gsn;         // Echo from KVRequest
    uint64_t t3;          // rdtsc: Worker completes operation
    uint64_t t1;          // rdtsc: copied from KVRequest.t1 (enters RequestQueue)
    uint64_t t2;          // rdtsc: copied from KVRequest.t2 (Worker dequeues)
    char     padding[8];
};
static_assert(sizeof(KVResponse) == 64, "KVResponse must be 64B");

// ============================================================================
// Hash Table Bucket (8B, no lock — single-writer per Worker partition)
// Spec: CXLBucket
//
// Worker i exclusively owns buckets where (bucket_id % num_workers == i).
// No locks, no atomic CAS on head_offset updates.
// ============================================================================

struct alignas(8) CXLBucket {
    // CXL offset to first CXLNode, 0 = empty
    uint64_t head_offset;
};

// ============================================================================
// KV Node in DataRegion (variable size, cache-line aligned header)
// Spec: CXLNode
//
// Stored in Worker i's exclusive DataRegion[i].
// data[] layout: key_len bytes of key, then val_len bytes of value.
// val_addr in KVResponse points to data + key_len.
// ============================================================================

struct alignas(64) CXLNode {
    uint64_t next_offset;  // Offset to next node in chain, 0 = end
    uint32_t key_len;
    uint32_t val_len;
    uint64_t key_hash;     // FNV-1a hash, for fast comparison
    char     _pad[40];     // Pad header to 64B; data[] follows immediately

    // data[] is key_len bytes of key + val_len bytes of value
    // Access via: node_ptr + sizeof(CXLNode)
};
static_assert(sizeof(CXLNode) == 64, "CXLNode header must be 64B");

// ============================================================================
// DataRegion Per-Worker Metadata (64B)
// One per Worker, stored at the start of each DataRegion segment.
// ============================================================================

struct alignas(64) DataRegionHeader {
    uint64_t base_offset;        // CXL offset of this DataRegion's start
    uint64_t total_size;         // Total bytes in this DataRegion
    uint64_t alloc_offset;       // Bump allocator: next free byte (relative to base_offset)
    uint64_t free_list_offset;   // CXL offset of first free CXLNode (0 = none)
    uint64_t nodes_allocated;    // Stats: total nodes ever allocated
    uint64_t nodes_recycled;     // Stats: total nodes in free list
    char     _pad[16];
};
static_assert(sizeof(DataRegionHeader) == 64);

// ============================================================================
// FreeIDQueue — Local SPSC for slot ID recycling (NOT in CXL)
//
// Producer: Response Thread j (recycles slot_ids)
// Consumer: Request Thread j (acquires slot_ids)
//
// Single host only — same process, different threads.
// Uses volatile + compiler barrier, no cache flushing needed.
// ============================================================================

template<uint32_t Cap = 4096>
struct alignas(64) FreeIDQueue {
    static_assert((Cap & (Cap - 1)) == 0, "Cap must be power of 2");

    alignas(64) volatile uint32_t write_idx;
    char _pw[60];
    alignas(64) volatile uint32_t read_idx;
    char _pr[60];
    uint32_t slots[Cap];

    // Initialize with slot_ids for client j:
    //   [j * slots_per_client, (j+1) * slots_per_client)
    void init(uint32_t client_id, uint32_t slots_per_client) {
        write_idx = 0;
        read_idx  = 0;
        uint32_t base = client_id * slots_per_client;
        uint32_t count = (slots_per_client < Cap) ? slots_per_client : Cap;
        for (uint32_t i = 0; i < count; i++) {
            slots[i] = base + i;
        }
        write_idx = count;
        // Memory barrier so consumer sees initialized slots
        asm volatile("" ::: "memory");
    }

    // Push a recycled slot_id (called by Response Thread)
    bool push(uint32_t slot_id) {
        uint32_t w = write_idx;
        uint32_t r = read_idx;
        asm volatile("" ::: "memory");
        if (w - r >= Cap) return false;  // full
        slots[w & (Cap - 1)] = slot_id;
        asm volatile("" ::: "memory");   // store-store barrier
        write_idx = w + 1;
        return true;
    }

    // Pop a free slot_id (called by Request Thread)
    // Spins until a slot is available.
    uint32_t pop_spin() {
        while (true) {
            uint32_t r = read_idx;
            asm volatile("" ::: "memory");
            uint32_t w = write_idx;
            if (r < w) {
                uint32_t id = slots[r & (Cap - 1)];
                asm volatile("" ::: "memory");
                read_idx = r + 1;
                return id;
            }
            _mm_pause();
        }
    }

    // Non-blocking pop (returns false if empty)
    bool pop(uint32_t& slot_id) {
        uint32_t r = read_idx;
        asm volatile("" ::: "memory");
        if (r >= write_idx) return false;
        slot_id = slots[r & (Cap - 1)];
        asm volatile("" ::: "memory");
        read_idx = r + 1;
        return true;
    }

    bool is_empty() const { return read_idx >= write_idx; }
    bool is_full()  const { return (write_idx - read_idx) >= Cap; }
    uint32_t size() const { return write_idx - read_idx; }
};

// ============================================================================
// 2RW CXL Memory Header (64B, at offset 0 of CXL region)
// ============================================================================

struct alignas(64) TwoRWHeader {
    uint64_t magic;           // HEADER_MAGIC
    uint32_t num_clients;     // n
    uint32_t num_workers;     // m
    uint32_t slots_per_client;
    uint32_t num_buckets;
    uint32_t queue_depth;
    uint32_t _pad0;
    uint64_t total_size;
    char     _pad[24];
};
static_assert(sizeof(TwoRWHeader) == 64);

} // namespace TwoRW