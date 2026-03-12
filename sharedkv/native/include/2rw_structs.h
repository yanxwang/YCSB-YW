#pragma once

// ============================================================================
// SharedKV 2RW Zero-Copy Architecture — Data Structures
// Spec: unified_block_zero_data_copy.txt v2.0
//
// Design:
//   - UnifiedBlock (2KB) serves as both request buffer and persistent hash node
//   - Only block_id (32 bits) flows through the control plane
//   - Block Swap Protocol: RT fills id_new, Worker links id_new into hash table,
//     returns id_old via KVResponse for recycling — zero memcpy
//   - All hash table links use block_id (uint32_t control plane,
//     uint64_t in UnifiedBlock.next_block_id for future >8TB CXL compat)
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

constexpr uint64_t BLOCK_MAGIC  = 0x534B;       // "SK" (kept for compat)
constexpr uint64_t HEADER_MAGIC = 0xC0DE2BC0ULL; // 2RW architecture magic

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
// UnifiedBlock (2048B = 32 cache lines)
// Spec v2.0: replaces KVPoolSlot + CXLNode
//
// Dual identity:
//   Phase 1 (request): RT writes key/value + metadata, sfence, submits block_id.
//   Phase 2 (data):    Worker links block directly into hash table — no copy.
//
// next_block_id is uint64_t for future CXL >8TB compatibility (control plane
// uses uint32_t block_id; casting is zero-cost on x86-64 via implicit zero-ext).
//
// Sentinel: block_id == 0 is reserved (never allocated), used as null/chain-end.
//
// Header layout (natural alignment, no __attribute__((packed))):
//   [0..7]   next_block_id (8B)
//   [8..11]  key_hash (4B)
//   [12..13] key_len (2B)
//   [14..15] implicit pad
//   [16..19] val_len (4B)
//   [20]     is_external (1B)
//   [21..23] implicit pad
//   [24..31] gsn (8B)
//   [32..39] t0 (8B)
//   [40..47] t1 (8B)
//   [48..55] t2 (8B)
//   [56..63] t3 (8B)
//   [64..]   data[] — key_len bytes of key, then val_len bytes of value
// ============================================================================

struct alignas(64) UnifiedBlock {
    // --- Header (64B) ---
    uint64_t next_block_id; // Hash chain pointer (0 = end); uint64_t for >8TB CXL
    uint32_t key_hash;      // FNV-1a, pre-computed by RT for fast Worker comparison
    uint16_t key_len;
    // [2B implicit pad for val_len alignment]
    uint32_t val_len;
    uint8_t  is_external;   // 1 = value stored in SpilloverRegion; data[0..7] = CXL offset
    // [3B implicit pad for gsn alignment]
    uint64_t gsn;           // Written by Worker from KVRequest.gsn
    uint64_t t0;            // RT: after filling block + _mm_sfence(), __rdtscp()
    uint64_t t1;            // RT: before enqueue to RequestQueue, __rdtscp()
    uint64_t t2;            // Synchronizer: before enqueue to WorkerRing, __rdtscp()
    uint64_t t3;            // Worker: after completing KV op, __rdtscp()

    // --- Data (1984B) ---
    // layout: key_len bytes of key || val_len bytes of value
    // When is_external == 1: data[0..7] stores uint64_t CXL offset of external value
    char data[1984];
};
static_assert(sizeof(UnifiedBlock) == 2048, "UnifiedBlock must be 2048B");
static_assert(offsetof(UnifiedBlock, data) == 64, "UnifiedBlock data must start at 64B");
static_assert(alignof(UnifiedBlock) == 64);

// Inline threshold for value storage in data[]:
//   data[] = 1984B, key <= 128B => max inline value ~1856B
// YCSB default 1000B value fits inline. Spillover path not triggered in normal bench.

// ============================================================================
// Control Plane Request (64B = 1 cache line)
// Spec: KVRequest
//
// Flows: RequestQueue[j] → Synchronizer → WorkerRing[i]
// Carries block_id (index into UnifiedBlockPool), never KV data itself.
//
// Field layout (natural alignment):
//   [0..3]   worker_id
//   [4..7]   block_id
//   [8..15]  gsn
//   [16..19] client_id
//   [20]     op_type
//   [21..23] _pad0
//   [24..31] t1
//   [32..39] t2
//   [40..43] key_hash   (RT-computed; worker retries CLFLUSHOPT until block matches)
//   [44..45] key_len    (RT-computed; paired with key_hash for confirmation)
//   [46..63] padding
// ============================================================================

struct alignas(64) KVRequest {
    uint32_t worker_id;   // Target Worker (pre-computed by RT)
    uint32_t block_id;    // Index into UnifiedBlockPool (was slot_id)
    uint64_t gsn;         // Global Sequence Number (assigned by Synchronizer)
    uint32_t client_id;   // Which client/response thread to route back to
    uint8_t  op_type;     // OpType enum: GET/PUT/UPDATE/DEL (was _pad0 uint32_t)
    uint8_t  _pad0[3];
    uint64_t t1;          // rdtscp: RT enqueues to RequestQueue
    uint64_t t2;          // rdtscp: Synchronizer enqueues to WorkerRing
    uint32_t key_hash;    // FNV-1a of key (RT fills; worker uses for bkt routing + CXL visibility check)
    uint16_t key_len;     // key length in bytes (RT fills; paired with key_hash for confirmation)
    char     padding[18];
};
static_assert(sizeof(KVRequest) == 64, "KVRequest must be 64B");

// ============================================================================
// Control Plane Response (64B = 1 cache line)
// Spec: KVResponse
//
// Flows: Worker i → ResponseQueue[client_id][i] → Response Thread j
//
// block_id_a: primary block to recycle (0 = none)
// block_id_b: secondary block to recycle, DEL only (0 = none)
//
// Response Thread unified recycle logic (no branching):
//   if (resp.block_id_a != 0) free_block_queue[cid].push(resp.block_id_a);
//   if (resp.block_id_b != 0) free_block_queue[cid].push(resp.block_id_b);
//
// Per-op semantics:
//   PUT/UPDATE (key existed):  block_id_a=id_old, block_id_b=0
//   PUT/UPDATE (new key):      block_id_a=0,      block_id_b=0
//   GET:                       block_id_a=id_req,  block_id_b=0, val_addr/val_len set
//   DEL (key existed):         block_id_a=id_cmd,  block_id_b=id_data
//   DEL (key not found):       block_id_a=id_cmd,  block_id_b=0
//
// t0 is copied by Worker from UnifiedBlock[id_new].t0, eliminating t0_table side table.
// sn_id replaces gsn (uint64_t→uint8_t): only used for per-SN latency attribution
//   when num_synchronizers > 1. Worker fills: sn_id = worker_id / workers_per_sn.
//
// Field layout (natural alignment):
//   [0..1]   status
//   [2]      op_type
//   [3]      sn_id
//   [4..7]   block_id_a
//   [8..11]  block_id_b
//   [12..15] val_len
//   [16..23] val_addr
//   [24..27] _pad0
//   [28..31] _pad1
//   [32..39] t0
//   [40..47] t1
//   [48..55] t2
//   [56..63] t3
// ============================================================================

struct alignas(64) KVResponse {
    uint16_t status;      // Status enum
    uint8_t  op_type;     // OpType enum
    uint8_t  sn_id;       // Synchronizer ID for per-SN attribution (was gsn uint64_t)
    uint32_t block_id_a;  // Primary recycled block (0 = none)
    uint32_t block_id_b;  // Secondary recycled block, DEL only (0 = none)
    uint32_t val_len;     // GET only, else 0
    uint64_t val_addr;    // GET only: cxl_base + id_found*2048 + 64 + key_len
    uint32_t _pad0;
    uint32_t _pad1;
    uint64_t t0;          // Copied by Worker from UnifiedBlock[id_new].t0
    uint64_t t1;          // Copied from KVRequest.t1
    uint64_t t2;          // Copied from KVRequest.t2
    uint64_t t3;          // rdtscp: Worker completes operation
};
static_assert(sizeof(KVResponse) == 64, "KVResponse must be 64B");

// ============================================================================
// Hash Table Bucket (8B, no lock — single-writer per Worker partition)
// Spec: CXLBucket
//
// Worker i exclusively owns buckets where (bucket_id % num_workers == i).
// head_block_id == 0 means empty bucket (block 0 is reserved sentinel).
// ============================================================================

struct alignas(8) CXLBucket {
    uint32_t head_block_id; // block_id of first UnifiedBlock in chain (0 = empty)
    uint32_t _pad;
};
static_assert(sizeof(CXLBucket) == 8);

// ============================================================================
// FreeBlockQueue — Local SPSC for block_id recycling (NOT in CXL)
//
// Producer: Response Thread j (pushes recycled block_ids)
// Consumer: Request Thread j (drains into LocalBlockCache.stack)
//
// Same implementation as the former FreeIDQueue; only semantics renamed.
// Single host only — same process, different threads.
// ============================================================================

template<uint32_t Cap = 4096>
struct alignas(64) FreeBlockQueue {
    static_assert((Cap & (Cap - 1)) == 0, "Cap must be power of 2");

    alignas(64) volatile uint32_t write_idx;
    char _pw[60];
    alignas(64) volatile uint32_t read_idx;
    char _pr[60];
    uint32_t slots[Cap];

    void init() {
        write_idx = 0;
        read_idx  = 0;
        asm volatile("" ::: "memory");
    }

    // Push a recycled block_id (called by Response Thread)
    bool push(uint32_t block_id) {
        uint32_t w = write_idx;
        uint32_t r = read_idx;
        asm volatile("" ::: "memory");
        if (w - r >= Cap) return false;  // full
        slots[w & (Cap - 1)] = block_id;
        asm volatile("" ::: "memory");   // store-store barrier
        write_idx = w + 1;
        return true;
    }

    // Non-blocking pop (returns false if empty)
    bool pop(uint32_t& block_id) {
        uint32_t r = read_idx;
        asm volatile("" ::: "memory");
        if (r >= write_idx) return false;
        block_id = slots[r & (Cap - 1)];
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
//
// Multi-machine fields (Section 6.1 of multi_machine_architecture_spec.txt):
//   num_synchronizers : S_total (was _pad0 in single-machine)
//   num_nodes         : number of machines in the cluster (1 = single-machine)
//   ready_flag        : master sets to HEADER_READY after init_cxl_memory()
//   global_stop_flag  : master sets to 1 to signal all nodes to stop
//
// Field layout:
//   [0..7]   magic (8B)
//   [8..11]  num_clients (4B) = N_total
//   [12..15] num_workers (4B) = M_total
//   [16..19] slots_per_client (4B)
//   [20..23] num_buckets (4B)
//   [24..27] queue_depth (4B)
//   [28..31] num_synchronizers (4B) = S_total
//   [32..39] total_size (8B)
//   [40..43] num_nodes (4B)
//   [44..47] ready_flag (4B)
//   [48..51] global_stop_flag (4B)
//   [52..63] _pad (12B)
// ============================================================================

constexpr uint32_t HEADER_READY    = 0xBEEF;
constexpr uint32_t MAX_NODES       = 16;

struct alignas(64) TwoRWHeader {
    uint64_t magic;               // HEADER_MAGIC
    uint32_t num_clients;         // N_total
    uint32_t num_workers;         // M_total
    uint32_t slots_per_client;
    uint32_t num_buckets;
    uint32_t queue_depth;
    uint32_t num_synchronizers;   // S_total (was _pad0)
    uint64_t total_size;
    uint32_t num_nodes;           // cluster size (1 = single-machine)
    uint32_t ready_flag;          // 0 = not ready, HEADER_READY = initialized
    volatile uint32_t global_stop_flag;  // 0 = running, 1 = stopping
    char     _pad[12];
};
static_assert(sizeof(TwoRWHeader) == 64);

// Per-node ready flags for distributed barrier (separate cache-line-aligned region).
// Placed after TwoRWHeader in CXL memory (at header_off + 64).
// Each node writes its own slot; all nodes read all slots.
struct alignas(64) CXLNodeSync {
    volatile uint32_t node_ready[MAX_NODES];  // 0 = not ready, 1 = ready
    char _pad[64 - MAX_NODES * sizeof(uint32_t)];
};
static_assert(sizeof(CXLNodeSync) == 64);

} // namespace TwoRW