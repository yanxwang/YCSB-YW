#pragma once

// ============================================================================
// SharedKV 2RW — CXL Memory Layout
//
// Computes byte offsets for every region in the CXL shared memory slab.
// All sizes are computed at runtime from TwoRWConfig.
//
// Layout (low → high):
//   [TwoRWHeader          64B]
//   [HashTable            num_buckets × 8B, aligned 64B]
//   [GlobalIDMap          ceil(total_blocks/8) bytes, aligned 64B]  ← bitmap
//   [UnifiedBlockPool     total_blocks × 2048B, aligned 2048B]
//   [SpilloverRegion      n_clients × spillover_per_client, aligned 64B]
//   [RequestQueue         num_clients × num_synchronizers × sizeof(ReqQueue)]
//                         indexed [client_id * s + sn_id]
//   [WorkerRing           num_workers × sizeof(ReqQueue)]
//   [ResponseQueue        num_clients × num_workers × sizeof(RespQueue)]
//
// UnifiedBlockPool sizing:
//   Remaining space after all other fixed regions is split:
//     80% → UnifiedBlockPool (rounded down to 2048B multiple)
//     20% → SpilloverRegion (divided equally among clients)
//
// block_id addressing:
//   physical = cxl_base + unifiedblockpool_off + (uint64_t)block_id * 2048
//   block_id 0 is reserved (sentinel / null pointer), never allocated.
// ============================================================================

#include "2rw_structs.h"
#include "cxl_spsc_queue.h"
#include "cxl_ptr.h"
#include <cstdint>
#include <cstddef>
#include <cassert>

namespace TwoRW {

// Queue depths are compile-time template parameters for CXLSpscQueue.
constexpr size_t QUEUE_CAP = 4096;

using ReqQueue  = CXLSpscQueue<KVRequest,  QUEUE_CAP>;
using RespQueue = CXLSpscQueue<KVResponse, QUEUE_CAP>;

// ============================================================================
// Layout Calculator
// ============================================================================

struct TwoRWLayout {
    // Offsets (from CXL base) for each region
    uint64_t header_off;
    uint64_t hash_table_off;
    uint64_t globalidmap_off;        // CXL bitmap: total_blocks / 8 bytes
    uint64_t unifiedblockpool_off;   // flat array of UnifiedBlock, 2048B aligned
    uint64_t spillover_off;          // n_clients × spillover_per_client
    uint64_t request_queue_off;
    uint64_t worker_ring_off;
    uint64_t response_queue_off;
    uint64_t total_size;

    // Derived sizes
    uint64_t spillover_per_client;   // bytes per client spillover segment
    uint32_t total_blocks;           // number of UnifiedBlocks in the pool

    // Config snapshot (needed for offset arithmetic)
    uint32_t num_clients;
    uint32_t num_workers;
    uint32_t slots_per_client;       // blocks pre-allocated per client at init
    uint32_t num_buckets;
    uint32_t num_synchronizers;      // s: number of synchronizer threads

    static TwoRWLayout calculate(uint32_t n_clients,
                                  uint32_t n_workers,
                                  uint32_t slots_per_client,
                                  uint32_t n_buckets,
                                  uint64_t total_mem,
                                  uint32_t n_synchronizers = 1) {
        TwoRWLayout L{};
        L.num_clients       = n_clients;
        L.num_workers       = n_workers;
        L.slots_per_client  = slots_per_client;
        L.num_buckets       = n_buckets;
        L.total_size        = total_mem;
        L.num_synchronizers = n_synchronizers;

        auto align_up = [](uint64_t off, uint64_t align) -> uint64_t {
            return (off + align - 1) & ~(align - 1);
        };

        uint64_t off = 0;

        // TwoRWHeader (64B)
        L.header_off = off;
        off += sizeof(TwoRWHeader);
        off = align_up(off, 64);

        // HashTable: num_buckets × CXLBucket (8B each)
        L.hash_table_off = off;
        off += static_cast<uint64_t>(n_buckets) * sizeof(CXLBucket);
        off = align_up(off, 64);

        // RequestQueue[n_clients × n_synchronizers]
        // (placed before UnifiedBlockPool to keep control structures together)
        L.request_queue_off = off;
        off += static_cast<uint64_t>(n_clients) * n_synchronizers * sizeof(ReqQueue);
        off = align_up(off, 64);

        // WorkerRing[n_workers]
        L.worker_ring_off = off;
        off += static_cast<uint64_t>(n_workers) * sizeof(ReqQueue);
        off = align_up(off, 64);

        // ResponseQueue[n_clients][n_workers]
        L.response_queue_off = off;
        off += static_cast<uint64_t>(n_clients) * n_workers * sizeof(RespQueue);
        off = align_up(off, 64);

        // Remaining space: 80% UnifiedBlockPool, 20% SpilloverRegion
        uint64_t remaining = total_mem - off;

        // GlobalIDMap: placeholder — sized after we know total_blocks.
        // We first compute UnifiedBlockPool size from 80% of remaining.
        uint64_t pool_budget = (remaining * 80) / 100;
        pool_budget = (pool_budget / 2048) * 2048;  // round down to 2KB multiple

        // total_blocks including sentinel block 0
        uint32_t tblocks = static_cast<uint32_t>(pool_budget / 2048);

        // GlobalIDMap: ceil(tblocks / 8) bytes, aligned 64B
        uint64_t idmap_size = align_up((tblocks + 7) / 8, 64);

        L.globalidmap_off = off;
        off += idmap_size;
        off = align_up(off, 2048);  // align pool to 2048B

        L.unifiedblockpool_off = off;
        // Recompute total_blocks after alignment shift
        uint64_t pool_end = L.unifiedblockpool_off + pool_budget;
        // SpilloverRegion occupies the rest up to total_mem, split among clients
        uint64_t spillover_total = total_mem - pool_end;
        spillover_total = (spillover_total / n_clients / 64) * 64 * n_clients; // per-client 64B aligned
        L.spillover_per_client = spillover_total / n_clients;

        L.spillover_off = pool_end;

        L.total_blocks = tblocks;
        L.total_size   = total_mem;

        return L;
    }

    // ========================================================================
    // Typed accessors — return pointers into CXL base
    // ========================================================================

    TwoRWHeader* header(void* base) const {
        return reinterpret_cast<TwoRWHeader*>(
            static_cast<char*>(base) + header_off);
    }

    CXLBucket* bucket(void* base, uint32_t bucket_id) const {
        assert(bucket_id < num_buckets);
        return reinterpret_cast<CXLBucket*>(
            static_cast<char*>(base) + hash_table_off) + bucket_id;
    }

    CXLBucket* bucket_table(void* base) const {
        return reinterpret_cast<CXLBucket*>(
            static_cast<char*>(base) + hash_table_off);
    }

    // GlobalIDMap: raw byte pointer to the CXL bitmap
    uint8_t* globalidmap(void* base) const {
        return reinterpret_cast<uint8_t*>(
            static_cast<char*>(base) + globalidmap_off);
    }

    // UnifiedBlock offset (CXL byte offset from base)
    uint64_t unified_block_offset(uint32_t block_id) const {
        return unifiedblockpool_off + static_cast<uint64_t>(block_id) * 2048ULL;
    }

    // UnifiedBlock pointer
    UnifiedBlock* unified_block_ptr(void* base, uint32_t block_id) const {
        return reinterpret_cast<UnifiedBlock*>(
            static_cast<char*>(base) + unified_block_offset(block_id));
    }

    // SpilloverRegion: CXL byte offset for client j's segment
    uint64_t spillover_client_offset(uint32_t client_id) const {
        assert(client_id < num_clients);
        return spillover_off + static_cast<uint64_t>(client_id) * spillover_per_client;
    }

    // RequestQueue[client_id * num_synchronizers + sn_id]
    ReqQueue* request_sub_queue(void* base, uint32_t client_id,
                                 uint32_t sn_id = 0) const {
        assert(client_id < num_clients && sn_id < num_synchronizers);
        uint64_t idx = static_cast<uint64_t>(client_id) * num_synchronizers + sn_id;
        return reinterpret_cast<ReqQueue*>(
            static_cast<char*>(base) + request_queue_off) + idx;
    }

    // WorkerRing[worker_id]
    ReqQueue* worker_ring(void* base, uint32_t worker_id) const {
        assert(worker_id < num_workers);
        return reinterpret_cast<ReqQueue*>(
            static_cast<char*>(base) + worker_ring_off) + worker_id;
    }

    // ResponseQueue[client_id][worker_id]
    RespQueue* response_queue(void* base,
                               uint32_t client_id,
                               uint32_t worker_id) const {
        assert(client_id < num_clients && worker_id < num_workers);
        uint64_t idx = static_cast<uint64_t>(client_id) * num_workers + worker_id;
        return reinterpret_cast<RespQueue*>(
            static_cast<char*>(base) + response_queue_off) + idx;
    }

    // Print layout summary to stderr
    void print() const;
};

} // namespace TwoRW