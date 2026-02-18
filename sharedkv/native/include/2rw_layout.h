#pragma once

// ============================================================================
// SharedKV 2RW — CXL Memory Layout
//
// Computes byte offsets for every region in the CXL shared memory slab.
// All sizes are computed at runtime from TwoRWConfig so that benchmark
// parameters (num_clients, num_workers, slots_per_client, queue_depth)
// can be freely changed at startup.
//
// Layout (low → high):
//   [TwoRWHeader          64B]
//   [HashTable            num_buckets × 8B, aligned 64B]
//   [DataRegionMeta       num_workers × 64B]
//   [Pool                 num_clients × slots_per_client × 2048B]
//   [RequestQueue         num_clients × sizeof(Queue<KVRequest>)]
//   [WorkerRing           num_workers × sizeof(Queue<KVRequest>)]
//   [ResponseQueue        num_clients × num_workers × sizeof(Queue<KVResponse>)]
//   [DataRegion           m equal segments, rest of CXL]
// ============================================================================

#include "2rw_structs.h"
#include "cxl_spsc_queue.h"
#include "cxl_ptr.h"
#include <cstdint>
#include <cstddef>
#include <cassert>

namespace TwoRW {

// Queue depths are compile-time template parameters for CXLSpscQueue.
// We use a fixed cap; actual depth ≤ cap is enforced at init time.
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
    uint64_t data_region_meta_off;
    uint64_t pool_off;
    uint64_t request_queue_off;
    uint64_t worker_ring_off;
    uint64_t response_queue_off;
    uint64_t data_region_off;       // Start of actual node storage
    uint64_t data_region_per_worker;// Bytes per Worker DataRegion segment
    uint64_t total_size;

    // Config snapshot (needed for offset arithmetic)
    uint32_t num_clients;
    uint32_t num_workers;
    uint32_t slots_per_client;
    uint32_t num_buckets;

    static TwoRWLayout calculate(uint32_t n_clients,
                                  uint32_t n_workers,
                                  uint32_t slots_per_client,
                                  uint32_t n_buckets,
                                  uint64_t total_mem) {
        TwoRWLayout L{};
        L.num_clients      = n_clients;
        L.num_workers      = n_workers;
        L.slots_per_client = slots_per_client;
        L.num_buckets      = n_buckets;
        L.total_size       = total_mem;

        auto align_up = [](uint64_t off, uint64_t align) -> uint64_t {
            return (off + align - 1) & ~(align - 1);
        };

        uint64_t off = 0;

        // TwoRWHeader
        L.header_off = off;
        off += sizeof(TwoRWHeader);
        off = align_up(off, 64);

        // HashTable: num_buckets × CXLBucket (8B each)
        L.hash_table_off = off;
        off += static_cast<uint64_t>(n_buckets) * sizeof(CXLBucket);
        off = align_up(off, 64);

        // DataRegionMeta: num_workers × DataRegionHeader (64B each)
        L.data_region_meta_off = off;
        off += static_cast<uint64_t>(n_workers) * sizeof(DataRegionHeader);
        off = align_up(off, 64);

        // Pool: n_clients × slots_per_client × KVPoolSlot (2048B each)
        L.pool_off = off;
        off += static_cast<uint64_t>(n_clients) * slots_per_client * sizeof(KVPoolSlot);
        off = align_up(off, 64);

        // RequestQueue[n_clients]
        L.request_queue_off = off;
        off += static_cast<uint64_t>(n_clients) * sizeof(ReqQueue);
        off = align_up(off, 64);

        // WorkerRing[n_workers]
        L.worker_ring_off = off;
        off += static_cast<uint64_t>(n_workers) * sizeof(ReqQueue);
        off = align_up(off, 64);

        // ResponseQueue[n_clients][n_workers]
        L.response_queue_off = off;
        off += static_cast<uint64_t>(n_clients) * n_workers * sizeof(RespQueue);
        off = align_up(off, 64);

        // DataRegion: rest of CXL, split equally among workers
        L.data_region_off = off;
        uint64_t remaining = total_mem - off;
        L.data_region_per_worker = remaining / n_workers;

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

    DataRegionHeader* data_region_meta(void* base, uint32_t worker_id) const {
        assert(worker_id < num_workers);
        return reinterpret_cast<DataRegionHeader*>(
            static_cast<char*>(base) + data_region_meta_off) + worker_id;
    }

    // Pool slot offset (for CXLPtr<KVPoolSlot>)
    // slot_id is the absolute index across all clients.
    uint64_t pool_slot_offset(uint32_t slot_id) const {
        return pool_off + static_cast<uint64_t>(slot_id) * sizeof(KVPoolSlot);
    }

    // Convert client-local slot index to global slot_id
    uint32_t global_slot_id(uint32_t client_id, uint32_t local_idx) const {
        return client_id * slots_per_client + local_idx;
    }

    // RequestQueue[client_id]
    ReqQueue* request_queue(void* base, uint32_t client_id) const {
        assert(client_id < num_clients);
        return reinterpret_cast<ReqQueue*>(
            static_cast<char*>(base) + request_queue_off) + client_id;
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

    // DataRegion start offset for worker_id (CXL offset)
    uint64_t data_region_offset(uint32_t worker_id) const {
        assert(worker_id < num_workers);
        return data_region_off + static_cast<uint64_t>(worker_id) * data_region_per_worker;
    }

    // Print layout summary to stderr
    void print() const;
};

} // namespace TwoRW