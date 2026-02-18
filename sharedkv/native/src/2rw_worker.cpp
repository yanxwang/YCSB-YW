// ============================================================================
// SharedKV 2RW — Worker Thread (CPU 2+n+i)
//
// Responsibilities:
//   1. Dequeue KVRequest from WorkerRing[worker_id]
//   2. Read key (and value for PUT/UPDATE) from Pool[slot_id] via CXLPtr
//   3. Execute KV operation on own DataRegion[i] (no locks)
//   4. Write KVResponse to ResponseQueue[client_id][worker_id]
//
// KV Operations:
//   PUT/UPDATE: allocate CXLNode (free list → bump), copy key+value, link to bucket
//   GET:        traverse bucket chain, set val_addr = offset of value bytes
//   DELETE:     unlink CXLNode, prepend to free list
//
// Memory fence protocol (spec §4):
//   - sfence before linking node to bucket (chain integrity)
//   - sfence before updating write_idx in ResponseQueue (result visibility)
// ============================================================================

#include "2rw_context.h"
#include "cxl_ptr.h"
#include <cstdio>
#include <cstring>
#include <immintrin.h>
#include <emmintrin.h>

namespace TwoRW {

// ============================================================================
// Hash Function (FNV-1a)
// ============================================================================

static inline uint64_t fnv1a(const char* data, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (uint32_t i = 0; i < len; i++) {
        h ^= static_cast<uint8_t>(data[i]);
        h *= 1099511628211ULL;
    }
    return h;
}

// ============================================================================
// DataRegion Allocator
// Returns CXL offset of allocated block, or 0 on OOM.
// Allocation is local to this worker — no contention.
// ============================================================================

static uint64_t region_alloc(DataRegionHeader* meta, uint64_t size,
                               uint64_t alignment = 64) {
    uint64_t aligned_off = (meta->alloc_offset + alignment - 1) & ~(alignment - 1);
    if (aligned_off + size > meta->total_size) {
        return 0;  // Out of memory
    }
    uint64_t result_cxl_off = meta->base_offset + aligned_off;
    meta->alloc_offset = aligned_off + size;
    meta->nodes_allocated++;
    return result_cxl_off;
}

// ============================================================================
// Free List: pop a reusable CXLNode large enough for (key_len + val_len)
// Returns offset, or 0 if no suitable node found.
// ============================================================================

static uint64_t freelist_pop(DataRegionHeader* meta,
                              uint32_t need_key, uint32_t need_val) {
    uint64_t prev_off = 0;
    uint64_t cur_off  = meta->free_list_offset;

    while (cur_off != 0) {
        CXLPtr<CXLNode> node(cur_off);
        uint32_t cap = node->key_len + node->val_len;
        uint32_t need = need_key + need_val;

        if (cap >= need) {
            // Unlink from free list
            if (prev_off == 0) {
                meta->free_list_offset = node->next_offset;
            } else {
                CXLPtr<CXLNode>(prev_off)->next_offset = node->next_offset;
            }
            meta->nodes_recycled--;
            return cur_off;
        }
        prev_off = cur_off;
        cur_off  = node.read_field<uint64_t>(offsetof(CXLNode, next_offset));
    }
    return 0;
}

// ============================================================================
// Free List: push a node (prepend)
// ============================================================================

static void freelist_push(DataRegionHeader* meta, uint64_t node_off) {
    CXLPtr<CXLNode> node(node_off);
    node->next_offset      = meta->free_list_offset;
    meta->free_list_offset = node_off;
    meta->nodes_recycled++;
}

// ============================================================================
// KV PUT / UPDATE
// ============================================================================

static KVResponse kv_put(const KVRequest& req, KVPoolSlot* slot,
                           DataRegionHeader* meta,
                           CXLBucket* buckets, uint32_t num_buckets) {
    KVResponse resp{};
    resp.slot_id = req.slot_id;
    resp.gsn     = req.gsn;

    const char*  key     = slot->key;
    uint32_t     key_len = slot->key_len;
    const char*  val     = slot->value;
    uint32_t     val_len = slot->val_len;
    uint64_t     hash    = fnv1a(key, key_len);
    uint32_t     bkt_id  = static_cast<uint32_t>(hash % num_buckets);
    CXLBucket*   bucket  = &buckets[bkt_id];

    // --- Search for existing key (update in-place if value fits) ---
    uint64_t cur_off = bucket->head_offset;
    while (cur_off != 0) {
        CXLPtr<CXLNode> node(cur_off);
        if (node->key_hash == hash && node->key_len == key_len) {
            char* node_data = reinterpret_cast<char*>(node.get()) + sizeof(CXLNode);
            if (memcmp(node_data, key, key_len) == 0) {
                // Found — update value if it fits
                if (val_len <= node->val_len) {
                    memcpy(node_data + key_len, val, val_len);
                    node->val_len = val_len;
                    _mm_sfence();
                    resp.status = static_cast<uint16_t>(Status::SUCCESS);
                    return resp;
                }
                // Value doesn't fit — fall through to allocate new node
                break;
            }
        }
        cur_off = node.read_field<uint64_t>(offsetof(CXLNode, next_offset));
    }

    // --- Allocate new CXLNode ---
    uint64_t node_size = sizeof(CXLNode) + key_len + val_len;
    // Round up to cache line to keep alignment
    node_size = (node_size + 63) & ~uint64_t(63);

    uint64_t node_off = freelist_pop(meta, key_len, val_len);
    if (node_off == 0) {
        node_off = region_alloc(meta, node_size);
        if (node_off == 0) {
            resp.status = static_cast<uint16_t>(Status::ERROR);
            return resp;
        }
    }

    // --- Initialize node ---
    CXLPtr<CXLNode> new_node(node_off);
    new_node->key_hash   = hash;
    new_node->key_len    = key_len;
    new_node->val_len    = val_len;
    new_node->next_offset = bucket->head_offset;  // Prepend to chain

    char* node_data = reinterpret_cast<char*>(new_node.get()) + sizeof(CXLNode);
    memcpy(node_data,           key, key_len);
    memcpy(node_data + key_len, val, val_len);

    // spec §4.3: sfence before linking to bucket (chain integrity)
    _mm_sfence();
    bucket->head_offset = node_off;

    resp.status = static_cast<uint16_t>(Status::SUCCESS);
    return resp;
}

// ============================================================================
// KV GET — zero-copy: return CXL offset of value bytes
// ============================================================================

static KVResponse kv_get(const KVRequest& req, KVPoolSlot* slot,
                           CXLBucket* buckets, uint32_t num_buckets) {
    KVResponse resp{};
    resp.slot_id = req.slot_id;
    resp.gsn     = req.gsn;

    const char* key     = slot->key;
    uint32_t    key_len = slot->key_len;
    uint64_t    hash    = fnv1a(key, key_len);
    uint32_t    bkt_id  = static_cast<uint32_t>(hash % num_buckets);

    // spec §4 (GET concurrency): lfence before traversal
    _mm_lfence();

    uint64_t cur_off = buckets[bkt_id].head_offset;
    while (cur_off != 0) {
        CXLPtr<CXLNode> node(cur_off);
        if (node->key_hash == hash && node->key_len == key_len) {
            const char* node_data = reinterpret_cast<const char*>(
                node.get()) + sizeof(CXLNode);
            if (memcmp(node_data, key, key_len) == 0) {
                // Found — return CXL offset of value bytes (zero-copy)
                resp.status   = static_cast<uint16_t>(Status::SUCCESS);
                resp.val_addr = cur_off + sizeof(CXLNode) + key_len;
                resp.val_len  = node->val_len;
                return resp;
            }
        }
        cur_off = node.read_field<uint64_t>(offsetof(CXLNode, next_offset));
    }

    resp.status = static_cast<uint16_t>(Status::NOT_FOUND);
    return resp;
}

// ============================================================================
// KV DELETE
// ============================================================================

static KVResponse kv_del(const KVRequest& req, KVPoolSlot* slot,
                           DataRegionHeader* meta,
                           CXLBucket* buckets, uint32_t num_buckets) {
    KVResponse resp{};
    resp.slot_id = req.slot_id;
    resp.gsn     = req.gsn;

    const char* key     = slot->key;
    uint32_t    key_len = slot->key_len;
    uint64_t    hash    = fnv1a(key, key_len);
    uint32_t    bkt_id  = static_cast<uint32_t>(hash % num_buckets);
    CXLBucket*  bucket  = &buckets[bkt_id];

    uint64_t prev_off = 0;
    uint64_t cur_off  = bucket->head_offset;

    while (cur_off != 0) {
        CXLPtr<CXLNode> node(cur_off);
        if (node->key_hash == hash && node->key_len == key_len) {
            const char* node_data = reinterpret_cast<const char*>(
                node.get()) + sizeof(CXLNode);
            if (memcmp(node_data, key, key_len) == 0) {
                // Unlink
                uint64_t next = node->next_offset;
                if (prev_off == 0) {
                    bucket->head_offset = next;
                } else {
                    CXLPtr<CXLNode>(prev_off)->next_offset = next;
                }
                _mm_sfence();
                // Reclaim to free list
                freelist_push(meta, cur_off);
                resp.status = static_cast<uint16_t>(Status::SUCCESS);
                return resp;
            }
        }
        prev_off = cur_off;
        cur_off  = node.read_field<uint64_t>(offsetof(CXLNode, next_offset));
    }

    resp.status = static_cast<uint16_t>(Status::NOT_FOUND);
    return resp;
}

// ============================================================================
// Worker Main Loop
// ============================================================================

void two_rw_worker_run(WorkerThreadState* s) {
    const uint32_t wid   = s->worker_id;
    const uint32_t m     = s->num_clients;  // actually num_workers, corrected below
    // Note: resp_producers is indexed [client_id * num_workers + worker_id]
    // We need num_workers from the layout.
    const uint32_t n_clients = s->num_clients;
    const uint32_t num_workers = static_cast<uint32_t>(
        s->layout->num_workers);  // added field to layout
    const uint32_t num_buckets = s->layout->num_buckets;

    CXLBucket* buckets = s->layout->bucket_table(s->cxl_base);
    DataRegionHeader* meta = s->region_meta;

    uint64_t ops_done    = 0;
    uint64_t empty_polls = 0;

    fprintf(stderr, "[Worker %u] Started. DataRegion: offset=0x%lx size=%zu MB\n",
            wid, meta->base_offset, (size_t)(meta->total_size >> 20));

    while (true) {
        KVRequest req;
        if (s->ring_consumer->dequeue(req)) {
            req.t2 = __rdtsc();

            // Resolve Pool slot via CXLPtr
            uint64_t slot_off = s->layout->pool_slot_offset(req.slot_id);
            KVPoolSlot* slot = CXLPtr<KVPoolSlot>(slot_off).get();

            // Execute operation
            KVResponse resp;
            auto op = static_cast<OpType>(slot->op_type);
            switch (op) {
                case OpType::PUT:
                case OpType::UPDATE:
                    resp = kv_put(req, slot, meta, buckets, num_buckets);
                    break;
                case OpType::GET:
                    resp = kv_get(req, slot, buckets, num_buckets);
                    break;
                case OpType::DEL:
                    resp = kv_del(req, slot, meta, buckets, num_buckets);
                    break;
                default:
                    resp.status   = static_cast<uint16_t>(Status::ERROR);
                    resp.slot_id  = req.slot_id;
                    resp.gsn      = req.gsn;
                    break;
            }

            resp.t3 = __rdtsc();
            resp.t1 = req.t1;
            resp.t2 = req.t2;

            // Route response to ResponseQueue[client_id][worker_id]
            uint32_t client_id = req.client_id;
            uint32_t resp_idx  = client_id * num_workers + wid;
            auto& resp_prod    = s->resp_producers[resp_idx];

            // Busy-spin on full response queue
            while (!resp_prod.enqueue(resp)) {
                _mm_pause();
                if (s->stop_flag->load(std::memory_order_relaxed)) break;
            }

            ops_done++;
        } else {
            empty_polls++;
            // Stop when requested AND ring is empty
            if (s->stop_flag->load(std::memory_order_acquire)) {
                s->ring_consumer->refresh_write_idx();
                if (s->ring_consumer->is_empty()) break;
            }
            _mm_pause();
        }
    }

    fprintf(stderr,
        "[Worker %u] Done. ops=%lu  empty_polls=%lu  nodes_alloc=%lu  recycled=%lu\n",
        wid, ops_done, empty_polls,
        meta->nodes_allocated, meta->nodes_recycled);
}

} // namespace TwoRW