#pragma once

// ============================================================================
// LocalSpscQueue — Single-Producer Single-Consumer queue for local DRAM
//
// Unlike CXLSpscQueue, this operates on coherent cache-coherent memory.
// No NT stores, no clflushopt/clwb, no sfence.
// Uses std::atomic release/acquire for correct ordering on all architectures.
//
// On x86 TSO: release = regular store (no hardware fence needed),
//   acquire = regular load. The compiler fence from std::atomic prevents
//   reordering around the index updates.
//
// Intended for Sync→Worker WorkerRing when --local-workerring is enabled,
// eliminating the 2×sfence CXL write overhead (C: ~938 → ~30 ticks).
// ============================================================================

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <immintrin.h>  // _mm_pause

namespace TwoRW {

// ============================================================================
// LocalSpscQueue — Shared queue structure (lives in local DRAM)
//
// Template parameters match CXLSpscQueue (size_t Capacity) for compatibility
// with QUEUE_CAP = constexpr size_t.
// ============================================================================

template<typename T, size_t Capacity = 4096>
struct alignas(64) LocalSpscQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");
    static_assert(std::is_trivially_copyable<T>::value,
                  "T must be trivially copyable");

    // Producer-owned index (own cache line)
    alignas(64) std::atomic<uint64_t> write_idx{0};
    char _pad_w[64 - sizeof(std::atomic<uint64_t>)];
    // Consumer-owned index (own cache line)
    alignas(64) std::atomic<uint64_t> read_idx{0};
    char _pad_r[64 - sizeof(std::atomic<uint64_t>)];
    // Slots (cache-line aligned, same layout as CXLSpscQueue)
    alignas(64) T slots[Capacity];

    void init() {
        write_idx.store(0, std::memory_order_relaxed);
        read_idx.store(0,  std::memory_order_relaxed);
        memset(slots, 0, sizeof(slots));
    }
};

// ============================================================================
// LocalSpscProducer — write side
// ============================================================================

template<typename T, size_t Capacity = 4096>
class LocalSpscProducer {
public:
    using Queue = LocalSpscQueue<T, Capacity>;

private:
    Queue*   q_           = nullptr;
    uint64_t cached_read_  = 0;   // stale view of consumer index
    uint64_t cached_write_ = 0;   // our own index (authoritative)

public:
    LocalSpscProducer() = default;

    void attach(Queue* q) {
        q_ = q;
        cached_read_  = q->read_idx.load(std::memory_order_acquire);
        cached_write_ = q->write_idx.load(std::memory_order_acquire);
    }

    // Enqueue one item. Returns false if full.
    bool enqueue(const T& item) {
        uint64_t w = cached_write_;
        if (w - cached_read_ >= Capacity) {
            // Refresh consumer's read_idx
            cached_read_ = q_->read_idx.load(std::memory_order_acquire);
            if (w - cached_read_ >= Capacity) return false;
        }

        // Write slot data (plain store — ordered before write_idx by release below)
        q_->slots[w & (Capacity - 1)] = item;

        // Release store: ensures slot write is visible before write_idx increment.
        // Consumer pairs this with an acquire load of write_idx, so it will see
        // the slot data after observing write_idx >= w+1.
        q_->write_idx.store(w + 1, std::memory_order_release);
        cached_write_ = w + 1;
        return true;
    }
};

// ============================================================================
// LocalSpscConsumer — read side
// ============================================================================

template<typename T, size_t Capacity = 4096>
class LocalSpscConsumer {
public:
    using Queue = LocalSpscQueue<T, Capacity>;

private:
    Queue*   q_           = nullptr;
    uint64_t cached_write_ = 0;   // stale view of producer index
    uint64_t cached_read_  = 0;   // our own index (authoritative)

public:
    LocalSpscConsumer() = default;

    void attach(Queue* q) {
        q_ = q;
        cached_write_ = q->write_idx.load(std::memory_order_acquire);
        cached_read_  = q->read_idx.load(std::memory_order_acquire);
    }

    // Dequeue one item. Returns false if empty.
    bool dequeue(T& item) {
        uint64_t r = cached_read_;
        if (r >= cached_write_) {
            // Acquire load: pairs with producer's release store to write_idx,
            // ensuring we see the slot data written before write_idx was updated.
            cached_write_ = q_->write_idx.load(std::memory_order_acquire);
            if (r >= cached_write_) return false;
        }

        // Read slot (safely ordered after the acquire load of write_idx above)
        item = q_->slots[r & (Capacity - 1)];

        // Release store: makes our read_idx visible to producer
        q_->read_idx.store(r + 1, std::memory_order_release);
        cached_read_ = r + 1;
        return true;
    }

    // Returns true if the queue is empty after a fresh acquire load of write_idx.
    // Equivalent to CXL's refresh_write_idx() + is_empty() for stop detection.
    bool is_empty_fresh() {
        cached_write_ = q_->write_idx.load(std::memory_order_acquire);
        return cached_read_ >= cached_write_;
    }

    bool is_empty() const { return cached_read_ >= cached_write_; }
};

} // namespace TwoRW
