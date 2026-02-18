#pragma once

// ============================================================================
// CXL Non-Coherent SPSC Queue
//
// A single-producer single-consumer circular queue designed for CXL Type 3
// shared memory WITHOUT global coherence domain (no Back-Invalidate).
//
// Key design:
//   - Producer uses non-temporal stores (_mm_stream_*) to bypass cache
//   - Consumer uses clflushopt + lfence to invalidate stale cache lines
//   - Each side caches its own index locally; only the other side's index
//     requires explicit cache invalidation
//   - All slots are cache-line aligned to prevent false sharing
//
// Requirements:
//   - x86-64 with CLFLUSHOPT and CLWB support (Intel Skylake+ / AMD Zen+)
//   - T must be trivially copyable
//   - Capacity must be a power of 2
// ============================================================================

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <type_traits>
#include <immintrin.h>  // _mm_sfence, _mm_lfence, _mm_stream_si64, etc.

// ============================================================================
// Cache Line Constants
// ============================================================================

#define CXL_CACHE_LINE_SIZE 64

// ============================================================================
// Cache Management Primitives (inline asm for clwb/clflushopt)
// ============================================================================

// Write back a cache line to memory, keep clean copy in cache.
// Use on producer side after writing data that needs to be visible to consumer.
static inline void cxl_clwb(const volatile void* addr) {
    asm volatile("clwb (%0)" :: "r"(addr) : "memory");
}

// Write back + invalidate a cache line.
// Use on consumer side to discard stale cached data before reading.
static inline void cxl_clflushopt(const volatile void* addr) {
    asm volatile("clflushopt (%0)" :: "r"(addr) : "memory");
}

// Flush a memory range to CXL memory (write back each cache line).
// Used after normal stores to push data to memory.
static inline void cxl_flush_range(const volatile void* addr, size_t len) {
    const volatile char* p = static_cast<const volatile char*>(addr);
    const volatile char* end = p + len;
    for (; p < end; p += CXL_CACHE_LINE_SIZE) {
        cxl_clwb(p);
    }
}

// Invalidate a memory range in local cache (flush + invalidate each line).
// Used before reads to ensure we fetch fresh data from memory.
static inline void cxl_invalidate_range(const volatile void* addr, size_t len) {
    const volatile char* p = static_cast<const volatile char*>(addr);
    const volatile char* end = p + len;
    for (; p < end; p += CXL_CACHE_LINE_SIZE) {
        cxl_clflushopt(p);
    }
}

// Non-temporal store: copy src to dst bypassing cache, using 64-bit streaming
// stores. dst must be 8-byte aligned. Writes go to WC buffer, flushed by sfence.
static inline void cxl_nt_memcpy(void* dst, const void* src, size_t len) {
    const uint64_t* s = static_cast<const uint64_t*>(src);
    uint64_t* d = static_cast<uint64_t*>(dst);
    size_t qwords = len / 8;
    size_t remainder = len % 8;

    for (size_t i = 0; i < qwords; i++) {
        _mm_stream_si64(reinterpret_cast<long long*>(&d[i]),
                        static_cast<long long>(s[i]));
    }

    // Handle remainder bytes (< 8) with normal store + clwb
    if (remainder > 0) {
        char* dst_tail = reinterpret_cast<char*>(&d[qwords]);
        const char* src_tail = reinterpret_cast<const char*>(&s[qwords]);
        memcpy(dst_tail, src_tail, remainder);
        cxl_clwb(dst_tail);
    }
}

// ============================================================================
// CXLSpscQueue - The shared memory structure
//
// This struct lives entirely in CXL shared memory. Both hosts mmap the same
// region and access this struct at the same offset.
//
// Memory layout:
//   [cacheline 0]  write_idx (written by producer only)
//   [cacheline 1]  read_idx  (written by consumer only)
//   [cacheline 2]  capacity, mask (immutable after init)
//   [cacheline 3+] slots[0..Capacity-1]
// ============================================================================

template<typename T, size_t Capacity = 4096>
struct alignas(CXL_CACHE_LINE_SIZE) CXLSpscQueue {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of 2");
    static_assert(std::is_trivially_copyable<T>::value,
                  "T must be trivially copyable for NT stores and memcpy");

    // Number of cache lines per slot (computed at compile time)
    static constexpr size_t SLOT_SIZE = sizeof(T);
    static constexpr size_t SLOT_CACHE_LINES =
        (SLOT_SIZE + CXL_CACHE_LINE_SIZE - 1) / CXL_CACHE_LINE_SIZE;

    // --- Cache line 0: Producer-owned index ---
    alignas(CXL_CACHE_LINE_SIZE) volatile uint64_t write_idx;
    char _pad_w[CXL_CACHE_LINE_SIZE - sizeof(uint64_t)];

    // --- Cache line 1: Consumer-owned index ---
    alignas(CXL_CACHE_LINE_SIZE) volatile uint64_t read_idx;
    char _pad_r[CXL_CACHE_LINE_SIZE - sizeof(uint64_t)];

    // --- Cache line 2: Immutable metadata ---
    alignas(CXL_CACHE_LINE_SIZE) uint64_t capacity_val;
    uint64_t mask;
    char _pad_meta[CXL_CACHE_LINE_SIZE - 2 * sizeof(uint64_t)];

    // --- Data slots (each T should ideally be cache-line aligned) ---
    alignas(CXL_CACHE_LINE_SIZE) T slots[Capacity];

    // Check if queue appears empty (reads volatile indices directly — may be stale).
    // Safe for polling without a consumer handle (e.g., Poller thread).
    bool is_empty() const { return read_idx >= write_idx; }

    // Initialize the queue. Must be called ONCE from one host before use.
    void init() {
        write_idx = 0;
        read_idx = 0;
        capacity_val = Capacity;
        mask = Capacity - 1;
        memset(slots, 0, sizeof(slots));

        // Flush everything to CXL memory so the other host sees it
        cxl_flush_range(this, sizeof(*this));
        _mm_sfence();
    }

    // Total byte size of this queue (for memory layout calculations)
    static constexpr size_t total_size() {
        return sizeof(CXLSpscQueue<T, Capacity>);
    }
};

// ============================================================================
// CXLSpscProducer - Producer-side handle (lives in Host A's local memory)
//
// Caches write_idx locally to avoid reading from shared memory.
// Only needs to read read_idx (via clflushopt) to check for space.
// ============================================================================

template<typename T, size_t Capacity = 4096>
class CXLSpscProducer {
public:
    using Queue = CXLSpscQueue<T, Capacity>;

private:
    Queue* q_;
    uint64_t cached_write_;  // Local cache of our own write index
    uint64_t cached_read_;   // Local cache of consumer's read index (may be stale)

public:
    CXLSpscProducer() : q_(nullptr), cached_write_(0), cached_read_(0) {}
    explicit CXLSpscProducer(Queue* queue)
        : q_(queue), cached_write_(0), cached_read_(0) {}

    // Attach to a queue that may already have state (e.g., after recovery)
    void attach(Queue* queue) {
        q_ = queue;
        // Read current indices from shared memory
        cxl_clflushopt(&q_->write_idx);
        cxl_clflushopt(&q_->read_idx);
        _mm_lfence();
        cached_write_ = q_->write_idx;
        cached_read_ = q_->read_idx;
    }

    // Enqueue a single item. Returns false if queue is full.
    bool enqueue(const T& item) {
        uint64_t w = cached_write_;

        // Check if we have space using our (possibly stale) cached_read_
        if (w - cached_read_ >= Capacity) {
            // Our cached read_idx might be stale — refresh it
            cxl_clflushopt(&q_->read_idx);
            _mm_lfence();
            cached_read_ = q_->read_idx;

            // Still full?
            if (w - cached_read_ >= Capacity) {
                return false;
            }
        }

        // Write data to slot using non-temporal stores (bypass cache → WC buffer)
        T* slot = &q_->slots[w & (Capacity - 1)];
        cxl_nt_memcpy(slot, &item, sizeof(T));

        // sfence: flush WC buffer → memory, ensuring data is in memory
        _mm_sfence();

        // Update write_idx with NT store
        _mm_stream_si64(reinterpret_cast<long long*>(
            const_cast<uint64_t*>(&q_->write_idx)),
            static_cast<long long>(w + 1));

        // sfence: flush write_idx → memory
        _mm_sfence();

        cached_write_ = w + 1;
        return true;
    }

    // Check available space (may return conservative estimate due to stale read_idx)
    uint64_t available_space() const {
        return Capacity - (cached_write_ - cached_read_);
    }

    // Force refresh of consumer's read_idx
    void refresh_read_idx() {
        cxl_clflushopt(&q_->read_idx);
        _mm_lfence();
        cached_read_ = q_->read_idx;
    }

    uint64_t write_pos() const { return cached_write_; }
};

// ============================================================================
// CXLSpscConsumer - Consumer-side handle (lives in Host B's local memory)
//
// Caches read_idx locally. Reads write_idx (via clflushopt) to check
// for available items. Invalidates slot cache lines before reading data.
// ============================================================================

template<typename T, size_t Capacity = 4096>
class CXLSpscConsumer {
public:
    using Queue = CXLSpscQueue<T, Capacity>;

private:
    Queue* q_;
    uint64_t cached_read_;   // Local cache of our own read index
    uint64_t cached_write_;  // Local cache of producer's write index (may be stale)

public:
    CXLSpscConsumer() : q_(nullptr), cached_read_(0), cached_write_(0) {}
    explicit CXLSpscConsumer(Queue* queue)
        : q_(queue), cached_read_(0), cached_write_(0) {}

    // Attach to a queue that may already have state
    void attach(Queue* queue) {
        q_ = queue;
        cxl_clflushopt(&q_->read_idx);
        cxl_clflushopt(&q_->write_idx);
        _mm_lfence();
        cached_read_ = q_->read_idx;
        cached_write_ = q_->write_idx;
    }

    // Dequeue a single item. Returns false if queue is empty.
    bool dequeue(T& item) {
        uint64_t r = cached_read_;

        // Check if there's data using our (possibly stale) cached_write_
        if (r >= cached_write_) {
            // Refresh write_idx from shared memory
            cxl_clflushopt(&q_->write_idx);
            _mm_lfence();
            cached_write_ = q_->write_idx;

            // Still empty?
            if (r >= cached_write_) {
                return false;
            }
        }

        // Invalidate the slot's cache lines to get fresh data from memory
        const T* slot = &q_->slots[r & (Capacity - 1)];
        cxl_invalidate_range(slot, sizeof(T));
        _mm_lfence();

        // Read the data (will fetch from memory since cache was invalidated)
        item = *slot;

        // Update read_idx
        const_cast<volatile uint64_t&>(q_->read_idx) = r + 1;

        // Flush read_idx to memory so producer can see it
        cxl_clwb(&q_->read_idx);
        _mm_sfence();

        cached_read_ = r + 1;
        return true;
    }

    // Dequeue up to max_count items in a batch. Returns number dequeued.
    // More efficient than single dequeue: amortizes write_idx flush cost.
    uint64_t dequeue_batch(T* items, uint64_t max_count) {
        uint64_t r = cached_read_;

        // Refresh write_idx to see how many items are available
        cxl_clflushopt(&q_->write_idx);
        _mm_lfence();
        cached_write_ = q_->write_idx;

        uint64_t available = cached_write_ - r;
        if (available == 0) {
            return 0;
        }

        uint64_t count = (available < max_count) ? available : max_count;

        for (uint64_t i = 0; i < count; i++) {
            uint64_t idx = (r + i) & (Capacity - 1);
            const T* slot = &q_->slots[idx];

            // Invalidate slot cache lines
            cxl_invalidate_range(slot, sizeof(T));
            _mm_lfence();

            // Read fresh data
            items[i] = *slot;
        }

        // Update read_idx once for the entire batch
        const_cast<volatile uint64_t&>(q_->read_idx) = r + count;
        cxl_clwb(&q_->read_idx);
        _mm_sfence();

        cached_read_ = r + count;
        return count;
    }

    // Check if queue is empty (may return false positive due to stale write_idx)
    bool is_empty() const {
        return cached_read_ >= cached_write_;
    }

    // Force refresh of producer's write_idx
    void refresh_write_idx() {
        cxl_clflushopt(&q_->write_idx);
        _mm_lfence();
        cached_write_ = q_->write_idx;
    }

    // Available items (may be stale — call refresh_write_idx() for accurate count)
    uint64_t available() const {
        return cached_write_ - cached_read_;
    }

    uint64_t read_pos() const { return cached_read_; }
};
