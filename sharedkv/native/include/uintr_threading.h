#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/syscall.h>
#include <pthread.h>

// ============================================================================
// UINTR Syscall Numbers (Linux kernel 5.14+)
// ============================================================================
#define __NR_uintr_register_handler   471
#define __NR_uintr_unregister_handler 472
#define __NR_uintr_create_fd          473
#define __NR_uintr_register_sender    474
#define __NR_uintr_unregister_sender  475
#define __NR_uintr_wait               476

// ============================================================================
// UINTR Syscall Wrappers (inline for performance)
// ============================================================================

static inline long uintr_register_handler(void* handler, unsigned int flags) {
    return syscall(__NR_uintr_register_handler, handler, flags);
}

static inline long uintr_unregister_handler(unsigned int flags) {
    return syscall(__NR_uintr_unregister_handler, flags);
}

static inline long uintr_create_fd(unsigned long long vector, unsigned int flags) {
    return syscall(__NR_uintr_create_fd, vector, flags);
}

static inline long uintr_register_sender(int fd, unsigned int flags) {
    return syscall(__NR_uintr_register_sender, fd, flags);
}

static inline long uintr_unregister_sender(int ipi_idx, unsigned int flags) {
    return syscall(__NR_uintr_unregister_sender, ipi_idx, flags);
}

static inline long uintr_wait(unsigned int flags) {
    return syscall(__NR_uintr_wait, flags);
}

// ============================================================================
// UINTR Intrinsics
// ============================================================================
// Note: GCC provides these in x86gprintrin.h, but we include for compatibility

#ifndef __has_include
  #define __has_include(x) 0
#endif

#if !__has_include(<x86gprintrin.h>)
// Enable user interrupts
static inline void _stui(void) {
    asm volatile("stui" ::: "memory");
}

// Disable user interrupts
static inline void _clui(void) {
    asm volatile("clui" ::: "memory");
}

// Send user IPI
static inline void _senduipi(unsigned long long idx) {
    asm volatile("senduipi %0" :: "r"(idx) : "memory");
}

// UINTR frame structure (passed to interrupt handler)
struct __uintr_frame {
    unsigned long long rip;
    unsigned long long rflags;
    unsigned long long rsp;
};
#endif

// ============================================================================
// MPSC (Multiple Producer Single Consumer) Lock-Free Queue
// Uses sequence numbers per slot to ensure correctness with multiple producers
// Cache-line aligned for performance
template<typename T>
struct alignas(64) LockFreeQueue {
    // Each slot has a sequence number to track write completion
    struct Slot {
        std::atomic<uint64_t> sequence;
        T data;
    };

    Slot* slots;
    size_t size;
    size_t mask;  // size - 1, for fast modulo (requires power-of-2 size)
    alignas(64) std::atomic<uint64_t> head{0};  // Producer reservation counter
    alignas(64) std::atomic<uint64_t> tail{0};  // Consumer read position

    LockFreeQueue(size_t queue_size) : size(queue_size), mask(queue_size - 1) {
        // Ensure size is power of 2 for fast modulo
        if ((queue_size & (queue_size - 1)) != 0) {
            // Round up to next power of 2
            queue_size--;
            queue_size |= queue_size >> 1;
            queue_size |= queue_size >> 2;
            queue_size |= queue_size >> 4;
            queue_size |= queue_size >> 8;
            queue_size |= queue_size >> 16;
            queue_size |= queue_size >> 32;
            queue_size++;
            size = queue_size;
            mask = queue_size - 1;
        }

        // Allocate slots with cache-line alignment
        slots = static_cast<Slot*>(aligned_alloc(64, sizeof(Slot) * size));
        if (!slots) {
            throw std::bad_alloc();
        }

        // Initialize sequence numbers
        // Slot i starts with sequence = i (ready for write at position i)
        for (size_t i = 0; i < size; i++) {
            slots[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~LockFreeQueue() {
        free(slots);
    }

    // Producer: enqueue item (non-blocking)
    // MPSC-safe: Multiple producers can safely enqueue concurrently
    bool enqueue(const T& item) {
        uint64_t pos = head.load(std::memory_order_relaxed);

        while (true) {
            Slot& slot = slots[pos & mask];
            uint64_t seq = slot.sequence.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(pos);

            if (diff == 0) {
                // Slot is ready for writing at this position
                if (head.compare_exchange_weak(pos, pos + 1,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
                    // Successfully reserved this slot
                    slot.data = item;
                    // Mark slot as written by setting sequence to pos + 1
                    slot.sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
                // CAS failed, retry with updated pos
            } else if (diff < 0) {
                // Queue is full (consumer hasn't caught up)
                return false;
            } else {
                // Another producer reserved this slot, move to next
                pos = head.load(std::memory_order_relaxed);
            }
        }
    }

    // Consumer: dequeue item (non-blocking)
    // Single consumer only - not thread-safe for multiple consumers
    bool dequeue(T& item) {
        uint64_t pos = tail.load(std::memory_order_relaxed);
        Slot& slot = slots[pos & mask];
        uint64_t seq = slot.sequence.load(std::memory_order_acquire);
        int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(pos + 1);

        if (diff < 0) {
            // Slot not yet written
            return false;
        }

        // Slot is ready to read
        item = slot.data;
        // Mark slot as available for future writes
        // Next valid write position for this slot is pos + size
        slot.sequence.store(pos + size, std::memory_order_release);
        tail.store(pos + 1, std::memory_order_relaxed);
        return true;
    }

    // Check if queue is empty (for poller edge detection)
    bool is_empty() const {
        uint64_t pos = tail.load(std::memory_order_relaxed);
        const Slot& slot = slots[pos & mask];
        uint64_t seq = slot.sequence.load(std::memory_order_acquire);
        return static_cast<int64_t>(seq) - static_cast<int64_t>(pos + 1) < 0;
    }

    // Get current queue size (approximate)
    size_t get_size() const {
        uint64_t h = head.load(std::memory_order_acquire);
        uint64_t t = tail.load(std::memory_order_acquire);
        return (h >= t) ? (h - t) : 0;
    }
};

// ============================================================================
// CPU Affinity Helpers
// ============================================================================

// Pin a std::thread to a specific CPU
void pin_thread_to_cpu(std::thread& th, int cpu_id);

// Pin the current thread to a specific CPU
void pin_current_thread_to_cpu(int cpu_id);
