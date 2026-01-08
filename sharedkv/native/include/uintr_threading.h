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
// Lock-Free SPSC Queue (Single Producer Single Consumer)
// ============================================================================
// Cache-line aligned for performance, uses monotonic indices
template<typename T>
struct alignas(64) LockFreeQueue {
    T* entries;
    size_t size;
    alignas(64) std::atomic<uint64_t> head{0};  // Producer writes here
    alignas(64) std::atomic<uint64_t> tail{0};  // Consumer reads from here

    LockFreeQueue(size_t queue_size) : size(queue_size) {
        // Use aligned allocation for cache line alignment
        entries = static_cast<T*>(aligned_alloc(64, sizeof(T) * size));
        if (!entries) {
            throw std::bad_alloc();
        }
    }

    ~LockFreeQueue() {
        free(entries);
    }

    // Producer: enqueue item (non-blocking)
    bool enqueue(const T& item) {
        uint64_t h = head.load(std::memory_order_relaxed);
        uint64_t t = tail.load(std::memory_order_acquire);

        // Check if full (monotonic indices)
        if (h - t >= size) {
            return false;  // Queue full
        }

        entries[h % size] = item;
        head.store(h + 1, std::memory_order_release);
        return true;
    }

    // Consumer: dequeue item (non-blocking)
    bool dequeue(T& item) {
        uint64_t t = tail.load(std::memory_order_relaxed);
        uint64_t h = head.load(std::memory_order_acquire);

        // Check if empty
        if (t >= h) {
            return false;  // Queue empty
        }

        item = entries[t % size];
        tail.store(t + 1, std::memory_order_release);
        return true;
    }

    // Check if queue is empty (for poller edge detection)
    bool is_empty() const {
        uint64_t t = tail.load(std::memory_order_relaxed);
        uint64_t h = head.load(std::memory_order_acquire);
        return t >= h;
    }

    // Get current queue size
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
