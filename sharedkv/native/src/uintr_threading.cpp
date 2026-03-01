#include "uintr_threading.h"
#include <unistd.h>
#include <syscall.h>
#include <pthread.h>
#include <cstdio>
#include <cerrno>
#include <cstring>

// ============================================================================
// CPU Affinity Implementation
// ============================================================================

// Pin a std::thread to a specific CPU
void pin_thread_to_cpu(std::thread& th, int cpu_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);

    int rc = pthread_setaffinity_np(th.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        fprintf(stderr, "[PIN] WARNING: Failed to pin thread to CPU %d: %s\n",
                cpu_id, strerror(rc));
    } else {
        // fprintf(stderr, "[PIN] Successfully pinned thread to CPU %d\n", cpu_id);
    }
}

// Pin the current thread to a specific CPU
void pin_current_thread_to_cpu(int cpu_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);

    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        fprintf(stderr, "[PIN] WARNING: Failed to pin current thread to CPU %d: %s\n",
                cpu_id, strerror(rc));
    } else {
        // fprintf(stderr, "[PIN] Successfully pinned current thread to CPU %d\n", cpu_id);
    }
}
