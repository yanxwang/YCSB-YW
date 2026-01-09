// Global variables shared between benchmark and SharedKV context

#include <atomic>

// Global CPU allocation tracking (shared with kv_context.cpp and sharedkv_jni.cpp)
// CPU 0: Synchronizer (fixed)
// CPU 1: Poller (fixed)
// CPU 2+: Dynamically allocated to Worker threads and Client threads
std::atomic<int> next_available_cpu{2};  // Start from CPU 2
