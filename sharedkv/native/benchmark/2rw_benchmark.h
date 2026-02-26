#pragma once

// ============================================================================
// SharedKV 2RW YCSB Benchmark — Types and Declarations
//
// CPU Layout:
//   CPU  0          : Synchronizer       (TwoRWContext)
//   CPU  1          : Poller             (TwoRWContext)
//   CPU  worker_cpu..+m-1 : Worker threads (TwoRWContext)
//   CPU  cpu_start + j         : Request Thread j   (benchmark)
//   CPU  cpu_start + n + j     : Response Thread j  (benchmark)
//
// Each thread is pinned to its own dedicated core.
// Caller must ensure cpu_start >= worker_cpu + m to avoid overlap.
// ============================================================================

#include <cstdint>
#include <atomic>
#include <vector>
#include <pthread.h>

#include "2rw_context.h"
#include "ycsb_benchmark.h"

namespace TwoRW {

// ============================================================================
// Per-Client Control Block
// Shared between Request Thread j and Response Thread j.
// Padded to cache-line boundaries to prevent false sharing.
// ============================================================================

struct alignas(64) ClientControl {
    std::atomic<uint64_t> submitted{0};    // incremented by Request Thread on each submit
    std::atomic<uint64_t> responded{0};    // incremented by Response Thread on each response
    std::atomic<bool>     req_done{false}; // set by Request Thread when all ops submitted

    // t0 side-table indexed by (slot_id - base_slot_id).
    // Request Thread writes slot->t0 here; Response Thread reads before recycling.
    uint64_t* t0_table = nullptr;          // heap: uint64_t[slots_per_client], zero-init

    char _pad[24];
};
static_assert(sizeof(ClientControl) % 64 == 0);

// ============================================================================
// Thread Argument Structs
// ============================================================================

struct ReqThreadArgs {
    TwoRWContext*                     ctx;
    uint32_t                          client_id;
    int                               cpu_id;          // dedicated core: cpu_start + client_id
    const std::vector<YCSBOperation>* operations;
    uint32_t                          ops_start;       // first op index
    uint32_t                          ops_count;       // ops to run (ignored in throughput mode)
    bool                              throughput_mode; // true = loop until should_stop
    volatile bool*                    should_stop;
    pthread_barrier_t*                barrier;
    ClientControl*                    ctrl;
};

struct RespThreadArgs {
    TwoRWContext*         ctx;
    uint32_t              client_id;
    int                   cpu_id;          // dedicated core: cpu_start + n + client_id
    bool                  measure_latency;
    volatile bool*        should_stop;
    pthread_barrier_t*    barrier;
    ClientControl*        ctrl;

    // Outputs (filled when thread exits)
    uint64_t              out_completed;
    uint64_t              out_failed;
    // Decomposed latency (only populated when measure_latency=true):
    std::vector<uint64_t> out_stage0_ticks;  // t1 - t0: pool-fill → RequestQueue
    std::vector<uint64_t> out_stage1_ticks;  // t2 - t1: Sync dispatch latency
    std::vector<uint64_t> out_stage2_ticks;  // t3 - t2: Worker execution time
    std::vector<uint64_t> out_total_ticks;   // t3 - t0: end-to-end
    // Per-SN attribution (populated when num_synchronizers > 1):
    uint32_t              num_synchronizers = 1;
    std::vector<uint64_t> out_sn_ops;        // [s]: ops attributed to each SN
    std::vector<std::vector<uint64_t>> out_sn_stage1_ticks;  // [s][...]: t2-t1 per SN
    std::vector<std::vector<uint64_t>> out_sn_total_ticks;   // [s][...]: t3-t0 per SN
};

// ============================================================================
// Phase Result
// ============================================================================

struct PhaseResult {
    uint64_t              total_ops;
    uint64_t              failed_ops;
    uint64_t              duration_usec;
    // Decomposed latency histograms (empty unless measure_latency=true):
    std::vector<uint64_t> stage0_ticks;  // t1 - t0: pool-fill → RequestQueue
    std::vector<uint64_t> stage1_ticks;  // t2 - t1: Sync dispatch latency
    std::vector<uint64_t> stage2_ticks;  // t3 - t2: Worker execution time
    std::vector<uint64_t> total_ticks;   // t3 - t0: end-to-end
    // Per-SN stats (populated when num_synchronizers > 1):
    uint32_t              num_synchronizers = 1;
    std::vector<uint64_t> sn_ops;            // [s]: total ops attributed to each SN
    std::vector<std::vector<uint64_t>> sn_stage1_ticks;  // [s][...]: t2-t1 per SN
    std::vector<std::vector<uint64_t>> sn_total_ticks;   // [s][...]: t3-t0 per SN
};

// ============================================================================
// Phase Runner
//
// Spawns num_clients Request Threads + num_clients Response Threads.
//   Request Thread j  → dedicated CPU: cpu_start + j
//   Response Thread j → dedicated CPU: cpu_start + num_clients + j
//
// throughput_mode=true  : threads loop cyclically for duration_sec, then stop
// throughput_mode=false : Request Thread j runs exactly ops_per_client ops
//                         starting at ops[ops_per_client * j]
// ============================================================================
PhaseResult run_2rw_phase(
    TwoRWContext*                     ctx,
    const std::vector<YCSBOperation>& ops,
    uint32_t                          num_clients,
    int                               cpu_start,
    bool                              throughput_mode,
    int                               duration_sec,
    uint32_t                          ops_per_client,
    bool                              measure_latency
);

// ============================================================================
// Latency Utilities
// ============================================================================

// Measure TSC clock rate by sleeping 100 ms. Returns MHz.
uint64_t estimate_tsc_mhz();

// Print a single percentile table (p50/p75/p90/p95/p99/p99.9/max).
void print_latency_tsc(const std::vector<uint64_t>& ticks, uint64_t tsc_mhz);

// Print all four decomposed latency stages from a PhaseResult.
void print_latency_decomposed(const PhaseResult& r, uint64_t tsc_mhz);

} // namespace TwoRW
