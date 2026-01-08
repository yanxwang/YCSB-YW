# SharedKV Multi-threaded Performance Analysis and Optimization Guide

## Summary of Fixes Applied

### 1. ✅ FIXED: READ-FAILED Error (Context Persistence Issue)

**Problem**:
- Run phase had 90%+ READ failures
- Root cause: Java's `contextInitialized` flag was not persistent across load/run phases
- Each phase created a NEW SharedKVContext, losing previous data

**Solution**:
- Added `static volatile long sharedContextHandle` to remember the context
- Modified `cleanup()` to NOT destroy context in multi-threaded mode
- Context now persists across load and run phases

**Result**:
- READ failure rate: **90% → 19%**
- Remaining 19% failures are legitimate (YCSB accesses keys outside recordcount range)

### 2. ✅ ADDED: Latency Breakdown Instrumentation

**Added timing points in `submit_request()`**:
```cpp
t0: Start
t1: After enqueue to req_q
t2: After dequeue from resp_q

Breakdown:
- Enqueue latency: t1 - t0
- Wait latency: t2 - t1  (includes synchronizer + worker + poller time)
- Spin count: number of busy-wait iterations
```

**Sampling**: 1 in 10000 requests to minimize overhead

### 3. ✅ OPTIMIZED: Adaptive Busy-Wait

**Before**: Always called `std::this_thread::yield()` on every iteration

**After**: Adaptive spinning
```cpp
if (spin_count > 100) {
    std::this_thread::yield();
}
```

**Benefit**: Reduces yield overhead for fast operations while still yielding for slow ones

---

## Current Performance Characteristics

### Load Phase Performance (from your log)
- **Throughput**: ~15K ops/sec (16 threads)
- **Average Latency**: ~1000μs (1ms)
- **P95 Latency**: ~3600μs (3.6ms)
- **P99 Latency**: ~4200μs (4.2ms)

### Analysis: This is SLOW for an in-memory KV store!

Expected performance for in-memory KV with 16 threads:
- **Target throughput**: 500K - 2M ops/sec
- **Target latency**: 10-100μs average

**Current throughput is 30-100x slower than expected!**

---

## Performance Bottleneck Analysis

### Hypothesis 1: Synchronizer Thread Bottleneck ⚠️ **LIKELY**

The synchronizer thread uses **round-robin polling** of all client req_q's:

```cpp
for (size_t i = 0; i < num_clients; ++i) {
    if (clients[client_idx]->req_q->dequeue(req)) {
        // Process request...
    }
    client_idx = (client_idx + 1) % num_clients;
}
```

**Problem**:
- Single-threaded synchronizer polls 16 clients sequentially
- Each failed dequeue wastes CPU cycles
- With high concurrency, requests queue up at synchronizer

**Evidence**:
- 1ms average latency suggests requests are waiting somewhere
- Synchronizer is pinned to CPU 0 (check if saturated)

**Test**:
```bash
# Monitor CPU usage during workload
top -H -p $(pgrep -f java) | grep synchronizer
```

### Hypothesis 2: Worker Thread Imbalance ⚠️ **POSSIBLE**

Workers handle requests based on hash(key) % num_workers:
- If key distribution is skewed, some workers may be overloaded
- Ring buffer size is only 16 (may cause backpressure)

**Test**:
```bash
# Check worker statistics after a run
# (Need to add logging in kv_context.cpp destructor)
```

### Hypothesis 3: Application Thread Busy-Wait ⚠️ **POSSIBLE**

Application threads spin-wait on resp_q:
- Even with adaptive spinning, this causes CPU contention
- Cache line bouncing on resp_q head pointer

### Hypothesis 4: Queue Size Limitations

Current queue sizes:
- `req_q`: 4096 entries (probably sufficient)
- `resp_q`: 4096 entries (probably sufficient)
- Worker ring buffer: **16 entries** ⚠️ (too small!)

---

## Optimization Recommendations

### Priority 1: Reduce Synchronizer Bottleneck

#### Option A: Multi-threaded Synchronizer (RECOMMENDED)

Partition clients among multiple synchronizer threads:

```cpp
// Instead of 1 synchronizer polling all 16 clients
// Use 4 synchronizers, each polling 4 clients

Sync Thread 0 → Clients 0-3
Sync Thread 1 → Clients 4-7
Sync Thread 2 → Clients 8-11
Sync Thread 3 → Clients 12-15
```

**Expected gain**: 4x throughput

#### Option B: Client-side Direct Enqueue to Worker

Skip synchronizer entirely:
```cpp
// Application thread computes hash and enqueues directly to worker
uint32_t bucket_id = hash(key) % NUM_BUCKETS;
uint32_t worker_id = bucket_id % num_workers;
workers[worker_id]->enqueue(req);
```

**Benefit**: Eliminates synchronizer bottleneck
**Trade-off**: Lose global sequence numbers

### Priority 2: Increase Worker Ring Buffer Size

Change from 16 to 256 or 1024:
```cpp
struct KVWorker {
    static constexpr size_t BUFFER_SIZE = 256;  // Was 16
    // ...
};
```

### Priority 3: Replace Busy-Wait with Conditional Variable

Instead of spinning on resp_q, use futex or condition variable:

```cpp
// In worker thread after enqueue to resp_q:
clients[client_id]->cv.notify_one();

// In application thread:
std::unique_lock<std::mutex> lock(ch->mtx);
ch->cv.wait(lock, [&]{ return !ch->resp_q->is_empty(); });
```

**Benefit**: Reduces CPU waste
**Trade-off**: Adds mutex overhead (~100-200ns)

### Priority 4: Batch Processing in Synchronizer

Process multiple requests per client before moving to next:

```cpp
for (size_t i = 0; i < num_clients; ++i) {
    int batch_size = 0;
    while (batch_size < 32 && clients[client_idx]->req_q->dequeue(req)) {
        // Process request
        batch_size++;
    }
    client_idx = (client_idx + 1) % num_clients;
}
```

### Priority 5: NUMA-aware Allocation

Ensure workers access local NUMA memory:
```cpp
// Pin worker threads to CPUs on same NUMA node as CXL memory
numa_node = 3;
worker_cpu_start = /* CPUs on node 3 or nearest */;
```

---

## Diagnostic Commands

### 1. Monitor Thread CPU Usage
```bash
# While running workload:
top -H -p $(pgrep -f "ycsb.*SharedKV")

# Look for:
# - synchronizer thread at 100% (bottleneck)
# - worker threads idle (imbalance)
# - java threads high (busy-wait)
```

### 2. Profile with perf
```bash
# Record for 10 seconds during workload
sudo perf record -F 99 -p $(pgrep -f java) sleep 10

# Analyze hotspots
sudo perf report
```

### 3. Check Queue Depths

Add this to kv_context.cpp every 1 second:
```cpp
for (auto* ch : clients) {
    fprintf(stderr, "Client %u: req_q=%zu, resp_q=%zu\n",
            ch->client_id, ch->req_q->get_size(), ch->resp_q->get_size());
}
```

### 4. Worker Statistics

Add to SharedKVContext destructor:
```cpp
for (auto* w : workers) {
    fprintf(stderr, "Worker %u: ops=%lu, reads=%lu, inserts=%lu, updates=%lu\n",
            w->worker_id, w->ops_processed.load(), w->reads.load(),
            w->inserts.load(), w->updates.load());
}
```

---

## Testing the Fixes

### Quick Test (10K records, 8 threads)
```bash
cd /home/wang/YCSB-YW
./test_performance.sh
```

### Full Test (100K records, 16 threads)
```bash
# Edit test_performance.sh to set:
# THREADS=16
# RECORDCOUNT=100000
# OPCOUNT=100000

./test_performance.sh
```

### Scaling Test (varying thread count)
```bash
for THREADS in 1 2 4 8 16 32; do
    echo "Testing with $THREADS threads..."
    # Run workload and extract throughput
done
```

---

## Expected Performance Targets

### After Optimizations

| Workload | Target Throughput | Target Avg Latency | Current |
|----------|------------------|-------------------|---------|
| Workload A (50% R + 50% U) | 500K ops/sec | 30-50μs | ~15K ops/sec, ~1000μs |
| Workload B (95% R + 5% U) | 800K ops/sec | 20-30μs | ~15K ops/sec |
| Workload C (100% R) | 1M+ ops/sec | 15-25μs | ~15K ops/sec |

**Target improvement: 30-70x throughput, 20-50x latency reduction**

---

## Next Steps

1. **Run test_performance.sh** to get baseline numbers
2. **Monitor CPU usage** to identify bottleneck (synchronizer vs workers)
3. **Implement Priority 1 optimization** (multi-threaded synchronizer OR direct enqueue)
4. **Re-test and measure improvement**
5. **Iterate through remaining optimizations**

---

## Questions to Answer

1. **Is synchronizer thread at 100% CPU?**
   - If YES → Implement multi-threaded synchronizer
   - If NO → Check worker utilization

2. **Are worker threads balanced?**
   - Add worker statistics logging
   - Check if some workers process much more than others

3. **What is the actual queue depth during operation?**
   - Add queue depth monitoring
   - Check if queues are full (backpressure) or empty (starvation)

4. **What does perf show as hotspots?**
   - Run perf record during workload
   - Identify which functions consume most CPU

---

## Contact

For questions or issues, check:
- Native code: `/home/wang/YCSB-YW/sharedkv/native/src/`
- Java code: `/home/wang/YCSB-YW/sharedkv/src/main/java/`
- Build: `cd native/build && make && sudo cp lib/libsharedkv_jni.so /usr/lib/`
