#include "async_benchmark.h"
#include "uintr_threading.h"
#include <chrono>
#include <functional>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <x86gprintrin.h>  // For _stui, _clui

// ============================================================================
// Request Pool Recycle Function
// ============================================================================
// This function is set as KVRequest::recycle_func for pool-allocated requests
static void request_pool_recycle(void* ctx, KVRequest* req) {
    RequestPool* pool = static_cast<RequestPool*>(ctx);
    pool->free(req);
}

// ============================================================================
// UINTR Handler
// ============================================================================
// Empty UINTR handler - just used for wakeup from uintr_wait()
// Must use __attribute__((interrupt)) for proper stack frame handling
// Also need to disable SSE/general-regs-only for interrupt handlers
__attribute__((interrupt, target("general-regs-only")))
static void uintr_empty_handler(struct __uintr_frame* frame, unsigned long long vector) {
    // Empty handler - just return to wake up from uintr_wait
    (void)frame;
    (void)vector;
}

// ============================================================================
// Request Thread Implementation (simplified - no ClientChannel)
// ============================================================================

void* async_request_thread_func(void* arg) {
    AsyncBenchmarkContext* ctx = (AsyncBenchmarkContext*)arg;
    RequestPool* pool = ctx->request_pool;
    std::vector<uint32_t>& precomputed_worker_ids = *ctx->precomputed_worker_ids;

    // Get queue pointers directly (thread_id % num_queues mapping)
    LockFreeQueue<KVRequest*>* req_q = ctx->kv_ctx->get_req_queue(ctx->client_id);
    LockFreeQueue<KVResponse>* resp_q = ctx->kv_ctx->get_resp_queue(ctx->client_id);

    // Pin to dedicated CPU
    pin_current_thread_to_cpu(ctx->request_cpu);
    printf("[AsyncReq-%u] Pinned to CPU %d, pool capacity=%u\n",
           ctx->client_id, ctx->request_cpu, pool->capacity);

    // Wait for all threads to be ready
    pthread_barrier_wait(ctx->start_barrier);

    uint32_t op_idx = ctx->ops_start_idx;
    uint32_t ops_end = ctx->ops_start_idx + ctx->ops_count;
    uint32_t local_idx = 0;  // Index into precomputed arrays
    uint64_t pool_wait_count = 0;
    uint64_t enqueue_wait_count = 0;

    // Submit as fast as possible
    while (!*(ctx->should_stop) && !ctx->kv_ctx->stop_flag.load(std::memory_order_relaxed)) {
        // Get next operation (circular)
        const YCSBOperation& op = (*ctx->operations)[op_idx];

        // Allocate from pool (spin if pool exhausted - this is flow control)
        KVRequest* req = pool->alloc();
        while (req == nullptr) {
            pool_wait_count++;
            if (*(ctx->should_stop) || ctx->kv_ctx->stop_flag.load(std::memory_order_relaxed)) {
                goto done;
            }
            __builtin_ia32_pause();
            req = pool->alloc();
        }

        // Initialize request (minimal work in hot path)
        req->client_id = ctx->client_id;
        req->resp_q_ptr = resp_q;
        req->target_worker_id = precomputed_worker_ids[local_idx];

        // Only record timestamp if measuring latency (reduces overhead)
        req->timestamp = ctx->measure_latency ?
            std::chrono::steady_clock::now().time_since_epoch().count() : 0;

        // Set operation type and copy key/value using inline memcpy
        req->op_type = static_cast<KVOpType>(op.op_type);
        req->key_len = op.key.length();
        memcpy(req->key_data, op.key.data(), req->key_len);

        // Only copy value for INSERT/UPDATE
        if (op.op_type == YCSBOpType::INSERT || op.op_type == YCSBOpType::UPDATE) {
            req->set_value(op.value);
        } else {
            req->value_len = 0;
            req->value_data = nullptr;
        }

        // Enqueue to request queue (busy-spin if full)
        while (!req_q->enqueue(req)) {
            enqueue_wait_count++;
            if (*(ctx->should_stop) || ctx->kv_ctx->stop_flag.load(std::memory_order_relaxed)) {
                pool->free(req);
                goto done;
            }
            __builtin_ia32_pause();
        }

        ctx->requests_submitted.fetch_add(1, std::memory_order_relaxed);

        // Move to next operation
        op_idx++;
        local_idx++;
        if (op_idx >= ops_end) {
            op_idx = ctx->ops_start_idx;
            local_idx = 0;
        }
    }

done:
    printf("[AsyncReq-%u] Submitted %lu requests, pool_waits=%lu, enqueue_waits=%lu\n",
           ctx->client_id, ctx->requests_submitted.load(), pool_wait_count, enqueue_wait_count);
    return nullptr;
}

// ============================================================================
// Response Thread Implementation (simplified - no ClientChannel)
// ============================================================================

void* async_response_thread_func(void* arg) {
    AsyncBenchmarkContext* ctx = (AsyncBenchmarkContext*)arg;

    // Get queue pointers directly
    ResponseQueue* resp_queue = ctx->kv_ctx->get_resp_queue_obj(ctx->client_id);
    LockFreeQueue<KVResponse>* resp_q = resp_queue->queue;

    // Pin to dedicated CPU
    pin_current_thread_to_cpu(ctx->response_cpu);
    printf("[AsyncResp-%u] Pinned to CPU %d\n", ctx->client_id, ctx->response_cpu);

    // Register UINTR handler (using properly declared interrupt handler)
    int ret = uintr_register_handler((void*)uintr_empty_handler, 0);
    if (ret < 0) {
        fprintf(stderr, "[AsyncResp-%u] ERROR: uintr_register_handler failed: %d (errno=%d: %s)\n",
                ctx->client_id, ret, errno, strerror(errno));
        return nullptr;
    }

    // Create UINTR fd for Poller to send signals
    ctx->uintr_fd = uintr_create_fd(0, 0);
    if (ctx->uintr_fd < 0) {
        fprintf(stderr, "[AsyncResp-%u] ERROR: uintr_create_fd failed: %d (errno=%d: %s)\n",
                ctx->client_id, ctx->uintr_fd, errno, strerror(errno));
        return nullptr;
    }
    ctx->uintr_fd_ready.store(true, std::memory_order_release);

    // IMPORTANT: Register uintr_fd with ResponseQueue so Poller can find it
    resp_queue->uintr_fd = ctx->uintr_fd;
    resp_queue->uintr_fd_ready.store(true, std::memory_order_release);

    printf("[AsyncResp-%u] UINTR fd created: %d\n", ctx->client_id, ctx->uintr_fd);

    // Wait for all threads to be ready
    pthread_barrier_wait(ctx->start_barrier);

    uint64_t total_drained = 0;
    uint64_t poll_count = 0;
    uint64_t empty_count = 0;
    uint64_t yield_count = 0;
    uint64_t latency_calc_count = 0;
    uint64_t atomic_update_count = 0;
    uint64_t mutex_lock_count = 0;

    // Simple polling loop
    while (true) {
        KVResponse resp;
        poll_count++;

        // Try to dequeue a response
        if (resp_q->dequeue(resp)) {
            total_drained++;

            // Calculate latency
            latency_calc_count++;
            uint64_t current_time = std::chrono::steady_clock::now()
                                        .time_since_epoch().count();
            uint64_t latency_ns = current_time - resp.timestamp;

            // Process response
            if (resp.status == KVStatus::SUCCESS) {
                atomic_update_count++;
                ctx->responses_received.fetch_add(1, std::memory_order_relaxed);

                // Record latency if measurement enabled
                if (ctx->measure_latency) {
                    mutex_lock_count++;
                    pthread_mutex_lock(&ctx->latency_mutex);
                    ctx->latencies.push_back(latency_ns);
                    pthread_mutex_unlock(&ctx->latency_mutex);
                }
            } else {
                atomic_update_count++;
                ctx->failed_ops.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            empty_count++;
            // Queue is empty - check if we should stop
            if (*(ctx->should_stop)) {
                if (resp_q->is_empty()) {
                    break;
                }
            }
            yield_count++;
            std::this_thread::yield();
        }
    }

    printf("[AsyncResp-%u] polls=%lu, empty=%lu, drained=%lu, received=%lu (failed=%lu)\n",
           ctx->client_id, poll_count, empty_count, total_drained,
           ctx->responses_received.load(), ctx->failed_ops.load());
    fprintf(stderr, "[AsyncResp-%u] PERF: yields=%lu, latency_calcs=%lu, atomic_updates=%lu, mutex_locks=%lu\n",
            ctx->client_id, yield_count, latency_calc_count, atomic_update_count, mutex_lock_count);
    fprintf(stderr, "[AsyncResp-%u] Final resp_q state: head=%lu, tail=%lu, size=%zu\n",
            ctx->client_id, resp_q->head.load(), resp_q->tail.load(), resp_q->get_size());
    printf("[AsyncResp-%u] Received %lu responses (%lu failed), drained=%lu\n",
           ctx->client_id,
           ctx->responses_received.load(),
           ctx->failed_ops.load(),
           total_drained);

    uintr_unregister_handler(0);
    return nullptr;
}

// ============================================================================
// Async Benchmark Runner (simplified - no ClientChannel)
// ============================================================================

void run_async_transaction_phase(
    SharedKVContext* kv_ctx,
    std::vector<YCSBOperation>& operations,
    uint32_t num_threads,
    uint32_t duration_sec,
    int cpu_start,
    bool measure_latency,
    uint64_t* total_ops_out,
    uint64_t* failed_ops_out,
    std::vector<uint64_t>* all_latencies_out)
{
    printf("\n[AsyncBench] Starting async transaction phase...\n");
    printf("[AsyncBench] %u threads, %u seconds\n", num_threads, duration_sec);
    printf("[AsyncBench] Request threads: CPU %d-%d\n",
           cpu_start, cpu_start + num_threads - 1);
    printf("[AsyncBench] Response threads: CPU %d-%d\n",
           cpu_start + num_threads, cpu_start + 2 * num_threads - 1);

    // CRITICAL: Reset ResponseQueue uintr state for new phase
    for (auto* rq : kv_ctx->resp_queues) {
        rq->uintr_fd = -1;
        rq->uintr_fd_ready.store(false, std::memory_order_release);
    }

    // CRITICAL: Drain any leftover requests/responses from previous phase
    printf("[AsyncBench] Draining leftover data from previous phase...\n");
    for (auto* rq : kv_ctx->resp_queues) {
        KVResponse resp;
        int drained_resp = 0;
        while (rq->queue->dequeue(resp)) {
            drained_resp++;
        }
        if (drained_resp > 0) {
            printf("[AsyncBench] Drained %d leftover responses from resp_q %u\n",
                   drained_resp, rq->queue_id);
        }
    }

    // Create barrier for thread synchronization
    pthread_barrier_t start_barrier;
    pthread_barrier_init(&start_barrier, NULL, 2 * num_threads);  // Request + Response threads

    // Create async contexts
    std::vector<AsyncBenchmarkContext> contexts(num_threads);
    volatile bool should_stop = false;

    uint32_t ops_per_thread = operations.size() / num_threads;

    // Pre-compute worker IDs for all operations
    printf("[AsyncBench] Pre-computing worker IDs for %zu operations...\n", operations.size());
    std::vector<std::vector<uint32_t>> all_precomputed_worker_ids(num_threads);
    for (uint32_t c = 0; c < num_threads; c++) {
        uint32_t start_idx = c * ops_per_thread;
        uint32_t count = (c == num_threads - 1) ?
                         (operations.size() - c * ops_per_thread) : ops_per_thread;

        all_precomputed_worker_ids[c].reserve(count);
        for (uint32_t i = start_idx; i < start_idx + count; i++) {
            uint64_t hash = std::hash<std::string>{}(operations[i].key);
            uint32_t bucket_id = hash % NUM_BUCKETS;
            all_precomputed_worker_ids[c].push_back(bucket_id % kv_ctx->config.num_workers);
        }
    }
    printf("[AsyncBench] Pre-computation done\n");

    // Create object pools (one per thread, sized for max in-flight)
    const uint32_t pool_capacity = 8192;
    std::vector<RequestPool*> pools(num_threads);
    for (uint32_t i = 0; i < num_threads; i++) {
        pools[i] = new RequestPool(pool_capacity);
        for (uint32_t j = 0; j < pool_capacity; j++) {
            pools[i]->requests[j].recycle_func = request_pool_recycle;
            pools[i]->requests[j].recycle_ctx = pools[i];
        }
    }
    printf("[AsyncBench] Created %u request pools with capacity %u each\n", num_threads, pool_capacity);

    for (uint32_t i = 0; i < num_threads; i++) {
        contexts[i].client_id = i;
        contexts[i].num_clients = num_threads;
        contexts[i].request_cpu = cpu_start + i;
        contexts[i].response_cpu = cpu_start + num_threads + i;
        contexts[i].kv_ctx = kv_ctx;
        contexts[i].operations = &operations;
        contexts[i].ops_start_idx = i * ops_per_thread;
        contexts[i].ops_count = (i == num_threads - 1) ?
                                (operations.size() - i * ops_per_thread) : ops_per_thread;
        contexts[i].max_in_flight = 2048;
        contexts[i].request_pool = pools[i];
        contexts[i].precomputed_worker_ids = &all_precomputed_worker_ids[i];
        contexts[i].measure_latency = measure_latency;
        contexts[i].should_stop = &should_stop;
        contexts[i].start_barrier = &start_barrier;
        contexts[i].uintr_fd = -1;

        if (measure_latency) {
            contexts[i].latencies.reserve(100000);
            pthread_mutex_init(&contexts[i].latency_mutex, NULL);
        }
    }

    // Start response threads first (they need to set up UINTR fds)
    for (uint32_t i = 0; i < num_threads; i++) {
        pthread_create(&contexts[i].response_thread, NULL,
                      async_response_thread_func, &contexts[i]);
    }

    // Wait for all response threads to set up UINTR fds
    for (uint32_t i = 0; i < num_threads; i++) {
        while (!contexts[i].uintr_fd_ready.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    printf("[AsyncBench] All response threads ready\n");

    // Start request threads
    for (uint32_t i = 0; i < num_threads; i++) {
        pthread_create(&contexts[i].request_thread, NULL,
                      async_request_thread_func, &contexts[i]);
    }

    printf("[AsyncBench] All threads started, running for %u seconds...\n", duration_sec);

    // Wait for specified duration
    sleep(duration_sec);

    // Phase 1: Stop request threads from submitting new requests
    should_stop = true;
    printf("[AsyncBench] Signaled request threads to stop...\n");

    // Phase 2: Wait for request threads to finish
    for (uint32_t i = 0; i < num_threads; i++) {
        pthread_join(contexts[i].request_thread, NULL);
    }
    printf("[AsyncBench] Request threads joined\n");

    // Phase 3: Wait for all pending requests to be processed
    printf("[AsyncBench] Waiting for pipeline to drain...\n");
    for (int wait = 0; wait < 50; wait++) {
        bool all_empty = true;
        for (auto* rq : kv_ctx->req_queues) {
            if (!rq->queue->is_empty()) {
                all_empty = false;
                break;
            }
        }
        if (all_empty) {
            printf("[AsyncBench] Request queues drained\n");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Give workers extra time to process and write responses
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    printf("[AsyncBench] Stopping response threads...\n");

    // Phase 4: Join response threads
    for (uint32_t i = 0; i < num_threads; i++) {
        pthread_join(contexts[i].response_thread, NULL);
    }
    printf("[AsyncBench] Response threads joined\n");

    // Collect statistics
    uint64_t total_ops = 0;
    uint64_t failed_ops = 0;

    for (const auto& ctx : contexts) {
        total_ops += ctx.responses_received.load();
        failed_ops += ctx.failed_ops.load();

        // Merge latencies
        if (measure_latency && all_latencies_out) {
            pthread_mutex_t* mutex_ptr = const_cast<pthread_mutex_t*>(&ctx.latency_mutex);
            pthread_mutex_lock(mutex_ptr);
            all_latencies_out->insert(all_latencies_out->end(),
                                     ctx.latencies.begin(),
                                     ctx.latencies.end());
            pthread_mutex_unlock(mutex_ptr);
        }
    }

    // Output results
    *total_ops_out = total_ops;
    *failed_ops_out = failed_ops;

    // Cleanup
    pthread_barrier_destroy(&start_barrier);

    for (uint32_t i = 0; i < num_threads; i++) {
        if (measure_latency) {
            pthread_mutex_destroy(&contexts[i].latency_mutex);
        }
    }

    printf("[AsyncBench] Async transaction phase complete (pools intentionally not freed)\n");
}
