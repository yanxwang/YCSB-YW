# SharedKV Request/Response Flow Analysis

## Overview

This document explains how SharedKV workers write responses back to the correct response queue and how YCSB Java threads handle request submission and response retrieval.

## 1. Response Routing Mechanism

### How Workers Know Which Response Queue to Use

The key insight is that **each request carries a pointer to its client's response queue** (`resp_q_ptr`). This pointer is set by the Synchronizer thread when it dequeues a request from a client's request queue.

#### Data Flow:

```
[Java Thread] → [JNI] → [Client Request Queue] → [Synchronizer] → [Worker Ring Buffer] → [Worker] → [Client Response Queue] → [Java Thread]
```

#### Detailed Steps:

**Step 1: Java Thread Submits Request** ([sharedkv_jni.cpp:266](../src/sharedkv_jni.cpp#L266))
```cpp
// In nativeReadThreaded, nativeInsertThreaded, etc.
uint32_t client_id = get_client_id(ctx);  // Each Java thread gets unique client_id via TLS
KVRequest* req = new KVRequest();
req->op_type = KVOpType::READ;  // or INSERT/UPDATE/DELETE
req->set_key(k);
// Note: resp_q_ptr is NOT set yet

KVResponse resp = ctx->submit_request(client_id, req, 5000);  // Blocks until response
```

**Step 2: Request Enqueued to Client's Request Queue** ([kv_context.cpp:194](../src/kv_context.cpp#L194))
```cpp
// In SharedKVContext::submit_request()
ClientChannel* ch = clients[client_id];
ch->req_q->enqueue(req);  // Enqueue to client-specific request queue
```

**Step 3: Synchronizer Dequeues and Routes Request** ([kv_synchronizer.cpp:29-33](../src/kv_synchronizer.cpp#L29-L33))
```cpp
// Synchronizer thread polls all client request queues in round-robin
if (clients[client_idx]->req_q->dequeue(req)) {
    req->sequence_number = global_seq.fetch_add(1);
    req->client_id = client_idx;
    req->resp_q_ptr = clients[client_idx]->resp_q;  // ← KEY: Store response queue pointer

    // Hash-based worker selection
    uint64_t h = hash_key(req->get_key());
    uint32_t bucket_id = h % NUM_BUCKETS;
    uint32_t worker_id = bucket_id % num_workers;

    workers[worker_id]->try_handoff(req);  // Handoff to worker's ring buffer
}
```

**Step 4: Worker Processes Request and Writes Response** ([kv_worker.cpp:84-136](../src/kv_worker.cpp#L84-L136))
```cpp
// Worker thread processes requests from its ring buffer
if (worker->try_get_request(req)) {
    // Prepare response
    KVResponse resp;
    resp.client_id = req->client_id;
    resp.sequence_number = req->sequence_number;

    // Execute KV operation (READ/INSERT/UPDATE/DELETE)
    switch (req->op_type) {
        case KVOpType::READ:
            bool found = kv_get(table, base, req->get_key(), result, nullptr);
            resp.status = found ? KVStatus::SUCCESS : KVStatus::NOT_FOUND;
            break;
        // ... other operations
    }

    // ← KEY: Use stored response queue pointer to write back to correct client
    while (!req->resp_q_ptr->enqueue(resp)) {
        std::this_thread::yield();
    }

    delete req;
}
```

**Step 5: Java Thread Retrieves Response** ([kv_context.cpp:214](../src/kv_context.cpp#L214))
```cpp
// In submit_request(), Java thread spins waiting for response
while (true) {
    if (ch->resp_q->dequeue(resp)) {
        return resp;  // Response received!
    }

    if (timeout) {
        return error;
    }
}
```

### Key Design Decisions

1. **Each Java thread gets a unique `client_id`** via thread-local storage (TLS)
   - First call to JNI: `tls_client_id = next_client_id.fetch_add(1)`
   - Stored in `thread_local uint32_t tls_client_id` ([sharedkv_jni.cpp:79](../src/sharedkv_jni.cpp#L79))

2. **Each client has dedicated request/response queues**
   - `ClientChannel` contains `req_q` and `resp_q` ([kv_context.cpp:23-26](../src/kv_context.cpp#L23-L26))
   - Queue depth is configurable (default: 4096 entries)

3. **Synchronizer assigns `resp_q_ptr` to each request**
   - This decouples workers from knowing client topology
   - Workers simply write to `req->resp_q_ptr` without client lookup

4. **Response routing is zero-copy**
   - Response is written directly to client's queue
   - No intermediate buffering or copying

## 2. YCSB Java Thread Behavior

### How Java Threads Submit and Retrieve Requests

Each YCSB Java thread follows this pattern:

```java
// From SharedKVClient.java
public Status read(String table, String key, ...) {
    // This runs in YCSB Java thread context
    int ret = nativeReadThreaded(nativeHandle, key, stringResult);  // ← Blocks until response
    return ret == 0 ? Status.OK : Status.NOT_FOUND;
}
```

The JNI native method **blocks synchronously** until the response is received:

```cpp
// From sharedkv_jni.cpp:247
JNIEXPORT jint JNICALL
Java_site_ycsb_db_sharedkv_SharedKVClient_nativeReadThreaded(...) {
    KVResponse resp = ctx->submit_request(client_id, req, 5000);  // ← Blocks here
    return resp.status == KVStatus::SUCCESS ? 0 : 1;
}
```

Inside `submit_request()`:
```cpp
// Enqueue request
ch->req_q->enqueue(req);

// Spin-wait for response (with adaptive yielding)
while (true) {
    if (ch->resp_q->dequeue(resp)) {
        return resp;  // ← Returns to Java thread
    }

    if (spin_count > 100) {
        std::this_thread::yield();  // Reduce CPU usage after initial spinning
    }

    if (timeout) break;
}
```

### Key Characteristics:

1. **Synchronous blocking calls**
   - Java thread submits request and **blocks** until response arrives
   - No async callbacks or future-based APIs
   - Simple programming model for YCSB workload

2. **Spin-wait with adaptive yielding**
   - Initially spin on response queue (low latency)
   - After 100 spins, start yielding to scheduler (reduce CPU waste)
   - Typical wait time: < 100 microseconds for READ operations

3. **One request in-flight per Java thread**
   - Each Java thread processes one operation at a time
   - Request-response cycle is serialized per thread
   - Parallelism comes from multiple Java threads (e.g., `-threads 16`)

## 3. Comparison with Other YCSB Bindings

### SharedKV vs. Redis/MongoDB/etc.

**Other YCSB bindings (e.g., Redis, MongoDB):**
```java
// Typical YCSB binding
public class RedisClient extends DB {
    private Jedis jedis;  // One client per Java thread

    public Status read(String table, String key, ...) {
        // Synchronous blocking call to Redis server over network
        String result = jedis.get(key);
        return result != null ? Status.OK : Status.NOT_FOUND;
    }
}
```

- **Same synchronous model**: All YCSB bindings use synchronous blocking calls
- **Each thread has isolated client**: e.g., `Jedis jedis` per thread
- **Network I/O vs. Shared Memory I/O**: Redis uses TCP sockets, SharedKV uses lock-free queues

**SharedKV's Unique Architecture:**
```java
// SharedKV binding
public class SharedKVClient extends DB {
    private static volatile long sharedContextHandle;  // Shared across all threads!

    public Status read(String table, String key, ...) {
        // Each thread gets unique client_id from TLS
        // Writes to dedicated request queue → blocks on response queue
        int ret = nativeReadThreaded(sharedContextHandle, key, result);
        return ret == 0 ? Status.OK : Status.NOT_FOUND;
    }
}
```

### Key Differences:

| Aspect | Traditional YCSB Binding | SharedKV YCSB Binding |
|--------|--------------------------|------------------------|
| **Client Context** | Per-thread client instance | Shared singleton context |
| **Thread Isolation** | Each thread has own connection | Each thread gets dedicated queues |
| **Communication** | Network (TCP/UDP) or local socket | Lock-free SPSC queues + UINTR |
| **Worker Model** | Server handles requests | Dedicated worker threads (CPU-pinned) |
| **Response Routing** | Connection-based (implicit) | Explicit `resp_q_ptr` in request |
| **Latency** | ~1-10ms (network RTT) | ~10-100μs (shared memory) |
| **Synchronization** | Kernel (socket I/O) | Userspace (lock-free queues) |

### Why SharedKV Needs Explicit Response Routing:

In traditional client-server architecture:
- Each Java thread has its own TCP connection
- Server sends response back on the same connection
- OS kernel handles routing (socket file descriptor)

In SharedKV's shared-memory architecture:
- All Java threads share the same context
- Multiple workers process requests concurrently
- **Must explicitly track which client issued each request**
- Solution: Store `resp_q_ptr` in request, worker writes to correct queue

## 4. Thread Mapping Rules

### Safe Configuration:

```bash
THREADS=16       # Java threads
NUM_CLIENTS=16   # Client channels
NUM_WORKERS=8    # Worker threads
```

**Rule: `THREADS == NUM_CLIENTS`** (1:1 mapping)

- Each Java thread gets unique `client_id` (0-15)
- Each `client_id` maps to dedicated `ClientChannel` with own request/response queues
- Workers write responses to `req->resp_q_ptr` → correct Java thread receives it

### Unsafe Configuration:

```bash
THREADS=32       # Java threads
NUM_CLIENTS=16   # Client channels (TOO FEW!)
```

**Problem: Multiple Java threads share same `client_id`**

Thread assignment:
```
Java Thread 0  → client_id=0
Java Thread 1  → client_id=1
...
Java Thread 15 → client_id=15
Java Thread 16 → client_id=0  ← COLLISION! Wraps around via modulo
Java Thread 17 → client_id=1  ← COLLISION!
...
```

**Race condition:**
1. Java Thread 0 submits READ(key1) → req_q[0] → worker processes → writes response to resp_q[0]
2. Java Thread 16 submits READ(key2) → req_q[0] → worker processes → writes response to resp_q[0]
3. Java Thread 0 dequeues from resp_q[0] → **might get response for key2!**
4. Java Thread 16 dequeues from resp_q[0] → **might get response for key1!**

**Result: Response mismatch, incorrect data returned to application**

### Wasteful but Safe Configuration:

```bash
THREADS=8        # Java threads
NUM_CLIENTS=16   # Client channels (EXCESS)
```

- Only 8 client_ids are used (0-7)
- 8 `ClientChannel` instances are allocated but never used
- Wastes memory (~1MB per unused client with 4096 queue depth)
- **No correctness issues** - each active thread has dedicated queues

## 5. Performance Implications

### Request Latency Breakdown:

From [kv_context.cpp:218-224](../src/kv_context.cpp#L218-L224):
```cpp
auto enqueue_us = duration_cast<microseconds>(t1 - t0).count();  // Time to enqueue request
auto wait_us = duration_cast<microseconds>(t2 - t1).count();     // Time waiting for response

fprintf(stderr, "[Latency] client=%u enqueue=%ld us, wait=%ld us, spins=%lu\n",
        client_id, enqueue_us, wait_us, spin_count);
```

**Typical latencies (NUMA node 3, ring buffer=1024):**
- **Enqueue**: 0-5 μs (lock-free queue write)
- **Wait**: 10-100 μs (synchronizer → worker → response queue)
  - Synchronizer polling interval: ~1-10 μs
  - Worker processing: 5-50 μs (hash lookup, lock acquire, memcpy)
  - Response enqueue: 0-5 μs
- **Spin count**: 10-1000 iterations

### Bottlenecks:

1. **Synchronizer polling**
   - Round-robin over all client request queues
   - If `NUM_CLIENTS=32`, each client checked every ~32 iterations
   - Solution: Increase synchronizer CPU priority (already pinned to CPU 0)

2. **Worker ring buffer full**
   - If worker can't keep up, `try_handoff()` fails
   - Synchronizer sends ERROR response immediately
   - Solution: Increase `RING_BUFFER_SIZE` (e.g., 16→1024 gave 33.4x speedup)

3. **Response queue contention**
   - Multiple workers might write to same client's response queue (if keys hash to different workers)
   - Lock-free queue handles this gracefully
   - Solution: Ensure `queue_depth >= expected_in_flight_requests`

## 6. Summary

### How Response Routing Works:

1. **Java thread → JNI**: Gets unique `client_id` via TLS
2. **JNI → Request Queue**: Enqueues request to `clients[client_id]->req_q`
3. **Synchronizer**: Dequeues request, assigns `resp_q_ptr = clients[client_id]->resp_q`, routes to worker
4. **Worker**: Processes request, writes response to `req->resp_q_ptr`
5. **Java thread**: Spins on `clients[client_id]->resp_q`, receives response, returns to YCSB

### Key Insight:

**The request carries a pointer to its response destination** (`resp_q_ptr`), eliminating the need for workers to perform client lookups. This enables lock-free, zero-copy response routing in the SharedKV architecture.

### YCSB Thread Model:

- **All YCSB bindings** (Redis, MongoDB, SharedKV, etc.) use **synchronous blocking calls**
- **SharedKV is unique** in using shared memory + lock-free queues instead of network I/O
- **Response routing** is explicit in SharedKV (via `resp_q_ptr`), implicit in traditional bindings (via TCP connection)
