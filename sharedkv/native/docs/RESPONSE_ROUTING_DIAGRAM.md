# SharedKV Response Routing Visual Diagram

## Complete Request-Response Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         YCSB Java Process                                    │
│                                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │Java Thread 0 │  │Java Thread 1 │  │Java Thread 2 │  │Java Thread N │   │
│  │ client_id=0  │  │ client_id=1  │  │ client_id=2  │  │ client_id=N  │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
│         │                 │                 │                 │             │
│         │ JNI Call        │                 │                 │             │
│         │ (blocking)      │                 │                 │             │
│         ▼                 ▼                 ▼                 ▼             │
│  ╔═══════════════════════════════════════════════════════════════════════╗  │
│  ║              SharedKVContext (Native C++ Singleton)                   ║  │
│  ║                                                                       ║  │
│  ║  ┌─────────────────────────────────────────────────────────────┐     ║  │
│  ║  │         Client Channels (1 per Java Thread)                 │     ║  │
│  ║  │                                                              │     ║  │
│  ║  │  ClientChannel[0]   ClientChannel[1]   ClientChannel[N]     │     ║  │
│  ║  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │     ║  │
│  ║  │  │   req_q[0]   │   │   req_q[1]   │   │   req_q[N]   │    │     ║  │
│  ║  │  │ (lock-free)  │   │ (lock-free)  │   │ (lock-free)  │    │     ║  │
│  ║  │  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘    │     ║  │
│  ║  │         │                  │                  │             │     ║  │
│  ║  │         │  ┌───────────────┴──────────────────┘             │     ║  │
│  ║  │         │  │                                                 │     ║  │
│  ║  │         ▼  ▼                                                 │     ║  │
│  ║  │  ╔═══════════════════════════════════════════════╗           │     ║  │
│  ║  │  ║      Synchronizer Thread (CPU 0)              ║           │     ║  │
│  ║  │  ║  - Round-robin polls all req_q                ║           │     ║  │
│  ║  │  ║  - Assigns sequence numbers                   ║           │     ║  │
│  ║  │  ║  - Sets req->resp_q_ptr = clients[i]->resp_q  ║  ← KEY!   │     ║  │
│  ║  │  ║  - Hash-based routing to workers              ║           │     ║  │
│  ║  │  ╚═══════════════╤═══════════════════════════════╝           │     ║  │
│  ║  │                  │                                            │     ║  │
│  ║  │                  │ Hash(key) % NUM_WORKERS                    │     ║  │
│  ║  │                  │                                            │     ║  │
│  ║  │         ┌────────┼────────┬────────────────┐                 │     ║  │
│  ║  │         ▼        ▼        ▼                ▼                 │     ║  │
│  ║  │  ┌──────────┬──────────┬──────────┬───────────┐              │     ║  │
│  ║  │  │ Worker 0 │ Worker 1 │ Worker 2 │ Worker N  │              │     ║  │
│  ║  │  │  Ring    │  Ring    │  Ring    │  Ring     │              │     ║  │
│  ║  │  │  Buffer  │  Buffer  │  Buffer  │  Buffer   │              │     ║  │
│  ║  │  │ (1024)   │ (1024)   │ (1024)   │ (1024)    │              │     ║  │
│  ║  │  └────┬─────┴────┬─────┴────┬─────┴─────┬─────┘              │     ║  │
│  ║  │       │          │          │           │                    │     ║  │
│  ║  │  ╔════▼═════╗ ╔══▼═════╗ ╔══▼══════╗ ╔══▼═════════╗          │     ║  │
│  ║  │  ║ Worker 0 ║ ║Worker 1║ ║Worker 2 ║ ║ Worker N   ║          │     ║  │
│  ║  │  ║ Thread   ║ ║Thread  ║ ║Thread   ║ ║ Thread     ║          │     ║  │
│  ║  │  ║ (CPU 2)  ║ ║(CPU 3) ║ ║(CPU 4)  ║ ║ (CPU N+1)  ║          │     ║  │
│  ║  │  ║          ║ ║        ║ ║         ║ ║            ║          │     ║  │
│  ║  │  ║ Reads    ║ ║        ║ ║         ║ ║            ║          │     ║  │
│  ║  │  ║ req->    ║ ║        ║ ║         ║ ║            ║          │     ║  │
│  ║  │  ║ resp_q_  ║ ║        ║ ║         ║ ║            ║          │     ║  │
│  ║  │  ║ ptr      ║ ║        ║ ║         ║ ║            ║          │     ║  │
│  ║  │  ╚════╤═════╝ ╚══╤═════╝ ╚══╤══════╝ ╚══╤═════════╝          │     ║  │
│  ║  │       │          │          │           │                    │     ║  │
│  ║  │       │ Writes   │          │           │                    │     ║  │
│  ║  │       │ response │          │           │                    │     ║  │
│  ║  │       │ to       │          │           │                    │     ║  │
│  ║  │       │ correct  │          │           │                    │     ║  │
│  ║  │       │ resp_q   │          │           │                    │     ║  │
│  ║  │       │          │          │           │                    │     ║  │
│  ║  │       ▼          ▼          ▼           ▼                    │     ║  │
│  ║  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │     ║  │
│  ║  │  │  resp_q[0]   │   │  resp_q[1]   │   │  resp_q[N]   │    │     ║  │
│  ║  │  │  (lock-free) │   │  (lock-free) │   │  (lock-free) │    │     ║  │
│  ║  │  └──────▲───────┘   └──────▲───────┘   └──────▲───────┘    │     ║  │
│  ║  │         │                  │                  │             │     ║  │
│  ║  │         │  ╔═══════════════════════════════╗  │             │     ║  │
│  ║  │         │  ║  Poller Thread (CPU 1)        ║  │             │     ║  │
│  ║  │         │  ║  - Sends UINTR to wake Java   ║  │             │     ║  │
│  ║  │         │  ║    threads (optional)         ║  │             │     ║  │
│  ║  │         │  ╚═══════════════════════════════╝  │             │     ║  │
│  ║  └─────────┼──────────────────────────────────────┼─────────────┘     ║  │
│  ║            │                                      │                   ║  │
│  ╚════════════╪══════════════════════════════════════╪═══════════════════╝  │
│               │                                      │                      │
│         Spin-wait                               Spin-wait                   │
│         on resp_q[0]                            on resp_q[N]                │
│               │                                      │                      │
│               │                                      │                      │
│         ┌─────▼──────┐                         ┌─────▼──────┐              │
│         │Java Thread 0│                        │Java Thread N│              │
│         │  Receives   │                        │  Receives   │              │
│         │  Response   │                        │  Response   │              │
│         │  Returns to │                        │  Returns to │              │
│         │    YCSB     │                        │    YCSB     │              │
│         └─────────────┘                        └─────────────┘              │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                    CXL Memory (NUMA Node 3)                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                      SharedHashTable                                   │  │
│  │  ┌──────────┬──────────┬──────────┬──────────┐                        │  │
│  │  │ Bucket 0 │ Bucket 1 │ Bucket 2 │ Bucket N │  (1M buckets)          │  │
│  │  │  Lock    │  Lock    │  Lock    │  Lock    │                        │  │
│  │  │  Chain   │  Chain   │  Chain   │  Chain   │                        │  │
│  │  └──────────┴──────────┴──────────┴──────────┘                        │  │
│  │                                                                        │  │
│  │  Workers access buckets via hash(key) % NUM_BUCKETS                   │  │
│  │  Each worker "owns" subset of buckets (bucket_id % NUM_WORKERS)       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Key Points Illustrated:

### 1. Request Path (Java → Worker)

```
Java Thread 0 (client_id=0)
    ↓ [JNI call: nativeReadThreaded()]
    ↓ [Enqueue to req_q[0]]
    ↓
ClientChannel[0]->req_q
    ↓ [Synchronizer dequeues]
    ↓ [Sets req->resp_q_ptr = clients[0]->resp_q]  ← CRITICAL STEP
    ↓ [Hash(key) determines worker]
    ↓
Worker 2 Ring Buffer
    ↓ [Worker 2 dequeues from ring buffer]
    ↓
Worker 2 Thread processes request
```

### 2. Response Path (Worker → Java)

```
Worker 2 Thread
    ↓ [Completes READ operation]
    ↓ [Creates KVResponse with result]
    ↓ [Writes to req->resp_q_ptr]  ← Uses stored pointer
    ↓
ClientChannel[0]->resp_q
    ↓ [Java Thread 0 spinning on resp_q[0]]
    ↓
Java Thread 0 receives response
    ↓ [Returns to YCSB]
```

### 3. Why Response Routing Works Correctly:

**The Magic: `req->resp_q_ptr` field**

```cpp
// In kv_synchronizer.cpp:33
req->resp_q_ptr = clients[client_idx]->resp_q;  // Store destination

// In kv_worker.cpp:130
req->resp_q_ptr->enqueue(resp);  // Worker writes to correct queue
```

**No client lookup needed!** The request carries its own routing information.

### 4. Thread Pinning Strategy:

```
CPU 0:  Synchronizer Thread
        - Polls all client req_q in round-robin
        - Routes requests to workers
        - Handles ring buffer full conditions

CPU 1:  Poller Thread
        - Optional UINTR notifications
        - Wakes Java threads when responses arrive

CPU 2+: Worker Threads
        - Worker 0 on CPU 2
        - Worker 1 on CPU 3
        - Worker N on CPU N+1
        - Each owns subset of hash buckets
```

### 5. Lock-Free Synchronization:

```
req_q[i]:   SPSC (Single Producer, Single Consumer)
            Producer: Java Thread i
            Consumer: Synchronizer

resp_q[i]:  MPSC (Multiple Producer, Single Consumer)
            Producers: All Worker Threads (any worker can respond to client i)
            Consumer: Java Thread i

Ring Buffer: SPSC
            Producer: Synchronizer
            Consumer: Worker Thread
```

### 6. Example Scenario (3 Threads, 2 Workers):

```
Time T0:
  Thread 0 → INSERT("key1", "value1") → req_q[0]
  Thread 1 → READ("key2") → req_q[1]
  Thread 2 → UPDATE("key3", "value3") → req_q[2]

Time T1 (Synchronizer):
  Dequeue from req_q[0] → "key1"
    hash("key1") = 0x1234 → bucket 0x234 → worker 0
    req->resp_q_ptr = resp_q[0]
    Handoff to Worker 0 ring buffer

  Dequeue from req_q[1] → "key2"
    hash("key2") = 0x5678 → bucket 0x678 → worker 0
    req->resp_q_ptr = resp_q[1]  ← Different client!
    Handoff to Worker 0 ring buffer

  Dequeue from req_q[2] → "key3"
    hash("key3") = 0xABCD → bucket 0xBCD → worker 1
    req->resp_q_ptr = resp_q[2]
    Handoff to Worker 1 ring buffer

Time T2 (Workers):
  Worker 0 processes "key1" INSERT → writes response to resp_q[0]
  Worker 0 processes "key2" READ → writes response to resp_q[1]  ← Correct queue!
  Worker 1 processes "key3" UPDATE → writes response to resp_q[2]

Time T3 (Java Threads):
  Thread 0 dequeues from resp_q[0] → INSERT success
  Thread 1 dequeues from resp_q[1] → READ result
  Thread 2 dequeues from resp_q[2] → UPDATE success
```

**Note**: Worker 0 wrote to **both** resp_q[0] and resp_q[1] because it processed requests from different clients. The `resp_q_ptr` ensured correct routing.

## Comparison with Traditional Network-Based Architecture:

### Traditional (e.g., Redis):

```
┌──────────────┐     TCP Socket      ┌──────────────┐
│Java Thread 0 ├────────────────────►│ Redis Server │
│  (own Jedis) │◄────────────────────┤   (epoll)    │
└──────────────┘     Response        └──────────────┘
                     on same socket
                     (implicit routing)
```

- **Routing**: OS kernel routes response to correct socket file descriptor
- **Isolation**: Each thread has separate TCP connection
- **Latency**: ~1-10ms (network + kernel overhead)

### SharedKV:

```
┌──────────────┐     Lock-free Queues    ┌──────────────┐
│Java Thread 0 ├───────req_q[0]─────────►│              │
│ (client_id=0)│◄──────resp_q[0]─────────┤ Worker Pool  │
└──────────────┘     Explicit resp_q_ptr └──────────────┘
```

- **Routing**: Explicit `resp_q_ptr` in request
- **Isolation**: Each thread has dedicated queues (in shared context)
- **Latency**: ~10-100μs (shared memory + lock-free queues)

## Summary:

The **key innovation** in SharedKV's response routing is storing `resp_q_ptr` in each request. This allows:

1. **Workers to be stateless**: They don't need client topology knowledge
2. **Zero-copy routing**: Direct write to destination queue
3. **Load balancing**: Different requests from same client can go to different workers
4. **Correct delivery**: Even when multiple workers write to same client's resp_q

This design enables SharedKV to achieve **10-100x lower latency** than network-based KV stores while maintaining correctness in a multi-threaded environment.
