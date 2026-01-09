# SharedKV 多线程架构详解

## 架构概览

SharedKV 采用**异步多线程后端 + 同步 JNI 外观**的设计，通过专门的线程分工实现高并发、低延迟的 KV 存储访问。

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Java Application Layer                          │
│  (YCSB Benchmark - 16 threads executing read/write operations)         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ JNI calls (synchronous API)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           JNI Layer (sharedkv_jni.cpp)                  │
│  • Thread-local client_id assignment (TLS)                             │
│  • Synchronous wrapper around async native backend                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       Native Multi-threaded Backend                      │
│                         (SharedKVContext)                                │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │              Per-Client Channels (ClientChannel)               │   │
│  │  ┌──────────────┐         ┌──────────────┐                    │   │
│  │  │  Client 0    │   ...   │  Client 15   │                    │   │
│  │  │              │         │              │                    │   │
│  │  │ req_q  (4096)│         │ req_q  (4096)│   ← App threads    │   │
│  │  │ resp_q (4096)│         │ resp_q (4096)│   ← Workers        │   │
│  │  └──────────────┘         └──────────────┘                    │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │                  Thread Infrastructure                          │   │
│  │                                                                 │   │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  │   │
│  │  │ Synchronizer   │  │    Poller      │  │   16 Workers   │  │   │
│  │  │   (CPU 0)      │  │    (CPU 1)     │  │  (CPU 2-17)    │  │   │
│  │  │                │  │                │  │                │  │   │
│  │  │ Round-robin    │  │ Edge-trigger   │  │ Hash-based     │  │   │
│  │  │ polling        │  │ detection      │  │ partitioning   │  │   │
│  │  └────────────────┘  └────────────────┘  └────────────────┘  │   │
│  └────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  Shared Hash Table (16GB CXL Memory)                    │
│  • 4096 buckets, each with spinlock                                     │
│  • NUMA node 3 (260GB total CXL memory)                                 │
│  • Hash-based bucket distribution across workers                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 核心组件详解

### 1. **ClientChannel（客户端通道）**

每个客户端拥有一个独立的通道，用于与 native 层通信。

**数据结构** (`include/shared_kv.h:110-127`):
```cpp
struct ClientChannel {
    uint32_t client_id;                          // 客户端唯一ID

    LockFreeQueue<KVRequest*>* req_q;            // 请求队列 (4096 entries)
    LockFreeQueue<KVResponse>* resp_q;           // 响应队列 (4096 entries)

    int uintr_fd;                                // UINTR 文件描述符
    std::atomic<bool> response_ready{false};     // 响应就绪标志

    std::atomic<uint64_t> requests_sent{0};      // 统计：已发送请求数
    std::atomic<uint64_t> responses_received{0}; // 统计：已接收响应数
};
```

**关键特性**:
- **Lock-Free SPSC 队列**: req_q 和 resp_q 都是无锁单生产者单消费者队列
- **直接访问**: Java 应用线程直接 enqueue/dequeue，无需额外的请求/响应线程
- **容量**: 每个队列 4096 个条目，足够缓冲高并发场景

**数据流向**:
```
Java Thread (Producer)  →  req_q  →  Synchronizer (Consumer)
Worker (Producer)       →  resp_q →  Java Thread (Consumer)
```

---

### 2. **Synchronizer Thread（同步器线程）**

**职责**: 轮询所有客户端的请求队列，分发请求到对应的 Worker。

**实现** (`src/kv_synchronizer.cpp:16-68`):

```cpp
void synchronizer_thread_func(...) {
    size_t client_idx = 0;  // Round-robin index

    while (!stop_flag.load()) {
        // 轮询所有客户端
        for (size_t i = 0; i < num_clients; ++i) {
            KVRequest* req = nullptr;

            // 尝试从当前客户端的 req_q 取出请求
            if (clients[client_idx]->req_q->dequeue(req)) {
                // 分配全局序列号
                req->sequence_number = global_seq.fetch_add(1);
                req->client_id = client_idx;
                req->resp_q_ptr = clients[client_idx]->resp_q;

                // 基于 key 的 hash 选择 worker
                uint64_t h = hash_key(req->get_key());
                uint32_t bucket_id = h % NUM_BUCKETS;      // 4096 buckets
                uint32_t worker_id = bucket_id % num_workers;  // 分配给 worker

                // 直接交付给 worker 的 ring buffer
                if (!workers[worker_id]->try_handoff(req)) {
                    // Ring buffer 满了，返回错误响应
                    send_error_response(req);
                    delete req;
                }
            }

            client_idx = (client_idx + 1) % num_clients;  // Round-robin
        }

        std::this_thread::yield();
    }
}
```

**关键机制**:

1. **Round-Robin 轮询**:
   - 依次访问所有客户端的 req_q
   - 避免某个客户端饿死
   - 时间复杂度: O(num_clients) per iteration

2. **Hash-based Partitioning**:
   ```
   bucket_id = hash(key) % 4096
   worker_id = bucket_id % num_workers

   例如: 16 workers
   - Worker 0 处理 bucket: 0, 16, 32, ..., 4080
   - Worker 1 处理 bucket: 1, 17, 33, ..., 4081
   - Worker 2 处理 bucket: 2, 18, 34, ..., 4082
   ...
   ```

   **优势**: 每个 worker 负责不同的 bucket 集合，避免锁竞争

3. **全局序列号**:
   - 每个请求分配单调递增的序列号
   - 用于追踪、调试、顺序保证（如果需要）

**CPU 绑定**:
- 固定在 **CPU 0**
- 单线程设计，是当前架构的**潜在瓶颈**

---

### 3. **Worker Threads（工作线程）**

**职责**: 执行实际的 KV 操作（读、写、删除），并将结果返回给客户端。

#### 3.1 Worker 结构 (`include/shared_kv.h:80-106`)

```cpp
struct KVWorker {
    static constexpr size_t BUFFER_SIZE = 16;  // Ring buffer 大小

    alignas(64) KVRequest* buffer[BUFFER_SIZE]; // Request 指针数组
    alignas(64) std::atomic<uint64_t> write_idx{0};  // Synchronizer 写
    alignas(64) std::atomic<uint64_t> read_idx{0};   // Worker 读

    uint32_t worker_id;        // Worker ID (0-15)
    uint32_t num_workers;      // 总 Worker 数量

    // 统计信息
    std::atomic<uint64_t> ops_processed{0};
    std::atomic<uint64_t> reads{0};
    std::atomic<uint64_t> inserts{0};
    std::atomic<uint64_t> updates{0};
    std::atomic<uint64_t> deletes{0};

    bool try_handoff(KVRequest* req);     // Synchronizer 调用
    bool try_get_request(KVRequest*& req); // Worker 调用

    inline bool owns_bucket(uint32_t bucket_id) const {
        return (bucket_id % num_workers) == worker_id;
    }
};
```

**Ring Buffer 机制**:
- **大小**: 仅 16 个槽位（小缓冲，可能是瓶颈）
- **无锁**: 使用 atomic 索引实现 SPSC
- **满处理**: Synchronizer 在 buffer 满时丢弃请求（返回错误）

#### 3.2 Worker 线程函数 (`src/kv_worker.cpp:47-127`)

```cpp
void worker_thread_func(KVWorker* worker, SharedHashTable* table,
                        void* base, std::atomic<bool>& stop_flag) {
    while (!stop_flag.load()) {
        KVRequest* req = nullptr;

        // 从 ring buffer 取出请求
        if (worker->try_get_request(req)) {
            // 准备响应
            KVResponse resp;
            resp.client_id = req->client_id;
            resp.sequence_number = req->sequence_number;

            // 执行 KV 操作
            switch (req->op_type) {
                case KVOpType::READ: {
                    std::string result;
                    bool found = kv_get(table, base, req->get_key(), result);
                    resp.status = found ? SUCCESS : NOT_FOUND;
                    if (found) resp.set_result(result);
                    worker->reads++;
                    break;
                }

                case KVOpType::INSERT:
                case KVOpType::UPDATE: {
                    kv_put(table, base, req->get_key(), req->get_value());
                    resp.status = SUCCESS;
                    worker->inserts++; // or updates++
                    break;
                }

                case KVOpType::DELETE: {
                    bool deleted = kv_delete(table, base, req->get_key());
                    resp.status = deleted ? SUCCESS : NOT_FOUND;
                    worker->deletes++;
                    break;
                }
            }

            // 将响应 enqueue 到客户端的 resp_q (阻塞直到成功)
            while (!req->resp_q_ptr->enqueue(resp)) {
                if (stop_flag.load()) break;
                std::this_thread::yield();
            }

            // 清理请求内存
            req->cleanup();
            delete req;

            worker->ops_processed++;
        } else {
            // Ring buffer 为空，让出 CPU
            std::this_thread::yield();
        }
    }
}
```

**Bucket 分区**:
- 每个 worker 只处理特定的 bucket 集合
- Worker i 负责: `bucket_id % num_workers == i`
- **优势**: 避免多个 worker 同时访问同一个 bucket 的锁竞争

**CPU 绑定**:
- Worker 0 → CPU 2
- Worker 1 → CPU 3
- ...
- Worker 15 → CPU 17

---

### 4. **Poller Thread（轮询线程）**

**职责**: 检测客户端响应队列的状态变化，通过 UINTR 或标志通知应用线程。

**实现** (`src/kv_poller.cpp:23-84`):

```cpp
void poller_thread_func(std::vector<ClientChannel*>& clients,
                       std::atomic<bool>& stop_flag) {
    std::vector<PollerState> states(num_clients);

    // 为每个客户端初始化状态
    for (size_t i = 0; i < num_clients; ++i) {
        states[i].was_empty = true;
        states[i].uipi_index = -1;

        // TODO: 在完整实现中，注册 UINTR sender
        // states[i].uipi_index = uintr_register_sender(clients[i]->uintr_fd, 0);
    }

    // 主轮询循环（边缘触发检测）
    while (!stop_flag.load()) {
        for (size_t i = 0; i < num_clients; ++i) {
            bool is_empty_now = clients[i]->resp_q->is_empty();

            // 检测 empty → non-empty 转换（边缘触发）
            if (states[i].was_empty && !is_empty_now) {
                // 设置响应就绪标志
                clients[i]->response_ready.store(true);

                // 如果注册了 UINTR，发送中断
                if (states[i].uipi_index >= 0) {
                    _senduipi(states[i].uipi_index);
                }
            }

            states[i].was_empty = is_empty_now;
        }

        std::this_thread::yield();
    }

    // 清理 UINTR
    for (auto& state : states) {
        if (state.uipi_index >= 0) {
            uintr_unregister_sender(state.uipi_index, 0);
        }
    }
}
```

**边缘触发机制**:
- 只在队列从**空 → 非空**时触发通知
- 避免重复通知（水平触发会导致过多通知）

**UINTR 支持**:
- 当前实现中**未启用** UINTR（注释掉了注册代码）
- 仅使用 `response_ready` 标志
- 完整 UINTR 实现需要应用线程注册 handler 并创建 `uintr_fd`

**CPU 绑定**:
- 固定在 **CPU 1**

---

### 5. **Java Application Threads（应用线程）**

**数量**: 由 YCSB `-threads` 参数指定（例如 16 个线程）

**工作流程**:

1. **初始化**: 每个线程在第一次调用时分配唯一的 `client_id`
   ```cpp
   // JNI 层 (src/sharedkv_jni.cpp)
   static thread_local uint32_t tls_client_id = UINT32_MAX;

   uint32_t get_client_id(SharedKVContext* ctx) {
       if (tls_client_id != UINT32_MAX) return tls_client_id;

       // 原子分配新 client_id
       uint32_t client_id = next_client_id.fetch_add(1);
       tls_client_id = client_id;
       return client_id;
   }
   ```

2. **提交请求** (`src/kv_context.cpp:175-241`):
   ```cpp
   KVResponse SharedKVContext::submit_request(uint32_t client_id,
                                              KVRequest* req,
                                              uint64_t timeout_ms) {
       ClientChannel* ch = clients[client_id];

       auto t0 = now();

       // 阻塞 enqueue 到 req_q (如果满了会 yield)
       while (!ch->req_q->enqueue(req)) {
           std::this_thread::yield();
       }

       auto t1 = now();
       ch->requests_sent++;

       // 阻塞等待响应（自适应自旋）
       KVResponse resp;
       uint64_t spin_count = 0;
       auto deadline = now() + milliseconds(timeout_ms);

       while (true) {
           if (ch->resp_q->dequeue(resp)) {
               auto t2 = now();

               // 采样 1/10000 的请求进行延迟分析
               if ((ch->responses_received % 10000) == 0) {
                   fprintf(stderr, "[Latency] client=%u enqueue=%ld us, "
                           "wait=%ld us, spins=%lu\n",
                           client_id, (t1-t0), (t2-t1), spin_count);
               }

               ch->responses_received++;
               return resp;
           }

           spin_count++;

           // 自适应自旋: 前 100 次忙等，之后 yield
           if (spin_count > 100) {
               std::this_thread::yield();
           }

           // 超时检查
           if (now() >= deadline) {
               resp.status = ERROR;
               return resp;
           }
       }
   }
   ```

**CPU 使用**:
- 应用线程**不绑定**特定 CPU
- 由内核调度器自由分配
- 在等待响应时**忙等待**（busy-wait），导致高 CPU 使用率

---

## 完整数据流图

### 写操作流程 (INSERT/UPDATE)

```
┌─────────────┐
│Java Thread 5│ (分配 client_id = 5)
└──────┬──────┘
       │ 1. nativeInsert(key, value)
       ▼
┌────────────────────────────┐
│   JNI Layer                │
│   get_client_id() → 5      │
│   create KVRequest         │
└────────────┬───────────────┘
             │ 2. submit_request(client_id=5, req)
             ▼
┌────────────────────────────┐
│   ClientChannel[5]         │
│   req_q->enqueue(req)      │ ← Java thread 阻塞直到成功
└────────────┬───────────────┘
             │
             │ 3. Synchronizer 轮询到 client 5
             ▼
┌────────────────────────────┐
│   Synchronizer (CPU 0)     │
│   • dequeue from req_q[5]  │
│   • hash(key) → bucket_id  │
│   • bucket_id % 16 → worker_id │
│   • try_handoff(worker_id) │
└────────────┬───────────────┘
             │ 4. 交付给 Worker 3 (假设 bucket % 16 = 3)
             ▼
┌────────────────────────────┐
│   Worker 3 Ring Buffer     │
│   buffer[write_idx] = req  │
│   write_idx++              │
└────────────┬───────────────┘
             │ 5. Worker 3 从 ring buffer 取出
             ▼
┌────────────────────────────┐
│   Worker 3 (CPU 5)         │
│   • try_get_request(req)   │
│   • kv_put(table, key, val)│
│   • create KVResponse      │
│   • resp_q[5]->enqueue(resp)│
└────────────┬───────────────┘
             │ 6. Enqueue 响应到 client 5 的 resp_q
             ▼
┌────────────────────────────┐
│   ClientChannel[5]         │
│   resp_q->enqueue(resp)    │
└────────────┬───────────────┘
             │ 7. Poller 检测到 resp_q 非空
             ▼
┌────────────────────────────┐
│   Poller (CPU 1)           │
│   • detect empty→non-empty │
│   • response_ready = true  │
│   • (optional) _senduipi() │
└────────────┬───────────────┘
             │
             │ 8. Java thread 的 busy-wait 检测到响应
             ▼
┌────────────────────────────┐
│   Java Thread 5            │
│   • resp_q->dequeue(resp)  │
│   • return result to YCSB  │
└────────────────────────────┘
```

### 读操作流程 (READ)

流程与写操作类似，主要区别在 Worker 执行的操作：

```cpp
Worker 执行:
  kv_get(table, base, key, result)
  resp.status = found ? SUCCESS : NOT_FOUND
  if (found) resp.set_result(result)
```

---

## 关键设计决策

### 1. **方案 A: 无专用请求/响应线程**

**原始设计** (被否决):
- 每个 ClientChannel 有专用的 request thread 和 response thread
- 增加了 2 * num_clients 个线程（例如 32 个额外线程）

**当前设计** (方案 A):
- Java 应用线程**直接** enqueue/dequeue
- 减少了线程开销和上下文切换
- 简化了架构

### 2. **Hash-based Bucket Partitioning**

**目的**: 避免锁竞争

**机制**:
```
每个 bucket 有自己的 spinlock
Worker i 只访问 bucket_id % num_workers == i 的 buckets
结果: 不同 worker 永远不会竞争同一个 bucket 的锁
```

**例子** (4096 buckets, 16 workers):
- Worker 0: buckets [0, 16, 32, ..., 4080] (256 buckets)
- Worker 1: buckets [1, 17, 33, ..., 4081] (256 buckets)
- ...

### 3. **自适应忙等待 (Adaptive Busy-Wait)**

Java 应用线程等待响应时:
- 前 100 次循环: 纯忙等待（spin）
- 之后: `std::this_thread::yield()`

**权衡**:
- 优点: 低延迟（对于快速操作）
- 缺点: 高 CPU 使用率

### 4. **小 Ring Buffer (16 entries)**

**当前大小**: 16 个槽位

**影响**:
- 如果 Synchronizer 快速分发，Worker 来不及处理 → buffer 满
- Buffer 满时**丢弃请求**并返回错误

**优化方向**: 增加到 256 或 1024

---

## 性能瓶颈分析

### 当前性能表现

根据 `../../PERFORMANCE_ANALYSIS.md`:
- **吞吐量**: ~15K ops/sec (16 threads, 1M records)
- **期望吞吐量**: 500K-2M ops/sec
- **瓶颈**: **30-100x 慢于预期**

### 瓶颈 #1: 单线程 Synchronizer ⚠️ **主要瓶颈**

**问题**:
- 单个 Synchronizer 线程轮询 16 个客户端的 req_q
- 每次迭代 O(num_clients) 时间复杂度
- 在高并发下，请求在 req_q 中排队

**证据**:
- 平均延迟 ~1000μs (1ms)
- 期望延迟 10-100μs
- Synchronizer 可能达到 100% CPU 使用率

**解决方案**:

**选项 A: 多线程 Synchronizer**
```cpp
// 分区客户端
Synchronizer 0 → Clients [0-3]   (CPU 18)
Synchronizer 1 → Clients [4-7]   (CPU 19)
Synchronizer 2 → Clients [8-11]  (CPU 20)
Synchronizer 3 → Clients [12-15] (CPU 21)

预期提升: 4x 吞吐量
```

**选项 B: 客户端直接 Enqueue 到 Worker**
```cpp
// Java 线程计算 hash，直接 enqueue 到 worker
uint32_t worker_id = hash(key) % num_workers;
workers[worker_id]->req_q->enqueue(req);

优点: 消除 Synchronizer 瓶颈
缺点: 失去全局序列号、需要更改架构
```

### 瓶颈 #2: 小 Ring Buffer (16 entries)

**问题**:
- Worker ring buffer 只有 16 个槽位
- 高并发时容易满
- 满时丢弃请求 → 错误率上升

**解决方案**: 增加 BUFFER_SIZE
```cpp
struct KVWorker {
    static constexpr size_t BUFFER_SIZE = 256;  // 原来是 16
    ...
};
```

### 瓶颈 #3: 应用线程忙等待

**问题**:
- Java 线程在 `resp_q->dequeue()` 上自旋
- Cache line bouncing (多个 CPU 争用 resp_q 的 head 指针)
- 浪费 CPU 周期

**解决方案**: 使用条件变量或 Futex
```cpp
// Worker enqueue 响应后通知
clients[client_id]->cv.notify_one();

// 应用线程等待
std::unique_lock<std::mutex> lock(ch->mtx);
ch->cv.wait(lock, [&]{ return !ch->resp_q->is_empty(); });
```

**权衡**: 增加 100-200ns 互斥锁开销，但减少 CPU 浪费

---

## 线程 CPU 分配总结

| 线程类型          | 数量 | CPU 绑定     | 职责                          |
|-------------------|------|--------------|-------------------------------|
| Synchronizer      | 1    | CPU 0        | 轮询 req_q，分发到 worker     |
| Poller            | 1    | CPU 1        | 检测 resp_q 非空，触发通知    |
| Worker            | 16   | CPU 2-17     | 执行 KV 操作                  |
| Java App Thread   | 16   | 不绑定       | 提交请求，等待响应            |

**总线程数**: 18 个 native 线程 + 16 个 Java 线程 = **34 个线程**

---

## 优化路线图

### 短期优化 (1-2 周)

1. **增加 Worker Ring Buffer 大小**: 16 → 256
   - 修改 `include/shared_kv.h:81`
   - 重新编译测试

2. **多线程 Synchronizer** (4 个 synchronizer)
   - 修改 `src/kv_context.cpp` 启动 4 个 synchronizer 线程
   - 分区客户端: [0-3], [4-7], [8-11], [12-15]

3. **减少 yield 调用开销**
   - 在 worker/synchronizer 中增加 batch 处理

### 中期优化 (2-4 周)

4. **条件变量替代忙等待**
   - 修改 `submit_request()` 使用 `std::condition_variable`
   - 减少 CPU 浪费

5. **启用 UINTR 通知**
   - 完整实现 `src/kv_poller.cpp` 的 UINTR 注册
   - 应用线程注册 UINTR handler

### 长期优化 (1-2 月)

6. **客户端直接 Enqueue 到 Worker**
   - 消除 Synchronizer 瓶颈
   - 需要重新设计序列号机制

7. **NUMA 优化**
   - 将 Worker 绑定到 CXL 内存同一 NUMA 节点的 CPU
   - 减少跨 NUMA 访问延迟

---

## 调试和监控

### 延迟采样

当前实现每 10000 个请求采样一次延迟:
```
[Latency] client=5 enqueue=10 us, wait=950 us, spins=1240
```

**解读**:
- `enqueue`: req_q enqueue 耗时 (通常很短)
- `wait`: 等待响应的时间 (主要延迟来源)
- `spins`: 忙等待的自旋次数

### Worker 统计

在 `SharedKVContext` 析构函数中打印:
```cpp
~SharedKVContext() {
    for (auto* w : workers) {
        fprintf(stderr, "Worker %u: ops=%lu, reads=%lu, inserts=%lu\n",
                w->worker_id, w->ops_processed.load(),
                w->reads.load(), w->inserts.load());
    }
}
```

### CPU 使用率监控

使用 `scripts/monitor_threads.sh`:
```bash
./scripts/monitor_threads.sh
```

输出 CPU 分布和线程状态。

---

## 总结

SharedKV 的多线程架构通过**专门化线程分工**实现了：
- ✅ 无锁通信 (lock-free queues)
- ✅ CPU 绑定 (减少上下文切换)
- ✅ Hash 分区 (避免锁竞争)

但当前存在性能瓶颈：
- ⚠️ 单线程 Synchronizer (主要瓶颈)
- ⚠️ 小 Ring Buffer (16 entries)
- ⚠️ 应用线程忙等待

**优化后预期性能**: 30-70x 吞吐量提升，达到 500K-2M ops/sec。
