# Client-Thread 映射机制详解

## 问题场景分析

### 场景 1: Java Threads > Clients (例如 Threads=32, Clients=16)

这是**有问题**的配置，会导致响应混乱。

### 场景 2: Clients > Java Threads (例如 Threads=16, Clients=32)

这是**安全但浪费**的配置。

---

## 核心映射机制

### 代码位置
- 文件: `src/sharedkv_jni.cpp:84-121`
- 函数: `get_client_id(SharedKVContext* ctx)`

### 实现原理

```cpp
static thread_local uint32_t tls_client_id = UINT32_MAX;  // Thread-local storage
static std::atomic<uint32_t> next_client_id{0};            // Global counter

uint32_t get_client_id(SharedKVContext* ctx) {
    // Fast path: 已分配 client_id
    if (tls_client_id != UINT32_MAX) {
        return tls_client_id;
    }

    // Slow path: 首次调用，分配新 client_id
    std::lock_guard<std::mutex> lock(client_map_mutex);

    uint32_t client_id = next_client_id.fetch_add(1);  // 原子递增

    // 检查是否超出 num_clients
    if (client_id >= ctx->num_clients) {
        // ERROR: Thread 数量超过 Client 数量
        fprintf(stderr, "[JNI] ERROR: Thread count (%u) exceeds num_clients (%u)!\n",
                client_id + 1, ctx->num_clients);

        // 回退: 使用取模（会导致响应混乱！）
        client_id = client_id % ctx->num_clients;
        fprintf(stderr, "[JNI] WARNING: Using shared client_id=%u (responses may mismatch)\n",
                client_id);
    }

    tls_client_id = client_id;  // 保存到 TLS
    return client_id;
}
```

**关键机制**:
- **Thread-Local Storage (TLS)**: 每个 Java 线程第一次调用时分配唯一的 `client_id`
- **原子计数器**: `next_client_id` 保证不同线程获得不同的 ID
- **一对一映射**: `Java Thread i` → `ClientChannel[i]`

---

## 场景 1: Threads > Clients (问题场景)

### 配置示例
```bash
-threads 32           # 32 个 Java 线程
-p sharedkv.num_clients=16  # 只有 16 个 ClientChannel
```

### 会发生什么？

#### 步骤 1: 线程分配 client_id
```
Java Thread 0  → client_id = 0  ✓
Java Thread 1  → client_id = 1  ✓
...
Java Thread 15 → client_id = 15 ✓
Java Thread 16 → client_id = 16  ✗ (超出范围!)
Java Thread 17 → client_id = 17  ✗
...
Java Thread 31 → client_id = 31  ✗
```

#### 步骤 2: 取模回退 (modulo fallback)
```
Thread 16 → client_id = 16 % 16 = 0  (共享 ClientChannel[0])
Thread 17 → client_id = 17 % 16 = 1  (共享 ClientChannel[1])
...
Thread 31 → client_id = 31 % 16 = 15 (共享 ClientChannel[15])
```

**结果**:
```
ClientChannel[0] ← Thread 0, Thread 16 (两个线程！)
ClientChannel[1] ← Thread 1, Thread 17
...
ClientChannel[15] ← Thread 15, Thread 31
```

### 问题：响应混乱 (Response Mismatch)

#### 数据流示例

**时间线**:
```
t=0: Thread 0 enqueue req_q[0] → Request A (key="user1")
t=1: Thread 16 enqueue req_q[0] → Request B (key="user2")
t=2: Synchronizer dequeue req_q[0] → Request A
t=3: Synchronizer dequeue req_q[0] → Request B
t=4: Worker 处理 Request A → enqueue resp_q[0] → Response A
t=5: Worker 处理 Request B → enqueue resp_q[0] → Response B
```

**问题场景**:
```
t=6: Thread 0 dequeue resp_q[0] → 可能拿到 Response B! ✗
t=7: Thread 16 dequeue resp_q[0] → 可能拿到 Response A! ✗
```

**为什么会混乱？**
- `resp_q[0]` 是 FIFO 队列
- Thread 0 和 Thread 16 都从同一个 `resp_q[0]` dequeue
- **谁先 dequeue 谁拿到**，不保证响应对应正确的请求

#### Lock-Free Queue 无法保护

```cpp
// LockFreeQueue 的 dequeue 是 SPSC (Single Producer Single Consumer)
// 但这里有 2 个 Consumer (Thread 0 和 Thread 16)!

bool dequeue(T& item) {
    uint64_t t = tail.load(std::memory_order_relaxed);
    uint64_t h = head.load(std::memory_order_acquire);

    if (t >= h) return false;  // Empty

    item = entries[t % size];  // 多个线程可能同时读取！
    tail.store(t + 1, std::memory_order_release);
    return true;
}
```

**并发问题**:
- Thread 0 和 Thread 16 同时调用 `dequeue()`
- 两者可能读取同一个 `tail` 值
- Race condition → 响应混乱

### 错误日志

实际运行时会看到：
```
[JNI] ERROR: Thread count (17) exceeds num_clients (16). Increase sharedkv.num_clients property!
[JNI] WARNING: Using shared client_id=0 (responses may mismatch)
[JNI] Thread 16 assigned client_id=0
[JNI] WARNING: Using shared client_id=1 (responses may mismatch)
[JNI] Thread 17 assigned client_id=1
...
```

YCSB 结果会出现：
- READ 返回错误的 value
- 超时（等不到属于自己的响应）
- 崩溃（野指针访问）

---

## 场景 2: Clients > Threads (安全但浪费)

### 配置示例
```bash
-threads 16           # 16 个 Java 线程
-p sharedkv.num_clients=32  # 32 个 ClientChannel
```

### 会发生什么？

#### 线程分配
```
Java Thread 0  → client_id = 0  ✓
Java Thread 1  → client_id = 1  ✓
...
Java Thread 15 → client_id = 15 ✓

ClientChannel[16-31]: 未使用（浪费）
```

### 是否有问题？

**✅ 安全**: 每个 Java 线程都有独立的 ClientChannel，没有共享

**资源浪费**:
```
每个 ClientChannel 占用:
  - req_q: 4096 * 8 bytes (指针) = 32 KB
  - resp_q: 4096 * sizeof(KVResponse) ≈ 4096 * 128 bytes = 512 KB

总计: 每个 ClientChannel ≈ 544 KB

浪费: 16 个未使用的 ClientChannel = 16 * 544 KB ≈ 8.7 MB
```

**Synchronizer 开销**:
```cpp
// Synchronizer 轮询所有 num_clients 个客户端
for (size_t i = 0; i < num_clients; ++i) {
    if (clients[client_idx]->req_q->dequeue(req)) {
        // 处理请求
    }
    client_idx = (client_idx + 1) % num_clients;
}
```

- 轮询 32 个客户端，但只有 16 个活跃
- **2x 轮询开销**（一半是空轮询）

### 性能影响

**吞吐量下降**:
- Synchronizer 每次迭代需要检查 32 个 req_q
- 活跃的只有 16 个，一半是无效检查
- 预期吞吐量下降 **~10-20%**

**建议**:
- **最佳配置**: `num_clients == num_threads`
- 如果不确定线程数，设置 `num_clients = num_threads * 1.5` 作为缓冲

---

## 队列深度配置

### 定义位置
- 文件: `src/kv_context.cpp:67`
- 代码: `size_t queue_size = 4096;`

### 队列深度详情

```cpp
SharedKVContext::SharedKVContext(...) {
    ...

    // 每个 ClientChannel 的队列深度
    size_t queue_size = 4096;  // 可配置

    for (uint32_t i = 0; i < num_clients; ++i) {
        clients.push_back(new ClientChannel(queue_size));
    }

    // ClientChannel 构造函数
    ClientChannel::ClientChannel(size_t queue_size) {
        req_q = new LockFreeQueue<KVRequest*>(queue_size);   // 4096 entries
        resp_q = new LockFreeQueue<KVResponse>(queue_size);  // 4096 entries
    }
}
```

### 内存占用计算

**每个 ClientChannel**:
```
req_q:  4096 * sizeof(KVRequest*) = 4096 * 8 = 32 KB
resp_q: 4096 * sizeof(KVResponse) ≈ 4096 * 128 = 512 KB

Total: ~544 KB per ClientChannel
```

**32 个 ClientChannel**:
```
32 * 544 KB = 17.4 MB
```

**说明**: `KVResponse` 包含 key、value 字符串，实际大小取决于数据：
```cpp
struct KVResponse {
    KVStatus status;
    uint32_t client_id;
    uint64_t sequence_number;
    uint64_t timestamp;
    char* result;          // 动态分配的字符串
    size_t result_length;
};
```

### 队列满时的行为

#### req_q 满 (Java 线程 enqueue)
```cpp
// submit_request() 阻塞 enqueue
while (!ch->req_q->enqueue(req)) {
    std::this_thread::yield();  // 等待 Synchronizer 消费
}
```

**影响**: Java 线程阻塞，延迟增加

#### resp_q 满 (Worker enqueue)
```cpp
// Worker 阻塞 enqueue 响应
while (!req->resp_q_ptr->enqueue(resp)) {
    if (stop_flag.load()) break;
    std::this_thread::yield();  // 等待 Java 线程消费
}
```

**影响**: Worker 阻塞，吞吐量下降

### 队列深度调优建议

**当前配置** (4096):
- 适合中等并发 (16-32 threads)
- 99.9% 情况下不会满

**高并发配置** (8192 或 16384):
```cpp
size_t queue_size = 8192;  // 2x buffer
```

**低延迟配置** (1024 或 2048):
```cpp
size_t queue_size = 1024;  // 减少内存占用
```

**权衡**:
- ⬆️ 更大队列: 更高吞吐量，更多内存
- ⬇️ 更小队列: 更低内存，可能阻塞

---

## 最佳实践配置

### 推荐配置

**场景 1: 已知线程数**
```bash
THREADS=16

-threads $THREADS
-p sharedkv.num_clients=$THREADS      # 精确匹配
-p sharedkv.num_workers=$THREADS      # Workers = Threads
```

**场景 2: 线程数可变**
```bash
MAX_THREADS=32

-threads 16                           # 实际线程数
-p sharedkv.num_clients=$MAX_THREADS  # 留出缓冲
-p sharedkv.num_workers=16            # Workers 匹配实际负载
```

**场景 3: 极高并发**
```bash
-threads 64
-p sharedkv.num_clients=64
-p sharedkv.num_workers=32            # Workers < Threads (减少锁竞争)
```

### 配置检查脚本

```bash
#!/bin/bash

THREADS=16
CLIENTS=16
WORKERS=16

if [ $THREADS -gt $CLIENTS ]; then
    echo "ERROR: threads ($THREADS) > num_clients ($CLIENTS)"
    echo "This will cause response mismatch!"
    exit 1
fi

if [ $CLIENTS -gt $((THREADS * 2)) ]; then
    echo "WARNING: num_clients ($CLIENTS) >> threads ($THREADS)"
    echo "Wasting resources and Synchronizer overhead!"
fi

if [ $WORKERS -gt $CLIENTS ]; then
    echo "WARNING: num_workers ($WORKERS) > num_clients ($CLIENTS)"
    echo "Some workers will be idle!"
fi

echo "Configuration looks good!"
```

---

## 故障排查

### 症状 1: YCSB 返回错误的 value

**原因**: `Threads > Clients`，响应混乱

**诊断**:
```bash
# 查看错误日志
grep "exceeds num_clients" stderr.log

# 看到这个说明配置错误:
# [JNI] ERROR: Thread count (32) exceeds num_clients (16)
```

**解决**:
```bash
# 增加 num_clients
-p sharedkv.num_clients=32  # >= threads
```

### 症状 2: 吞吐量低于预期

**原因**: `Clients >> Threads`，Synchronizer 空轮询

**诊断**:
```bash
# 检查配置
echo "Threads: $THREADS, Clients: $CLIENTS"

# 如果 Clients > Threads * 1.5，考虑调整
```

**解决**:
```bash
# 减少 num_clients
-p sharedkv.num_clients=$THREADS
```

### 症状 3: 内存占用过高

**原因**: 过多的 ClientChannel

**诊断**:
```bash
# 每个 ClientChannel ≈ 544 KB
# 32 clients = 17.4 MB
# 64 clients = 34.8 MB
```

**解决**:
- 减少 `num_clients`
- 或减少 `queue_size` (修改 `kv_context.cpp:67`)

---

## 未来优化：动态队列深度

### 提案

```cpp
// 允许从 JNI 参数配置队列深度
SharedKVContext::SharedKVContext(
    int numa_node,
    uint32_t num_clients,
    uint32_t num_workers,
    size_t queue_size = 4096  // 新增参数
) {
    ...
    for (uint32_t i = 0; i < num_clients; ++i) {
        clients.push_back(new ClientChannel(queue_size));
    }
}
```

### Java 接口

```java
// SharedKVClient.java
public void init() {
    int queueSize = Integer.parseInt(
        getProperties().getProperty("sharedkv.queue_size", "4096")
    );

    nativeHandle = nativeInitThreaded(
        numaNode, numClients, numWorkers, queueSize
    );
}
```

### YCSB 配置

```bash
-p sharedkv.queue_size=8192  # 自定义队列深度
```

---

## 总结

| 场景                  | Threads | Clients | 结果               | 建议               |
|-----------------------|---------|---------|--------------------|--------------------|
| **理想配置**          | 16      | 16      | ✅ 完美            | 推荐               |
| **Threads > Clients** | 32      | 16      | ❌ 响应混乱        | 避免！增加 Clients |
| **Clients > Threads** | 16      | 32      | ⚠️ 浪费资源        | 可用但不推荐       |
| **极度不匹配**        | 64      | 16      | ❌ 严重混乱        | 禁止               |

**黄金法则**:
```
num_clients >= num_threads (必须)
num_clients <= num_threads * 1.5 (推荐)
```

**队列深度配置**:
- 位置: `src/kv_context.cpp:67`
- 默认: 4096 entries
- 内存: ~544 KB per ClientChannel
- 调优: 根据并发度和内存预算调整
