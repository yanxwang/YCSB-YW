# Benchmark 流程详解

## Quick Test 命令解析

### 完整命令

```bash
benchmark/build/sharedkv_benchmark -w workloadc -n 3 -c 4 -W 4 -s 64 -t 5
```

### 参数详解

| 参数 | 值 | 含义 | 详细说明 |
|-----|---|------|---------|
| `-w` | `workloadc` | **Workload 名称** | 指定使用 Workload C（100% READ 操作）<br>会加载文件：<br>- `benchmark/workloads/workloadc_load.txt`<br>- `benchmark/workloads/workloadc_trans.txt` |
| `-n` | `3` | **NUMA 节点** | 在 NUMA node 3 上分配 16GB CXL 内存<br>（你的系统中 node 3 是 CXL memory） |
| `-c` | `4` | **Client 线程数** | 创建 4 个客户端线程<br>模拟 4 个并发的 YCSB 客户端<br>每个线程会执行一部分 workload 操作 |
| `-W` | `4` | **Worker 线程数** | 创建 4 个 worker 线程<br>这些是 SharedKV 的后台线程<br>负责实际执行 KV 操作（GET/PUT/DELETE） |
| `-s` | `64` | **Client 线程起始 CPU** | Client 线程从 CPU 64 开始 pin<br>Client 0 → CPU 64<br>Client 1 → CPU 65<br>Client 2 → CPU 66<br>Client 3 → CPU 67 |
| `-t` | `5` | **运行时长（秒）** | Transaction phase 运行 5 秒<br>在这 5 秒内，尽可能多地执行操作<br>最后统计 throughput (ops/sec) |

---

## 完整 Benchmark 流程

### 阶段 0: 初始化（Initialization）

```
┌─────────────────────────────────────────────────────┐
│  1. 解析命令行参数                                   │
│     -w workloadc -n 3 -c 4 -W 4 -s 64 -t 5         │
├─────────────────────────────────────────────────────┤
│  2. 加载 Workload 文件                               │
│     - workloadc_load.txt  (10,000 INSERT)          │
│     - workloadc_trans.txt (50,000 READ)            │
├─────────────────────────────────────────────────────┤
│  3. 初始化 SharedKVContext                           │
│     - 分配 16GB CXL memory (NUMA node 3)            │
│     - 初始化 HashTable (4096 buckets)               │
│     - 创建 4 个 ClientChannel (req_q + resp_q)      │
│     - 创建 4 个 KVWorker                            │
├─────────────────────────────────────────────────────┤
│  4. 启动基础设施线程                                 │
│     - Worker 0 → CPU 2                              │
│     - Worker 1 → CPU 3                              │
│     - Worker 2 → CPU 4                              │
│     - Worker 3 → CPU 5                              │
│     - Synchronizer → CPU 0 (固定)                   │
│     - Poller → CPU 1 (固定)                         │
└─────────────────────────────────────────────────────┘
```

**关键点**:
- Worker threads 从 CPU 2 开始动态分配（2, 3, 4, 5）
- Synchronizer 固定在 CPU 0
- Poller 固定在 CPU 1
- Client threads 稍后在 CPU 64+ 启动

---

### 阶段 1: Load Phase（数据加载阶段）

**目的**: 将 10,000 条 key-value 数据插入到 SharedKV

```
┌─────────────────────────────────────────────────────┐
│  启动 4 个 Client 线程                               │
│  每个线程负责 2,500 条 INSERT 操作                   │
├─────────────────────────────────────────────────────┤
│  Client 0 (CPU 64): INSERT user0 ~ user2499         │
│  Client 1 (CPU 65): INSERT user2500 ~ user4999      │
│  Client 2 (CPU 66): INSERT user5000 ~ user7499      │
│  Client 3 (CPU 67): INSERT user7500 ~ user9999      │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  每个 Client 线程的操作流程：                        │
│                                                     │
│  for (i = start; i < end; i++) {                   │
│      KVRequest* req = new KVRequest()              │
│      req->op_type = INSERT                         │
│      req->client_id = my_client_id                 │
│      req->set_key("user" + i)                      │
│      req->set_value(ycsb_value)  // 1KB            │
│      req->resp_q_ptr = my_response_queue           │
│      req->target_worker_id = hash(key) % 4         │
│                                                     │
│      // 提交请求并等待响应                          │
│      KVResponse resp = ctx->submit_request(...)    │
│  }                                                  │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  submit_request() 内部流程：                         │
│                                                     │
│  1. enqueue(req) → clients[client_id]->req_q       │
│                                                     │
│  2. Synchronizer 轮询发现新请求                      │
│     - 分配 sequence_number                          │
│     - 路由到对应的 Worker (基于 target_worker_id)   │
│                                                     │
│  3. Worker 执行 INSERT                              │
│     - 计算 bucket_id = hash(key) % 4096            │
│     - 找到 bucket，插入 KVNode                      │
│     - 创建 KVResponse (status=SUCCESS)             │
│                                                     │
│  4. enqueue(resp) → clients[client_id]->resp_q     │
│                                                     │
│  5. Client 线程 dequeue(resp) 并返回                │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  所有 Client 线程完成后统计：                        │
│                                                     │
│  Total operations:      10,000                     │
│  Successful operations: 10,000                     │
│  Failed operations:     0                          │
│  Duration:              0.03 seconds               │
│  Throughput:            383K ops/sec               │
└─────────────────────────────────────────────────────┘
```

**Load Phase 特点**:
- **固定操作数**: 每个线程执行固定数量的 INSERT（2,500 条）
- **无时间限制**: 线程执行完所有操作后立即退出
- **目的**: 准备数据集，为 Transaction Phase 提供可读取的数据

---

### 阶段 2: Transaction Phase（事务执行阶段）

**目的**: 测量 READ 操作的 throughput

```
┌─────────────────────────────────────────────────────┐
│  启动 4 个 Client 线程                               │
│  每个线程分配 50000/4 = 12,500 条 READ 操作          │
├─────────────────────────────────────────────────────┤
│  Client 0: operations[0 ~ 12499]                   │
│  Client 1: operations[12500 ~ 24999]               │
│  Client 2: operations[25000 ~ 37499]               │
│  Client 3: operations[37500 ~ 49999]               │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  Throughput 模式执行流程：                           │
│                                                     │
│  uint32_t op_idx = start_idx                       │
│                                                     │
│  while (!should_stop) {  // 运行 5 秒               │
│      // 执行当前操作                                │
│      execute_operation(ops[op_idx])                │
│      local_ops++                                   │
│                                                     │
│      // 循环回到起始位置                            │
│      op_idx++                                      │
│      if (op_idx >= end_idx)                        │
│          op_idx = start_idx                        │
│  }                                                  │
│                                                     │
│  // 5 秒后 main thread 设置 should_stop = true      │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  单次 READ 操作流程：                                │
│                                                     │
│  1. Client 创建 KVRequest                           │
│     req->op_type = READ                            │
│     req->set_key("user1234")                       │
│     req->target_worker_id = hash("user1234") % 4   │
│                                                     │
│  2. enqueue → req_q                                │
│                                                     │
│  3. Synchronizer 分配 sequence_number 并路由        │
│                                                     │
│  4. Worker 执行 kv_get()                            │
│     - 找到 bucket (hash % 4096)                    │
│     - 遍历链表找到 key                              │
│     - 复制 value 到 response                        │
│                                                     │
│  5. enqueue → resp_q                               │
│                                                     │
│  6. Client dequeue 并继续下一个操作                 │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  5 秒后 Main Thread 停止所有 Client 线程             │
│                                                     │
│  sleep(5)                                          │
│  should_stop = true                                │
│                                                     │
│  pthread_join(all clients)                         │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  统计结果：                                         │
│                                                     │
│  Client 0: 2,200,690 ops                           │
│  Client 1: 2,178,224 ops                           │
│  Client 2: 2,186,345 ops                           │
│  Client 3: 2,227,335 ops                           │
│  ──────────────────────────                        │
│  Total:    8,792,594 ops                           │
│                                                     │
│  Duration: 5.00 seconds                            │
│  Throughput: 1,758,227 ops/sec                     │
└─────────────────────────────────────────────────────┘
```

**Transaction Phase 特点**:
- **时间限制**: 运行固定时间（5 秒），而非固定操作数
- **循环执行**: 操作列表执行完后从头开始循环
- **测量 throughput**: 统计 5 秒内完成的总操作数

---

## CPU Pinning 策略详解

### 动态 CPU 分配机制

```
全局变量: next_available_cpu = 2 (初始值)

启动顺序和 CPU 分配:
┌──────────────────────────────────────────────────────┐
│  1. Worker 线程启动 (在 Client 线程之前)              │
│     Worker 0: cpu = allocate_cpu() → 2               │
│     Worker 1: cpu = allocate_cpu() → 3               │
│     Worker 2: cpu = allocate_cpu() → 4               │
│     Worker 3: cpu = allocate_cpu() → 5               │
│     next_available_cpu 现在 = 6                      │
├──────────────────────────────────────────────────────┤
│  2. Synchronizer 线程启动                             │
│     固定 pin 到 CPU 0 (不使用 allocate_cpu)          │
├──────────────────────────────────────────────────────┤
│  3. Poller 线程启动                                   │
│     固定 pin 到 CPU 1 (不使用 allocate_cpu)          │
├──────────────────────────────────────────────────────┤
│  4. Client 线程启动 (从 CPU 64 开始)                  │
│     Client 0: pin_to_cpu(64 + 0) → 64                │
│     Client 1: pin_to_cpu(64 + 1) → 65                │
│     Client 2: pin_to_cpu(64 + 2) → 66                │
│     Client 3: pin_to_cpu(64 + 3) → 67                │
│     (不使用动态分配，直接 cpu_start + thread_id)      │
└──────────────────────────────────────────────────────┘
```

### CPU 分配总结

```
CPU 分配图:

CPU 0  ███ Synchronizer (固定)
CPU 1  ███ Poller (固定)
CPU 2  ███ Worker 0 (动态分配)
CPU 3  ███ Worker 1 (动态分配)
CPU 4  ███ Worker 2 (动态分配)
CPU 5  ███ Worker 3 (动态分配)
CPU 6-63  (空闲，可供其他 Workers 使用)
CPU 64 ███ Client 0 (参数 -s 64)
CPU 65 ███ Client 1
CPU 66 ███ Client 2
CPU 67 ███ Client 3
CPU 68+ (空闲，可供更多 Clients 使用)
```

**为什么 Client 从 CPU 64 开始？**
1. **避免冲突**: Workers 可能占用 CPU 2-33（如果有 32 个 workers）
2. **NUMA 优化**: 某些系统中，CPU 0-31 在 socket 0，CPU 32-63 在 socket 1，CPU 64+ 在 socket 2
3. **隔离性**: 将 benchmark 客户端和基础设施线程分开，减少缓存竞争

---

## 数据流详解

### 请求路径 (Client → Worker)

```
┌──────────────┐
│ Client 线程   │ CPU 64
└──────┬───────┘
       │ 1. 创建 KVRequest
       │    - op_type = READ
       │    - key = "user1234"
       │    - client_id = 0
       │    - resp_q_ptr = clients[0]->resp_q
       │    - target_worker_id = hash("user1234") % 4 = 2
       │
       ▼
┌──────────────────────┐
│ Request Queue        │ Lock-free SPSC queue
│ clients[0]->req_q    │
└──────┬───────────────┘
       │ 2. enqueue(req)
       │
       ▼
┌──────────────────────┐
│ Synchronizer Thread  │ CPU 0
│                      │
│ Round-robin polling: │
│   for each client {  │
│     if (dequeue(req))│
│       seq = global++ │
│       handoff(req)   │
│   }                  │
└──────┬───────────────┘
       │ 3. 分配 sequence_number = 42
       │    worker_id = req->target_worker_id = 2
       │
       ▼
┌──────────────────────┐
│ Worker Ring Buffer   │ workers[2]->buffer[16]
│ Worker 2             │
└──────┬───────────────┘
       │ 4. try_handoff(req) → buffer[write_idx]
       │
       ▼
┌──────────────────────┐
│ Worker 2 Thread      │ CPU 4
│                      │
│ while (true) {       │
│   if (try_get(req))  │
│     execute(req)     │
│ }                    │
└──────┬───────────────┘
       │ 5. 执行 kv_get()
       │    - bucket_id = hash("user1234") % 4096 = 3456
       │    - 找到 KVNode，复制 value
       │
       ▼
┌──────────────────────┐
│ KVResponse           │
│   status = SUCCESS   │
│   client_id = 0      │
│   sequence_num = 42  │
│   result = "field0..."│
└──────┬───────────────┘
       │ 6. enqueue(resp) → clients[0]->resp_q
       │
       ▼
┌──────────────────────┐
│ Response Queue       │
│ clients[0]->resp_q   │
└──────┬───────────────┘
       │ 7. Poller 检测到 resp_q 非空
       │    发送 UINTR 通知 (可选)
       │
       ▼
┌──────────────────────┐
│ Client 线程 (spinning)│
│                      │
│ while (true) {       │
│   if (dequeue(resp)) │
│     return resp      │
│ }                    │
└──────────────────────┘
```

---

## Workload 文件格式

### workloadc_load.txt (Load Phase)

```
INSERT user0 field0=aaa...,field1=bbb...,field2=ccc...
INSERT user1 field0=xxx...,field1=yyy...,field2=zzz...
INSERT user2 field0=ppp...,field1=qqq...,field2=rrr...
...
INSERT user9999 field0=...,field1=...,field2=...
```

**格式**:
- `INSERT <key> <value>`
- Key: `user{number}`
- Value: 10 个 field，每个 100 字节 → 总共 ~1KB

### workloadc_trans.txt (Transaction Phase)

```
READ user4523
READ user1234
READ user8765
READ user42
READ user9999
...
```

**格式**:
- `READ <key>`
- Key 分布: Zipfian distribution（热点集中在低编号的 keys）

**Zipfian 分布示例**:
```
user0:    ████████████████ (访问最频繁)
user1:    ████████████
user2:    ██████████
user3:    ████████
...
user9998: █
user9999: █
```

---

## 性能指标解读

### Load Phase 输出

```
==============================================
LOAD PHASE Results:
==============================================
Total operations:      10,000          ← 总共执行的操作数
Successful operations: 10,000          ← 成功的操作数
Failed operations:     0               ← 失败的操作数（应该为 0）
Duration:              0.03 seconds    ← 实际耗时
Throughput:            383K ops/sec    ← 吞吐量 = 10000 / 0.03
==============================================
```

**关键点**:
- **Failed = 0**: 验证正确性（所有 INSERT 都成功）
- **Throughput**: INSERT 性能（包含分配内存、hash 计算、链表插入）

### Transaction Phase 输出

```
==============================================
TRANSACTION PHASE Results:
==============================================
Total operations:      8,792,594       ← 5 秒内执行的总操作数
Successful operations: 8,792,594       ← 所有操作都成功
Failed operations:     0               ← 没有失败
Duration:              5.00 seconds    ← 运行时长
Throughput:            1,758,227 ops/sec  ← 主要性能指标！
==============================================
```

**关键指标**:
- **Throughput = 1.75M ops/sec**: 每秒可以处理 175 万次 READ 操作
- **Zero failures**: 所有读取都能找到对应的 key（验证数据一致性）

---

## 对比：Throughput 模式 vs Latency 模式

### Throughput 模式 (`-t 5`)

**特点**:
- 运行固定时间（5 秒）
- 尽可能多地执行操作
- 操作列表循环执行
- 测量 **吞吐量**（ops/sec）

**适用场景**:
- 测试系统最大容量
- 压力测试
- Scalability 测试

### Latency 模式 (`-l -o 100000`)

**特点**:
- 执行固定数量操作（100,000）
- 每次操作测量延迟
- 统计延迟分布（p50, p95, p99）
- 测量 **响应时间**

**适用场景**:
- SLA 验证
- Tail latency 分析
- 性能调优

**示例输出**:
```
Latency Statistics (microseconds):
  Average:  1.97 us   ← 平均延迟
  Median:   1.85 us   ← 50% 的请求 < 1.85us
  95th:     2.34 us   ← 95% 的请求 < 2.34us
  99th:     3.12 us   ← 99% 的请求 < 3.12us (tail latency)
  99.9th:   5.67 us   ← 99.9% 的请求 < 5.67us
```

---

## 总结

### Quick Test 执行流程

1. **清理环境** → 清空共享内存
2. **生成 Workload** → 10K records, 50K operations
3. **初始化 SharedKV** → 16GB CXL memory, 4 workers, 4 clients
4. **Load Phase** → 插入 10,000 条数据（~0.03 秒，383K ops/sec）
5. **Transaction Phase** → 5 秒内执行尽可能多的 READ（1.75M ops/sec）
6. **清理** → 关闭所有线程，释放资源

### 关键参数总结

| 参数 | 值 | 影响 |
|-----|---|------|
| `-c 4` | 4 个客户端 | 并发度 = 4，模拟 4 个并发用户 |
| `-W 4` | 4 个 workers | 后端处理能力 = 4 并发执行 |
| `-s 64` | CPU 64 起始 | 客户端线程 pin 在 64-67 |
| `-t 5` | 5 秒 | Throughput 测试运行 5 秒 |
| `-n 3` | NUMA node 3 | 使用 CXL memory（16GB） |

### 性能数字含义

- **1.75M ops/sec**: 在 4 client + 4 worker 配置下，每秒可以完成 175 万次 READ 操作
- **Zero JNI overhead**: 这是纯 C++ 性能，没有 Java/JNI 的额外损耗
- **对比 Java YCSB**: 预计是 3-5x 的性能提升
