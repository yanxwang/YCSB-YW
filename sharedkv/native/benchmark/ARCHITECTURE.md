# SharedKV Native Benchmark

## 快速开始

### 1. 编译 Benchmark

```bash
cd ~/YCSB-YW/sharedkv/native/benchmark
./build.sh
```

编译成功后会生成 `build/sharedkv_benchmark`。

### 2. 生成 Workload 文件

```bash
# 生成 Workload C (100% READ)
./gen_workload.py -w c -r 100000 -o 1000000

# 生成 Workload A (50% READ + 50% UPDATE)
./gen_workload.py -w a -r 100000 -o 500000

# 参数说明：
#   -w c/a/b    Workload 类型
#   -r 100000   记录数（Load Phase 插入的 key 数量）
#   -o 1000000  操作数（Transaction Phase 的操作数量）
```

生成的文件在 `workloads/` 目录下：
- `workloadc_load.txt` - Load Phase 的 INSERT 操作
- `workloadc_trans.txt` - Transaction Phase 的 READ/UPDATE 操作

### 3. 运行 Benchmark

#### 方式一：直接运行可执行文件

```bash
# Throughput 模式（运行 30 秒）
./build/sharedkv_benchmark -w workloadc -n 3 -c 32 -W 32 -t 30 -s 64

# Latency 模式（运行 10 万次操作）
./build/sharedkv_benchmark -w workloadc -n 3 -c 16 -W 16 -l -o 100000 -s 64

# 异步模式（更高吞吐量）
./build/sharedkv_benchmark -w workloadc -n 3 -c 32 -W 32 -t 30 -s 64 -a
```

#### 方式二：使用运行脚本

```bash
# Throughput 测试
./run_benchmark.sh -w workloadc -c 32 -W 32 -t 30

# Latency 测试
./run_benchmark.sh -w workloadc -c 16 -W 16 --latency
```

### 4. 命令行参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-w <workload>` | Workload 名称 (workloada/b/c) | workloada |
| `-n <numa_node>` | CXL 内存的 NUMA 节点 | 3 |
| `-c <clients>` | Client 线程数 | 16 |
| `-W <workers>` | Worker 线程数 | 16 |
| `-t <seconds>` | Throughput 模式运行时长（秒） | 10 |
| `-l` | 启用 Latency 模式 | - |
| `-o <ops>` | Latency 模式的操作数 | 10000 |
| `-s <cpu_start>` | Client 线程起始 CPU | 64 |
| `-a, --async` | 异步模式（Request + Response 线程分离） | - |
| `-h` | 显示帮助信息 | - |

### 5. 典型测试示例

```bash
# 快速验证（4 clients, 4 workers, 5 秒）
./build/sharedkv_benchmark -w workloadc -n 3 -c 4 -W 4 -t 5 -s 64

# 高吞吐量测试（32 clients, 32 workers, 30 秒）
./build/sharedkv_benchmark -w workloadc -n 3 -c 32 -W 32 -t 30 -s 64

# 延迟测试（16 clients, 100K 操作）
./build/sharedkv_benchmark -w workloadc -n 3 -c 16 -W 16 -l -o 100000 -s 64

# 异步高吞吐测试
./build/sharedkv_benchmark -w workloadc -n 3 -c 32 -W 32 -t 30 -s 64 -a
```

### 6. 输出示例

**Throughput 模式**：
```
==============================================
TRANSACTION PHASE Results:
==============================================
Total operations:      8,792,594
Successful operations: 8,792,594
Failed operations:     0
Duration:              5.00 seconds
Throughput:            1,758,227 ops/sec
==============================================
```

**Latency 模式**：
```
Latency Statistics (microseconds):
  Average:  1.97 us
  Median:   1.85 us
  95th:     2.34 us
  99th:     3.12 us
  99.9th:   5.67 us
```

### 7. 注意事项

1. **NUMA 节点**：确保 `-n` 参数指定的 NUMA 节点有足够的内存
   ```bash
   numactl --hardware | grep "node 3"
   ```

2. **CPU 绑定**：`-s` 参数应避开 Workers 使用的 CPU (0-1 是 Synchronizer/Poller，2+ 是 Workers)

3. **清理共享内存**：如果遇到问题，清理共享内存后重试
   ```bash
   ../scripts/clean_shm.sh
   ```

4. **Workload 文件**：确保先生成对应的 workload 文件再运行

---

# SharedKV Client Architecture: Synchronous vs Asynchronous

## Overview

SharedKV 支持两种客户端架构模式：
- **Synchronous Mode**（默认）：传统的同步请求-响应模式
- **Asynchronous Mode**（`-a` 或 `--async` 参数）：将请求提交和响应处理分离到不同线程

---

## Synchronous Mode（同步模式）

### 架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Thread (单线程)                    │
│                                                              │
│  1. Submit Request:                                          │
│     - create KVRequest                                       │
│     - enqueue to req_q (busy-spin if full)                   │
│                                                              │
│  2. Wait for Response:                                       │
│     - dequeue from resp_q (busy-wait/yield)                  │
│     - process response                                       │
│     - record statistics                                      │
│                                                              │
│  循环执行下一个操作                                           │
└─────────────────────────────────────────────────────────────┘
          │                              ▲
          │ req_q                        │ resp_q
          ▼                              │
     ┌────────────────────────────────────────┐
     │      SharedKV Core                     │
     │  (Synchronizer → Workers → Poller)     │
     └────────────────────────────────────────┘
```

### 特点

- **单线程per client**：每个 client 只有一个线程
- **阻塞语义**：submit_request() 提交后，必须等待 response 才能继续
- **简单直接**：代码逻辑清晰，易于调试
- **CPU利用率**：Client thread 在等待响应时会 busy-wait 或 yield

### API

```cpp
// 同步提交并等待响应
KVResponse submit_request(client_id, request, timeout_ms);
```

### 使用场景

- 延迟测量场景（需要精确的单个操作延迟）
- 调试和开发
- 简单的benchmark场景

---

## Asynchronous Mode（异步模式）

### 架构

```
┌──────────────────────────┐      ┌──────────────────────────┐
│  Request Thread          │      │  Response Thread         │
│  (per client)            │      │  (per client)            │
│                          │      │                          │
│  while (running) {       │      │  while (running) {       │
│    1. create KVRequest   │      │    1. uintr_wait()       │
│    2. enqueue to req_q   │      │       ▲                  │
│       (busy-spin if full)│      │       │ UINTR signal     │
│    3. continue           │      │    2. dequeue resp_q     │
│  }                       │      │    3. process response   │
│                          │      │    4. record statistics  │
│  不等待response           │      │  }                       │
└──────────────────────────┘      └──────────────────────────┘
          │                                  ▲
          │ req_q                           │ resp_q
          ▼                                  │
     ┌────────────────────────────────────────────┐
     │           SharedKV Core                    │
     │   Synchronizer → Workers → Poller          │
     │                              │             │
     │   Poller: Edge-triggered     │             │
     │   - detect empty→non-empty   │             │
     │   - send UINTR to Response   │             │
     │     Thread (_senduipi)       │             │
     └─────────────────────────────────────────────┘
```

### 线程配置

假设 `-c 8`（8个clients）：
- **8 个 Request Threads**
  - CPU pinning: cpu_start + 0, cpu_start + 1, ..., cpu_start + 7
  - 例如：CPU 64-71

- **8 个 Response Threads**
  - CPU pinning: cpu_start + num_clients + 0, ..., cpu_start + num_clients + 7
  - 例如：CPU 72-79

### 工作流程

#### Request Thread

1. 循环生成请求：
   ```cpp
   while (!should_stop) {
       // 从 workload 获取下一个操作
       KVRequest* req = create_request(operation);
       req->timestamp = now();  // 记录时间戳

       // 非阻塞提交（busy-spin if queue full）
       while (!submit_request(client_id, req)) {
           yield();
       }

       // 立即继续下一个请求，不等待响应
   }
   ```

2. **不等待响应**：提交后立即开始下一个请求
3. **Flow control**：通过队列满/空状态自然实现

#### Response Thread

1. **UINTR 初始化**：
   ```cpp
   // 注册 UINTR handler（空handler，仅用于唤醒）
   uintr_register_handler(empty_handler, 0);

   // 创建 UINTR fd，供 Poller 发送信号
   int uintr_fd = uintr_create_fd(0, 0);
   ```

2. **主循环**：
   ```cpp
   while (!should_stop) {
       // 等待 Poller 的 UINTR 信号
       uintr_wait(0);

       // 被唤醒后，drain response queue
       KVResponse resp;
       while (resp_q->dequeue(resp)) {
           // 计算延迟
           uint64_t latency = now() - resp.timestamp;

           // 更新统计
           if (resp.status == SUCCESS) {
               responses_received++;
               latencies.push_back(latency);
           } else {
               responses_failed++;
           }
       }

       // 重新检查队列（避免竞态）
       if (!resp_q->is_empty()) {
           continue;  // 不回到 uintr_wait，继续drain
       }
   }
   ```

#### Poller 职责

Poller 负责检测响应队列状态变化并唤醒 Response Threads：

```cpp
// 初始化：为每个 client 注册为 UINTR sender
for (client_id = 0; client_id < num_clients; client_id++) {
    // 等待 Response Thread 创建 uintr_fd（lazy initialization）
    while (!clients[client_id]->uintr_fd_ready.load()) {
        sleep(10ms);
    }

    // 注册为 sender
    uipi_index[client_id] = uintr_register_sender(
        clients[client_id]->uintr_fd, 0
    );
}

// 主循环：Edge-triggered detection
for (client_id = 0; client_id < num_clients; client_id++) {
    bool is_empty_now = resp_q->is_empty();

    // 检测 empty → non-empty 转换
    if (was_empty[client_id] && !is_empty_now) {
        // 发送 UINTR 信号唤醒 Response Thread
        _senduipi(uipi_index[client_id]);
    }

    was_empty[client_id] = is_empty_now;
}
```

### API

```cpp
// 非阻塞提交（立即返回）
bool submit_request(client_id, request);  // 返回 true=success, false=queue_full

// 阻塞获取响应（仅供 Response Thread 使用）
bool get_response(client_id, response&, timeout_ms);
```

### 延迟测量

异步模式下的延迟测量：
1. **Request Thread** 在创建 KVRequest 时记录 `req->timestamp`
2. **Response Thread** 收到响应后计算：`latency = now() - resp.timestamp`
3. 所有延迟数据在 Response Thread 中收集

### 特点

- **解耦请求生成和响应处理**：Request Thread 可以持续高速提交请求
- **UINTR 驱动**：Response Thread 使用硬件中断机制，高效低延迟
- **更高吞吐量**：Request Thread 不会被响应处理阻塞
- **适合吞吐量测试**：最大化系统吞吐能力

---

## 模式对比

| 特性 | Synchronous Mode | Asynchronous Mode |
|------|------------------|-------------------|
| **线程数** | `num_clients` 个线程 | `num_clients * 2` 个线程 |
| **Request提交** | 阻塞等待响应 | 立即返回，继续下一个 |
| **Response处理** | 同一线程busy-wait | 专用Response Thread + UINTR |
| **吞吐量** | 较低（受响应延迟限制） | 更高（请求生成不阻塞） |
| **延迟测量** | 精确单次延迟 | 批量延迟统计 |
| **CPU占用** | Busy-wait占用较多 | UINTR节省CPU |
| **实现复杂度** | 简单 | 较复杂（需要UINTR支持） |
| **使用场景** | 延迟测试、调试 | 吞吐量测试、生产环境 |

---

## 使用示例

### Synchronous Mode

```bash
# 8个client threads, 8个worker threads, 运行10秒
./sharedkv_benchmark -w workloadc -c 8 -W 8 -t 10 -s 64
```

输出示例：
```
Architecture:   Synchronous
...
[Benchmark] Thread 0 completed: 1654797 ops (0 failed)
```

### Asynchronous Mode

```bash
# 8个request threads + 8个response threads, 运行10秒
./sharedkv_benchmark -w workloadc -c 8 -W 8 -t 10 -s 64 -a
```

输出示例：
```
Architecture:   Async (Request+Response threads)
...
[AsyncReq-0] Submitted 1556973 requests
[AsyncResp-0] Received 1554252 responses (673 failed)
```

---

## 实现注意事项

### Lazy Initialization

Response Threads 采用 lazy initialization（延迟初始化）：
- Response Thread 在 **首次调用 `submit_request()`** 时创建
- 创建后，Response Thread 设置 `uintr_fd_ready = true`
- Poller 动态检测并注册新的 Response Threads

### Flow Control

异步模式下的流量控制：
- **Request Thread**：如果 req_q 满，busy-spin retry
- **Response Thread**：自动drain resp_q，不会溢出

### 同步保证

- **Synchronizer** 保证所有 requests 按顺序交付给 Workers
- **Poller** 的 edge-triggered 检测保证不会丢失响应信号
- Response Thread 的 re-check 逻辑避免竞态条件

---

## 性能调优建议

### Synchronous Mode
- 使用较大的 queue depth（`-W` 参数增大 worker ring buffer）
- 合理设置 client 数量，避免过多context switch

### Asynchronous Mode
- Request Threads 和 Response Threads 应 pin 在不同的 CPU core
- 使用 `-s` 参数调整起始 CPU（避开基础设施线程的 CPU 0-1）
- 监控 in-flight requests 数量，调整 queue depth

---

## 未来扩展

### Asynchronous Mode 可能的优化方向：
1. **Completion Queue**：Response Thread 将结果放入 completion queue，供上层应用批量处理
2. **Callback 机制**：支持用户注册回调函数，在响应到达时调用
3. **Adaptive Flow Control**：根据队列深度动态调整请求速率
4. **批量处理**：Response Thread 批量处理多个响应，减少函数调用开销
