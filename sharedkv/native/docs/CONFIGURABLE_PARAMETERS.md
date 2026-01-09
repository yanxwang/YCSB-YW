# SharedKV 可配置参数文档

## 概述

SharedKV 现在支持通过 YCSB 属性动态配置队列深度和 ring buffer 大小，无需重新编译代码。

---

## 新增可配置参数

### 1. `sharedkv.queue_depth` - 客户端队列深度

**含义**: 每个 ClientChannel 的 req_q 和 resp_q 的容量

**默认值**: 4096

**范围**: 256 - 65536（推荐）

**内存占用**:
```
每个 ClientChannel:
  req_q:  queue_depth × 8 bytes (指针)
  resp_q: queue_depth × 128 bytes (估计，包含响应数据)

示例（16 clients，queue_depth=4096）:
  内存: 16 × (4096×8 + 4096×128) ≈ 8.5 MB
```

**调优建议**:
- **低延迟场景** (< 100μs): 1024 - 2048
- **均衡场景** (100-500μs): 4096 (默认)
- **高吞吐场景** (> 500μs): 8192 - 16384

**影响**:
- ⬆️ 更大: 更高缓冲能力，减少阻塞
- ⬇️ 更小: 更低内存占用，但可能导致应用线程/Worker 阻塞

---

### 2. `sharedkv.ring_buffer_size` - Worker Ring Buffer 大小

**含义**: 每个 Worker 的 ring buffer 容量（Synchronizer → Worker 之间）

**默认值**: 1024

**范围**: 64 - 8192（推荐）

**内存占用**:
```
每个 Worker:
  ring_buffer: ring_buffer_size × 8 bytes (指针)

示例（16 workers，ring_buffer_size=1024）:
  内存: 16 × 1024 × 8 = 128 KB
```

**调优建议**:
- **低并发** (< 8 threads): 256 - 512
- **中等并发** (8-32 threads): 1024 (默认)
- **高并发** (> 32 threads): 2048 - 4096

**影响**:
- ⬆️ 更大: Synchronizer 可以快速分发，减少丢弃请求
- ⬇️ 更小: 更低内存占用，但高并发时可能满载

**历史性能对比**:
| Ring Buffer 大小 | 吞吐量 (ops/sec) | 说明                |
|------------------|------------------|---------------------|
| 16               | ~15,000          | 经常满载，丢弃请求  |
| 1024             | ~500,000         | **33.4x 提升**      |
| 2048+            | 待测试           | 可能进一步优化      |

---

## 使用方法

### 1. 通过 YCSB 命令行

```bash
java -Djava.library.path=/usr/lib \
  -cp "$CLASSPATH" \
  site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workloada \
  -p sharedkv.mode=cxl \
  -p sharedkv.threading=multi \
  -p sharedkv.numa_node=3 \
  -p sharedkv.num_clients=16 \
  -p sharedkv.num_workers=16 \
  -p sharedkv.queue_depth=8192 \          # 自定义队列深度
  -p sharedkv.ring_buffer_size=2048 \     # 自定义 ring buffer 大小
  -threads 16 \
  -load \
  -p recordcount=1000000
```

### 2. 通过测试脚本

修改 `sharedkv/native/scripts/test_performance.sh`:

```bash
# Test configuration
NUMA_NODE=3
THREADS=16
NUM_CLIENTS=16
NUM_WORKERS=16
QUEUE_DEPTH=8192          # 自定义队列深度
RING_BUFFER_SIZE=2048     # 自定义 ring buffer 大小
RECORDCOUNT=1000000
OPCOUNT=1000000
```

然后运行:
```bash
./sharedkv/native/scripts/test_performance.sh
```

### 3. 通过 Java Properties 文件

创建 `config.properties`:
```properties
sharedkv.mode=cxl
sharedkv.threading=multi
sharedkv.numa_node=3
sharedkv.num_clients=16
sharedkv.num_workers=16
sharedkv.queue_depth=8192
sharedkv.ring_buffer_size=2048
```

使用:
```bash
java ... -P config.properties ...
```

---

## 实现细节

### C++ 层

#### 1. SharedKVContext 构造函数
```cpp
SharedKVContext::SharedKVContext(
    uint32_t num_clients,
    uint32_t num_workers,
    int numa_node,
    size_t client_queue_depth = 4096,        // 新增参数
    size_t worker_ring_buffer_size = 1024    // 新增参数
);
```

#### 2. KVWorker 动态分配
```cpp
struct KVWorker {
    size_t buffer_size;       // 运行时大小
    KVRequest** buffer;       // 动态分配

    KVWorker(size_t ring_buffer_size = 1024);
    ~KVWorker();
};
```

分配代码:
```cpp
KVWorker::KVWorker(size_t ring_buffer_size) : buffer_size(ring_buffer_size) {
    buffer = (KVRequest**)aligned_alloc(64, sizeof(KVRequest*) * buffer_size);
}
```

### Java 层

#### Native 方法签名
```java
private native long nativeInitThreaded(
    int numaNode,
    int numClients,
    int numWorkers,
    int queueDepth,          // 新增参数
    int ringBufferSize       // 新增参数
);
```

#### 参数读取
```java
int queueDepth = Integer.parseInt(
    getProperties().getProperty("sharedkv.queue_depth", "4096")
);
int ringBufferSize = Integer.parseInt(
    getProperties().getProperty("sharedkv.ring_buffer_size", "1024")
);

sharedContextHandle = nativeInitThreaded(
    numaNode, numClients, numWorkers,
    queueDepth, ringBufferSize
);
```

---

## 性能调优指南

### 场景 1: 低延迟优先

**目标**: < 50μs P99 延迟

**配置**:
```bash
-p sharedkv.queue_depth=1024
-p sharedkv.ring_buffer_size=256
-threads 8
-p sharedkv.num_clients=8
-p sharedkv.num_workers=8
```

**原理**: 小队列减少轮询开销，降低延迟

---

### 场景 2: 高吞吐优先

**目标**: > 1M ops/sec

**配置**:
```bash
-p sharedkv.queue_depth=16384
-p sharedkv.ring_buffer_size=4096
-threads 32
-p sharedkv.num_clients=32
-p sharedkv.num_workers=32
```

**原理**: 大队列提供充足缓冲，支持高并发

---

### 场景 3: 内存受限

**目标**: 最小化内存占用

**配置**:
```bash
-p sharedkv.queue_depth=512
-p sharedkv.ring_buffer_size=128
-threads 4
-p sharedkv.num_clients=4
-p sharedkv.num_workers=4
```

**内存占用**:
```
ClientChannel: 4 × 512 × 136 bytes ≈ 270 KB
Worker buffers: 4 × 128 × 8 bytes = 4 KB
总计: < 1 MB
```

---

### 场景 4: 均衡配置（推荐）

**目标**: 平衡延迟、吞吐和内存

**配置**:
```bash
-p sharedkv.queue_depth=4096      # 默认
-p sharedkv.ring_buffer_size=1024 # 默认
-threads 16
-p sharedkv.num_clients=16
-p sharedkv.num_workers=16
```

**预期性能**:
- 吞吐量: 500K - 700K ops/sec
- P99 延迟: < 100μs
- 内存: ~9 MB

---

## 监控和诊断

### 启动日志

成功配置后，你会在 stderr 看到:

```
[SharedKVContext] Initializing with 16 clients, 16 workers on NUMA node 3
[SharedKVContext] Queue depth: 4096, Worker ring buffer: 1024
[SharedKVContext] Created 16 client channels (queue_depth=4096)
[SharedKVContext] Created 16 workers (ring_buffer_size=1024)
[PIN] Successfully pinned thread to CPU 2
...
```

### 故障排查

#### 问题 1: 未看到参数日志

**原因**: 参数传递失败或使用了旧的 native 库

**解决**:
```bash
# 重新编译和安装
cd /home/wang/YCSB-YW/sharedkv/native/build
make clean && cmake .. && make -j$(nproc)
sudo cp lib/libsharedkv_jni.so /usr/lib/

# 重新编译 Java
cd /home/wang/YCSB-YW
mvn clean package -pl sharedkv -am -DskipTests
```

#### 问题 2: 队列经常满

**症状**: 日志中看到 "[Synchronizer] Ring buffer full"

**解决**: 增大 `ring_buffer_size`
```bash
-p sharedkv.ring_buffer_size=2048  # 或更大
```

#### 问题 3: 内存占用过高

**症状**: `top` 显示进程内存 > 1 GB

**解决**: 减小队列深度
```bash
-p sharedkv.queue_depth=2048      # 从 4096 降低
-p sharedkv.ring_buffer_size=512  # 从 1024 降低
```

---

## 参数组合推荐

| 工作负载       | Threads | Clients | Workers | Queue Depth | Ring Buffer | 预期吞吐量    |
|----------------|---------|---------|---------|-------------|-------------|---------------|
| 轻量级测试     | 2-4     | 4       | 2       | 1024        | 256         | 50K ops/sec   |
| 开发调试       | 8       | 8       | 8       | 2048        | 512         | 200K ops/sec  |
| 生产环境(小)   | 16      | 16      | 16      | 4096        | 1024        | 500K ops/sec  |
| 生产环境(中)   | 32      | 32      | 32      | 8192        | 2048        | 1M ops/sec    |
| 生产环境(大)   | 64      | 64      | 64      | 16384       | 4096        | 2M+ ops/sec   |

---

## 向后兼容性

如果不指定新参数，使用默认值:
- `sharedkv.queue_depth`: 4096
- `sharedkv.ring_buffer_size`: 1024

旧的测试脚本和命令无需修改即可继续工作。

---

## 未来扩展

### 计划支持的参数

1. **`sharedkv.worker_batch_size`**: Worker 批量处理请求数量
2. **`sharedkv.synchronizer_threads`**: 多线程 Synchronizer
3. **`sharedkv.adaptive_tuning`**: 自动调优队列大小

### 动态调整（未实现）

未来可能支持运行时动态调整:
```java
// 伪代码
ctx.setQueueDepth(8192);
ctx.setRingBufferSize(2048);
```

---

## 总结

通过这些可配置参数，你可以:
✅ 根据工作负载动态调整性能特性
✅ 在延迟、吞吐、内存之间进行权衡
✅ 无需重新编译即可测试不同配置
✅ 通过脚本快速进行 A/B 测试

**建议**: 从默认值开始，根据性能指标逐步调优。
