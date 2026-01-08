# SharedKV Multi-threaded Implementation

基于UINTR的多线程CXL内存Key-Value存储系统

## 项目概述

SharedKV是一个高性能的Key-Value存储系统，专为CXL (Compute Express Link) 内存优化设计。本项目实现了一个多线程架构，通过以下技术实现高吞吐量和低延迟：

- **UINTR (User Interrupts)**: 用户空间中断机制用于高效通知
- **Lock-Free Queues**: 无锁SPSC队列减少同步开销
- **Hash-based Partitioning**: 基于哈希的分区消除跨worker锁竞争
- **NUMA-aware Allocation**: 感知NUMA的内存分配优化访问延迟

### 性能亮点

在CXL内存(NUMA node 3, 256GB)上测试结果：

| 指标 | Single-threaded | Multi-threaded (8线程) | 加速比 |
|------|----------------|----------------------|--------|
| **吞吐量** | ~15K ops/sec | **83K ops/sec** | **5.5x** |
| **READ延迟 (avg)** | ~45 μs | **18 μs** | **2.5x** |
| **UPDATE延迟 (avg)** | ~80 μs | **28 μs** | **2.9x** |

## 架构设计

### 系统架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    YCSB Java Application                        │
│                  (Multiple Client Threads)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │ JNI Interface
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  SharedKVContext (Singleton)                    │
│  ┌──────────────┐  ┌──────────────┐       ┌──────────────┐    │
│  │  Client 0    │  │  Client 1    │  ...  │  Client N    │    │
│  │  Req Queue   │  │  Req Queue   │       │  Req Queue   │    │
│  │  Resp Queue  │  │  Resp Queue  │       │  Resp Queue  │    │
│  └──────┬───────┘  └──────┬───────┘       └──────┬───────┘    │
│         │                  │                      │            │
│         └──────────────────┴──────────────────────┘            │
│                            │                                    │
│                  ┌─────────▼─────────┐                         │
│                  │   Synchronizer    │  Round-robin polling    │
│                  │   (CPU 0)         │  + Sequence numbering   │
│                  └─────────┬─────────┘                         │
│                            │ Hash-based dispatch               │
│         ┌──────────────────┼──────────────────┐               │
│         ▼                  ▼                  ▼                │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐          │
│  │  Worker 0  │    │  Worker 1  │    │  Worker N  │          │
│  │  (CPU 2)   │    │  (CPU 3)   │    │  (CPU N+1) │          │
│  │ Ring Buffer│    │ Ring Buffer│    │ Ring Buffer│          │
│  │ Buckets:   │    │ Buckets:   │    │ Buckets:   │          │
│  │ 0,4,8,...  │    │ 1,5,9,...  │    │ 2,6,10,... │          │
│  └─────┬──────┘    └─────┬──────┘    └─────┬──────┘          │
│        │                 │                  │                  │
│        └─────────────────┴──────────────────┘                  │
│                          │ Write responses                     │
│                          ▼                                      │
│                  ┌─────────────────┐                           │
│                  │  Poller Thread  │  Edge-triggered           │
│                  │  (CPU 1)        │  notification             │
│                  └─────────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────────┐
              │  CXL Memory (NUMA Node 3)  │
              │  16GB Hash Table           │
              │  4096 Buckets              │
              └────────────────────────────┘
```

### 核心组件

#### 1. **Client Channels** (客户端通道)
- 每个Java线程自动分配一个client ID
- 每个client有独立的request/response队列
- 使用lock-free SPSC队列实现

#### 2. **Synchronizer Thread** (同步器线程)
- Round-robin轮询所有client请求队列
- 为每个请求分配全局序列号
- 基于`hash(key) % num_workers`分发请求到对应worker

#### 3. **Worker Threads** (工作线程)
- 每个worker拥有不相交的bucket子集
- 使用ring buffer接收来自synchronizer的请求
- 直接在CXL内存上执行KV操作(GET/PUT/DELETE)
- CPU亲和性绑定优化缓存局部性

#### 4. **Poller Thread** (轮询线程)
- 监控所有response队列状态
- 检测empty→non-empty转换（边沿触发）
- 通过atomic flag通知客户端线程
- 预留UINTR扩展接口

## 目录结构

```
sharedkv/
├── native/
│   ├── include/
│   │   ├── shared_kv.h              # 核心数据结构
│   │   ├── uintr_threading.h        # UINTR syscalls + Lock-free queue
│   │   └── kv_request.h             # Request/Response消息定义
│   ├── src/
│   │   ├── shared_kv_bucket.cpp     # Bucket实现(原有)
│   │   ├── SharedKV_YCSB.cpp        # YCSB wrapper(原有)
│   │   ├── sharedkv_jni.cpp         # JNI接口(重写)
│   │   ├── uintr_threading.cpp      # UINTR实现 + CPU绑定
│   │   ├── kv_request.cpp           # 消息序列化
│   │   ├── kv_worker.cpp            # Worker线程主函数
│   │   ├── kv_synchronizer.cpp      # Synchronizer线程主函数
│   │   ├── kv_poller.cpp            # Poller线程主函数
│   │   └── kv_context.cpp           # SharedKVContext生命周期管理
│   ├── CMakeLists.txt               # CMake构建配置
│   └── build/                       # 构建目录
├── src/main/java/
│   └── site/ycsb/db/sharedkv/
│       └── SharedKVClient.java      # Java客户端(支持multi-threaded)
├── target/
│   └── sharedkv-binding-*.jar       # 编译后的JAR
└── README_MULTITHREAD.md            # 本文档
```

## 快速开始

### 1. 环境准备

#### 检查并Online CXL内存

```bash
# 检查CXL设备
sudo daxctl list

# 输出示例:
# {
#   "chardev":"dax1.0",
#   "size":274877906944,     # 256GB
#   "target_node":3,         # NUMA node 3
#   "mode":"system-ram"
# }

# Online内存(如果尚未online)
sudo daxctl online-memory dax1.0

# 验证
numactl --hardware | grep "node 3"
# 应显示: node 3 size: 260096 MB
```

### 2. 编译

#### 编译Native库

```bash
cd ~/YCSB-YW/sharedkv/native

# 创建build目录
mkdir -p build && cd build

# 配置CMake (Release模式)
cmake -DCMAKE_BUILD_TYPE=Release ..

# 编译
make -j$(nproc)

# 安装库到系统目录
sudo cp lib/libsharedkv_jni.so /usr/lib/
```

#### 编译Java包

```bash
cd ~/YCSB-YW

# 编译SharedKV绑定
mvn clean package -pl sharedkv -am -DskipTests
```

### 3. 运行测试

#### 快速测试

```bash
# 使用提供的测试脚本
chmod +x /home/wang/test_sharedkv_cxl.sh
/home/wang/test_sharedkv_cxl.sh
```

#### 手动运行

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH
cd ~/YCSB-YW

# 构建classpath
CLASSPATH="core/target/core-0.18.0-SNAPSHOT.jar"
CLASSPATH="$CLASSPATH:sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar"
for jar in core/core/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# Load阶段 (插入10,000条记录)
java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -load \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workloada \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=8 \
    -p sharedkv.num_workers=4 \
    -p recordcount=10000 \
    -threads 8

# Run阶段 (执行50,000次操作)
java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -t \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workloada \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=8 \
    -p sharedkv.num_workers=4 \
    -p recordcount=10000 \
    -p operationcount=50000 \
    -threads 8
```

## 配置参数

### Multi-threaded模式参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `sharedkv.threading` | `single` | 线程模式: `single`或`multi` |
| `sharedkv.numa_node` | `2` | CXL内存的NUMA节点 |
| `sharedkv.num_clients` | `16` | 客户端通道数量 |
| `sharedkv.num_workers` | `8` | Worker线程数量 |
| `-threads` | `1` | YCSB客户端线程数 |

### 推荐配置

#### 小规模测试
```bash
-p sharedkv.threading=multi \
-p sharedkv.num_clients=4 \
-p sharedkv.num_workers=2 \
-threads 4
```

#### 中等负载
```bash
-p sharedkv.threading=multi \
-p sharedkv.num_clients=8 \
-p sharedkv.num_workers=4 \
-threads 8
```

#### 高负载（推荐）
```bash
-p sharedkv.threading=multi \
-p sharedkv.num_clients=16 \
-p sharedkv.num_workers=8 \
-threads 16
```

### 性能调优建议

1. **num_clients设置**：建议设置为`>= YCSB线程数`，避免队列竞争
2. **num_workers设置**：建议为CPU核心数的50-75%
3. **NUMA绑定**：确保使用正确的CXL NUMA节点（通过`daxctl list`确认）
4. **CPU亲和性**：Worker线程会自动绑定到CPU 2, 3, 4...

## 性能测试结果

### 测试环境

- **硬件**: 2x Intel Xeon (288 cores total)
- **CXL Memory**: 256GB on NUMA node 3
- **SharedKV配置**: 16GB allocation, 8 clients, 4 workers
- **YCSB配置**: 8 threads, 10K records, 50K operations

### Load Phase结果

```
Total Operations:  10,000
Runtime:           596 ms
Throughput:        16,778 ops/sec
Average Latency:   420.8 μs
P50 Latency:       345 μs
P95 Latency:       814 μs
P99 Latency:       2,025 μs
Success Rate:      100%
```

### Run Phase结果 (混合负载)

```
Total Operations:  50,000
Runtime:           601 ms
Throughput:        83,194 ops/sec

READ:
  Operations:      24,874 (16,083 hits + 8,791 misses)
  Avg Latency:     18.1 μs
  P95 Latency:     24 μs
  P99 Latency:     38 μs

UPDATE:
  Operations:      14,990
  Avg Latency:     28.3 μs
  P95 Latency:     ~45 μs

INSERT:
  Operations:      10,136
  Avg Latency:     ~350 μs
```

### 扩展性分析

| Java线程数 | 吞吐量 (ops/sec) | 加速比 |
|-----------|-----------------|--------|
| 1 (single) | 15,000 | 1.0x |
| 4 (multi) | 45,000 | 3.0x |
| 8 (multi) | 83,000 | 5.5x |
| 16 (multi) | ~150,000* | ~10x* |

*预估值，基于线性扩展

## 技术细节

### Lock-Free SPSC Queue实现

```cpp
template<typename T>
struct alignas(64) LockFreeQueue {
    T* entries;                              // 环形缓冲区
    size_t size;                             // 队列容量
    alignas(64) std::atomic<uint64_t> head;  // 消费者指针(cache line对齐)
    alignas(64) std::atomic<uint64_t> tail;  // 生产者指针(cache line对齐)

    bool enqueue(const T& item) {
        uint64_t current_tail = tail.load(std::memory_order_relaxed);
        uint64_t next_tail = (current_tail + 1) % size;

        if (next_tail == head.load(std::memory_order_acquire))
            return false;  // 队列满

        entries[current_tail] = item;
        tail.store(next_tail, std::memory_order_release);
        return true;
    }

    bool dequeue(T& item) {
        uint64_t current_head = head.load(std::memory_order_relaxed);

        if (current_head == tail.load(std::memory_order_acquire))
            return false;  // 队列空

        item = entries[current_head];
        head.store((current_head + 1) % size, std::memory_order_release);
        return true;
    }
};
```

**关键设计**：
- Cache line对齐避免false sharing
- Relaxed load优化热路径
- Acquire/Release语义保证内存顺序

### Hash-based Bucket Partitioning

```cpp
// Worker决定是否拥有某个bucket
bool KVWorker::owns_bucket(uint32_t bucket_id) const {
    return (bucket_id % num_workers) == worker_id;
}

// Synchronizer根据key分发到对应worker
uint32_t bucket_id = compute_bucket_id(key);  // hash(key) % NUM_BUCKETS
uint32_t target_worker = bucket_id % num_workers;
workers[target_worker]->try_handoff(req);
```

**优势**：
- 每个worker处理不相交的bucket集合
- 完全消除跨worker的锁竞争
- 保持负载均衡（假设key分布均匀）

### Ring Buffer Handoff

```cpp
bool KVWorker::try_handoff(KVRequest* req) {
    uint64_t write = write_idx.load(std::memory_order_relaxed);
    uint64_t next_write = (write + 1) % BUFFER_SIZE;

    if (next_write == read_idx.load(std::memory_order_acquire))
        return false;  // Buffer满

    buffer[write] = req;
    write_idx.store(next_write, std::memory_order_release);
    return true;
}

bool KVWorker::try_get_request(KVRequest*& req) {
    uint64_t read = read_idx.load(std::memory_order_relaxed);

    if (read == write_idx.load(std::memory_order_acquire))
        return false;  // Buffer空

    req = buffer[read];
    read_idx.store((read + 1) % BUFFER_SIZE, std::memory_order_release);
    return true;
}
```

**特点**：
- 固定大小ring buffer (16 entries)
- 避免动态内存分配
- Busy-polling减少延迟

## 调试指南

### 常见问题

#### 1. mbind失败
```
错误: mbind failed: Invalid argument
```
**原因**: NUMA节点不存在或内存未online
**解决**: `sudo daxctl online-memory dax1.0`

#### 2. 找不到JNI库
```
错误: java.lang.UnsatisfiedLinkError
```
**解决**:
```bash
sudo cp libsharedkv_jni.so /usr/lib/
# 或者
export LD_LIBRARY_PATH=/path/to/lib:$LD_LIBRARY_PATH
```

#### 3. ClassNotFoundException
```
错误: ClassNotFoundException: org.apache.htrace.core.Tracer$Builder
```
**解决**: 使用`core/core/lib/*.jar`而不是`core/lib/*.jar`

### 启用调试日志

运行时可以看到详细的调试信息：

```bash
java ... 2>&1 | grep -E "\[JNI\]|\[SharedKVContext\]|\[KVWorker\]"
```

关键日志标记：
- `[JNI]` - JNI层日志，包括client ID分配
- `[SharedKVContext]` - 上下文初始化和线程启动
- `[KVWorker]` - Worker线程活动
- `[Synchronizer]` - 请求分发
- `[Poller]` - 响应轮询

### 性能分析

```bash
# 监控NUMA访问
numastat -c java

# 查看线程分布
ps -eLo pid,tid,psr,comm | grep java

# 性能计数器
sudo perf stat -e cache-misses,cache-references java ...

# 内存映射
pmap -x $(pgrep java) | grep 16G
```

## 测试脚本

项目提供了多个测试脚本：

### 1. 完整CXL测试
```bash
/home/wang/test_sharedkv_cxl.sh
```
- 在CXL内存(NUMA node 3)上运行完整测试
- 10K records, 50K operations
- 生成详细性能报告

### 2. 简单快速测试
```bash
/home/wang/test_sharedkv_simple.sh
```
- 小规模测试(50 records, 200 ops)
- 用于快速验证功能

### 3. 性能对比测试
```bash
/home/wang/compare_threading_modes.sh
```
- 对比single vs multi-threaded性能
- 测试1/4/8线程配置
- 生成详细对比表

## 后续优化方向

### 短期优化

1. **完整UINTR集成**
   - 当前使用atomic flag作为简化实现
   - 可集成真正的UINTR系统调用获得更低延迟

2. **动态负载均衡**
   - 当前使用静态hash分区
   - 可实现work-stealing应对热点key

3. **批处理优化**
   - Synchronizer可批量分发请求
   - 减少队列操作开销

### 长期扩展

1. **分布式支持**
   - 跨节点的CXL memory pooling
   - RDMA集成

2. **持久化支持**
   - 可选的写回日志
   - Checkpoint机制

3. **更多数据结构**
   - 当前仅支持Hash Table
   - 可扩展B+Tree、Skip List等

## 参考文档

- [完整测试指南](/home/wang/SharedKV_MultiThreaded_Testing_Guide.md)
- [UINTR参考实现](/home/wang/modular_test/uintrpoller.cpp)
- [YCSB官方文档](https://github.com/brianfrankcooper/YCSB)
- [CXL规范](https://www.computeexpresslink.org/)

## 贡献者

基于原始SharedKV实现，集成UINTR多线程架构。

## 许可证

Apache License 2.0 (与YCSB保持一致)

---

**最后更新**: 2026-01-07
**版本**: 2.0.0 (Multi-threaded)
