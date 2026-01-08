#!/bin/bash

# ============================================================================
# SharedKV Threading Mode Comparison Test
# 对比Multi-threaded模式和Single-threaded模式的性能
# ============================================================================

set -e

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH

cd /home/wang/YCSB-YW

# Build classpath
CLASSPATH="core/target/core-0.18.0-SNAPSHOT.jar"
CLASSPATH="$CLASSPATH:sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar"
for jar in core/core/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

echo "============================================================================"
echo "SharedKV Threading Mode Performance Comparison"
echo "============================================================================"
echo ""
echo "测试配置:"
echo "  - 数据集: 5,000 records"
echo "  - 操作数: 25,000 operations"
echo "  - NUMA node: 3 (CXL Memory)"
echo ""

RECORDCOUNT=5000
OPCOUNT=25000

# ============================================================================
# Test 1: Single-threaded Mode
# ============================================================================
echo "============================================================================"
echo "Test 1: Single-threaded Mode (1 thread)"
echo "============================================================================"

echo "Load phase..."
START=$(date +%s)
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -load \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=single \
    -p sharedkv.numa_node=3 \
    -p recordcount=$RECORDCOUNT \
    -threads 1 \
    2>&1 | tee /tmp/single_load.log | grep -E "(Throughput|AverageLatency|RunTime)"

END=$(date +%s)
SINGLE_LOAD_TIME=$((END - START))

echo ""
echo "Run phase..."
START=$(date +%s)
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -t \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=single \
    -p sharedkv.numa_node=3 \
    -p recordcount=$RECORDCOUNT \
    -p operationcount=$OPCOUNT \
    -threads 1 \
    2>&1 | tee /tmp/single_run.log | grep -E "(Throughput|AverageLatency|RunTime)"

END=$(date +%s)
SINGLE_RUN_TIME=$((END - START))

SINGLE_LOAD_THROUGHPUT=$(grep "Throughput" /tmp/single_load.log | awk '{print $3}')
SINGLE_RUN_THROUGHPUT=$(grep "Throughput" /tmp/single_run.log | awk '{print $3}')

echo ""
echo "Single-threaded 结果:"
echo "  Load: ${SINGLE_LOAD_TIME}s, ${SINGLE_LOAD_THROUGHPUT} ops/sec"
echo "  Run:  ${SINGLE_RUN_TIME}s, ${SINGLE_RUN_THROUGHPUT} ops/sec"

# ============================================================================
# Test 2: Multi-threaded Mode (4 threads)
# ============================================================================
echo ""
echo "============================================================================"
echo "Test 2: Multi-threaded Mode (4 threads, 4 clients, 2 workers)"
echo "============================================================================"

echo "Load phase..."
START=$(date +%s)
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -load \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=4 \
    -p sharedkv.num_workers=2 \
    -p recordcount=$RECORDCOUNT \
    -threads 4 \
    2>&1 | tee /tmp/multi4_load.log | grep -E "(Throughput|AverageLatency|RunTime)"

END=$(date +%s)
MULTI4_LOAD_TIME=$((END - START))

echo ""
echo "Run phase..."
START=$(date +%s)
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -t \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=4 \
    -p sharedkv.num_workers=2 \
    -p recordcount=$RECORDCOUNT \
    -p operationcount=$OPCOUNT \
    -threads 4 \
    2>&1 | tee /tmp/multi4_run.log | grep -E "(Throughput|AverageLatency|RunTime)"

END=$(date +%s)
MULTI4_RUN_TIME=$((END - START))

MULTI4_LOAD_THROUGHPUT=$(grep "Throughput" /tmp/multi4_load.log | awk '{print $3}')
MULTI4_RUN_THROUGHPUT=$(grep "Throughput" /tmp/multi4_run.log | awk '{print $3}')

echo ""
echo "Multi-threaded (4 threads) 结果:"
echo "  Load: ${MULTI4_LOAD_TIME}s, ${MULTI4_LOAD_THROUGHPUT} ops/sec"
echo "  Run:  ${MULTI4_RUN_TIME}s, ${MULTI4_RUN_THROUGHPUT} ops/sec"

# ============================================================================
# Test 3: Multi-threaded Mode (8 threads)
# ============================================================================
echo ""
echo "============================================================================"
echo "Test 3: Multi-threaded Mode (8 threads, 8 clients, 4 workers)"
echo "============================================================================"

echo "Load phase..."
START=$(date +%s)
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -load \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=8 \
    -p sharedkv.num_workers=4 \
    -p recordcount=$RECORDCOUNT \
    -threads 8 \
    2>&1 | tee /tmp/multi8_load.log | grep -E "(Throughput|AverageLatency|RunTime)"

END=$(date +%s)
MULTI8_LOAD_TIME=$((END - START))

echo ""
echo "Run phase..."
START=$(date +%s)
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -t \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=8 \
    -p sharedkv.num_workers=4 \
    -p recordcount=$RECORDCOUNT \
    -p operationcount=$OPCOUNT \
    -threads 8 \
    2>&1 | tee /tmp/multi8_run.log | grep -E "(Throughput|AverageLatency|RunTime)"

END=$(date +%s)
MULTI8_RUN_TIME=$((END - START))

MULTI8_LOAD_THROUGHPUT=$(grep "Throughput" /tmp/multi8_load.log | awk '{print $3}')
MULTI8_RUN_THROUGHPUT=$(grep "Throughput" /tmp/multi8_run.log | awk '{print $3}')

echo ""
echo "Multi-threaded (8 threads) 结果:"
echo "  Load: ${MULTI8_LOAD_TIME}s, ${MULTI8_LOAD_THROUGHPUT} ops/sec"
echo "  Run:  ${MULTI8_RUN_TIME}s, ${MULTI8_RUN_THROUGHPUT} ops/sec"

# ============================================================================
# Performance Comparison Summary
# ============================================================================
echo ""
echo "============================================================================"
echo "性能对比总结"
echo "============================================================================"
echo ""
printf "%-20s | %-15s | %-15s\n" "模式" "Load吞吐量" "Run吞吐量"
printf "%-20s-+-%-15s-+-%-15s\n" "--------------------" "---------------" "---------------"
printf "%-20s | %-15s | %-15s\n" "Single (1 thread)" "$SINGLE_LOAD_THROUGHPUT" "$SINGLE_RUN_THROUGHPUT"
printf "%-20s | %-15s | %-15s\n" "Multi (4 threads)" "$MULTI4_LOAD_THROUGHPUT" "$MULTI4_RUN_THROUGHPUT"
printf "%-20s | %-15s | %-15s\n" "Multi (8 threads)" "$MULTI8_LOAD_THROUGHPUT" "$MULTI8_RUN_THROUGHPUT"

echo ""
echo "扩展性分析:"

# Calculate speedup
if [ -n "$SINGLE_RUN_THROUGHPUT" ] && [ -n "$MULTI4_RUN_THROUGHPUT" ]; then
    SPEEDUP4=$(echo "scale=2; $MULTI4_RUN_THROUGHPUT / $SINGLE_RUN_THROUGHPUT" | bc)
    echo "  4线程加速比:  ${SPEEDUP4}x"
fi

if [ -n "$SINGLE_RUN_THROUGHPUT" ] && [ -n "$MULTI8_RUN_THROUGHPUT" ]; then
    SPEEDUP8=$(echo "scale=2; $MULTI8_RUN_THROUGHPUT / $SINGLE_RUN_THROUGHPUT" | bc)
    echo "  8线程加速比:  ${SPEEDUP8}x"
fi

echo ""
echo "延迟对比 (Run Phase):"
echo ""

# Extract latency information
echo "READ延迟:"
printf "%-20s | %-10s | %-10s | %-10s\n" "模式" "平均(μs)" "P95(μs)" "P99(μs)"
printf "%-20s-+-%-10s-+-%-10s-+-%-10s\n" "--------------------" "----------" "----------" "----------"

SINGLE_READ_AVG=$(grep "\[READ\].*AverageLatency" /tmp/single_run.log | awk '{print $3}')
SINGLE_READ_P95=$(grep "\[READ\].*95thPercentileLatency" /tmp/single_run.log | awk '{print $3}')
SINGLE_READ_P99=$(grep "\[READ\].*99thPercentileLatency" /tmp/single_run.log | awk '{print $3}')
printf "%-20s | %-10s | %-10s | %-10s\n" "Single" "$SINGLE_READ_AVG" "$SINGLE_READ_P95" "$SINGLE_READ_P99"

MULTI4_READ_AVG=$(grep "\[READ\].*AverageLatency" /tmp/multi4_run.log | awk '{print $3}')
MULTI4_READ_P95=$(grep "\[READ\].*95thPercentileLatency" /tmp/multi4_run.log | awk '{print $3}')
MULTI4_READ_P99=$(grep "\[READ\].*99thPercentileLatency" /tmp/multi4_run.log | awk '{print $3}')
printf "%-20s | %-10s | %-10s | %-10s\n" "Multi (4 threads)" "$MULTI4_READ_AVG" "$MULTI4_READ_P95" "$MULTI4_READ_P99"

MULTI8_READ_AVG=$(grep "\[READ\].*AverageLatency" /tmp/multi8_run.log | awk '{print $3}')
MULTI8_READ_P95=$(grep "\[READ\].*95thPercentileLatency" /tmp/multi8_run.log | awk '{print $3}')
MULTI8_READ_P99=$(grep "\[READ\].*99thPercentileLatency" /tmp/multi8_run.log | awk '{print $3}')
printf "%-20s | %-10s | %-10s | %-10s\n" "Multi (8 threads)" "$MULTI8_READ_AVG" "$MULTI8_READ_P95" "$MULTI8_READ_P99"

echo ""
echo "UPDATE延迟:"
printf "%-20s | %-10s | %-10s | %-10s\n" "模式" "平均(μs)" "P95(μs)" "P99(μs)"
printf "%-20s-+-%-10s-+-%-10s-+-%-10s\n" "--------------------" "----------" "----------" "----------"

SINGLE_UPDATE_AVG=$(grep "\[UPDATE\].*AverageLatency" /tmp/single_run.log | awk '{print $3}')
SINGLE_UPDATE_P95=$(grep "\[UPDATE\].*95thPercentileLatency" /tmp/single_run.log | awk '{print $3}')
SINGLE_UPDATE_P99=$(grep "\[UPDATE\].*99thPercentileLatency" /tmp/single_run.log | awk '{print $3}')
printf "%-20s | %-10s | %-10s | %-10s\n" "Single" "$SINGLE_UPDATE_AVG" "$SINGLE_UPDATE_P95" "$SINGLE_UPDATE_P99"

MULTI4_UPDATE_AVG=$(grep "\[UPDATE\].*AverageLatency" /tmp/multi4_run.log | awk '{print $3}')
MULTI4_UPDATE_P95=$(grep "\[UPDATE\].*95thPercentileLatency" /tmp/multi4_run.log | awk '{print $3}')
MULTI4_UPDATE_P99=$(grep "\[UPDATE\].*99thPercentileLatency" /tmp/multi4_run.log | awk '{print $3}')
printf "%-20s | %-10s | %-10s | %-10s\n" "Multi (4 threads)" "$MULTI4_UPDATE_AVG" "$MULTI4_UPDATE_P95" "$MULTI4_UPDATE_P99"

MULTI8_UPDATE_AVG=$(grep "\[UPDATE\].*AverageLatency" /tmp/multi8_run.log | awk '{print $3}')
MULTI8_UPDATE_P95=$(grep "\[UPDATE\].*95thPercentileLatency" /tmp/multi8_run.log | awk '{print $3}')
MULTI8_UPDATE_P99=$(grep "\[UPDATE\].*99thPercentileLatency" /tmp/multi8_run.log | awk '{print $3}')
printf "%-20s | %-10s | %-10s | %-10s\n" "Multi (8 threads)" "$MULTI8_UPDATE_AVG" "$MULTI8_UPDATE_P95" "$MULTI8_UPDATE_P99"

echo ""
echo "============================================================================"
echo "测试完成！"
echo "============================================================================"
echo ""
echo "详细日志文件:"
echo "  - /tmp/single_*.log  (单线程模式)"
echo "  - /tmp/multi4_*.log  (4线程模式)"
echo "  - /tmp/multi8_*.log  (8线程模式)"
echo ""
