#!/bin/bash

# ============================================================================
# SharedKV Multi-threaded Test on CXL Memory (NUMA node 3)
# ============================================================================

set -e  # Exit on error

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
echo "SharedKV Multi-threaded Test on CXL Memory"
echo "============================================================================"
echo ""
echo "Configuration:"
echo "  - NUMA node: 3 (CXL Memory, 256GB)"
echo "  - Memory size: 16GB allocated"
echo "  - Clients: 8"
echo "  - Workers: 4"
echo "  - Java threads: 8"
echo "  - Records: 10,000"
echo "  - Operations: 50,000"
echo ""

# Verify CXL memory is available
echo "Verifying CXL memory availability..."
NUMA3_SIZE=$(numactl --hardware | grep "node 3 size" | awk '{print $4}')
if [ "$NUMA3_SIZE" -lt 100000 ]; then
    echo "ERROR: NUMA node 3 has insufficient memory ($NUMA3_SIZE MB)"
    echo "Please ensure CXL memory is online: sudo daxctl online-memory dax1.0"
    exit 1
fi
echo "✓ NUMA node 3 available with $NUMA3_SIZE MB"
echo ""

# Test 1: Load phase
echo "============================================================================"
echo "Test 1: Load Phase - Inserting 10,000 records"
echo "============================================================================"
START_TIME=$(date +%s)

timeout 120 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -load \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=8 \
    -p sharedkv.num_workers=4 \
    -p recordcount=10000 \
    -threads 8 \
    2>&1 | tee /tmp/sharedkv_load.log

LOAD_RC=$?
END_TIME=$(date +%s)
LOAD_TIME=$((END_TIME - START_TIME))

echo ""
if [ $LOAD_RC -eq 0 ]; then
    echo "✓ Load phase completed successfully in ${LOAD_TIME}s"
    LOAD_OPS=$(grep "Throughput(ops/sec)" /tmp/sharedkv_load.log | awk '{print $3}')
    echo "  Throughput: $LOAD_OPS ops/sec"
else
    echo "✗ Load phase failed with exit code $LOAD_RC"
    exit 1
fi

# Extract load statistics
echo ""
echo "Load Phase Statistics:"
grep -E "\[INSERT\].*Latency" /tmp/sharedkv_load.log | head -10

echo ""
echo "============================================================================"
echo "Test 2: Run Phase - Mixed workload (50,000 operations)"
echo "============================================================================"
START_TIME=$(date +%s)

timeout 120 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -t \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=3 \
    -p sharedkv.num_clients=8 \
    -p sharedkv.num_workers=4 \
    -p recordcount=10000 \
    -p operationcount=50000 \
    -threads 8 \
    2>&1 | tee /tmp/sharedkv_run.log

RUN_RC=$?
END_TIME=$(date +%s)
RUN_TIME=$((END_TIME - START_TIME))

echo ""
if [ $RUN_RC -eq 0 ]; then
    echo "✓ Run phase completed successfully in ${RUN_TIME}s"
    RUN_OPS=$(grep "Throughput(ops/sec)" /tmp/sharedkv_run.log | awk '{print $3}')
    echo "  Throughput: $RUN_OPS ops/sec"
else
    echo "✗ Run phase failed with exit code $RUN_RC"
    exit 1
fi

# Extract run statistics
echo ""
echo "Run Phase Statistics:"
echo "-------------------"
grep -E "\[READ\].*Operations" /tmp/sharedkv_run.log
grep -E "\[READ\].*AverageLatency" /tmp/sharedkv_run.log
grep -E "\[READ\].*95thPercentileLatency" /tmp/sharedkv_run.log
echo ""
grep -E "\[UPDATE\].*Operations" /tmp/sharedkv_run.log
grep -E "\[UPDATE\].*AverageLatency" /tmp/sharedkv_run.log
grep -E "\[UPDATE\].*95thPercentileLatency" /tmp/sharedkv_run.log
echo ""
grep -E "\[INSERT\].*Operations" /tmp/sharedkv_run.log
grep -E "\[INSERT\].*AverageLatency" /tmp/sharedkv_run.log
grep -E "\[INSERT\].*95thPercentileLatency" /tmp/sharedkv_run.log

echo ""
echo "============================================================================"
echo "All Tests Passed!"
echo "============================================================================"
echo "Summary:"
echo "  - Load phase: ${LOAD_TIME}s, $LOAD_OPS ops/sec"
echo "  - Run phase:  ${RUN_TIME}s, $RUN_OPS ops/sec"
echo ""
echo "Log files:"
echo "  - Load: /tmp/sharedkv_load.log"
echo "  - Run:  /tmp/sharedkv_run.log"
echo ""
