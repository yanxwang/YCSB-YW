#!/bin/bash

# Test script for SharedKV multi-threaded mode

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH

cd /home/wang/YCSB-YW

echo "=== Testing SharedKV Multi-threaded Mode ==="
echo "Configuration:"
echo "  - Threading: multi"
echo "  - NUMA node: 2"
echo "  - Clients: 4"
echo "  - Workers: 2"
echo "  - Java threads: 4"
echo ""

# Build classpath
CLASSPATH="core/target/core-0.18.0-SNAPSHOT.jar"
CLASSPATH="$CLASSPATH:sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar"
for jar in core/core/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

echo "=== Phase 1: Load (insert 100 records) ==="
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -load \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=2 \
    -p sharedkv.num_clients=4 \
    -p sharedkv.num_workers=2 \
    -p recordcount=100 \
    -threads 4 \
    -s 2>&1

echo ""
echo "=== Phase 2: Run (mixed operations on 100 records) ==="
timeout 60 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -t \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=2 \
    -p sharedkv.num_clients=4 \
    -p sharedkv.num_workers=2 \
    -p recordcount=100 \
    -p operationcount=500 \
    -threads 4 \
    -s 2>&1

echo ""
echo "=== Test Complete ==="
