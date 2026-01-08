#!/bin/bash

# Simple test for SharedKV multi-threaded mode on NUMA node 0

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH

cd /home/wang/YCSB-YW

echo "=== SharedKV Multi-threaded Quick Test ==="
echo "Using NUMA node 0, 4 clients, 2 workers, 4 threads"
echo ""

# Build classpath
CLASSPATH="core/target/core-0.18.0-SNAPSHOT.jar"
CLASSPATH="$CLASSPATH:sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar"
for jar in core/core/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

echo "=== Load Phase: Inserting 50 records ==="
timeout 30 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -load \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=0 \
    -p sharedkv.num_clients=4 \
    -p sharedkv.num_workers=2 \
    -p recordcount=50 \
    -threads 4 \
    2>&1

RC=$?
echo ""
if [ $RC -eq 0 ]; then
    echo "✓ Load phase completed successfully"
else
    echo "✗ Load phase failed with exit code $RC"
fi

echo ""
echo "=== Run Phase: Mixed operations on 50 records ==="
timeout 30 java -cp "$CLASSPATH" \
    -Djava.library.path=/usr/lib \
    site.ycsb.Client \
    -t \
    -db site.ycsb.db.sharedkv.SharedKVClient \
    -P workloads/workload_sharedkv_multithread_test \
    -p sharedkv.threading=multi \
    -p sharedkv.numa_node=0 \
    -p sharedkv.num_clients=4 \
    -p sharedkv.num_workers=2 \
    -p recordcount=50 \
    -p operationcount=200 \
    -threads 4 \
    2>&1

RC=$?
echo ""
if [ $RC -eq 0 ]; then
    echo "✓ Run phase completed successfully"
else
    echo "✗ Run phase failed with exit code $RC"
fi

echo ""
echo "=== Test Complete ==="
