#!/bin/bash

# Test CPU pinning with different thread configurations

CLASSPATH="sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar:core/target/core-0.18.0-SNAPSHOT.jar:$(find core/target/dependency -name '*.jar' | tr '\n' ':')"

NUMA_NODE=3
RECORDCOUNT=100
OPCOUNT=100

echo "=========================================="
echo "CPU Pinning Test"
echo "=========================================="
echo ""

# Test 1: 4 workers, 4 Java threads
echo "Test 1: 4 workers + 4 Java threads"
echo "Expected CPU allocation:"
echo "  - Workers: CPU 2, 3, 4, 5"
echo "  - Synchronizer: CPU 0 (fixed)"
echo "  - Poller: CPU 1 (fixed)"
echo "  - Java threads: CPU 6, 7, 8, 9"
echo ""

./sharedkv/native/scripts/clean_shm.sh > /dev/null 2>&1

java -Djava.library.path=/usr/lib \
  -cp "$CLASSPATH" \
  site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workloadc \
  -p sharedkv.mode=cxl \
  -p sharedkv.threading=multi \
  -p sharedkv.numa_node=$NUMA_NODE \
  -p sharedkv.num_clients=4 \
  -p sharedkv.num_workers=4 \
  -p sharedkv.queue_depth=512 \
  -p sharedkv.ring_buffer_size=512 \
  -threads 4 \
  -load \
  -p recordcount=$RECORDCOUNT 2>&1 | grep -E "(Worker.*pinned|Synchronizer.*pinned|Poller.*pinned|assigned client_id.*pinned|Return=)"

echo ""
echo "=========================================="
echo ""

# Test 2: 8 workers, 8 Java threads
echo "Test 2: 8 workers + 8 Java threads"
echo "Expected CPU allocation:"
echo "  - Workers: CPU 2-9"
echo "  - Synchronizer: CPU 0 (fixed)"
echo "  - Poller: CPU 1 (fixed)"
echo "  - Java threads: CPU 10-17"
echo ""

./sharedkv/native/scripts/clean_shm.sh > /dev/null 2>&1

java -Djava.library.path=/usr/lib \
  -cp "$CLASSPATH" \
  site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workloadc \
  -p sharedkv.mode=cxl \
  -p sharedkv.threading=multi \
  -p sharedkv.numa_node=$NUMA_NODE \
  -p sharedkv.num_clients=8 \
  -p sharedkv.num_workers=8 \
  -p sharedkv.queue_depth=512 \
  -p sharedkv.ring_buffer_size=512 \
  -threads 8 \
  -load \
  -p recordcount=$RECORDCOUNT 2>&1 | grep -E "(Worker.*pinned|Synchronizer.*pinned|Poller.*pinned|assigned client_id.*pinned|Return=)"

echo ""
echo "=========================================="
echo "CPU Pinning Test Complete"
echo "=========================================="
