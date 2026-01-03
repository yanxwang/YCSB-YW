#!/bin/bash

# SharedKV benchmark script - runs both load and transaction phases

NUMA_NODE=${1:-2}
echo "Running SharedKV benchmark on NUMA node $NUMA_NODE"

cd ~/YCSB-YW

echo "=== Phase 1: Loading data ==="
sudo -E java -cp "core/target/core-0.18.0-SNAPSHOT.jar:sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar:core/core/lib/*" \
  -Djava.library.path=/usr/lib \
  site.ycsb.Client \
  -load -t \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workload_sharedkv_test \
  -p sharedkv.cxl=true \
  -p sharedkv.numa_node=$NUMA_NODE \
  -s

echo ""
echo "=== Benchmark Complete ==="
