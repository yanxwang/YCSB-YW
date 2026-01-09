#!/bin/bash

# Small-scale test for debugging

cd /home/wang/YCSB-YW

CLASSPATH="sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar:core/target/core-0.18.0-SNAPSHOT.jar:$(find core/target/dependency -name '*.jar' 2>/dev/null | tr '\n' ':')"

NUMA_NODE=3
THREADS=2
NUM_CLIENTS=2
NUM_WORKERS=2
RECORDCOUNT=100000
OPCOUNT=100000

echo "=========================================="
echo "Small-scale Performance Test"
echo "=========================================="
echo "Configuration:"
echo "  Threads: $THREADS"
echo "  Clients: $NUM_CLIENTS"
echo "  Workers: $NUM_WORKERS"
echo "  RecordCount: $RECORDCOUNT"
echo "  OperationCount: $OPCOUNT"
echo ""

# Load phase
echo "Running LOAD phase (Workload A)..."
java -Djava.library.path=/usr/lib \
  -cp "$CLASSPATH" \
  site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workloada \
  -p sharedkv.mode=cxl \
  -p sharedkv.threading=multi \
  -p sharedkv.numa_node=$NUMA_NODE \
  -p sharedkv.num_clients=$NUM_CLIENTS \
  -p sharedkv.num_workers=$NUM_WORKERS \
  -threads $THREADS \
  -load \
  -p recordcount=$RECORDCOUNT 2>/dev/null | tee results_workloada_load.txt

echo ""
echo "Results saved to results_workloada_load.txt"
