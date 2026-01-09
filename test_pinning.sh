#!/bin/bash

# Quick test to verify CPU pinning

CLASSPATH="sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar:core/target/core-0.18.0-SNAPSHOT.jar:$(find core/target/dependency -name '*.jar' | tr '\n' ':')"

echo "=========================================="
echo "CPU Pinning Verification Test"
echo "=========================================="

# Small workload to verify pinning
NUMA_NODE=3
THREADS=4
NUM_CLIENTS=8
NUM_WORKERS=4
RECORDCOUNT=10000

echo "Configuration:"
echo "  Java Threads: $THREADS"
echo "  Clients: $NUM_CLIENTS"
echo "  Workers: $NUM_WORKERS"
echo "  RecordCount: $RECORDCOUNT"
echo ""

echo "Running load phase..."
java -Djava.library.path=/usr/lib \
  -cp "$CLASSPATH" \
  site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workloadc \
  -p sharedkv.mode=cxl \
  -p sharedkv.threading=multi \
  -p sharedkv.numa_node=$NUMA_NODE \
  -p sharedkv.num_clients=$NUM_CLIENTS \
  -p sharedkv.num_workers=$NUM_WORKERS \
  -threads $THREADS \
  -load \
  -p recordcount=$RECORDCOUNT
