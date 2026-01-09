#!/bin/bash

# Test script to diagnose SharedKV multi-threaded performance

CLASSPATH="sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar:core/target/core-0.18.0-SNAPSHOT.jar:$(find core/target/dependency -name '*.jar' | tr '\n' ':')"

echo "=========================================="
echo "SharedKV Performance Test"
echo "=========================================="

# Test configuration
NUMA_NODE=3
THREADS=16
NUM_CLIENTS=16
NUM_WORKERS=16
QUEUE_DEPTH=2048
RING_BUFFER_SIZE=1024
RECORDCOUNT=100000
OPCOUNT=100000

echo "Configuration:"
echo "  Threads: $THREADS"
echo "  Clients: $NUM_CLIENTS"
echo "  Workers: $NUM_WORKERS"
echo "  Queue Depth: $QUEUE_DEPTH"
echo "  Ring Buffer Size: $RING_BUFFER_SIZE"
echo "  RecordCount: $RECORDCOUNT"
echo "  OperationCount: $OPCOUNT"
echo ""

# Load phase
echo "=========================================="
echo "LOAD PHASE (Workload A)"
echo "=========================================="
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
  -p sharedkv.queue_depth=$QUEUE_DEPTH \
  -p sharedkv.ring_buffer_size=$RING_BUFFER_SIZE \
  -threads $THREADS \
  -load \
  -p recordcount=$RECORDCOUNT \
  2>&1 | grep -E "Throughput|AverageLatency|95thPercentileLatency|99thPercentileLatency|Return=" | grep INSERT

echo ""
echo "=========================================="
echo "RUN PHASE (Workload A - 50% Read + 50% Update)"
echo "=========================================="
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
  -p sharedkv.queue_depth=$QUEUE_DEPTH \
  -p sharedkv.ring_buffer_size=$RING_BUFFER_SIZE \
  -threads $THREADS \
  -t \
  -p recordcount=$RECORDCOUNT \
  -p operationcount=$OPCOUNT \
  2>&1 | grep -E "Throughput|AverageLatency|95thPercentileLatency|99thPercentileLatency|Return=" | grep -E "READ|UPDATE|OVERALL"

echo ""
echo "=========================================="
echo "RUN PHASE (Workload C - 100% Read)"
echo "=========================================="
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
  -p sharedkv.queue_depth=$QUEUE_DEPTH \
  -p sharedkv.ring_buffer_size=$RING_BUFFER_SIZE \
  -threads $THREADS \
  -t \
  -p recordcount=$RECORDCOUNT \
  -p operationcount=$OPCOUNT \
  2>&1 | grep -E "Throughput|AverageLatency|95thPercentileLatency|99thPercentileLatency|Return=" | grep -E "READ|OVERALL"

echo ""
echo "=========================================="
echo "Test Complete"
echo "=========================================="
