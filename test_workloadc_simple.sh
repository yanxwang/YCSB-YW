#!/bin/bash

# Simple test for Workload C load and run phases

CLASSPATH="sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar:core/target/core-0.18.0-SNAPSHOT.jar:$(find core/target/dependency -name '*.jar' | tr '\n' ':')"

NUMA_NODE=3
THREADS=2
NUM_CLIENTS=2
NUM_WORKERS=2
QUEUE_DEPTH=512
RING_BUFFER_SIZE=512
RECORDCOUNT=1000
OPCOUNT=1000

echo "===================="
echo "Cleaning shared memory..."
echo "===================="
./sharedkv/native/scripts/clean_shm.sh

echo ""
echo "===================="
echo "LOAD PHASE"
echo "===================="
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
  -load \
  -p recordcount=$RECORDCOUNT 2>&1 | tee /tmp/load.log

LOAD_EXIT=$?
echo "Load phase exit code: $LOAD_EXIT"

if [ $LOAD_EXIT -ne 0 ]; then
    echo "ERROR: Load phase failed!"
    exit 1
fi

echo ""
echo "Checking shared memory..."
ls -lh /dev/shm/sharedkv* 2>&1

echo ""
echo "===================="
echo "RUN PHASE"
echo "===================="
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
  -p operationcount=$OPCOUNT 2>&1 | tee /tmp/run.log

RUN_EXIT=$?
echo "Run phase exit code: $RUN_EXIT"

echo ""
echo "===================="
echo "RESULTS"
echo "===================="
echo "Load phase:"
grep "Return=" /tmp/load.log | grep INSERT
echo ""
echo "Run phase:"
grep "Return=" /tmp/run.log | grep READ

echo ""
echo "Test complete!"
