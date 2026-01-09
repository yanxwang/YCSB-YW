#!/bin/bash

# Verify CPU pinning with actual workload configuration

CLASSPATH="sharedkv/target/sharedkv-binding-0.18.0-SNAPSHOT.jar:core/target/core-0.18.0-SNAPSHOT.jar:$(find core/target/dependency -name '*.jar' | tr '\n' ':')"

# Run workload in background
java -Djava.library.path=/usr/lib \
  -cp "$CLASSPATH" \
  site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workloadc \
  -p sharedkv.mode=cxl \
  -p sharedkv.threading=multi \
  -p sharedkv.numa_node=3 \
  -p sharedkv.num_clients=16 \
  -p sharedkv.num_workers=16 \
  -threads 16 \
  -load \
  -p recordcount=100000 > /tmp/workload_output.log 2>&1 &

WORKLOAD_PID=$!
echo "Workload started with PID: $WORKLOAD_PID"

# Wait for initialization
sleep 5

# Find Java process
JAVA_PID=$(pgrep -f "ycsb.*SharedKV" | head -1)

if [ -z "$JAVA_PID" ]; then
    echo "ERROR: No YCSB Java process found"
    exit 1
fi

echo "Java PID: $JAVA_PID"
echo ""
echo "==================================="
echo "Thread CPU Affinity Distribution"
echo "==================================="
echo ""

# Show CPU distribution
echo "CPU Distribution Summary:"
ps -eLo psr,comm -p $JAVA_PID 2>/dev/null | grep -v PSR | sort -n | uniq -c

echo ""
echo "==================================="
echo "Native Thread Details (first 30)"
echo "==================================="
ps -eLo pid,tid,psr,comm -p $JAVA_PID 2>/dev/null | head -30

# Let it run for a bit
echo ""
echo "Letting workload run for 10 seconds..."
sleep 10

echo ""
echo "==================================="
echo "Thread count: $(ps -T -p $JAVA_PID | wc -l)"
echo "==================================="

# Check pinning messages from stderr
echo ""
echo "==================================="
echo "Pinning Messages from stderr:"
echo "==================================="
grep '\[PIN\]' /tmp/workload_output.log | head -20

# Cleanup
kill $WORKLOAD_PID 2>/dev/null
wait $WORKLOAD_PID 2>/dev/null

echo ""
echo "Test complete."
