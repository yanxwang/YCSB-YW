#!/bin/bash

# Monitor SharedKV thread distribution across CPUs

echo "Monitoring SharedKV thread CPU distribution"
echo "Press Ctrl+C to stop"
echo ""

while true; do
    clear
    date
    echo "========================================"

    # Find Java process
    JAVA_PID=$(pgrep -f "ycsb.*SharedKV" | head -1)

    if [ -z "$JAVA_PID" ]; then
        echo "No YCSB process found"
        sleep 1
        continue
    fi

    echo "Process PID: $JAVA_PID"
    echo "========================================"
    echo ""

    # Show thread distribution by CPU
    echo "CPU Distribution:"
    ps -eLo psr,comm -p $JAVA_PID 2>/dev/null | grep -v PSR | sort -n | uniq -c | head -20

    echo ""
    echo "========================================"
    echo "Native Threads (if visible):"
    ps -eLo psr,comm -p $JAVA_PID 2>/dev/null | grep -E "sync|poll|work" | head -20

    echo ""
    echo "========================================"
    echo "Thread Count: $(ps -T -p $JAVA_PID | wc -l)"

    sleep 2
done