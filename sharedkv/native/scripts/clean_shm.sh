#!/bin/bash

# Script to clean SharedKV shared memory segments
# Run this before tests to ensure a clean state

echo "=========================================="
echo "Cleaning SharedKV Shared Memory Segments"
echo "=========================================="

# List all SharedKV shared memory segments
echo "Current SharedKV shared memory segments:"
ls -lh /dev/shm/ | grep sharedkv || echo "  (none found)"

echo ""
echo "Removing SharedKV shared memory segments..."

# Remove all SharedKV shared memory segments
for shm_file in /dev/shm/sharedkv_*; do
    if [ -e "$shm_file" ]; then
        echo "  Removing: $shm_file"
        rm -f "$shm_file"
    fi
done

echo ""
echo "Cleanup complete!"
echo ""

# Verify cleanup
echo "Remaining SharedKV segments:"
ls -lh /dev/shm/ | grep sharedkv || echo "  (none - cleanup successful)"
echo ""
