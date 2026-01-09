#!/bin/bash

# Build script for SharedKV JNI library
# Run from the native/ directory root

# Find Java home
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME not set. Trying to find it..."
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
fi

echo "Using JAVA_HOME: $JAVA_HOME"

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Create build directory if it doesn't exist
mkdir -p build

# Compile
echo "Compiling SharedKV JNI library..."
g++ -std=c++17 -O3 -fPIC -shared -muintr -g \
    -I"include" \
    -I"$JAVA_HOME/include" \
    -I"$JAVA_HOME/include/linux" \
    -o build/libsharedkv_jni.so \
    src/shared_kv_bucket.cpp \
    src/SharedKV_YCSB.cpp \
    src/sharedkv_jni.cpp \
    src/kv_context.cpp \
    src/kv_worker.cpp \
    src/kv_synchronizer.cpp \
    src/kv_poller.cpp \
    src/kv_request.cpp \
    src/uintr_threading.cpp \
    -pthread -lnuma -lrt

if [ $? -eq 0 ]; then
    echo "Build successful! Library: build/libsharedkv_jni.so"
    ls -lh build/libsharedkv_jni.so
else
    echo "Build failed!"
    exit 1
fi

sudo cp /home/wang/YCSB-YW/sharedkv/native/build/libsharedkv_jni.so /usr/lib/
sudo ldconfig