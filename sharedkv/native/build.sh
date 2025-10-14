#!/bin/bash

# Find Java home
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME not set. Trying to find it..."
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
fi

echo "Using JAVA_HOME: $JAVA_HOME"

# Compile
g++ -std=c++17 -O3 -fPIC -shared \
    -I"$JAVA_HOME/include" \
    -I"$JAVA_HOME/include/linux" \
    -o libsharedkv_jni.so \
    shared_kv.cpp \
    SharedKV_YCSB.cpp \
    sharedkv_jni.cpp \
    -pthread

if [ $? -eq 0 ]; then
    echo "Build successful! Library: libsharedkv_jni.so"
    ls -lh libsharedkv_jni.so
else
    echo "Build failed!"
    exit 1
fi