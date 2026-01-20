#!/bin/bash

# Build script for SharedKV JNI library
# Run from the native/ directory root

# Find Java home
cd ~/YCSB-YW/sharedkv/native/benchmark/build
cmake -DCMAKE_BUILD_TYPE=Debug ..
# cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8