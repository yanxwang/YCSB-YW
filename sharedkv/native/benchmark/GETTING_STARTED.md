# Getting Started with SharedKV Native Benchmark

## 🚀 Quick Start (30 seconds)

```bash
cd /home/wang/YCSB-YW/sharedkv/native/benchmark

# 1. Build benchmark
./build.sh

# 2. Generate test workload (10K records)
./gen_workload.py -w c -r 10000 -o 50000

# 3. Run quick test
./quick_test.sh
```

Expected output:
```
Throughput: ~1.7M ops/sec (4 clients, 4 workers)
```

## 📊 Performance Comparison

| Test Type | Throughput | Notes |
|-----------|------------|-------|
| **Native Benchmark** | **~1.75M ops/sec** | Pure C++, zero JNI overhead |
| Java YCSB | ~500K ops/sec | Includes JNI overhead (~50-100ns per call) |

**Speedup**: ~3.5x faster with native benchmark

## 🎯 What You Get

### 1. Zero JNI Overhead
- Direct C++ function calls
- No Java ↔ C++ type conversions
- No string/map serialization

### 2. Maximum Performance Measurement
- Measures SharedKV's theoretical limit
- Useful for:
  - Performance debugging
  - Optimization validation
  - Bottleneck identification

### 3. FUSEE-style Architecture
- Multi-threaded client simulation
- CPU pinning for optimal performance
- Workload file format compatible with YCSB

## 📁 File Structure

```
benchmark/
├── ycsb_benchmark.h         # Benchmark API
├── ycsb_benchmark.cc        # Core implementation
├── benchmark_main.cc        # Main program
├── globals.cc               # Global variables
├── gen_workload.py          # Workload generator
├── build.sh                 # Build script
├── run_benchmark.sh         # Convenience runner
├── quick_test.sh            # Quick test
├── CMakeLists.txt           # CMake config
├── README.md                # Full documentation
├── GETTING_STARTED.md       # This file
└── workloads/               # Generated workload files
    ├── workloadc_load.txt
    └── workloadc_trans.txt
```

## 🔧 Common Use Cases

### Case 1: Measure Maximum Throughput

```bash
# Generate large workload
./gen_workload.py -w c -r 1000000 -o 10000000

# Run with 32 clients, 32 workers for 60 seconds
./run_benchmark.sh -w workloadc -c 32 -W 32 -t 60
```

### Case 2: Measure Latency Distribution

```bash
# Run in latency mode (100K ops per thread)
./run_benchmark.sh -w workloadc -c 16 -W 16 --latency
```

Output includes percentiles:
```
Latency Statistics (microseconds):
  Average:  1.97 us
  Median:   1.85 us
  95th:     2.34 us
  99th:     3.12 us
  99.9th:   5.67 us
```

### Case 3: Test Scalability

```bash
# Test with different client counts
for clients in 1 2 4 8 16 32; do
    echo "Testing with $clients clients..."
    ./run_benchmark.sh -w workloadc -c $clients -W $clients -t 10
done
```

### Case 4: Validate Optimization

Before optimization:
```bash
./run_benchmark.sh -w workloadc -c 32 -W 32 -t 30 > before.txt
```

After optimization:
```bash
./run_benchmark.sh -w workloadc -c 32 -W 32 -t 30 > after.txt
```

Compare results:
```bash
grep "Throughput:" before.txt after.txt
```

## 🔍 Understanding Results

### Throughput Mode Output

```
==============================================
TRANSACTION PHASE Results:
==============================================
Total operations:      8770060
Successful operations: 8770060
Failed operations:     0
Duration:              5.00 seconds
Throughput:            1753751.74 ops/sec    ← Main metric
==============================================
```

**Key Metrics**:
- **Throughput**: Operations per second (higher is better)
- **Failed operations**: Should be 0 for correct implementation
- **Duration**: Actual run time (may be slightly longer than requested)

### Latency Mode Output

```
Latency Statistics (microseconds):
  Average:  1.97 us    ← Mean latency
  Median:   1.85 us    ← 50th percentile
  95th:     2.34 us    ← 95th percentile (tail latency)
  99th:     3.12 us    ← 99th percentile
  99.9th:   5.67 us    ← 99.9th percentile
```

**Key Metrics**:
- **Median**: Typical case performance
- **95th/99th percentile**: Tail latency (important for SLA)
- **Average**: Overall performance (affected by outliers)

## 🎛️ Configuration Tuning

### CPU Pinning

```bash
# Default: Client threads start at CPU 64
./run_benchmark.sh -w workloadc -c 32 -W 32 -s 64

# Use different starting CPU (e.g., 32)
./run_benchmark.sh -w workloadc -c 32 -W 32 -s 32
```

**CPU Layout**:
- CPU 0: Synchronizer (fixed)
- CPU 1: Poller (fixed)
- CPU 2+: Worker threads (dynamic)
- CPU 64+: Client threads (configurable with `-s`)

### NUMA Node

```bash
# Use NUMA node 3 (CXL memory)
./run_benchmark.sh -w workloadc -c 32 -W 32 -n 3

# Use NUMA node 0 (local DRAM)
./run_benchmark.sh -w workloadc -c 32 -W 32 -n 0
```

### Workload Size

```bash
# Small (quick test): 10K records
./gen_workload.py -w c -r 10000 -o 50000

# Medium (standard): 100K records
./gen_workload.py -w c -r 100000 -o 1000000

# Large (realistic): 1M records
./gen_workload.py -w c -r 1000000 -o 10000000

# Extra large (stress test): 10M records
./gen_workload.py -w c -r 10000000 -o 100000000
```

## 🐛 Troubleshooting

### Problem: "Cannot open workload file"

**Solution**: Run from correct directory or regenerate workload:
```bash
cd /home/wang/YCSB-YW/sharedkv/native
./benchmark/gen_workload.py -w c -r 10000 -o 50000
```

### Problem: Shared memory errors

**Solution**: Clean shared memory before running:
```bash
./scripts/clean_shm.sh
```

### Problem: Low throughput

**Checklist**:
- [ ] Are you running on correct NUMA node? (use `-n 3` for CXL)
- [ ] Are CPUs isolated? (check `/proc/cmdline` for `isolcpus`)
- [ ] Are there other processes running? (use `top` or `htop`)
- [ ] Is CPU frequency scaling disabled? (use `cpupower frequency-set -g performance`)

### Problem: Build fails

**Solution**: Rebuild from scratch:
```bash
cd /home/wang/YCSB-YW/sharedkv/native/benchmark
rm -rf build
./build.sh
```

## 📈 Performance Tips

### 1. Maximize Throughput

- **Match threads to cores**: Set `-c` and `-W` to number of physical cores
  ```bash
  # Check core count
  lscpu | grep "Core(s) per socket"

  # Use all cores (e.g., 32 cores)
  ./run_benchmark.sh -c 32 -W 32
  ```

- **Use local memory**: Run on correct NUMA node
  ```bash
  # Check NUMA topology
  numactl --hardware

  # Use CXL memory (node 3)
  ./run_benchmark.sh -n 3
  ```

- **Pin to dedicated CPUs**: Avoid shared CPUs
  ```bash
  # Check CPU usage
  mpstat -P ALL 1

  # Use isolated CPUs (64+)
  ./run_benchmark.sh -s 64
  ```

### 2. Minimize Latency Variance

- **Reduce contention**: Use fewer threads
  ```bash
  ./run_benchmark.sh -c 4 -W 8 --latency
  ```

- **Warm up caches**: Run throughput test first
  ```bash
  ./run_benchmark.sh -t 10  # Warm-up
  ./run_benchmark.sh --latency  # Latency measurement
  ```

- **Large sample size**: Use more operations
  ```bash
  ./build/sharedkv_benchmark -w workloadc -c 1 -W 8 -l -o 1000000
  ```

## 📊 Example Benchmark Session

```bash
# 1. Build and setup
cd /home/wang/YCSB-YW/sharedkv/native/benchmark
./build.sh
./gen_workload.py -w c -r 100000 -o 1000000

# 2. Baseline test (single thread)
./build/sharedkv_benchmark -w workloadc -c 1 -W 8 -t 30 -s 64 > baseline.txt

# 3. Scalability test (multi-threaded)
for threads in 2 4 8 16 32; do
    echo "Testing $threads threads..." | tee -a scalability.txt
    ./run_benchmark.sh -c $threads -W $threads -t 30 | \
        grep "Throughput:" | tee -a scalability.txt
done

# 4. Latency profiling
./run_benchmark.sh -c 16 -W 16 --latency > latency.txt

# 5. Compare with Java YCSB
cd /home/wang/YCSB-YW
./bin/ycsb run sharedkv -P workloads/workloadc \
    -p sharedkv.threading=multi -p threadcount=16 \
    -p operationcount=1000000 > java_ycsb.txt

# 6. Analyze results
echo "Native Benchmark:"
grep "Throughput:" benchmark/native/throughput.txt

echo "Java YCSB:"
grep "Throughput" java_ycsb.txt
```

## 🎓 Next Steps

1. **Read full documentation**: [README.md](README.md)
2. **Understand architecture**: Check CPU pinning strategy
3. **Try different workloads**: Generate Workload A (50/50 read/update)
4. **Compare with Java YCSB**: Measure JNI overhead
5. **Profile with perf**: Identify hotspots

## 📞 Support

If you encounter issues:
1. Check [README.md](README.md) for detailed documentation
2. Review troubleshooting section above
3. Examine log output for error messages
4. Verify system configuration (NUMA, CPU pinning, etc.)
