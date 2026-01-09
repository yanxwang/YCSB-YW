# SharedKV Native YCSB Benchmark

Pure C++ YCSB benchmark for SharedKV - zero JNI overhead, measures native performance limits.

Based on FUSEE's YCSB benchmarking architecture.

## Features

- **Zero JNI Overhead**: Pure C++ implementation, no Java/JNI layer
- **Multi-threaded**: Supports configurable number of client threads
- **CPU Pinning**: Automatic CPU affinity for optimal performance
- **Workload Support**: Standard YCSB workloads (A, B, C)
- **Dual Modes**:
  - **Throughput Mode**: Run for fixed duration, measure ops/sec
  - **Latency Mode**: Run fixed operations, measure latency distribution

## Quick Start

### 1. Generate Workload Files

```bash
# Generate Workload C (100% read, 100K records, 1M operations)
cd /home/wang/YCSB-YW/sharedkv/native/benchmark
./gen_workload.py -w c -r 100000 -o 1000000

# Generate Workload A (50% read, 50% update)
./gen_workload.py -w a -r 100000 -o 1000000

# Generate Workload B (95% read, 5% update)
./gen_workload.py -w b -r 100000 -o 1000000
```

This creates workload files in `benchmark/workloads/`:
- `workloadc_load.txt` - Load phase operations (INSERT)
- `workloadc_trans.txt` - Transaction phase operations (READ/UPDATE)

### 2. Build Benchmark

```bash
cd /home/wang/YCSB-YW/sharedkv/native/benchmark
./build.sh
```

This compiles the benchmark executable: `build/sharedkv_benchmark`

### 3. Run Benchmark

#### Throughput Mode (default)

```bash
# Run Workload C with 32 clients, 32 workers for 30 seconds
./run_benchmark.sh -w workloadc -c 32 -W 32 -t 30

# Run with different configuration
./run_benchmark.sh -w workloada -c 16 -W 16 -t 60
```

#### Latency Mode

```bash
# Measure latency distribution (100K operations per client)
./run_benchmark.sh -w workloadc -c 32 -W 32 --latency
```

### 4. Advanced Usage

Run benchmark directly with custom parameters:

```bash
./build/sharedkv_benchmark \
    -w workloadc \        # Workload name
    -n 3 \                # NUMA node
    -c 32 \               # Number of client threads
    -W 32 \               # Number of worker threads
    -t 30 \               # Duration in seconds
    -s 64                 # Starting CPU for client threads
```

For latency measurement:

```bash
./build/sharedkv_benchmark \
    -w workloadc \
    -c 16 \
    -W 16 \
    -l \                  # Latency mode
    -o 100000             # Operations per client
```

## Architecture

```
┌──────────────────────────────────────────────────┐
│  benchmark_main (main thread)                    │
│  - Parse arguments                               │
│  - Load workload files                           │
│  - Initialize SharedKV context                   │
│  - Create client threads                         │
│  - Collect and print statistics                  │
└───────────────┬──────────────────────────────────┘
                │
                │ pthread_create (N clients)
                ▼
       ┌─────────────────────┐
       │  Client Thread      │
       │  - CPU pinning      │
       │  - Execute ops      │
       │  - Measure latency  │
       └──────┬──────────────┘
              │
              │ Direct C++ calls (zero JNI overhead!)
              ▼
       ┌────────────────────────┐
       │  SharedKVContext       │
       │  - submit_request()    │
       │  - Synchronizer        │
       │  - Workers             │
       │  - Poller              │
       └────────────────────────┘
```

## CPU Pinning Strategy

The benchmark uses dynamic CPU allocation:

- **CPU 0**: Synchronizer (fixed)
- **CPU 1**: Poller (fixed)
- **CPU 2+**: Worker threads (dynamically allocated)
- **CPU 64+**: Client threads (configurable via `-s` option)

Example with 32 workers + 32 clients:
- Synchronizer → CPU 0
- Poller → CPU 1
- Workers → CPUs 2-33
- Clients → CPUs 64-95

## Workload File Format

Simple text format, one operation per line:

```
# Load phase (workloadc_load.txt)
INSERT user0 field0=xxx,field1=yyy,...
INSERT user1 field0=xxx,field1=yyy,...
...

# Transaction phase (workloadc_trans.txt)
READ user42
READ user123
UPDATE user7 field0=xxx,field1=yyy,...
...
```

## Output Format

### Throughput Mode

```
==============================================
TRANSACTION PHASE Results:
==============================================
Total operations:      15234567
Successful operations: 15234567
Failed operations:     0
Duration:              30.00 seconds
Throughput:            507818.90 ops/sec
==============================================
```

### Latency Mode

```
==============================================
TRANSACTION PHASE Results:
==============================================
Total operations:      3200000
Successful operations: 3200000
Failed operations:     0
Duration:              6.32 seconds
Throughput:            506329.11 ops/sec

Latency Statistics (microseconds):
  Average:  1.97 us
  Median:   1.85 us
  95th:     2.34 us
  99th:     3.12 us
  99.9th:   5.67 us
==============================================
```

## Comparison: Native vs Java YCSB

| Metric | Native Benchmark | Java YCSB |
|--------|------------------|-----------|
| **Language** | Pure C++ | Java + JNI |
| **JNI Overhead** | 0 ns | ~50-100 ns per call |
| **Type Conversion** | None | String ↔ jstring, Map ↔ jobject |
| **Use Case** | Measure native max performance | Fair comparison with other DBs |
| **Result Comparability** | Not comparable to other DBs | Industry standard |
| **Implementation** | Custom workload parser | Standard YCSB framework |

**Recommendation**: Use both!
- Native benchmark → Measure SharedKV's theoretical maximum
- Java YCSB → Fair comparison with Redis, MongoDB, etc.

## Troubleshooting

### Workload files not found

```bash
# Make sure workloads directory exists and contains files
ls -la benchmark/workloads/

# If empty, generate workloads:
./gen_workload.py -w c -r 100000 -o 1000000
```

### Shared memory errors

```bash
# Clean shared memory before running
../scripts/clean_shm.sh
```

### Permission errors

```bash
# Ensure scripts are executable
chmod +x build.sh run_benchmark.sh gen_workload.py
```

## Performance Tuning

### Maximize Throughput

1. **Match threads to cores**: Set `-c` and `-W` to number of physical cores
2. **NUMA awareness**: Use correct NUMA node with `-n`
3. **CPU isolation**: Ensure CPUs are not used by other processes
4. **Large workload**: Use more records/operations to avoid cache effects

### Accurate Latency Measurement

1. **Fewer threads**: Use `-c 1` or `-c 4` for low contention
2. **Many operations**: Use `-o 1000000` for better percentile accuracy
3. **Warm-up**: Run throughput test first to warm up caches

## Examples

### Small Test (100K records, 10 seconds)

```bash
./gen_workload.py -w c -r 100000 -o 500000
./run_benchmark.sh -w workloadc -c 16 -W 16 -t 10
```

### Large Throughput Test (1M records, 60 seconds)

```bash
./gen_workload.py -w c -r 1000000 -o 10000000
./run_benchmark.sh -w workloadc -c 32 -W 32 -t 60
```

### Latency Profiling (minimal contention)

```bash
./gen_workload.py -w c -r 100000 -o 100000
./run_benchmark.sh -w workloadc -c 1 -W 8 --latency
```

## Acknowledgments

Based on FUSEE's YCSB benchmarking architecture:
- Pure C++ implementation
- Multi-threaded design
- Workload file format
