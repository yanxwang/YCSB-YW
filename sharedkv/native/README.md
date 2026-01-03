# SharedKV - Persistent Memory Key-Value Store

A high-performance, lock-free key-value store designed for persistent memory (PMEM) with YCSB integration for benchmarking.

## Overview

SharedKV is a shared-memory hash table implementation optimized for CXL memory and persistent memory devices. It features:

- **NUMA-based CXL memory**: Default storage on CXL memory via NUMA node binding
- **Offset-based pointers**: Allows multiple processes to access the same shared memory
- **Lock-free design**: Spinlock-based concurrency control per bucket
- **Linear allocator**: Append-only allocation strategy
- **YCSB integration**: Compatible with Yahoo! Cloud Serving Benchmark
- **JNI bridge**: Java Native Interface for seamless Java integration
- **Legacy PMem support**: Optional persistent memory device support

## Architecture

```
┌─────────────────────────────────────────┐
│    YCSB Java Client (Benchmark)         │
└──────────────┬──────────────────────────┘
               │ JNI calls
               ▼
┌─────────────────────────────────────────┐
│    JNI Bridge (src/jni/)                │
│    - Java ↔ C++ type conversion         │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│    YCSB Wrapper (src/ycsb/)             │
│    - YCSB DB interface implementation   │
│    - Value serialization/deserialization│
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│    Core Hash Table (src/core/)          │
│    - KV operations (put/get/delete)     │
│    - Spinlock management                │
│    - Memory allocation                  │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│    CXL Memory (NUMA Node 2)             │
│    or Legacy PMem Device (/dev/pmem0)   │
└─────────────────────────────────────────┘
```

## Directory Structure

```
native/
├── include/sharedkv/       # Public headers
│   ├── shared_kv.h         # Core data structures
│   └── ycsb_wrapper.h      # YCSB interface
├── src/                    # Implementation
│   ├── core/               # Hash table engine
│   ├── ycsb/               # YCSB wrapper
│   └── jni/                # JNI bridge
├── scripts/                # Build scripts
├── docs/                   # Documentation
├── tests/                  # Test suite
├── examples/               # Usage examples
└── build/                  # Build output (gitignored)
```

## Prerequisites

- **C++ Compiler**: g++ with C++17 support
- **Java Development Kit**: JDK 8 or higher (for JNI)
- **CMake**: 3.10 or higher (optional, for CMake build)
- **Persistent Memory**: /dev/pmem0 or similar device (optional)

## Building

### Option 1: Using CMake (Recommended)

```bash
cd native/
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
```

The library will be built to `build/lib/libsharedkv_jni.so`

### Option 2: Using build.sh

```bash
cd native/
./scripts/build.sh
```

The library will be built to `build/libsharedkv_jni.so`

## Configuration

### CXL Mode (Default)

Uses NUMA node allocation to bind memory to CXL device on NUMA node 2:

**Properties**:
- `sharedkv.mode=cxl` (default)
- `sharedkv.numa_node=2` (default - targets CXL memory)

**Example**:
```bash
java -cp <classpath> site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workload_a \
  -p sharedkv.numa_node=2
```

### PMem Mode (Legacy)

Uses device-backed memory mapping:

**Properties**:
- `sharedkv.mode=pmem`
- `sharedkv.device=/dev/pmem0` (default device path)

**Example**:
```bash
java -cp <classpath> site.ycsb.Client \
  -db site.ycsb.db.sharedkv.SharedKVClient \
  -P workloads/workload_a \
  -p sharedkv.mode=pmem \
  -p sharedkv.device=/dev/pmem0
```

### Memory Parameters

Defined in [include/shared_kv.h](include/shared_kv.h):
- **Memory Size**: 16 GiB default
- **Buckets**: 4096 (fixed at compile-time)
- **Magic Number**: `0xC0DEBEEFDEADF00DULL`

## Data Structures

### SharedHashTable

The main hash table structure with:
- 4096 buckets with spinlocks
- Offset-based linked list per bucket
- Linear allocator with atomic free_offset

### KVEntry

Key-value entry structure:
- Hash value (64-bit)
- Key and value lengths
- Next offset for chaining
- Variable-length key and value data

See [include/sharedkv/shared_kv.h](include/sharedkv/shared_kv.h) for detailed structure definitions.

## API

### Core Operations

```cpp
// Put a key-value pair
int kv_put(SharedHashTable* ht, void* base,
           const std::string& key, const std::string& value);

// Get value by key
int kv_get(SharedHashTable* ht, void* base,
           const std::string& key, std::string& value);

// Delete a key
int kv_delete(SharedHashTable* ht, void* base,
              const std::string& key);
```

### YCSB Interface

```cpp
class SharedKV_YCSB : public DB {
public:
    int read(const std::string& key, std::map<std::string, std::string>& result);
    int insert(const std::string& key, const std::map<std::string, std::string>& values);
    int update(const std::string& key, const std::map<std::string, std::string>& values);
    int delete_op(const std::string& key);
};
```

## Performance Characteristics

### Concurrency Model

- **Bucket-level locking**: Each of 4096 buckets has its own spinlock
- **Lock instrumentation**: Tracks lock wait time per thread
- **Atomic operations**: Memory order acquire/release semantics

### Memory Allocation

- **Linear allocator**: Append-only, no deallocation
- **8-byte alignment**: All allocations aligned for performance
- **Atomic allocation**: Uses `fetch_add` for thread-safe allocation

### Known Limitations

1. **No persistence**: CXL mode uses volatile DRAM (data lost on restart)
2. **No space reclamation**: Delete operations don't free memory
3. **Fixed bucket count**: 4096 buckets (not configurable at runtime)
4. **Linear collision handling**: Simple chaining (no tree balancing)

## Usage with YCSB

1. Build the JNI library
2. Copy `libsharedkv_jni.so` to Java library path
3. Load in YCSB client:

```java
System.loadLibrary("sharedkv_jni");
```

4. Run YCSB workloads as normal

## Development

### Adding New Features

1. Update headers in `include/sharedkv/`
2. Implement in appropriate `src/` subdirectory
3. Update CMakeLists.txt if adding new files
4. Add tests in `tests/`

### Code Style

- C++17 standard
- Consistent naming (snake_case for functions/variables)
- Clear comments for complex lock-free operations

## Troubleshooting

### Build Issues

**Error**: `jni.h: No such file or directory`
- **Solution**: Set JAVA_HOME environment variable

**Error**: `shared_kv.cpp: No such file or directory`
- **Solution**: Use updated build.sh (references `shared_kv_bucket.cpp`)

### Runtime Issues

**Error**: `NUMA not available`
- **Solution**: Install `libnuma-dev` package or check NUMA support in kernel

**Error**: `mbind failed` or `Failed to bind memory to NUMA node 2`
- **Solution**: Verify NUMA node 2 exists with `numactl --hardware`
- Check that CXL memory is available: `daxctl list`

**Error**: `Cannot open /dev/pmem0` (PMem mode only)
- **Solution**: Set `sharedkv.mode=cxl` or specify correct device path

**Error**: `Magic number mismatch`
- **Solution**: Hash table not initialized, will auto-initialize on first use

## License

See parent project for license information.

## Contributing

Contributions welcome! Please ensure:
- Code compiles without warnings
- New features include tests
- Documentation is updated

## References

- [YCSB Project](https://github.com/brianfrankcooper/YCSB)
- [Persistent Memory Programming](https://pmem.io/)
