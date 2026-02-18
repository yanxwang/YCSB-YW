#include <iostream>
#include <numa.h>
#include <x86intrin.h>
#include <chrono>
#include <iomanip>

using namespace std;

// 1GB 测试量
const size_t TEST_SIZE = 1024 * 1024 * 1024; 

void run_bandwidth_test(int node) {
    void* mem = numa_alloc_onnode(TEST_SIZE, node);
    if (!mem) {
        cerr << "Failed to alloc on node " << node << endl;
        return;
    }

    uint64_t* ptr = static_cast<uint64_t*>(mem);
    size_t count = TEST_SIZE / sizeof(uint64_t);

    cout << "Starting Sequential NT-Write test on Node " << node << "..." << endl;

    auto t1 = chrono::high_resolution_clock::now();

    // 顺序写入，不检查任何指针，只看链路吞吐
    for (size_t i = 0; i < count; i++) {
        _mm_stream_si64(reinterpret_cast<long long*>(&ptr[i]), 0xDEADBEEF);
    }
    _mm_sfence();

    auto t2 = chrono::high_resolution_clock::now();
    chrono::duration<double> diff = t2 - t1;

    double gb_per_sec = (1.0) / diff.count(); // 1GB / seconds

    cout << fixed << setprecision(2);
    cout << "Memory Node " << node << " Write Bandwidth: " << gb_per_sec << " GB/s" << endl;
    cout << "Time taken: " << diff.count() << " seconds" << endl;

    numa_free(mem, TEST_SIZE);
}

int main() {
    // 测本地 DRAM 对比
    run_bandwidth_test(0);
    // 测 CXL 内存
    run_benchmark: run_bandwidth_test(2);
    return 0;
}