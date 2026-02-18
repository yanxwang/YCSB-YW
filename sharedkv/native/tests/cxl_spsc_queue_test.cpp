#include <iostream>
#include <atomic>
#include <thread>
#include <vector>
#include <iomanip>
#include <numa.h>
#include <numaif.h>
#include <x86intrin.h>
#include "../include/cxl_spsc_queue.h"

using namespace std;

// 建议使用较小的 Payload 来测试同步协议本身的开销
struct alignas(64) Payload {
    uint64_t data; 
};

// 获取当前 CPU 频率 (Hz) 用于时间换算
double get_cpu_freq() {
    return 2.0e9; 
}

static inline uint64_t timer_start() {
    unsigned int unused;
    return __rdtscp(&unused);
}

static inline uint64_t timer_end() {
    unsigned int unused;
    uint64_t t = __rdtscp(&unused);
    _mm_lfence();
    return t;
}

template<size_t CAP>
void run_benchmark(int prod_cpu, int cons_cpu, int mem_node) {
    size_t q_size = sizeof(CXLSpscQueue<Payload, CAP>);
    printf("Allocating queue of size %zu bytes on NUMA node %d...\n", q_size, mem_node);
    void* raw_mem = numa_alloc_onnode(q_size, mem_node);
    if (!raw_mem) {
        cerr << "Memory allocation failed on node " << mem_node << endl;
        return;
    }

    auto* queue = new (raw_mem) CXLSpscQueue<Payload, CAP>();
    queue->init();

    const size_t ITERATIONS = 10000000; // 1M 次成功的 dequeue
    atomic<bool> start_flag{false};
    atomic<bool> exit_flag{false};
    
    // 统计量
    atomic<uint64_t> prod_success{0};
    atomic<uint64_t> prod_attempts{0};
    atomic<uint64_t> cons_attempts{0};
    uint64_t total_cycles = 0;

    cout << "\n--- Benchmarking: Prod(CPU " << prod_cpu << ") | Cons(CPU " << cons_cpu 
         << ") | Mem(Node " << mem_node << ") ---" << endl;

    // --- Consumer Thread ---
    thread consumer([&]() {
        cpu_set_t cpuset; CPU_ZERO(&cpuset); CPU_SET(cons_cpu, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        CXLSpscConsumer<Payload, CAP> cons(queue);
        cons.attach(queue);

        while (!start_flag.load());

        Payload item;
        uint64_t received = 0;
        uint64_t attempts = 0;

        uint64_t t1 = timer_start();
        while (received < ITERATIONS) {
            attempts++;
            if (cons.dequeue(item)) {
                received++;
            }
        }
        uint64_t t2 = timer_end();
        
        total_cycles = t2 - t1;
        cons_attempts.store(attempts);
        exit_flag.store(true);
    });

    // --- Producer Thread ---
    thread producer([&]() {
        cpu_set_t cpuset; CPU_ZERO(&cpuset); CPU_SET(prod_cpu, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        CXLSpscProducer<Payload, CAP> prod(queue);
        prod.attach(queue);

        Payload item = {0xDEADBEEF};
        uint64_t attempts = 0;
        uint64_t success = 0;
        start_flag.store(true);

        while (!exit_flag.load()) {
            attempts++;
            if (prod.enqueue(item)) {
                success++;
            }
        }
        prod_attempts.store(attempts);
        prod_success.store(success);
    });

    producer.join();
    consumer.join();

    // 计算结果
    double cpu_hz = get_cpu_freq();
    double avg_cycles = (double)total_cycles / ITERATIONS;
    double avg_ns = (avg_cycles / cpu_hz) * 1e9;
    double throughput = (double)ITERATIONS / (total_cycles / cpu_hz) / 1e6;

    cout << fixed << setprecision(2);
    cout << "--- Statistics ---" << endl;
    cout << "Producer Success: " << prod_success.load() << " / " << prod_attempts.load() 
         << " (" << (100.0 * prod_success.load() / prod_attempts.load()) << "%)" << endl;
    cout << "Consumer Success: " << ITERATIONS << " / " << cons_attempts.load() 
         << " (" << (100.0 * ITERATIONS / cons_attempts.load()) << "%)" << endl;
    cout << "--- Final Performance ---" << endl;
    cout << "Avg Latency : " << avg_cycles << " cycles (" << avg_ns << " ns)" << endl;
    cout << "Throughput  : " << throughput << " Mops/s" << endl;

    numa_free(raw_mem, q_size);
}

int main(int argc, char** argv) {
    if (argc < 4) {
        cout << "Usage: ./cxl_spsc_queue_test <prod_cpu> <cons_cpu> <mem_node>" << endl;
        return 1;
    }
    run_benchmark<4096>(atoi(argv[1]), atoi(argv[2]), atoi(argv[3]));
    return 0;
}