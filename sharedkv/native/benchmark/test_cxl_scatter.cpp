#include <cstdio>
#include <cstdint>
#include <cstring>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <numaif.h>
#include <numa.h>

static volatile char* region;
static uint64_t region_size;
static std::atomic<bool> go{false};

void worker(int id, int nworkers) {
    while (!go.load()) ;

    // Each thread scatters reads/writes across the full region
    // Mimics hash table + block access pattern
    uint64_t sum = 0;
    uint64_t seed = 0xdeadbeef + id;
    const uint64_t stride = 2048;  // UnifiedBlock size

    for (uint64_t iter = 0; iter < 100000; iter++) {
        // xorshift64 pseudo-random
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;

        uint64_t offset = (seed % (region_size / stride)) * stride;
        // Read 64 bytes from a scattered location (like reading UnifiedBlock header)
        volatile uint64_t* p = (volatile uint64_t*)(region + offset);
        sum += p[0];  // read key_hash area
        sum += p[1];  // read key_len area

        // Write (like updating bucket head or next_block_id)
        p[2] = iter;
    }

    printf("[thread %d] done, sum=%lu\n", id, sum);
}

int main(int argc, char** argv) {
    int nthreads = argc > 1 ? atoi(argv[1]) : 10;
    int numa_node = argc > 2 ? atoi(argv[2]) : 1;
    uint64_t gb = argc > 3 ? atoi(argv[3]) : 4;  // default 4GB

    region_size = gb * 1024ULL * 1024ULL * 1024ULL;

    printf("Allocating %lu GB on NUMA node %d...\n", gb, numa_node);

    void* p = mmap(nullptr, region_size, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (p == MAP_FAILED) { perror("mmap"); return 1; }

    struct bitmask* m = numa_bitmask_alloc(numa_max_node() + 1);
    numa_bitmask_setbit(m, numa_node);
    mbind(p, region_size, MPOL_BIND, m->maskp, m->size + 1, 0);
    numa_bitmask_free(m);

    printf("Pre-faulting (memset)...\n");
    memset(p, 0xAB, region_size);
    printf("Pre-fault done.\n");

    region = (volatile char*)p;

    std::thread threads[128];
    for (int i = 0; i < nthreads; i++)
        threads[i] = std::thread(worker, i, nthreads);

    printf("Starting %d threads, scattered R/W across %lu GB CXL...\n", nthreads, gb);
    go.store(true);

    for (int i = 0; i < nthreads; i++) threads[i].join();
    printf("All threads completed successfully.\n");
    return 0;
}
