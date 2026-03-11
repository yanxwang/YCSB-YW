#include <cstdio>
#include <cstdint>
#include <cstring>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <numaif.h>
#include <numa.h>

static volatile uint64_t* bitmap;
static const int NWORDS = 1024;
static std::atomic<bool> go{false};
static std::atomic<uint64_t> total_claimed{0};

void worker(int id, int nworkers) {
    while (!go.load()) ;
    uint64_t claimed = 0;
    for (int round = 0; round < 100; round++) {
        for (int w = id; w < NWORDS; w += nworkers) {
            for (int bit = 0; bit < 64; bit++) {
                bool was_set;
                asm volatile("lock btrq %2, %0"
                             : "+m"(bitmap[w]), "=@ccc"(was_set)
                             : "Ir"((uint64_t)bit) : "memory");
                if (was_set) claimed++;
            }
        }
        // refill for next round
        for (int w = id; w < NWORDS; w += nworkers)
            bitmap[w] = 0xFFFFFFFFFFFFFFFFULL;
    }
    total_claimed += claimed;
}

int main(int argc, char** argv) {
    int nthreads = argc > 1 ? atoi(argv[1]) : 8;
    size_t sz = NWORDS * sizeof(uint64_t);
    void* p = mmap(nullptr, sz, PROT_READ|PROT_WRITE,
                   MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    struct bitmask* m = numa_bitmask_alloc(numa_max_node()+1);
    numa_bitmask_setbit(m, 1);
    mbind(p, sz, MPOL_BIND, m->maskp, m->size+1, 0);
    numa_bitmask_free(m);

    bitmap = (volatile uint64_t*)p;
    for (int i = 0; i < NWORDS; i++) bitmap[i] = 0xFFFFFFFFFFFFFFFFULL;

    std::thread threads[64];
    for (int i = 0; i < nthreads; i++)
        threads[i] = std::thread(worker, i, nthreads);

    printf("Starting %d threads doing lock btr on CXL NUMA 1...\n", nthreads);
    go.store(true);

    for (int i = 0; i < nthreads; i++) threads[i].join();
    printf("Done. Total bits claimed: %lu\n", total_claimed.load());
    return 0;
}
