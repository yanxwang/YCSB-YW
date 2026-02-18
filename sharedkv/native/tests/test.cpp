#include <thread>
#include <iostream>
#include <atomic>

int data = 0;
std::atomic<bool> ready(false);

void producer() {
    data = 42;  // 普通写
    ready.store(true, std::memory_order_release);
}

void consumer() {
    while (!ready.load(std::memory_order_acquire)) {
    }
    std::cout << data << std::endl;  // 一定打印 42
}

int main() {
    std::thread t1(producer);
    std::thread t2(consumer);
    t1.join();
    t2.join();
}
