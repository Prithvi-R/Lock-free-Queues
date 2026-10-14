#include <iostream>
#include <vector>
#include <cassert>
#include <thread>
#include <chrono>
#include <mutex>
using namespace std;


/*
while running SPSC buffer using regular 'int' variables,
we wasn't able to observe the data race, the terminal output reports 0 corruption due to hardware optimization.
    -> On an x86-64 CPU, 4-byte or 8-byte memory copies are naturally atomic at the hardware level. 
Furthermore, x86 uses "Total Store Order" (TSO), preventing writes from passing other writes in hardware. 
Thus, standard SPSC counts matched up perfectly (even tho we increased probability of data race to 100%)

             HOW WE FORCED AND OBSERVED THE DATA RACE
            -----------------------------------------
    we systematically bypassed the CPU and compiler defense layers:

DEFEATING VECTOR REGISTERS (Bumping to a Massive 4KB Struct):
   Modern registers (AVX/SSE) can copy 16-to-32 bytes in a single atomic burst.
   We increased the struct payload size to 4KB(4096 bytes), forcing the compiler 
   out of hardware vector paths and into a multi-instruction copy loop (memcpy).
   
SIMULATING SOFTWARE REORDERING (Explicit Split Writes):
   Even at 4KB, we observed x86 TSO keeps writes ordered sequentially. 
   To expose the race, we manually split the write, updating the 'head++' pointer EARLY, 
   mid-way through the 4KB copy loop.

    RESULTS - UNPROTECTED RUN:

            ------------ DATA RACE FORCEFULLY OBSERVED ------------
                    Corrupted Reads: 127
                    Status:          EXPOSED (Consumer grabbed a partially written 4KB block mid-copy)
                    Execution Time:  584 ms


    RESULTS - PROTECTED MUTEX RUN:

            ------------ FIXED WITH LOCKS SPSC ------------
                    Corrupted Reads: 0
                    Status:          PERFECTLY SAFE (No Races)
                    Execution Time:  31382 ms

*/


struct MassiveItem {
    long long data[512]; // 4KB
};

template <typename T>
class RingBuffer {
public:
    RingBuffer(size_t cap) {
        capacity = cap;
        mask = cap - 1;
        ring_buffer.resize(capacity);
    }

    // Force explicit software reordering by breaking the execution flow
    bool push_corrupted(long long id) {
        lock_guard<mutex> lock(mtx);

        if (is_full()) return false;
        size_t current_slot = head & mask;
        
        ring_buffer[current_slot].data[0] = id;   // Write the front of the payload
        ring_buffer[current_slot].data[511] = id; // Write the back of the payload late
        head++;                                   // DATA RACE TRIGGER: Publish index early!
        
        return true;
    }

    bool pop(T& out_item) {
        lock_guard<mutex> lock(mtx);
        if (is_empty()) return false;
        out_item = ring_buffer[tail & mask];
        tail++; 
        return true;
    }

    bool is_empty() const { return head == tail; }
    bool is_full() const { return (head - tail) == capacity; }
    size_t size() const { return head - tail; }
    size_t cap() const { return capacity; }

private:
    vector<T> ring_buffer;
    size_t capacity;
    size_t mask;
    size_t head{0};
    size_t tail{0};
    mutable mutex mtx;
};

int main() {
    auto start_time = chrono::high_resolution_clock::now();
    long long CAP = 5'000'000; 
    RingBuffer<MassiveItem> rb(2); 

    long long corruption_count = 0;
    vector<thread> threads;

    threads.emplace_back([&]() {
        for (long long i = 1; i <= CAP; ++i) {
            while (!rb.push_corrupted(i)) {}
        }
    });

    threads.emplace_back([&]() {
        for (long long i = 1; i <= CAP; ++i) {
            MassiveItem val;
            while (!rb.pop(val)) {}
            
            // If the consumer grabs the item while the producer is between step 2 and step 3,
            // the front of the array will match the current ID, but the back will be completely mismatched!
            if (val.data[0] != val.data[511]) {
                corruption_count++;
            }
        }
    });

    for (auto& t : threads) {
        t.join();
    }
    auto end_time = chrono::high_resolution_clock::now();
    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count();

    cout << "------------ SIMULATED COMPILER REORDERING SPSC ------------\n";
    cout << "Corrupted Reads: " << corruption_count << "\n";
    cout << "Status:          " << (corruption_count == 0 ? "PERFECTLY SAFE (No Races)" : "FAILED") << "\n";
    cout << "Execution Time:  " << elapsed_ms << " ms\n";
    return 0;
}
