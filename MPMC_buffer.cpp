#include <iostream>
#include <vector>
#include <optional>
#include <cassert>
#include <thread>
#include <utility>
#include <atomic>
#include <mutex>
#include <chrono> 

using namespace std;

/*
All threads start reading simulataneously so we are getting such discrepancy
    ---------DATA RACE---------
    Total items Attempted:   4194304
    Pushes reported by code: 4088945
    Pops reported by code:   4194303  
    Items left in buffer:    18446744073709466433
    Execution Time:          527 ms
  (would get different results on each run)

    POP count > PUSH count
(as there is no atomic operation on tail! two tail can simulataneously read and 
increment the variable; leading to incrementing the tail only once which should be doen twice)
*/


/*
After Locking and Mutex
        ------------FIXED RESULTS------------
        Total items Attempted:   1000000
        Pushes reported by code: 794169
        Pops reported by code:   793145
        Items left in buffer:    1024
        Execution Time:          611 ms
  (would get different results on each run)
but here the total Pushes = total Pops + remaining items in our buffer!!
*/

// UNSAFE MULTIPLE-THREADED RING BUFFER
template <typename T>
class RingBuffer {
public:
    RingBuffer(size_t cap) {
        assert(cap > 0 && (cap & (cap - 1)) == 0 && "Capacity must be a power of 2!");
        capacity = cap;
        mask = cap - 1;
        ring_buffer.resize(capacity);
    }

    bool push(const T& item) {
        lock_guard<mutex> lock(mtx);
        if (is_full()) 
            return false;
        
        ring_buffer[head & mask] = item;
        head++;
        return true;
    }

    bool pop(T& out_item) {
        lock_guard<mutex> lock(mtx);
        if (is_empty()) 
            return false;

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
    int NUM_PRODUCERS = 4;
    int NUM_CONSUMERS = 4;
    int ITEMS_PER_THREAD = 1<<20;
    auto start_time = chrono::high_resolution_clock::now();
    RingBuffer<int> rb(1024);

    atomic<int> total_successful_pushes{0};
    atomic<int> total_successful_pops{0};

    vector<thread> threads;

    // Producer Threads
    for (int p = 0; p < NUM_PRODUCERS; ++p) {
        threads.emplace_back([&rb,&total_successful_pushes,ITEMS_PER_THREAD]() {
            for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
                if (rb.push(1)) {
                    total_successful_pushes++;
                }
            }
        });
    }

    // Consumer Threads
    for (int c = 0; c < NUM_CONSUMERS; ++c) {
        threads.emplace_back([&rb,&total_successful_pops,ITEMS_PER_THREAD]() {
            for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
                int val = 0;
                if (rb.pop(val)) {
                    total_successful_pops += val;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    auto end_time = chrono::high_resolution_clock::now();
    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count();

    cout << "------------FINAL RESULTS------------\n";
    cout << "Total items Attempted:   " << NUM_PRODUCERS * ITEMS_PER_THREAD << "\n";
    cout << "Pushes reported by code: " << total_successful_pushes.load() << "\n";
    cout << "Pops reported by code:   " << total_successful_pops.load() << "\n";
    cout << "Items left in buffer:    " << rb.size() << "\n"; 
    cout << "Execution Time:          " << elapsed_ms << " ms\n";
    // cout << (total_successful_pushes.load() == rb.size()+total_successful_pops.load())?1:0;

    return 0;
}
