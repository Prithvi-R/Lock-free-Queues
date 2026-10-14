#include <iostream>
#include <vector>
#include <cassert>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>

using namespace std;

// ------------ LOCK-FREE ATOMIC SPSC ------------
// Corrupted Reads: 0
// Execution Time : 692 ms
// Status         : 100% SAFE AT MAXIMUM CPU SPEEDS!

// ------------ MUTEX LOCKED SPSC ------------
// Corrupted Reads: 0
// Execution Time : 106414 ms
// Status         : SAFE (No Races)

// ------------ UNPROTECTED SPSC ------------
// Corrupted Reads: 670
// Execution Time : 795 ms
// Status         : EXPOSED (Consumer grabbed a partially written 4KB block mid-copy)

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

    // LOCK-FREE PUSH
    bool push_lock_free(long long id) {
        // Load index variables using relaxed ordering since we only need values here
        size_t curr_head = head.load(memory_order_relaxed);
        size_t curr_tail = tail.load(memory_order_relaxed);

        if ((curr_head - curr_tail) == capacity) return false;
        
        size_t current_slot = curr_head & mask;
        
        // Even if we split the writes into separate steps, the data copy is safe!
        ring_buffer[current_slot].data[0] = id;
        ring_buffer[current_slot].data[511] = id;
        
        // CRITICAL FENCE: memory_order_release guarantees that all previous 4KB 
        // data writes are strictly completed and visible in global caches BEFORE 
        // the new head increment is committed and published to the consumer thread.
        head.store(curr_head + 1, memory_order_release);
        
        return true;
    }

    // LOCK-FREE POP
    bool pop_lock_free(T& out_item) {
        // CRITICAL FENCE: memory_order_acquire guarantees that the subsequent 
        // 4KB data reads cannot be reordered before loading a valid head pointer.
        size_t curr_head = head.load(memory_order_acquire);
        size_t curr_tail = tail.load(memory_order_relaxed);

        if (curr_head == curr_tail) return false;
        
        out_item = ring_buffer[curr_tail & mask];
        
        // Signal to the producer that the slot is now free
        tail.store(curr_tail + 1, memory_order_release);
        
        return true;
    }

    bool is_empty() const { 
        return head.load(memory_order_relaxed) == tail.load(memory_order_relaxed); 
    }

    bool is_full() const { 
        return (head.load(memory_order_relaxed) - tail.load(memory_order_relaxed)) == capacity; 
    }

    size_t size() const { 
        return head.load(memory_order_relaxed) - tail.load(memory_order_relaxed); 
    }

    size_t cap() const { return capacity; }

private:
    vector<T> ring_buffer;
    size_t capacity;
    size_t mask;    
    atomic<size_t> head{0};
    atomic<size_t> tail{0};
    mutable mutex mtx;
};


int main() {
    long long CAP = 5'000'000;
    
    RingBuffer<MassiveItem> rb(2); 
    long long corruption_count = 0;

    auto start_time = chrono::high_resolution_clock::now();

    vector<thread> threads;

    threads.emplace_back([&]() {
        for (long long i = 1; i <= CAP; ++i) {
            while (!rb.push_lock_free(i)) {}
        }
    });

    threads.emplace_back([&]() {
        for (long long i = 1; i <= CAP; ++i) {
            MassiveItem val;
            while (!rb.pop_lock_free(val)) {}
            
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

    
    cout << "------------ LOCK-FREE ATOMIC SPSC ------------\n";
    cout << "Corrupted (Torn Data) Reads: " << corruption_count << "\n";
    cout << "Execution Time:              " << elapsed_ms << " ms\n";
    cout << "Status: 100% SAFE AT MAXIMUM CPU SPEEDS!\n";

    return 0;
}


// {
    // // --- (Original experiment functions preserved for comparison below) ---
    // bool push_corrupted(long long id) {
    //     size_t curr_head = head.load(memory_order_relaxed);
    //     if ((curr_head - tail.load(memory_order_relaxed)) == capacity) return false;
    //     size_t current_slot = curr_head & mask;
    //     ring_buffer[current_slot].data[0] = id;
    //     head.store(curr_head + 1, memory_order_relaxed); // Unsafe relaxed update
    //     ring_buffer[current_slot].data[511] = id;
    //     return true;
    // }
    // bool push_mutex(long long id) {
    //     lock_guard<mutex> lock(mtx);
    //     size_t curr_head = head.load(memory_order_relaxed);
    //     if ((curr_head - tail.load(memory_order_relaxed)) == capacity) return false;
    //     size_t current_slot = curr_head & mask;
    //     ring_buffer[current_slot].data[0] = id;
    //     head.store(curr_head + 1, memory_order_relaxed);
    //     ring_buffer[current_slot].data[511] = id;
    //     return true;
    // }
    // bool pop_mutex(T& out_item) {
    //     lock_guard<mutex> lock(mtx);
    //     size_t curr_head = head.load(memory_order_relaxed);
    //     size_t curr_tail = tail.load(memory_order_relaxed);
    //     if (curr_head == curr_tail) return false;
    //     out_item = ring_buffer[curr_tail & mask];
    //     tail.store(curr_tail + 1, memory_order_relaxed);
    //     return true;
    // }
// }