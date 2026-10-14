#include <iostream>
#include <vector>
#include <optional>
#include <cassert>
#include <thread>
#include <utility>
#include <atomic>
#include <chrono> 

using namespace std;

/*
========================================================================================
    LAB REPORT: LOCK-FREE MULTI-PRODUCER MULTI-CONSUMER (MPMC) RING BUFFER
========================================================================================
    THE MPMC CHALLENGE:
    In SPSC, release/acquire fences on head/tail are enough because only one thread writes 
    to each pointer. In MPMC, two producers can simultaneously increment 'head'. 
    If Thread A increments 'head' but gets delayed before writing the data, Thread B 
    could increment 'head' again and finish its write. A consumer checking 'head' would 
    think BOTH items are ready, reading completely uninitialized garbage data from Thread A!

    THE LOCK-FREE MPMC DESIGN (Vyukov's Queue):
    We wrap every array element with an accompanying atomic `sequence` tracker. 
    1. A slot is only writable if `sequence == head`.
    2. After writing, the producer sets `sequence = head + 1`. This safely signals 
       consumers that the data is physically ready.
    3. A slot is only readable if `sequence == tail + 1`.
    4. After reading, the consumer sets `sequence = tail + capacity`. This safely signals 
       producers that the slot is vacant and ready to be overwritten in the next wrap-around.
========================================================================================
*/

template <typename T>
class RingBuffer {
    
    public:
    RingBuffer(size_t cap) {
        assert(cap > 0 && (cap & (cap - 1)) == 0 && "Capacity must be a power of 2!");
        capacity = cap;
        mask = cap - 1;
        
        // Allocate raw array space to completely avoid vector vector copy/move errors
        ring_buffer = new Cell[capacity];
        
        for (size_t i = 0; i < capacity; ++i) {
            ring_buffer[i].sequence.store(i, memory_order_relaxed);
        }
    }
    
    bool push(const T& item) {
        Cell* cell;
        size_t pos = head.load(memory_order_relaxed);
        
        while (true) {
            cell = &ring_buffer[pos & mask];
            size_t seq = cell->sequence.load(memory_order_acquire);
            intptr_t diff = (intptr_t)seq - (intptr_t)pos;
            
            if (diff == 0) {
                if (head.compare_exchange_weak(pos, pos + 1, memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = head.load(memory_order_relaxed);
            }
        }

        cell->data = item;
        cell->sequence.store(pos + 1, memory_order_release);
        return true;
    }
    
    bool pop(T& out_item) {
        Cell* cell;
        size_t pos = tail.load(memory_order_relaxed);
        
        while (true) {
            cell = &ring_buffer[pos & mask];
            size_t seq = cell->sequence.load(memory_order_acquire);
            intptr_t diff = (intptr_t)seq - (intptr_t)(pos + 1);
            
            if (diff == 0) {
                if (tail.compare_exchange_weak(pos, pos + 1, memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = tail.load(memory_order_relaxed);
            }
        }
        
        out_item = cell->data;
        cell->sequence.store(pos + capacity, memory_order_release);
        return true;
    }
    
    size_t size() const {
        size_t h = head.load(memory_order_relaxed);
        size_t t = tail.load(memory_order_relaxed);
        return (h >= t) ? (h - t) : 0;
    }
private:
    struct Cell {
        T data;
        atomic<size_t> sequence;
    };

    // To prevent copy/move construction errors with std::atomic fields,
    // we manage an internal array pointer or use vector size initialization.
    Cell* ring_buffer; 
    size_t capacity;
    size_t mask;
    
    alignas(64) atomic<size_t> head{0};
    alignas(64) atomic<size_t> tail{0};
};

int main() {
    int NUM_PRODUCERS = 4;
    int NUM_CONSUMERS = 4;
    int ITEMS_PER_THREAD = 1 << 20; 
    
    RingBuffer<int> rb(1024);

    atomic<int> total_successful_pushes{0};
    atomic<int> total_successful_pops{0};

    vector<thread> threads;
    auto start_time = chrono::high_resolution_clock::now();

    for (int p = 0; p < NUM_PRODUCERS; ++p) {
        threads.emplace_back([&rb, &total_successful_pushes, ITEMS_PER_THREAD]() {
            for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
                if (rb.push(1)) {
                    total_successful_pushes++;
                }
            }
        });
    }

    for (int c = 0; c < NUM_CONSUMERS; ++c) {
        threads.emplace_back([&rb, &total_successful_pops, ITEMS_PER_THREAD]() {
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

    cout << "------------ LOCK-FREE MPMC RESULTS ------------\n";
    cout << "Total items Attempted:   " << NUM_PRODUCERS * ITEMS_PER_THREAD << "\n";
    cout << "Pushes reported by code: " << total_successful_pushes.load() << "\n";
    cout << "Pops reported by code:   " << total_successful_pops.load() << "\n";
    cout << "Items left in buffer:    " << rb.size() << "\n"; 
    cout << "Execution Time:          " << elapsed_ms << " ms\n";

    if (total_successful_pushes.load() == (total_successful_pops.load() + rb.size())) {
        cout << "Verification Check:      SAFE\n";
    } else {
        cout << "Verification Check:      ERROR!\n";
    }

    return 0;
}