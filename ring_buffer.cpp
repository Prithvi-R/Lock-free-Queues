#include <iostream>
#include <vector>
#include <optional>
#include <cassert>
#include <cstddef>
#include <string>

using namespace std;
// Modulo ops take 10-20 CPU clock cycles
// Bitwise AND & take ony 1

template <typename T>

class RingBuffer {
public:
    RingBuffer(size_t cap) {
        // Enforce cap to be a power of 2 for bitwise masking
        assert(cap > 0 && (cap & (cap - 1)) == 0 && "Capacity must be a power of 2!");
        capacity = cap;
        mask = cap - 1;

        ring_buffer.resize(capacity);
    }

    // Move version of push
    bool push(const T& item) {
        if (is_full()) {
            return false;
            // tail++;
        }
        
        // ring_buffer[head & mask] = item;
        // This allocates new dynamic memory on the heap and performs deep copy of the string contents. (Very slow)

        ring_buffer[head & mask] = move(item);
        // memory pointer from the item to buffer slot. ~1 CPU cycle (zero dynamic memory allocations).
        head++;
        return true;
    }

    // Pop an item from the buffer. 
    // optional<T> is a container introduced in C++17 that holds either a value of type T or nothing (nullopt)
    // Returns nullopt if empty. or we can manage to modify it like
    //                                                                   buffer.pop().valure_or(-1)
    // Why need it? as we are using generic type returning -1 or nullptr doesnt work
    optional<T> pop() {
        if (is_empty()) {
            return nullopt;
        }

        T item = move(ring_buffer[tail & mask]); // 1 CPU cycle
        tail++;
        return item;
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
};


void INT_example() {
    // Initialize buffer with cap 4 (must be power of 2)
    RingBuffer<int> rb(4);

    cout << "Push(till max limit) 10, 20, 30, 40...\n";
    int i=10;
    while(!rb.is_full()){
        rb.push(i);
        i+=10;
    }
    // rb.push(20);
    // rb.push(30);
    // rb.push(40);
    
    // Buffer is now full
    cout << "Is full: " << (rb.is_full() ? "Yes" : "No") << "\n";
    cout << "Push" << i << "(should fail): " << (rb.push(i) ? "Success" : "Failed") << "\n";
    i+=10;
    
    // Pop items
    cout << "Popped: " << rb.pop().value_or(-1) << "\n"; // 60
    cout << "Popped: " << rb.pop().value_or(-1) << "\n"; // 70
    cout << "Buffer size: " << rb.size() << "\n";
    
    cout << "Pusheed till max! \n";
    while(!rb.is_full()){
        rb.push(i);
        i+=10;
    }
    cout << "Buffer size: " << rb.size() << "\n";
    
    
    while (!rb.is_empty()) {
        cout << "Popped: " << rb.pop().value() << "\n";
    }

    cout << "Popped: " << rb.pop().value_or(-1) << "\n"; // -1

    // return 0;
}

void STRING_example() {
    // Initialize buffer with capacity 4 (must be power of 2)
    RingBuffer<string> rb(4);

    cout << "Pushing A, B, C, D...\n";
    rb.push("A");
    rb.push("B");
    rb.push("C");
    rb.push("D");

    // Buffer is now full
    cout << "Is full: " << (rb.is_full() ? "Yes" : "No") << "\n";
    cout << "Push E (should fail): " << (rb.push("E") ? "Success" : "Failed") << "\n";

    // Pop 2 items
    cout << "Popped: " << rb.pop().value_or("NOTHING") << "\n"; // 10
    cout << "Popped: " << rb.pop().value_or("NOTHING") << "\n"; // 20
    
    // Push 2 more items (demonstrating wrap-around)
    rb.push("F");
    rb.push("G");
    
    cout << "Buffer size: " << rb.size() << "\n";
    
    // Drain remaining elements
    while (!rb.is_empty()) {
        cout << "Popped: " << rb.pop().value() << "\n";
    }

    cout << "Popped: " << rb.pop().value_or("Popped NOTHING") << "\n"; // 20

    // return 0;
}

int main(){
    // STRING_example();
    INT_example();

    return 0;
}
