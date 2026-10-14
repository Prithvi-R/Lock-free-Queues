
/*
You are given three inputs (command line)

1. a string
2. char count (integer)
3. thread count - no of Threads(integer).

Each thread then should start printing the chars in the string sequentially in the following format:
eg:

> myprinter ABCDEFGHIJ 3 4

outPut:
Using 4 threads to print ABCDEFGHIJ alternatively in set of 3 chars
Thread1: ABC
Thread2: DEF

Thread3: GHI
Thread4: JAB
Thread1: CDE
Thread2: FGH
Thread3: IJA

*/

// Can be done like Multiple Producer Multiple Consumer(MPMC) Problem_:
// Nah as it isnt at once; like everyone is printing in sequence not concurrently


#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace std;

// Shared synchronization primitives
mutex mtx;
condition_variable cv;
int turn = 0; // Tracks which thread index is allowed to print next

void print_sequence(int thread_id, const string& str, int char_count, int num_threads, int total_chars_to_print) {
    // Total characters printed globally across all threads
    static int global_printed_chars = 0; 
    
    // Track the absolute starting index in the infinite repeating string pattern
    static int string_cursor = 0; 

    while (true) {
        unique_lock<mutex> lock(mtx);

        // Wait until it is this specific thread's turn
        cv.wait(lock, [thread_id]() { return turn == thread_id; });

        // Exit condition: Check if the full required sequence has been completely printed
        if (global_printed_chars >= total_chars_to_print) {
            // Pass the turn to the next thread so they can wake up, check the exit condition, and terminate
            turn = (turn + 1) % num_threads;
            cv.notify_all();
            break;
        }

        // Determine how many characters this thread should print in this round
        int chars_to_print = min(char_count, total_chars_to_print - global_printed_chars);

        // Print thread identifier
        cout << "Thread_" << (thread_id + 1) << ": ";

        // Print the slice of characters sequentially, wrapping around the string length
        for (int i = 0; i < chars_to_print; ++i) {
            cout << str[string_cursor % str.length()];
            string_cursor++;
        }
        cout << "\n";

        global_printed_chars += chars_to_print;

        // Pass the turn cleanly to the next thread in numerical order
        turn = (turn + 1) % num_threads;
        
        // Notify all waiting threads to re-evaluate their condition predicates
        cv.notify_all();
    }
}


int main() {
    string str;
    int char_count = 3;
    int num_threads = 4;

    // Read inputs using cin
    // cout << "Enter the string to print: ";
    cin >> str;

    // cout << "Enter the character count per turn: ";
    // cin >> char_count;

    // cout << "Enter the number of threads: ";
    // cin >> num_threads;

    // Basic input validation
    if (str.empty() || char_count <= 0 || num_threads <= 0) {
        cerr << "Error: Invalid inputs. Ensure counts are positive and string is not empty.\n";
        return 1;
    }

    // Set a total target boundary for the demo run (2 full rounds across all threads)
    int total_chars_to_print = num_threads * char_count * 2; 

    cout << "\nUsing " << num_threads << " threads to print " << str 
         << " alternatively in set of " << char_count << " chars\n\n";

    vector<thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(print_sequence, i, str, char_count, num_threads, total_chars_to_print);
    }

    for (auto& t : threads) {
        t.join();
    }

    return 0;
}