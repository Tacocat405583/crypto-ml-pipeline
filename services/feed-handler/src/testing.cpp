// =============================================================================
//  C++ Concurrency in Action -- Chapter 2: Managing threads
//  Runnable notes.  Every section of the chapter has: condensed notes, the
//  book's listing as real compiling code, and a demo you can run on its own.
//
//  BUILD -- standalone, needs no Boost/OpenSSL unlike the real feed_handler.
//  From the repo root:
//      g++ -std=c++20 -Wall -Wextra -O2 -pthread -o ch2 services/feed-handler/src/testing.cpp
//  Or via CMake from services/feed-handler (EXCLUDE_FROM_ALL, so the normal
//  feed_handler build never touches it):
//      cmake --preset mingw
//      cmake --build build --target ch2_notes     # -> build/ch2_notes.exe
//
//  RUN:
//      ./ch2                # list every demo
//      ./ch2 all            # run every safe demo, in chapter order
//      ./ch2 join           # run one demo by name
//
//  Demos tagged [UNSAFE] deliberately reproduce the bugs the book warns about
//  (dangling reference, std::terminate).  `all` skips them; run them by name
//  when you want to watch the failure happen.
// =============================================================================

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <exception>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// -----------------------------------------------------------------------------
// Scaffolding: printing from several threads at once.
// std::cout will not corrupt itself, but the pieces of two "<<" chains can
// interleave.  One mutex fixes that.  Mutexes are chapter 3 -- this is here only
// so the output stays readable.
// -----------------------------------------------------------------------------
namespace lab {

std::mutex io_mtx;

void say(const std::string& msg) {
    std::lock_guard<std::mutex> lk(io_mtx);
    std::cout << msg << '\n';
}

std::string id_str(std::thread::id id) {
    std::ostringstream os;
    os << id;
    return os.str();
}

std::string this_id() { return id_str(std::this_thread::get_id()); }

void nap(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

}  // namespace lab

using lab::id_str;
using lab::nap;
using lab::say;
using lab::this_id;


// =============================================================================
// 2.1.1  Launching a thread
// =============================================================================
// * A thread starts the moment you construct a std::thread with a callable.
//   Any callable works: function pointer, function object, lambda.
// * A function object is COPIED into storage owned by the new thread, so the
//   copy must behave like the original.
// * MOST VEXING PARSE:
//       std::thread my_thread(background_task());
//   That declares a *function* named my_thread returning std::thread.  Dodge it
//   with a named variable, extra parens ((background_task())), or braces
//   {background_task()}.
// * Before the std::thread object is destroyed you MUST have called join() or
//   detach().  Otherwise ~thread() calls std::terminate() and the program dies.
//   The deadline is the destructor, not the end of the thread's work.
// * If you don't wait, every piece of data the thread touches must outlive it.
//
// TRY: uncomment the vexing-parse line below and read the compiler error.
// TRY: delete one of the join() calls and watch the process terminate.
// -----------------------------------------------------------------------------

namespace s2_1_1 {

void do_some_work() { say("  [t1] plain void() function"); }

class background_task {
public:
    void operator()() const { say("  [t2] function object (copied into the thread)"); }
};

void demo_launch() {
    std::thread t1(do_some_work);          // function pointer

    background_task f;
    std::thread t2(f);                     // named object -- no parse ambiguity

    // std::thread bad(background_task()); // <-- MOST VEXING PARSE: a declaration.
    std::thread t3{background_task()};     // braces make it unambiguous
    std::thread t4((background_task()));   // so do extra parens

    std::thread t5([] { say("  [t5] lambda"); });

    say("  [main] five threads launched; joining them now");
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    say("  [main] all joined");
}

// --- Listing 2.1: a function that returns while a thread still uses its locals -
struct func {
    int& i;
    explicit func(int& i_) : i(i_) {}
    void operator()() {
        for (unsigned j = 0; j < 1000000; ++j) {
            i += 1;   // after oops() returns this is a dangling reference
        }
    }
};

void oops() {
    int some_local_state = 0;
    func my_func(some_local_state);
    std::thread my_thread(my_func);
    my_thread.detach();          // we explicitly do NOT wait
}                                // some_local_state dies HERE, thread keeps writing

void demo_dangle() {
    say("  [UNSAFE] listing 2.1 -- detached thread writing to a dead local.");
    say("  Undefined behaviour: may print nothing, corrupt the stack, or crash.");
    oops();
    nap(50);
    say("  [main] survived this time -- which is exactly what makes it dangerous");
}

}  // namespace s2_1_1


// =============================================================================
// 2.1.2  Waiting for a thread to complete
// =============================================================================
// * join() blocks until the thread finishes, then cleans up its storage.
// * join() is all-or-nothing.  "Has it finished?" or "wait 100ms" needs
//   condition variables / futures (chapter 4).
// * After join() the std::thread is associated with no thread at all:
//   joinable() == false, and joining twice throws std::system_error.
//
// TRY: swap join() for detach() in demo_join and see what the counter reads.
// -----------------------------------------------------------------------------

namespace s2_1_2 {

void demo_join() {
    int counter = 0;
    // Parens here are the most vexing parse (gcc: -Wvexing-parse); braces are not.
    std::thread t{s2_1_1::func(counter)};   // holds int& -- safe only because we join

    say(std::string("  joinable() before join: ") + (t.joinable() ? "true" : "false"));
    t.join();
    say(std::string("  joinable() after  join: ") + (t.joinable() ? "true" : "false"));
    say("  counter after join = " + std::to_string(counter) + " (deterministic: 1000000)");

    try {
        t.join();                            // second join on a non-joinable thread
    } catch (const std::system_error& e) {
        say(std::string("  second join() threw system_error: ") + e.what());
    }
}

}  // namespace s2_1_2


// =============================================================================
// 2.1.3  Waiting in exceptional circumstances
// =============================================================================
// * The dangerous window is: thread started -> exception thrown -> join() skipped
//   -> ~thread() -> std::terminate().
// * Listing 2.2 fixes it with try/catch + join on both paths.  Verbose, and easy
//   to get the scope slightly wrong.
// * Listing 2.3 fixes it properly with RAII: thread_guard joins in its
//   destructor, which runs on every exit path.
// * thread_guard checks joinable() first (join twice == error) and deletes the
//   copy ctor / copy assignment, because a copy could outlive the thread it
//   refers to.
//
// TRY: delete the guard in f_raii and call it with should_throw=true -- that is
//      the terminate() the guard exists to prevent.
// -----------------------------------------------------------------------------

namespace s2_1_3 {

void do_something_in_current_thread(bool should_throw) {
    say("  [main] doing local work...");
    if (should_throw) throw std::runtime_error("boom in the current thread");
}

// --- Listing 2.2: manual try/catch -------------------------------------------
void f_manual(bool should_throw) {
    int some_local_state = 0;
    std::thread t{s2_1_1::func(some_local_state)};
    try {
        do_something_in_current_thread(should_throw);
    } catch (...) {
        t.join();          // join on the exceptional path...
        throw;
    }
    t.join();              // ...and on the normal one
}

// --- Listing 2.3: RAII --------------------------------------------------------
class thread_guard {
    std::thread& t;

public:
    explicit thread_guard(std::thread& t_) : t(t_) {}

    ~thread_guard() {
        if (t.joinable()) {      // join() may be called only once
            t.join();
        }
    }

    thread_guard(const thread_guard&) = delete;
    thread_guard& operator=(const thread_guard&) = delete;
};

void f_raii(bool should_throw) {
    int some_local_state = 0;
    std::thread t{s2_1_1::func(some_local_state)};
    thread_guard g(t);                          // destroyed before t, joins it
    do_something_in_current_thread(should_throw);
}

void demo_try_join() {
    say("  -- listing 2.2, normal exit:");
    f_manual(false);
    say("  -- listing 2.2, exceptional exit:");
    try {
        f_manual(true);
    } catch (const std::exception& e) {
        say(std::string("  caught in demo: ") + e.what() + " (thread was joined first)");
    }
}

void demo_guard() {
    say("  -- listing 2.3, normal exit:");
    f_raii(false);
    say("  -- listing 2.3, exceptional exit:");
    try {
        f_raii(true);
    } catch (const std::exception& e) {
        say(std::string("  caught in demo: ") + e.what() + " (~thread_guard joined it)");
    }
}

}  // namespace s2_1_3


// =============================================================================
// 2.1.4  Running threads in the background (detach)
// =============================================================================
// * detach() severs the std::thread from the thread of execution.  You can never
//   get it back and never join it; the C++ runtime reclaims its resources.
// * Detached == "daemon thread": long-running background work, or fire-and-forget.
// * You can only detach when joinable() is true -- same precondition as join().
// * Listing 2.4: a word processor spawning one detached thread per document.
// * NOTE: when main() returns the process exits and detached threads are killed
//   mid-stride.  That is why this demo sleeps before returning.
//
// TRY: remove the nap() below and see how much of the workers output vanishes.
// -----------------------------------------------------------------------------

namespace s2_1_4 {

void background_worker() {
    for (int i = 0; i < 3; ++i) {
        say("  [daemon " + this_id() + "] tick " + std::to_string(i));
        nap(30);
    }
    say("  [daemon] done");
}

// --- Listing 2.4 shape (no GUI, just the thread part) -------------------------
void edit_document(const std::string& filename) {
    say("  [editor] opened " + filename + " on thread " + this_id());
    nap(40);
    say("  [editor] closed " + filename);
}

void demo_detach() {
    std::thread t(background_worker);
    t.detach();
    say(std::string("  joinable() after detach: ") + (t.joinable() ? "true" : "false"));

    const std::string docs[] = {"notes.txt", "chapter2.md"};
    for (const std::string& name : docs) {
        std::thread doc(edit_document, name);   // listing 2.4: one thread per document
        doc.detach();
    }

    say("  [main] not waiting for anyone -- just sleeping so you can see the output");
    nap(200);
}

}  // namespace s2_1_4


// =============================================================================
// 2.2  Passing arguments to a thread function
// =============================================================================
// * Extra ctor args are COPIED into the thread internal storage, then passed to
//   the callable as RVALUES -- even when the parameter is a reference.
// * Consequence 1: a char buffer[] passed where a std::string const& is expected
//   is copied AS A POINTER; the conversion to std::string happens on the new
//   thread, possibly after the buffer died.  Fix: convert at the call site --
//   std::thread t(f, 3, std::string(buffer)).
// * Consequence 2: a non-const reference parameter will NOT compile (you cannot
//   bind an rvalue to it).  Fix: std::ref(data).
// * Semantics match std::bind, so member functions work too:
//       std::thread t(&X::do_lengthy_work, &my_x, arg1, ...);
// * Move-only arguments (std::unique_ptr) go in with std::move: ownership moves
//   into the thread storage, then into the function parameter.
//
// TRY: delete the std::ref in demo_ref and read the (famously ugly) error.
// TRY: pass buffer directly instead of std::string(buffer) in not_oops.
// -----------------------------------------------------------------------------

namespace s2_2 {

void f(int i, const std::string& s) {
    say("  [thread] f(" + std::to_string(i) + ", " + s + ")");
}

void not_oops(int some_param) {
    char buffer[1024];
    std::snprintf(buffer, sizeof(buffer), "%i", some_param);
    std::thread t(f, 3, std::string(buffer));   // convert HERE, not on the new thread
    t.join();
}

void print_value(int v) { say("  [thread] got a copy: " + std::to_string(v)); }

void demo_args() {
    std::thread t(f, 3, "hello");    // travels as char const*, converts on thread t
    t.join();

    not_oops(42);                    // the dangling-buffer fix

    int n = 1;
    std::thread t2(print_value, n);
    n = 99;                          // no effect: n was copied at construction
    t2.join();
    say("  [main] n is now " + std::to_string(n) + ", the thread still saw 1");
}

struct widget_data {
    int value = 0;
};

void update_data_for_widget(int widget_id, widget_data& data) {
    data.value = widget_id * 10;
    say("  [thread] wrote " + std::to_string(data.value) + " through the reference");
}

void demo_ref() {
    widget_data data;
    // std::thread bad(update_data_for_widget, 7, data);  // <-- does not compile
    std::thread t(update_data_for_widget, 7, std::ref(data));
    t.join();
    say("  [main] data.value = " + std::to_string(data.value));
}

class X {
public:
    void do_lengthy_work(int n) {
        say("  [thread] X::do_lengthy_work(" + std::to_string(n) + ") on " + this_id());
    }
};

void demo_member_fn() {
    X my_x;
    std::thread t(&X::do_lengthy_work, &my_x, 5);   // object pointer is argument #1
    t.join();
}

struct big_object {
    int data = 0;
    void prepare_data(int v) { data = v; }
};

void process_big_object(std::unique_ptr<big_object> p) {
    say("  [thread] owns the big_object now, data = " + std::to_string(p->data));
}

void demo_move_arg() {
    std::unique_ptr<big_object> p(new big_object);
    p->prepare_data(42);
    std::thread t(process_big_object, std::move(p));   // move-only argument
    say(std::string("  [main] p is now ") + (p ? "non-null" : "null -- ownership left"));
    t.join();
}

}  // namespace s2_2


// =============================================================================
// 2.3  Transferring ownership of a thread
// =============================================================================
// * std::thread is movable, not copyable -- like std::unique_ptr / std::ifstream.
//   Exactly one std::thread object owns a given thread of execution.
// * Moving from a temporary is implicit; moving from a named object needs
//   std::move.
// * DANGER: move-assigning onto a std::thread that ALREADY owns a running thread
//   calls std::terminate().  Same rule as the destructor -- you may not silently
//   drop a thread.
// * Listing 2.5: return a std::thread from a function (ownership out).  A
//   function taking std::thread by value takes ownership in.
// * Listing 2.6 scoped_thread: OWNS the thread instead of referencing it, so it
//   cannot outlive it and nobody else can join/detach it.  It checks joinable()
//   in the CONSTRUCTOR (throws logic_error), so the destructor can just join().
// * Listing 2.7 joining_thread: the C++17 proposal that landed in C++20 as
//   std::jthread (minus cooperative cancellation via stop_token).
// * Listing 2.8: std::vector<std::thread> + emplace_back, then join them all.
//
// TRY: replace joining_thread with std::jthread and delete the join loop.
// -----------------------------------------------------------------------------

namespace s2_3 {

void some_function() { say("  [thread] some_function on " + this_id()); nap(60); }

void some_other_function(int n) {
    say("  [thread] some_other_function(" + std::to_string(n) + ") on " + this_id());
    nap(60);
}

void demo_move() {
    std::thread t1(some_function);
    say("  t1 owns " + id_str(t1.get_id()));

    std::thread t2 = std::move(t1);          // explicit move: named source
    say("  after t2 = move(t1):  t1 -> " + id_str(t1.get_id()) +
        " (not-any-thread), t2 -> " + id_str(t2.get_id()));

    t1 = std::thread(some_other_function, 42);  // implicit: source is a temporary
    say("  t1 now owns " + id_str(t1.get_id()));

    std::thread t3;                            // default-constructed: owns nothing
    t3 = std::move(t2);
    say("  after t3 = move(t2):  t2 -> " + id_str(t2.get_id()) +
        ", t3 -> " + id_str(t3.get_id()));

    // t1 = std::move(t3);  // <-- t1 already owns a thread: std::terminate().
    //                            See the move-terminate demo.

    t1.join();
    t3.join();
}

// --- Listing 2.5: ownership out of / into a function --------------------------
std::thread make_thread_by_temporary() { return std::thread(some_function); }

std::thread make_thread_by_named() {
    std::thread t(some_other_function, 42);
    return t;                                   // implicit move on return
}

void take_ownership(std::thread t) {            // ownership in, by value
    say("  [main] take_ownership got " + id_str(t.get_id()));
    t.join();
}

void demo_move_terminate() {
    say("  [UNSAFE] move-assigning onto a std::thread that already owns one.");
    say("  Expect std::terminate() -- the process dies right here.");
    std::cout.flush();
    std::thread t1(some_function);
    std::thread t3(some_function);
    t1 = std::move(t3);                         // <-- terminate()
    say("  unreachable");
    t1.join();
}

}  // namespace s2_3

namespace s2_3 {

void demo_transfer() {
    // Listing 2.5 in both directions.
    std::thread a = make_thread_by_temporary();
    std::thread b = make_thread_by_named();
    say("  [main] got " + id_str(a.get_id()) + " and " + id_str(b.get_id()) + " back");
    a.join();
    b.join();

    take_ownership(std::thread(some_function));   // temporary: implicit move
    std::thread c(some_function);
    take_ownership(std::move(c));                 // named: explicit move
    say(std::string("  [main] c after handing it over: ") +
        (c.joinable() ? "still joinable?!" : "empty, as expected"));
}

// --- Listing 2.6: scoped_thread ------------------------------------------------
class scoped_thread {
    std::thread t;

public:
    explicit scoped_thread(std::thread t_) : t(std::move(t_)) {
        if (!t.joinable()) throw std::logic_error("No thread");
    }
    ~scoped_thread() { t.join(); }

    scoped_thread(const scoped_thread&) = delete;
    scoped_thread& operator=(const scoped_thread&) = delete;
};

void demo_scoped() {
    int some_local_state = 0;
    {
        scoped_thread t{std::thread(s2_1_1::func(some_local_state))};
        say("  [main] scoped_thread owns the thread; doing local work");
    }   // destructor joins here
    say("  [main] joined by ~scoped_thread, counter = " + std::to_string(some_local_state));

    try {
        scoped_thread bad{std::thread()};       // no associated thread
    } catch (const std::logic_error& e) {
        say(std::string("  ctor rejected an empty thread: ") + e.what());
    }
}

}  // namespace s2_3

namespace s2_3 {

// --- Listing 2.7: joining_thread (the hand-rolled pre-std::jthread) -----------
class joining_thread {
    std::thread t;

public:
    joining_thread() noexcept = default;

    template <typename Callable, typename... Args>
    explicit joining_thread(Callable&& func, Args&&... args)
        : t(std::forward<Callable>(func), std::forward<Args>(args)...) {}

    explicit joining_thread(std::thread t_) noexcept : t(std::move(t_)) {}

    joining_thread(joining_thread&& other) noexcept : t(std::move(other.t)) {}

    joining_thread& operator=(joining_thread&& other) noexcept {
        if (joinable()) join();          // never silently drop a running thread
        t = std::move(other.t);
        return *this;
    }

    joining_thread& operator=(std::thread other) noexcept {
        if (joinable()) join();
        t = std::move(other);
        return *this;
    }

    ~joining_thread() noexcept {
        if (joinable()) join();
    }

    void swap(joining_thread& other) noexcept { t.swap(other.t); }
    std::thread::id get_id() const noexcept { return t.get_id(); }
    bool joinable() const noexcept { return t.joinable(); }
    void join() { t.join(); }
    void detach() { t.detach(); }
    std::thread& as_thread() noexcept { return t; }
    const std::thread& as_thread() const noexcept { return t; }
};

void demo_joining() {
    {
        joining_thread jt(some_other_function, 7);
        say("  [main] joining_thread owns " + id_str(jt.get_id()));
    }   // joins here, no explicit call
    say("  [main] the destructor joined it");

    joining_thread a(some_function);
    a = joining_thread(some_other_function, 99);   // move-assign joins a first
    say("  [main] move-assignment joined the old thread before taking the new one");
}

// --- Listing 2.8: a vector of threads -----------------------------------------
void do_work(unsigned id) {
    if (id % 5 == 0) say("  [worker " + std::to_string(id) + "] on " + this_id());
}

void demo_vector() {
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < 20; ++i) {
        threads.emplace_back(do_work, i);     // constructs the thread in place
    }
    for (auto& entry : threads) entry.join();
    say("  [main] all 20 joined (only every 5th one printed)");
}

}  // namespace s2_3


// =============================================================================
// 2.4  Choosing the number of threads at runtime
// =============================================================================
// * std::thread::hardware_concurrency() is a HINT (often the core count).  It may
//   return 0 when the answer is not available -- pick your own fallback.
// * Oversubscription (more threads than the hardware supports) costs you in
//   context switches, so cap at the hint.
// * Listing 2.9 parallel_accumulate:
//     - min_per_thread avoids 32 threads for 5 elements
//     - max_threads = ceil(length / min_per_thread)
//     - num_threads = min(hardware or fallback 2, max_threads)
//     - launch num_threads-1 threads; THIS thread handles the last, ragged block
//     - results land in a vector<T> passed by std::ref, because a thread cannot
//       return a value (that is futures, chapter 4)
// * Caveats the book flags: float/double addition is not associative, so the
//   blocked result can differ from std::accumulate; iterators must be forward,
//   not input; T must be default-constructible; and this version is NOT
//   exception-safe (the std::thread ctor itself can throw) -- chapter 8 fixes it.
//
// TRY: set min_per_thread to 1 and time it -- watch oversubscription cost you.
// TRY: swap long long for double and diff the two sums.
// -----------------------------------------------------------------------------

namespace s2_4 {

template <typename Iterator, typename T>
struct accumulate_block {
    void operator()(Iterator first, Iterator last, T& result) {
        result = std::accumulate(first, last, result);
    }
};

template <typename Iterator, typename T>
T parallel_accumulate(Iterator first, Iterator last, T init) {
    const unsigned long length = static_cast<unsigned long>(std::distance(first, last));
    if (!length) return init;

    const unsigned long min_per_thread = 25;
    const unsigned long max_threads = (length + min_per_thread - 1) / min_per_thread;
    const unsigned long hardware_threads = std::thread::hardware_concurrency();
    const unsigned long num_threads =
        std::min(hardware_threads != 0 ? hardware_threads : 2UL, max_threads);
    const unsigned long block_size = length / num_threads;

    std::vector<T> results(num_threads);
    std::vector<std::thread> threads(num_threads - 1);   // one fewer: we are a worker

    Iterator block_start = first;
    for (unsigned long i = 0; i < (num_threads - 1); ++i) {
        Iterator block_end = block_start;
        std::advance(block_end, block_size);
        threads[i] = std::thread(accumulate_block<Iterator, T>(), block_start, block_end,
                                 std::ref(results[i]));
        block_start = block_end;
    }
    accumulate_block<Iterator, T>()(block_start, last, results[num_threads - 1]);

    for (auto& entry : threads) entry.join();
    return std::accumulate(results.begin(), results.end(), init);
}

void demo_accumulate() {
    const unsigned hc = std::thread::hardware_concurrency();
    say("  hardware_concurrency() = " + std::to_string(hc) +
        (hc == 0 ? " (unknown -- fall back to 2)" : ""));

    std::vector<long long> v(20000000);
    for (std::size_t i = 0; i < v.size(); ++i) v[i] = static_cast<long long>(i % 100);

    const auto t0 = std::chrono::steady_clock::now();
    const long long serial = std::accumulate(v.begin(), v.end(), 0LL);
    const auto t1 = std::chrono::steady_clock::now();
    const long long parallel = parallel_accumulate(v.begin(), v.end(), 0LL);
    const auto t2 = std::chrono::steady_clock::now();

    const auto ms = [](auto a, auto b) {
        return std::to_string(
            std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count());
    };
    say("  std::accumulate      = " + std::to_string(serial) + "  in " + ms(t0, t1) + " ms");
    say("  parallel_accumulate  = " + std::to_string(parallel) + "  in " + ms(t1, t2) + " ms");
    say(std::string("  identical: ") + (serial == parallel ? "yes" : "NO"));
    say("  NOTE: summing longs is memory-bandwidth bound, so the speedup is well");
    say("  below Nx. Put real arithmetic in accumulate_block to see cores matter.");
}

}  // namespace s2_4


// =============================================================================
// 2.5  Identifying threads
// =============================================================================
// * std::thread::id comes from t.get_id() or std::this_thread::get_id().
// * A default-constructed id means "not any thread"; so does get_id() on a
//   std::thread with no associated thread of execution.
// * ids are copyable, comparable, and TOTALLY ORDERED (<, <=, ...), so they work
//   as std::map keys; std::hash<std::thread::id> exists for unordered_map.
// * Common trick: stash the master thread id before spawning, then branch on
//   std::this_thread::get_id() == master_thread inside the shared code path.
// * Streaming an id gives an implementation-defined string.  The only guarantee:
//   equal ids print the same, unequal ids print differently.  Debugging and
//   logging only -- the value carries no meaning, so do not index arrays with it.
//
// TRY: key an unordered_map on thread::id and count per-thread work.
// -----------------------------------------------------------------------------

namespace s2_5 {

std::thread::id master_thread;

void some_core_part_of_algorithm() {
    if (std::this_thread::get_id() == master_thread) {
        say("  [" + this_id() + "] I am the master -- doing the extra master work");
    }
    say("  [" + this_id() + "] doing the common work");
}

void demo_ids() {
    std::thread::id nobody;                       // "not any thread"
    std::thread empty;
    say("  default-constructed id : " + id_str(nobody));
    say("  empty thread .get_id() : " + id_str(empty.get_id()) +
        (empty.get_id() == nobody ? "  (equal -- both are not-any-thread)" : ""));

    master_thread = std::this_thread::get_id();
    say("  master thread id       : " + id_str(master_thread));

    std::vector<std::thread> workers;
    for (int i = 0; i < 3; ++i) workers.emplace_back(some_core_part_of_algorithm);
    some_core_part_of_algorithm();                // the master runs it too
    for (auto& w : workers) w.join();

    // ids as map keys -- the total ordering is what makes this legal
    std::map<std::thread::id, std::string> names;
    std::mutex m;
    std::vector<std::thread> named;
    for (int i = 0; i < 3; ++i) {
        named.emplace_back([i, &names, &m] {
            std::lock_guard<std::mutex> lk(m);
            names[std::this_thread::get_id()] = "worker-" + std::to_string(i);
        });
    }
    for (auto& t : named) t.join();
    say("  map<thread::id, name>, in id order:");
    for (const auto& kv : names) say("    " + id_str(kv.first) + " -> " + kv.second);
}

}  // namespace s2_5


// =============================================================================
//  Self-check -- answer these without scrolling up
// =============================================================================
//  1. A std::thread is destroyed while still joinable.  What happens, and why did
//     the committee pick that over an implicit join or detach?
//  2. Why does thread_guard test joinable() in its destructor while scoped_thread
//     tests it in its constructor?
//  3. std::thread t(f, 3, buffer) with char buffer[1024]: what exactly gets
//     copied, and when is the std::string built?
//  4. The argument is copied anyway, so why does a non-const reference parameter
//     refuse to compile without std::ref?
//  5. In parallel_accumulate, why launch num_threads-1 threads, and why is the
//     LAST block the ragged one?
//  6. Why does parallel_accumulate need forward iterators when std::accumulate
//     is happy with input iterators?
//  7. Which of these listings leak or dangle if the std::thread ctor throws?
// =============================================================================


// =============================================================================
//  Runner
// =============================================================================

struct Demo {
    const char* name;
    const char* section;
    const char* blurb;
    void (*fn)();
    bool safe;      // false => skipped by `all`; run it by name on purpose
};

const Demo demos[] = {
    {"launch",         "2.1.1", "function / functor / lambda, vexing parse", s2_1_1::demo_launch,       true},
    {"dangle",         "2.1.1", "[UNSAFE] listing 2.1 dangling local ref",   s2_1_1::demo_dangle,       false},
    {"join",           "2.1.2", "join, joinable, double join throws",        s2_1_2::demo_join,         true},
    {"try-join",       "2.1.3", "listing 2.2 manual try/catch join",         s2_1_3::demo_try_join,     true},
    {"guard",          "2.1.3", "listing 2.3 thread_guard RAII join",        s2_1_3::demo_guard,        true},
    {"detach",         "2.1.4", "detach + listing 2.4 daemon threads",       s2_1_4::demo_detach,       true},
    {"args",           "2.2",   "args are copied; dangling-buffer fix",      s2_2::demo_args,           true},
    {"ref",            "2.2",   "std::ref for reference parameters",         s2_2::demo_ref,            true},
    {"member-fn",      "2.2",   "&X::method plus an object pointer",         s2_2::demo_member_fn,      true},
    {"move-arg",       "2.2",   "std::move a unique_ptr into a thread",      s2_2::demo_move_arg,       true},
    {"move",           "2.3",   "t1/t2/t3 ownership dance",                  s2_3::demo_move,           true},
    {"transfer",       "2.3",   "listing 2.5 ownership in and out",          s2_3::demo_transfer,       true},
    {"move-terminate", "2.3",   "[UNSAFE] assign onto a live thread",        s2_3::demo_move_terminate, false},
    {"scoped",         "2.3",   "listing 2.6 scoped_thread",                 s2_3::demo_scoped,         true},
    {"joining",        "2.3",   "listing 2.7 joining_thread (pre-jthread)",  s2_3::demo_joining,        true},
    {"vector",         "2.3",   "listing 2.8 vector<thread> + join loop",    s2_3::demo_vector,         true},
    {"accumulate",     "2.4",   "listing 2.9 parallel_accumulate + timing",  s2_4::demo_accumulate,     true},
    {"ids",            "2.5",   "thread::id, master check, id as map key",   s2_5::demo_ids,            true},
};

void list_demos() {
    std::cout << "C++ Concurrency in Action -- chapter 2, Managing threads\n\n"
              << "  ch2 <name>    run one demo\n"
              << "  ch2 all       run every safe demo, in chapter order\n\n";
    for (const Demo& d : demos) {
        std::cout << "  " << std::left << std::setw(16) << d.name << std::setw(8)
                  << d.section << d.blurb << '\n';
    }
    std::cout << "\n[UNSAFE] demos reproduce the chapter's bugs on purpose; `all` skips them.\n";
}

void run(const Demo& d) {
    std::cout << "\n=== " << d.section << "  " << d.name << " -- " << d.blurb << " ===\n";
    d.fn();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        list_demos();
        return 0;
    }

    const std::string which = argv[1];

    if (which == "all") {
        for (const Demo& d : demos) {
            if (d.safe) run(d);
        }
        std::cout << "\nDone. Unsafe demos skipped -- run those by name.\n";
        return 0;
    }

    for (const Demo& d : demos) {
        if (which == d.name) {
            run(d);
            return 0;
        }
    }

    std::cout << "unknown demo: " << which << "\n\n";
    list_demos();
    return 1;
}
