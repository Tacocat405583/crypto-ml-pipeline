#pragma once

#include <atomic>
#include <cstddef>
#include <thread>
#include <utility>
#include <vector>

// Phase 1 steps 3 and 4: a lock-free single-producer / single-consumer ring.
//
// SINGLE producer and SINGLE consumer is not a soft guideline here, it is the
// whole proof. Each index is written by exactly one thread, which is why no
// mutex is needed. Point two producers at this and it breaks silently.
//
// Correctness rests entirely on the memory ordering:
//
//   producer: write the slot, THEN release-store the head.
//             The release publishes the slot write to anyone who acquires head.
//   consumer: acquire-load the head, THEN read the slot.
//             The acquire is what makes that slot write visible. A relaxed load
//             here would compile to the same instruction on x86 and still be a
//             data race -- the compiler is free to hoist the slot read above it.
//
// Each side loads its OWN index relaxed: nobody else writes it, so there is
// nothing to synchronise with.
template <typename T>
class SpscRing
{
public:
    explicit SpscRing(std::size_t capacity)
        : mask_(round_up_pow2(capacity) - 1), slots_(mask_ + 1)
    {
    }

    // false = full. One slot is always left empty so that a full ring and an
    // empty ring do not both read as head == tail.
    bool push(T value)
    {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next = (head + 1) & mask_;

        if (next == tail_.load(std::memory_order_acquire))
            return false;

        slots_[head] = std::move(value);
        head_.store(next, std::memory_order_release);
        return true;
    }

    // false = empty right now. Non-blocking.
    bool pop(T& out)
    {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);

        if (tail == head_.load(std::memory_order_acquire))
            return false;

        out = std::move(slots_[tail]);
        tail_.store((tail + 1) & mask_, std::memory_order_release);
        return true;
    }

    // Same shape as BoundedQueue::pop_wait so one templated consumer loop can
    // drive either. The trade is visible right here: there is no condition
    // variable to sleep on, so an idle consumer burns CPU spinning. Lower
    // latency when busy, wasted cycles when not.
    bool pop_wait(T& out, const std::atomic<bool>& closed)
    {
        for (;;)
        {
            if (pop(out))
                return true;
            if (closed.load(std::memory_order_acquire) && empty())
                return false;
            std::this_thread::yield();
        }
    }

    bool empty() const
    {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    // Only meaningful when called BY the producer: tail only ever moves in the
    // direction that frees space, so "not full" cannot go stale underneath us.
    // That lets the producer test for room without moving its value into a
    // push() that might fail and leave the value gutted.
    bool full() const
    {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        return ((head + 1) & mask_) == tail_.load(std::memory_order_acquire);
    }

    std::size_t capacity() const { return mask_; }

    // Present so this is drop-in with BoundedQueue's reporting. The ring itself
    // never drops -- push() returns false and the caller decides.
    uint64_t dropped() const { return 0; }
    std::size_t high_water() const { return 0; }

private:
    static std::size_t round_up_pow2(std::size_t n)
    {
        std::size_t p = 1;
        while (p < n)
            p <<= 1;
        return p < 2 ? 2 : p;
    }

    // Step 4, and the reason it is a separate benchmarked step: head_ and tail_
    // are written by DIFFERENT threads. Sitting adjacent they land on one cache
    // line, so every producer store invalidates the line the consumer is
    // reading -- the cores ping-pong a line they share no data through. That is
    // false sharing. alignas puts each index on its own line and the traffic
    // disappears.
    static constexpr std::size_t kCacheLine = 64;

    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
    alignas(kCacheLine) std::size_t mask_;

    std::vector<T> slots_;
};
