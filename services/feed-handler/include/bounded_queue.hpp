#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>

// Phase 1 steps 2 and 6: a bounded producer/consumer queue with an explicit
// overflow policy.
//
// Bounded, because "unbounded" is not a policy. If the consumer falls behind an
// unbounded queue converts a throughput problem into an out-of-memory crash
// hours later, a long way from the cause. A bound forces the decision GoalState
// insists on -- block the producer, or drop -- and turns it into a number.
enum class Overflow
{
    Block,      // producer waits for room; nothing is ever lost, the feed stalls
    DropNewest  // producer never waits; the newest tick is discarded and counted
};

template <typename T>
class BoundedQueue
{
public:
    explicit BoundedQueue(std::size_t capacity, Overflow policy = Overflow::Block)
        : capacity_(capacity ? capacity : 1), policy_(policy)
    {
    }

    // false = dropped (DropNewest, queue full) or the queue is closed.
    bool push(T value)
    {
        std::unique_lock<std::mutex> lk(mutex_);

        if (policy_ == Overflow::DropNewest)
        {
            if (closed_)
                return false;
            if (queue_.size() >= capacity_)
            {
                ++dropped_;
                return false;
            }
        }
        else
        {
            // The predicate is re-checked on every wake, which is exactly what
            // makes a spurious wakeup harmless: it finds the condition still
            // false and goes back to waiting. A bare wait() would fall through.
            not_full_.wait(lk, [this] { return closed_ || queue_.size() < capacity_; });
            if (closed_)
                return false;
        }

        queue_.push(std::move(value));
        if (queue_.size() > high_water_)
            high_water_ = queue_.size();

        lk.unlock();            // notify outside the lock: waking a thread that
        not_empty_.notify_one();// would immediately block on the mutex is waste
        return true;
    }

    // false = closed AND drained. Blocks while empty and open.
    bool pop_wait(T& out, const std::atomic<bool>& /*unused*/)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        not_empty_.wait(lk, [this] { return closed_ || !queue_.empty(); });

        if (queue_.empty())
            return false;       // closed and nothing left

        out = std::move(queue_.front());
        queue_.pop();

        lk.unlock();
        not_full_.notify_one();
        return true;
    }

    // Wake everyone. pop_wait keeps handing out items until the queue is empty,
    // so work already in flight is drained rather than thrown away -- that is
    // what "SIGTERM must never lose a buffered record" means in practice.
    void close()
    {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    uint64_t dropped() const
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return dropped_;
    }

    std::size_t high_water() const
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return high_water_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;

    std::queue<T> queue_;
    std::size_t capacity_;
    Overflow policy_;

    bool closed_ = false;
    uint64_t dropped_ = 0;
    std::size_t high_water_ = 0;
};
