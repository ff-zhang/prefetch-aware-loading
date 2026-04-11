//
// Created by Felix Zhang on 10/9/25.
//

#pragma once

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "concurrentqueue/concurrentqueue.h"
#include "concurrentqueue/lightweightsemaphore.h"

namespace fdl {

template<typename T>
class ConcurrentQueue {
public:
    explicit ConcurrentQueue(size_t capacity) :
        queue_(capacity * moodycamel::ConcurrentQueue<T>::BLOCK_SIZE), capacity_(capacity)
    {}

    bool enqueue_bulk(T* itemFirst, size_t count) {
        moodycamel::ProducerToken token(queue_);
        if ((moodycamel::details::likely)(queue_.enqueue_bulk(token, std::forward<T*>(itemFirst), count))) {
            semaphore_.signal(static_cast<ssize_t>(count));
            size_.fetch_add(count, std::memory_order_release);
            return true;
        }
        return false;
    }

    size_t wait_dequeue_bulk(T* itemFirst, const size_t count) {
        assert(count <= capacity_);

        size_t acquired = 0;
        while (acquired != count) {
            acquired = static_cast<size_t>(semaphore_.waitMany(static_cast<ssize_t>(count)));
        }

        size_t dequeued = 0;
        moodycamel::ConsumerToken token(queue_);
        while (dequeued != count) {
            dequeued += queue_.try_dequeue_bulk(token, itemFirst, count - dequeued);
        }
        size_.fetch_sub(dequeued, std::memory_order_release);
        return dequeued;
    }

    size_t size() const {
        return size_;
    }

private:
    moodycamel::ConcurrentQueue<T> queue_;
    moodycamel::LightweightSemaphore semaphore_;

    size_t capacity_;
    std::atomic<size_t> size_;
};

}
