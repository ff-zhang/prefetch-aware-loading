//
// Created by Felix Zhang on 2025-09-10.
//

#pragma once

#include <array>
#include <cassert>

#include "append_log.h"
#include "concurrent_queue.h"
#include "ring_buffer.h"
#include "parameters/configuration.h"
#include "pipelines/RDMA/request.h"
#include "pipelines/RDMA/ring_buffer_remote.h"
#include "utils/parameters/derived.h"

namespace fdl {

#ifdef DOCA_ARCH_DPU
using UnifiedBuffer = RingBuffer<PARTITION_BUFFER_SIZE, PARTITION_BUFFER_PACKETS>;
using UnifiedHandle = RingBufferHandle<PARTITION_BUFFER_SIZE>;
#endif
#ifdef DOCA_ARCH_HOST
using UnifiedBuffer = RingBuffer<CLIENT_BUFFER_SIZE, CLIENT_BUFFER_PACKETS>;
using UnifiedHandle = RingBufferHandle<CLIENT_BUFFER_SIZE>;
#endif
using UnifiedRegion = RingBufferRegion;

using MemoryBuffer = common::AppendLog<HOST_BUFFER_SIZE>;

template<typename T, std::size_t N>
class BufferPool {
public:
    std::vector<T*> acquire(size_t n) {
        assert(n <= N);

        auto buffers = std::vector<T*>(n);
        auto count = available_.wait_dequeue_bulk(buffers.data(), n);
        assert(count == n);
        return buffers;
    }

    void release(std::vector<T*>&& indexes) {
        available_.enqueue_bulk(indexes.data(), indexes.size());
    }

    char* data() const {
        return data_;
    }

    // Number of buffers in the pool
    static constexpr size_t capacity() {
        return N;
    }

    static constexpr size_t size_of() {
        return N * T::size_of();
    }

private:
    friend struct BufferPools;

    BufferPool() {
        // Ensure queue is constructed before use (by other threads), including making the memory effects of construction visible
        std::atomic_thread_fence(std::memory_order_acq_rel);

        data_ = new(std::align_val_t{64}) char[size_of()];
        std::array<T*, N> free{};
        for (std::size_t i = 0; i < N; ++i) {
            buffers_[i].initialize(data_ + i * T::size_of());
            free[i] = &buffers_[i];
        }
        available_.enqueue_bulk(free.data(), buffers_.size());
    }

    ~BufferPool() {
        delete[] data_;
        data_ = nullptr;
    }

    char* data_{};
    std::array<T, N> buffers_{};

    ConcurrentQueue<T*> available_{N * moodycamel::ConcurrentQueueDefaultTraits::BLOCK_SIZE};
};

struct BufferPools {
#ifdef DOCA_ARCH_DPU
    // Used by the frontend to store incoming data for each partition
    static BufferPool<UnifiedBuffer, PARTITION_BUFFER_COUNT>& unified() {
        static BufferPool<UnifiedBuffer, PARTITION_BUFFER_COUNT> pool;
        return pool;
    }
#endif
#ifdef DOCA_ARCH_HOST
    // Used by the frontend to store incoming data for each partition
    static BufferPool<UnifiedBuffer, CLIENT_BUFFER_COUNT>& unified() {
        static BufferPool<UnifiedBuffer, CLIENT_BUFFER_COUNT> pool;
        return pool;
    }
#endif

    // Used to store temporary data for EC
    static BufferPool<UnifiedBuffer, EC_PIPELINE_COUNT>& scratch() {
        static BufferPool<UnifiedBuffer, EC_PIPELINE_COUNT> pool;
        return pool;
    }

    // Used to store information about remote ring buffers per job
    static constexpr size_t VIEW_BUFFER_COUNT = MAX_PARALLEL_JOB_COUNT * MAX_CLIENT_COUNT + SERVER_COUNT * PARTITION_BUFFER_COUNT;
    static BufferPool<rdma::ServerUnifiedBuffer, VIEW_BUFFER_COUNT>& remote() {
        static BufferPool<rdma::ServerUnifiedBuffer, VIEW_BUFFER_COUNT> pool;
        return pool;
    }

#ifdef DOCA_ARCH_HOST
    // Used by the host memory backend to materialize partition data
    static BufferPool<MemoryBuffer, HOST_BUFFER_COUNT>& memory() {
        static BufferPool<MemoryBuffer, HOST_BUFFER_COUNT> pool;
        return pool;
    }
#endif
};

}
