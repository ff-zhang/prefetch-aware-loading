//
// Created by Felix Zhang on 2025-09-19.
//

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <utility>

#include "types.h"

namespace fdl::common {

template<std::size_t N>
class AppendLog {
public:
    AppendLog() = default;

    void initialize(char* buffer) noexcept {
        assert(buffer_ == nullptr && "Buffer has been previously set");
        buffer_ = buffer;
    }

    /// @brief Produce data into the buffer. Note the data is copied
    /// @param data pointer to data to be copied
    /// @param size size of the data to be copied
    void produce(const char* data, const size_t size) {
        // Update the producer index
        uint64_t producer_idx;
        do {
            producer_idx = producer_index_.value.load(std::memory_order_relaxed);
            assert(producer_idx + size < N);
        }
        while (!producer_index_.value.compare_exchange_weak(producer_idx, producer_idx + size));

        std::memcpy(buffer_ + producer_idx, data, size);

        // Update progress
        uint64_t progress;
        do {
            progress = progress_.value.load(std::memory_order_relaxed);
        } while (!progress_.value.compare_exchange_weak(progress, progress + size));
    }

    /// @brief Produce two buffers into the buffer as one request. Note the data is copied
    /// @param data1 pointer to first buffer to be copied
    /// @param data2 pointer to second buffer to be copied
    /// @param size1 size of the first buffer to be copied
    /// @param size2 size of the second buffer to be copied
    void produce(const char *data1, const char *data2, const size_t size1, const size_t size2) {
        const size_t size = size1 + size2;

        // Update the producer index
        uint64_t producer_idx;
        do {
            producer_idx = producer_index_.value.load(std::memory_order_relaxed);
            assert(producer_idx + size < capacity());
        }
        while (!producer_index_.value.compare_exchange_weak(producer_idx, producer_idx + size));

        std::memcpy(buffer_ + producer_idx, data1, size);
        std::memcpy(buffer_ + producer_idx, data2, size);

        // Update progress
        uint64_t progress;
        do {
            progress = progress_.value.load(std::memory_order_relaxed);
        } while (!progress_.value.compare_exchange_weak(progress, progress + size));

    }

    std::pair<char*, size_t> read(const size_t consumer_index) {
        return {buffer_ + consumer_index, producer_index_.value.load() - consumer_index};
    }

    // Getters and setters
    char* buffer() const noexcept {
        return buffer_;
    };

    static size_t capacity() { return N; }
    static size_t size_of() { return N; }

private:
    CacheAligned<std::atomic<uint64_t>> producer_index_{ .value = 0 };
    CacheAligned<std::atomic<uint64_t>> progress_{ .value = 0 };

    static_assert((N > 1) & !(N & (N - 1)), "Buffer size must be a power of two");
    char* buffer_{nullptr};
};

}
