//
// Created by Felix Zhang on 3/4/26.
//

#pragma once

#include <atomic>
#include <cstdint>

namespace fdl {

struct context_t {
    alignas(64) std::atomic<uint64_t> count_;

    context_t() : count_(0) {}

    context_t(const context_t& other) : count_(other.count_.load(std::memory_order_acquire)) {};
    context_t& operator=(const context_t& other) {
        if (this != &other) {
            count_.store(other.count_.load(std::memory_order_acquire), std::memory_order_release);
        }
        return *this;
    }

    void set(const uint64_t count) { count_.store(count, std::memory_order_release); };
};

inline void produce_callback(void* context) {
    static_cast<context_t*>(context)->count_.fetch_sub(1, std::memory_order_acq_rel);
}

}
