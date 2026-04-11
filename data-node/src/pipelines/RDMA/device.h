//
// Created by Felix Zhang on 4/6/26.
//

#pragma once

#include <atomic>
#include <mutex>
#include <infiniband/verbs.h>

#include "config.h"
#include "utils/debug.h"

namespace fdl::rdma {

// Owner of the verbs context and protection domain shared by every QP
class Device {
public:
    static Device& instance() {
        static Device instance;
        return instance;
    }

    int open();

    ibv_context* context() const { return ctx_;  }
    ibv_pd* pd() const { return pd_;   }
    const ibv_device_attr& attr() const { return attr_; }

    // Round-robins across physical ports for load distribution
    uint8_t next_port_num();

    int max_rd_atomic() const {
        return std::min(attr_.max_qp_rd_atom, attr_.max_qp_init_rd_atom);
    }

private:
    Device() = default;
    ~Device();

    std::mutex mutex_{};

    ibv_context* ctx_ = nullptr;
    ibv_pd* pd_ = nullptr;
    ibv_device_attr attr_{};

    std::atomic<uint8_t> port_counter_{0};
};

}