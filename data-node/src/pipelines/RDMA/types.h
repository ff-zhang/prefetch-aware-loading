//
// Created by Felix ZXhangZhang on 1/7/26.
//

#pragma once

#include <atomic>
#include <vector>

#include <doca_rdma.h>

namespace fdl::rdma {

/* Buffers available to a single pipeline */
struct buffers_t {
    std::vector<doca_buf*>& unified;
    std::vector<doca_buf*>& wrap;
};

/* RDMA resources required per pipeline */
struct resources_t {
    doca_rdma* rdma_context = nullptr;          /* DOCA RDMA context */
    bool run_pe_progress = false;               /* Should the PE keep on progressing? */
};

/* RDMA resources required per connection */
struct connection_t {
    const void *rdma_conn_descriptor;	        /* DOCA RDMA connection descriptor */
    size_t rdma_conn_descriptor_size;	        /* DOCA RDMA connection descriptor size */
    void *remote_rdma_conn_descriptor;	        /* DOCA RDMA remote connection descriptor */
    size_t remote_rdma_conn_descriptor_size;    /* DOCA RDMA remote connection descriptor size */
};

/* User-supplied data for RDMA tasks */
struct user_data_t {
    std::atomic<doca_error_t> result{DOCA_SUCCESS};
    std::atomic<uint8_t> count{0};
};

struct op_data_t {
    uint8_t kind;
    uint32_t job;
    uint32_t partition;
    uint32_t slot;

    op_data_t() = default;
    explicit op_data_t(const uint32_t packed) {
        kind = static_cast<uint8_t>((packed >> 31) & 0x1);
        job = (packed >> 20) & 0x7FF;
        partition = (packed >> 12) & 0xFF;
        slot = packed & 0xFFF;
    }
    uint32_t pack() const {
        return (static_cast<uint32_t>(kind) << 31) | (job << 20) | (partition << 12) | slot;
    }
};

}
