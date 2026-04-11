//
// Created by Felix Zhang on 3/26/26.
//

#pragma once

#include <array>
#include <infiniband/verbs.h>

#include "network.h"

namespace fdl::rdma {

constexpr size_t MAX_NUM_MR = 4;
constexpr uint32_t MAX_INLINE_DATA = 16;

enum mr_t {
    UNIFIED = 0,
    SCRATCH = 1,
    JOB = 2,
};

struct ibv_qp_info {
    union {
        uint16_t lid;
        ibv_gid gid;
    };
    uint32_t qp_num;
    uint8_t port_num;
    uint32_t psn;
    int max_rd_atomic;
    std::array<uint64_t, MAX_NUM_MR> addr;
    std::array<uint32_t, MAX_NUM_MR> rkey;
};

struct ibv_write_t {
    const char* source;
    const uint32_t length;
    const uint64_t destination;
    const mr_t local;
    const mr_t remote;
};

}

namespace fdl::header {

struct JobSetup {
    enum {SYN, ACK} kind;
    job_id_t job_id;
    partition_id_t num_partitions;
    size_t rank;
    rdma::ibv_qp_info qp_info;
};

}