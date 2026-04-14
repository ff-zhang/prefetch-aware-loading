//
// Created by Felix Zhang on 3/19/26.
//

#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "constants.h"

namespace fdl {

// IP addresses of the servers in the cluster
constexpr std::array<const char*, 2> SERVER_IPS = {"10.10.1.1",  "10.10.1.2"};

constexpr size_t CLIENT_BUFFER_SIZE = 4 * MiB;
constexpr size_t CLIENT_BUFFER_PACKETS = CLIENT_BUFFER_SIZE / 1024;

}

namespace fdl::rdma {

// Queue pair configuration
constexpr size_t MAX_CQE = 128;
constexpr int POLL_BATCH_SIZE = 16;
constexpr size_t MAX_INFLIGHT = 32;
static_assert((MAX_CQE & (MAX_CQE - 1)) == 0);

}
