//
// Created by Felix Zhang on 3/19/26.
//

#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "constants.h"

namespace fdl {

// Max. number of jobs that can be run in parallel
constexpr size_t MAX_PARALLEL_JOB_COUNT = 4;

// Max. number of clients that can participate in a shufflejob
constexpr size_t MAX_CLIENT_COUNT = 16;

// IP addresses of the servers in the shuffle service
constexpr std::array<const char*, 2> SERVER_IPS = {"10.10.2.200",  "10.10.2.201"};
constexpr uint16_t OOB_PORT = 8765;

// IP address of the coordinator node
constexpr std::pair<const char*, uint16_t> BROKER_ADDRESS = {SERVER_IPS[0], 4321};

// Used to buffer data when using TCP
constexpr size_t STREAM_BUFFER_SIZE = 4 * MiB;

constexpr size_t CLIENT_BUFFER_SIZE = 4 * MiB;
constexpr size_t CLIENT_BUFFER_COUNT = 256;
constexpr size_t CLIENT_BUFFER_PACKETS = CLIENT_BUFFER_SIZE / 1024;

constexpr size_t PARTITION_BUFFER_SIZE = 16 * MiB;
constexpr size_t PARTITION_BUFFER_COUNT = 256;
constexpr size_t PARTITION_BUFFER_PACKETS = PARTITION_BUFFER_SIZE / 1024;

constexpr size_t SCRATCH_BUFFER_SIZE = 2 * MiB;

constexpr size_t DMA_PIPELINE_COUNT = 8;
constexpr size_t SPDK_PIPELINE_COUNT = 8; // TODO(joz): Determine tha good count
constexpr size_t EC_PIPELINE_COUNT = 32;

constexpr size_t HOST_BUFFER_SIZE = 64 * MiB;
constexpr size_t HOST_BUFFER_COUNT = 256;

constexpr size_t EC_DATA_BLOCK_COUNT = 1;
constexpr size_t EC_PARITY_BLOCK_COUNT = 1;

// Controls whether local NVMe or NVMe-oF is used
constexpr bool USE_NVME_OF = true;

// Local NVMe device name.
const std::string NVME_PCIE_CONTROLLER_NAME = "c1:00.0"; // lspci | grep Non-Volatile
const std::string NVME_DEVICE_BASE_NAME = "Nvme0"; // ./scripts/rpc.py bdev_nvme_attach_controller -b Nvme0 -t PCIe -a NVME_PCIE_DEVICE_NAME

// NVMe-oF configuration (used when USE_NVME_OF is true)
constexpr bool NVME_OF_USE_RDMA = true;
const std::string NVME_OF_TARGET_ADDR = "10.10.2.100";
const std::string NVME_OF_TARGET_PORT = "4420";
const std::string NVME_OF_SUBNQN = "nqn.2016-06.io.spdk:cnode1";
const std::string NVME_OF_HOSTNQN = "";  // Empty uses default

}

namespace fdl::rdma {

// Queue pair configuration
constexpr size_t MAX_CQE = 128;
constexpr int POLL_BATCH_SIZE = 16;
constexpr size_t MAX_INFLIGHT = 32;
static_assert((MAX_CQE & (MAX_CQE - 1)) == 0);

}
