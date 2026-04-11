//
// Created by joz on 2/15/26.
//

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <spdk/bdev.h>
#include <spdk/nvme.h>
#include <spdk/stdinc.h>

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "utils/constants.h"
#include "utils/configuration.h"

namespace fdl::spdk {
    using io_completion_cb = std::function<void(void *, int)>;

    // NVMe-oF transport types
    enum class nvmf_transport_type {
        LOCAL, // Local PCIe NVMe
        RDMA, // NVMe-oF over RDMA
        TCP // NVMe-oF over TCP
    };

    // NVMe-oF connection configuration
    struct nvmf_config_t {
        nvmf_transport_type transport = nvmf_transport_type::LOCAL;
        std::string target_addr;
        std::string target_port = "4420";
        std::string subnqn;
        std::string hostnqn;
    };

    // Metadata for a single written chunk
    struct chunk_metadata_t {
        uint64_t lba; // Starting LBA on device
        uint32_t lba_count; // Number of blocks
        uint32_t size; // Size in bytes
    };

    // Metadata tracking per buffer index
    struct buffer_metadata_t {
        std::mutex mutex;
        std::vector<chunk_metadata_t> chunks; // Chunks written to this buffer index
        std::atomic<uint64_t> next_lba{0}; // Next available LBA for writing
    };

    struct io_context_t {
        void *buffer;
        uint64_t lba; // Logical block address
        uint32_t lba_count;
        io_completion_cb callback;
        void *user_arg;

        // For synchronous operations
        std::atomic<bool> completed{false};
        std::atomic<int> result{0};
    };

    struct pipeline_t {
        spdk_bdev_desc *bdev_desc = nullptr; // Device handle
        spdk_bdev *bdev = nullptr;
        std::string bdev_name;
        std::string nvmf_bdev_name; // For NVMe-oF cleanup
        uint32_t block_size = 0;
        nvmf_transport_type transport = nvmf_transport_type::LOCAL;
    };

    class pipeline {
    public:
        pipeline_t pipe_;
        bool initialized_ = false;

        static pipeline &instance() {
            static pipeline instance;
            return instance;
        }

        /* Initialize SPDK with a local NVMe Device (bdev layer).
         * @param bdev_name: The device name (e.g. "nvme1n1").
         * @param num_buffers: Number of buffer indices to track metadata for.
         */
        int initialize(const std::string &bdev_base_name, size_t num_buffers = REMOTE_BUFFER_COUNT);

        /* Initialize SPDK with NVMe-oF (NVMe over Fabrics).
         * @param config: NVMe-oF connection configuration.
         * @param num_buffers: Number of buffer indices to track metadata for.
         */
        int initialize_nvmf(const nvmf_config_t &config, size_t num_buffers = REMOTE_BUFFER_COUNT);

        static void cleanup_thread();

        void cleanup();

        static void *allocate_buffer(size_t size);

        static void free_buffer(void *buffer);

        [[nodiscard]] pipeline_t *get_pipeline();

        // Asynchronous I/O operations
        int read(pipeline_t *pipeline, io_context_t *context);
        int write(pipeline_t *pipeline, io_context_t *context);

        // Synchronous I/O operations
        int read_sync(void *buffer, uint64_t lba, uint32_t lba_count);
        int write_sync(void *buffer, uint64_t lba, uint32_t lba_count);

        // Metadata management
        // Record a chunk write and return the assigned LBA
        uint64_t record_write(size_t buffer_index, uint32_t size);

        // Get metadata for a specific chunk
        std::optional<chunk_metadata_t> get_chunk_metadata(size_t buffer_index, size_t chunk_index);

        // Get the number of chunks written to a buffer index
        size_t get_chunk_count(size_t buffer_index);

        // Clear metadata for a buffer index (called on release)
        void clear_metadata(size_t buffer_index);

        // Get block size
        [[nodiscard]] uint32_t get_block_size() const { return pipe_.block_size; }

        // Calculate LBA count from byte size
        [[nodiscard]] uint32_t bytes_to_lba_count(uint32_t bytes) const {
            return (bytes + pipe_.block_size - 1) / pipe_.block_size;
        }

    private:
        pipeline() = default;

        ~pipeline() { cleanup(); }

        spdk_io_channel *get_io_channel();

        // Common initialization logic shared between initialize and initialize_nvmf
        int initialize_common(const std::string &bdev_base_name, spdk_nvme_transport_id trid,
                              spdk_nvme_ctrlr_opts drv_opts, size_t num_buffers);

        // Metadata tracking per buffer
        std::vector<std::unique_ptr<buffer_metadata_t>> metadata_;
    };
} // namespace fdl::spdk
