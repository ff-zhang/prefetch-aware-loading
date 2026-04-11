//
// Created by joz on 2/15/26.
//
#include "spdk.h"

#include <spdk/dma.h>
#include <spdk/log.h>
#include <spdk/module/bdev/nvme.h>
#include <spdk/nvme.h>
#include <spdk/thread.h>

namespace {
    void on_io_complete(spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
        if (auto *context = static_cast<fdl::spdk::io_context_t *>(cb_arg)) {
            // Handle synchronous completion
            context->result.store(success ? 0 : -EIO, std::memory_order_release);
            context->completed.store(true, std::memory_order_release);
            // Handle async callback
            if (context->callback) {
                context->callback(context->user_arg, success ? 0 : -EIO);
            }
        }
        spdk_bdev_free_io(bdev_io);
    }

    void bdev_event_cb(spdk_bdev_event_type type, spdk_bdev *bdev, void *event_ctx) {
        if (type == SPDK_BDEV_EVENT_REMOVE) {
            SPDK_WARNLOG("Bdev %s was removed!\n", spdk_bdev_get_name(bdev));
        }
    }

    void on_nvme_create_cb(void *ctx, size_t bdev_count, int rc) {
        auto *pipe_instance = static_cast<fdl::spdk::pipeline *>(ctx);

        if (rc != 0) {
            SPDK_ERRLOG("Async NVMe create failed with rc=%d\n", rc);
            // TODO(joz): Handle failure state here
            return;
        }

        std::string expected_bdev_name = pipe_instance->pipe_.bdev_name;

        spdk_bdev *bdev = spdk_bdev_get_by_name(expected_bdev_name.c_str());
        if (!bdev) {
            SPDK_ERRLOG("Failed to find SPDK block device: %s\n", expected_bdev_name.c_str());
            return;
        }

        spdk_bdev_desc *bdev_desc = nullptr;
        if (int open_rc = spdk_bdev_open_ext(expected_bdev_name.c_str(), true, bdev_event_cb,
                                             nullptr, &bdev_desc);
            open_rc != 0) {
            SPDK_ERRLOG("Failed to open SPDK block device: %s\n", expected_bdev_name.c_str());
            return;
        }
        pipe_instance->pipe_.bdev = bdev;
        pipe_instance->pipe_.bdev_desc = bdev_desc;
        pipe_instance->pipe_.block_size = spdk_bdev_get_block_size(bdev);

        pipe_instance->initialized_ = true;
        SPDK_NOTICELOG("Pipeline fully initialized in callback! block_size=%u\n",
                       pipe_instance->pipe_.block_size);
    }
} // namespace

namespace fdl::spdk {
    static thread_local spdk_io_channel *t_io_channel = nullptr;

    spdk_io_channel *pipeline::get_io_channel() {
        if (t_io_channel) {
            return t_io_channel;
        }
        if (!pipe_.bdev_desc)
            return nullptr;
        t_io_channel = spdk_bdev_get_io_channel(pipe_.bdev_desc);
        if (!t_io_channel) {
            SPDK_ERRLOG("Failed to get SPDK I/O channel\n");
            return nullptr;
        }
        return t_io_channel;
    }

    int pipeline::initialize_common(const std::string &bdev_base_name, spdk_nvme_transport_id trid,
                                    spdk_nvme_ctrlr_opts drv_opts, size_t num_buffers) {
        if (initialized_) {
            SPDK_ERRLOG("Pipeline already initialized\n");
            return -1;
        }

        metadata_.reserve(num_buffers);
        for (size_t i = 0; i < num_buffers; ++i) {
            metadata_.push_back(std::make_unique<buffer_metadata_t>());
        }

        pipe_.bdev_name = bdev_base_name + "n1";

        spdk_bdev_nvme_ctrlr_opts bdev_opts = {};
        spdk_bdev_nvme_get_default_ctrlr_opts(&bdev_opts);

        if (const int rc = spdk_bdev_nvme_create(&trid, bdev_base_name.c_str(), nullptr, 0,
                                                 on_nvme_create_cb, this, &drv_opts, &bdev_opts);
            rc != 0) {
            SPDK_ERRLOG("Failed to kick off NVMe creation for %s, rc=%d\n", bdev_base_name.c_str(),
                        rc);
            return -1;
        }

        while (!initialized_) {
            spdk_thread_poll(spdk_get_thread(), 0, 0);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        return 0;
    }

    int pipeline::initialize(const std::string &bdev_base_name, size_t num_buffers) {
        spdk_nvme_transport_id trid = {};
        spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
        snprintf(trid.traddr, sizeof(trid.traddr), "%s", NVME_PCIE_CONTROLLER_NAME.c_str());

        spdk_nvme_ctrlr_opts drv_opts = {};
        spdk_nvme_ctrlr_get_default_ctrlr_opts(&drv_opts, sizeof(drv_opts));

        return initialize_common(bdev_base_name, trid, drv_opts, num_buffers);
    }

    int pipeline::initialize_nvmf(const nvmf_config_t &config, size_t num_buffers) {
        std::string bdev_base_name = "nvmf_" + config.target_addr;

        spdk_nvme_transport_id trid = {};
        spdk_nvme_trid_populate_transport(&trid, (config.transport == nvmf_transport_type::TCP)
                                                         ? SPDK_NVME_TRANSPORT_TCP
                                                         : SPDK_NVME_TRANSPORT_RDMA);
        trid.adrfam = SPDK_NVMF_ADRFAM_IPV4;
        snprintf(trid.traddr, sizeof(trid.traddr), "%s", config.target_addr.c_str());
        snprintf(trid.trsvcid, sizeof(trid.trsvcid), "%s", config.target_port.c_str());
        snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", config.subnqn.c_str());

        spdk_nvme_ctrlr_opts drv_opts = {};
        spdk_nvme_ctrlr_get_default_ctrlr_opts(&drv_opts, sizeof(drv_opts));
        if (!config.hostnqn.empty()) {
            snprintf(drv_opts.hostnqn, sizeof(drv_opts.hostnqn), "%s", config.hostnqn.c_str());
        }
        drv_opts.keep_alive_timeout_ms = 10000;

        pipe_.transport = config.transport;
        pipe_.nvmf_bdev_name = bdev_base_name;

        return initialize_common(bdev_base_name, trid, drv_opts, num_buffers);
    }

    void pipeline::cleanup_thread() {
        if (t_io_channel) {
            spdk_put_io_channel(t_io_channel);
            t_io_channel = nullptr;
        }
    }

    void pipeline::cleanup() {
        if (!initialized_)
            return;

        cleanup_thread();

        if (pipe_.bdev_desc) {
            spdk_bdev_close(pipe_.bdev_desc);
            pipe_.bdev_desc = nullptr;
        }

        pipe_.bdev = nullptr;
        pipe_.block_size = 0;
        metadata_.clear();
        initialized_ = false;
        SPDK_NOTICELOG("Pipeline cleaned up\n");
    }

    pipeline_t *pipeline::get_pipeline() {
        if (!initialized_)
            return nullptr;
        return &pipe_;
    }

    void *pipeline::allocate_buffer(size_t size) { return spdk_dma_malloc(size, 4096, nullptr); }

    void pipeline::free_buffer(void *buffer) { spdk_dma_free(buffer); }

    int pipeline::read(pipeline_t *pipeline, io_context_t *context) {
        if (!initialized_ || !pipeline || !context) {
            SPDK_ERRLOG("Invalid arguments to read\n");
            return -1;
        }

        spdk_io_channel *io_channel = get_io_channel();
        if (!io_channel) {
            SPDK_ERRLOG("Failed to get I/O channel for read\n");
            return -1;
        }
        const uint64_t offset = context->lba * pipeline->block_size;
        const uint64_t nbytes = context->lba_count * pipeline->block_size;
        return spdk_bdev_read(pipeline->bdev_desc, io_channel, context->buffer, offset, nbytes,
                              on_io_complete, context);
    }

    int pipeline::write(pipeline_t *pipeline, io_context_t *context) {
        if (!initialized_ || !pipeline || !context) {
            SPDK_ERRLOG("Invalid arguments to write\n");
            return -1;
        }

        spdk_io_channel *io_channel = get_io_channel();
        if (!io_channel) {
            SPDK_ERRLOG("Failed to get I/O channel for write\n");
            return -1;
        }
        const uint64_t offset = context->lba * pipeline->block_size;
        const uint64_t nbytes = context->lba_count * pipeline->block_size;
        return spdk_bdev_write(pipeline->bdev_desc, io_channel, context->buffer, offset, nbytes,
                               on_io_complete, context);
    }

    int pipeline::read_sync(void *buffer, uint64_t lba, uint32_t lba_count) {
        io_context_t context{};
        context.buffer = buffer;
        context.lba = lba;
        context.lba_count = lba_count;
        context.completed.store(false, std::memory_order_relaxed);

        if (read(&pipe_, &context) != 0) {
            return -1;
        }

        spdk_thread *thread = spdk_get_thread();
        while (!context.completed.load(std::memory_order_acquire)) {
            spdk_thread_poll(thread, 0, 0);
            std::this_thread::yield();
        }

        return context.result.load(std::memory_order_acquire);
    }

    int pipeline::write_sync(void *buffer, uint64_t lba, uint32_t lba_count) {
        std::mutex cv_mutex;
        std::condition_variable cv;

        io_context_t context{};
        context.buffer = buffer;
        context.lba = lba;
        context.lba_count = lba_count;
        context.completed.store(false, std::memory_order_relaxed);

        if (write(&pipe_, &context) != 0) {
            return -1;
        }

        spdk_thread *thread = spdk_get_thread();
        while (!context.completed.load(std::memory_order_acquire)) {
            spdk_thread_poll(thread, 0, 0);
            std::this_thread::yield();
        }

        return context.result.load(std::memory_order_acquire);
    }

    uint64_t pipeline::record_write(size_t buffer_index, uint32_t size) {
        if (buffer_index >= metadata_.size()) {
            SPDK_ERRLOG("Buffer index %zu out of range\n", buffer_index);
            return UINT64_MAX;
        }

        auto &meta = metadata_[buffer_index];
        uint32_t lba_count = bytes_to_lba_count(size);

        std::lock_guard lock(meta->mutex);
        uint64_t lba = meta->next_lba.load(std::memory_order_relaxed);
        meta->chunks.push_back({lba, lba_count, size});
        meta->next_lba.store(lba + lba_count, std::memory_order_relaxed);

        return lba;
    }

    std::optional<chunk_metadata_t> pipeline::get_chunk_metadata(size_t buffer_index,
                                                                 size_t chunk_index) {
        if (buffer_index >= metadata_.size()) {
            return std::nullopt;
        }

        auto &meta = metadata_[buffer_index];
        std::lock_guard lock(meta->mutex);

        if (chunk_index >= meta->chunks.size()) {
            return std::nullopt;
        }

        return meta->chunks[chunk_index];
    }

    size_t pipeline::get_chunk_count(size_t buffer_index) {
        if (buffer_index >= metadata_.size()) {
            return 0;
        }

        auto &meta = metadata_[buffer_index];
        std::lock_guard lock(meta->mutex);
        return meta->chunks.size();
    }

    void pipeline::clear_metadata(size_t buffer_index) {
        if (buffer_index >= metadata_.size()) {
            return;
        }

        auto &meta = metadata_[buffer_index];
        std::lock_guard lock(meta->mutex);
        meta->chunks.clear();
        meta->next_lba.store(0, std::memory_order_relaxed);
    }
} // namespace fdl::spdk
