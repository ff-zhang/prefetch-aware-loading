//
// Created by joz on 2/16/26.
//

#include "spdk.h"

#include <atomic>
#include <cstring>
#include <functional>
#include <future>
#include <rte_errno.h>
#include <thread>
#include <utility>

#include <spdk/bdev.h>
#include <spdk/env.h>
#include <spdk/init.h>
#include <spdk/log.h>
#include <spdk/thread.h>

#include "pipelines/SPDK/spdk.h"
#include "utils/constants.h"
#include "utils/configuration.h"
#include "utils/types.h"

namespace {
    using UnifiedHandle = fdl::UnifiedHandle;
    using UnifiedRegion = fdl::UnifiedRegion;

    // Helper to compute total size from regions
    size_t get_regions_size(const UnifiedHandle::regions_t &regions) {
        return std::visit(
                fdl::overloaded{[](const UnifiedRegion &region) -> size_t { return region.size; },
                                [](const std::array<UnifiedRegion, 2> &pair) -> size_t {
                                    return pair[0].size + pair[1].size;
                                }},
                regions);
    }

    // Helper to copy regions to a contiguous DMA buffer
    void copy_regions_to_buffer(void *dst, const UnifiedHandle::regions_t &regions) {
        std::visit(fdl::overloaded{[dst](const UnifiedRegion &region) {
                                       std::memcpy(dst, region.data, region.size);
                                   },
                                   [dst](const std::array<UnifiedRegion, 2> &pair) {
                                       std::memcpy(dst, pair[0].data, pair[0].size);
                                       std::memcpy(static_cast<char *>(dst) + pair[0].size,
                                                   pair[1].data, pair[1].size);
                                   }},
                   regions);
    }

    // Helper to copy from DMA buffer to regions
    void copy_buffer_to_regions(const void *src, [[maybe_unused]] size_t size,
                                const UnifiedHandle::regions_t &regions) {
        std::visit(fdl::overloaded{[src](const UnifiedRegion &region) {
                                       std::memcpy(region.data, src, region.size);
                                   },
                                   [src](const std::array<UnifiedRegion, 2> &pair) {
                                       std::memcpy(pair[0].data, src, pair[0].size);
                                       std::memcpy(pair[1].data,
                                                   static_cast<const char *>(src) + pair[0].size,
                                                   pair[1].size);
                                   }},
                   regions);
    }
} // namespace

namespace fdl {
    // SPDK thread pointer
    static thread_local spdk_thread *t_spdk_thread = nullptr;

    // Controls whether to keep the reactor thread running
    static std::atomic g_reactor_running{false};

    // Signaling for reactor thread readiness and subsystem init/fini
    static std::promise<int> *g_subsys_init_promise = nullptr;
    static std::promise<int> *g_subsys_fini_promise = nullptr;
    static std::atomic<bool> g_reactor_ready{false};
    static std::atomic<bool> g_subsys_fini_done{false};

    // Tracks whether the SPDK environment (DPDK EAL) has been initialized.
    // DPDK EAL persists for the lifetime of the process — spdk_env_fini only
    // tears down SPDK's layer on top, not DPDK itself. On re-init we must
    // pass NULL to spdk_env_init() so it takes the PCI-only reinit path.
    static bool g_env_initialized = false;

    // Main reactor thread
    static std::thread g_reactor_thread;

    // Register the current thread with SPDK
    // Must be called from each thread that will perform SPDK I/O
    static spdk_thread *register_spdk_thread(const char *name) {
        if (t_spdk_thread) {
            return t_spdk_thread; // Already registered
        }

        t_spdk_thread = spdk_thread_create(name, nullptr);
        if (!t_spdk_thread) {
            SPDK_ERRLOG("Failed to create SPDK thread: %s\n", name);
            return nullptr;
        }
        spdk_set_thread(t_spdk_thread);
        return t_spdk_thread;
    }

    // Unregister the current thread from SPDK
    static void unregister_spdk_thread() {
        if (t_spdk_thread) {
            // Clean up any I/O channels first
            spdk::pipeline::cleanup_thread();

            spdk_thread_exit(t_spdk_thread);
            // Poll until thread is exited
            while (!spdk_thread_is_exited(t_spdk_thread)) {
                spdk_thread_poll(t_spdk_thread, 0, 0);
            }
            spdk_thread_destroy(t_spdk_thread);
            t_spdk_thread = nullptr;
        }
    }

    // Callback for spdk_subsystem_init completion
    static void on_subsystem_init_complete(int rc, void *arg) {
        auto *promise = static_cast<std::promise<int> *>(arg);
        promise->set_value(rc);
    }

    // Callback for spdk_subsystem_fini completion
    static void on_subsystem_fini_complete(void *arg) {
        auto *promise = static_cast<std::promise<int> *>(arg);
        promise->set_value(0);
        g_subsys_fini_done.store(true, std::memory_order_release);
    }

    // Reactor entrypoint
    static void reactor_loop() {
        // Register this thread with SPDK
        spdk_thread *reactor_thread = register_spdk_thread("reactor");
        if (!reactor_thread) {
            SPDK_ERRLOG("Failed to create reactor thread\n");
            if (g_subsys_init_promise) {
                g_subsys_init_promise->set_value(-1);
            }
            return;
        }

        // Initialize SPDK subsystems from the app thread (this thread)
        spdk_subsystem_init(on_subsystem_init_complete, g_subsys_init_promise);

        // Poll until subsystem init completes (promise is set)
        // We need to poll to process the async init
        while (!g_reactor_ready.load(std::memory_order_acquire)) {
            spdk_thread_poll(reactor_thread, 0, 0);
            std::this_thread::yield();
        }

        // Now enter the main polling loop
        while (g_reactor_running.load(std::memory_order_relaxed)) {
            // Poll all SPDK threads for completions
            spdk_thread_poll(reactor_thread, 0, 0);

            std::this_thread::yield();
        }

        // Cleanup the pipeline on the reactor thread (where bdev resources were opened)
        spdk::pipeline::instance().cleanup();

        // Run subsystem fini on the reactor thread (where pollers were registered)
        if (g_subsys_fini_promise) {
            g_subsys_fini_done.store(false, std::memory_order_release);
            spdk_subsystem_fini(on_subsystem_fini_complete, g_subsys_fini_promise);

            // Poll until fini completes
            while (!g_subsys_fini_done.load(std::memory_order_acquire)) {
                spdk_thread_poll(reactor_thread, 0, 0);
                std::this_thread::yield();
            }
        }

        unregister_spdk_thread();
    }

    int SPDKBackend::start(bool resilient) {
        // Initialize SPDK environment (once per process lifetime).
        // We never call spdk_env_fini(), so the env stays alive across
        // stop/start cycles — DPDK EAL, vtophys, and mem_map all persist.
        if (!g_env_initialized) {
            spdk_env_opts opts{};
            spdk_env_opts_init(&opts);
            opts.opts_size = sizeof(opts);
            opts.name = "fdl_spdk_backend";
            opts.shm_id = -1;

            if (spdk_env_init(&opts) < 0) {
                SPDK_ERRLOG("Failed to initialize SPDK environment\n");
                return -1;
            }
            g_env_initialized = true;
        }

        // Initialize SPDK thread library
        if (spdk_thread_lib_init(nullptr, 0) != 0) {
            SPDK_ERRLOG("Failed to initialize SPDK thread library\n");
            fprintf(stderr, "DPDK Error Code: %d (%s)\n", rte_errno, rte_strerror(rte_errno));
            return -1;
        }

        // Set up promise for subsystem init result (reactor thread will use this)
        std::promise<int> subsys_promise;
        auto subsys_future = subsys_promise.get_future();
        g_subsys_init_promise = &subsys_promise;
        g_reactor_ready.store(false, std::memory_order_release);

        // Start the reactor thread for polling completions
        g_reactor_running.store(true, std::memory_order_release);
        g_reactor_thread = std::thread(reactor_loop);

        // Wait for subsystem init to complete (called from reactor thread)
        if (subsys_future.get() != 0) {
            SPDK_ERRLOG("Failed to initialize SPDK subsystems\n");
            g_reactor_ready.store(true, std::memory_order_release); // Let reactor exit
            stop();
            return -1;
        }

        // Signal reactor that subsystem init is done, it can proceed to main loop
        g_reactor_ready.store(true, std::memory_order_release);
        g_subsys_init_promise = nullptr;

        // Make the reactor thread initialize the pipeline
        std::promise<int> init_promise;
        auto init_future = init_promise.get_future();
        // Initialize based on config
        int send_rc = spdk_thread_send_msg(
                spdk_thread_get_app_thread(),
                [](void *arg) {
                    auto *p = static_cast<std::promise<int> *>(arg);
                    int res;
                    if constexpr (USE_NVME_OF) {
                        spdk::nvmf_config_t config{};
                        config.transport = NVME_OF_USE_RDMA ? spdk::nvmf_transport_type::RDMA
                                                            : spdk::nvmf_transport_type::TCP;
                        config.target_addr = NVME_OF_TARGET_ADDR;
                        config.target_port = NVME_OF_TARGET_PORT;
                        config.subnqn = NVME_OF_SUBNQN;
                        config.hostnqn = NVME_OF_HOSTNQN;

                        SPDK_NOTICELOG("Initializing NVMe-oF connection to %s:%s (%s)\n",
                                       NVME_OF_TARGET_ADDR.c_str(), NVME_OF_TARGET_PORT.c_str(),
                                       NVME_OF_USE_RDMA ? "RDMA" : "TCP");
                        res = spdk::pipeline::instance().initialize_nvmf(config);
                    } else {
                        SPDK_NOTICELOG("Initializing local NVMe device: %s\n",
                                       NVME_DEVICE_BASE_NAME.c_str());
                        res = spdk::pipeline::instance().initialize(NVME_DEVICE_BASE_NAME);
                    }
                    p->set_value(res);
                },
                &init_promise);

        if (send_rc != 0) {
            SPDK_ERRLOG("Failed to send pipeline init message to reactor thread\n");
            stop();
            return -1;
        }

        if (init_future.get() != 0) {
            SPDK_ERRLOG("Failed to initialize SPDK pipeline\n");
            stop();
            return -1;
        }

        // Allocate buffers to the buffer pool.
        std::array<BufferEntry, REMOTE_BUFFER_COUNT> entries{};
        for (size_t i = 0; i < REMOTE_BUFFER_COUNT; i++) {
            void *buffer = spdk::pipeline::allocate_buffer(PARTITION_BUFFER_SIZE);
            if (!buffer) {
                SPDK_ERRLOG("Failed to allocate SPDK buffer for partition %zu\n", i);
                stop();
                return -1;
            }
            entries[i] = {buffer, i};
        }
        available_.buffers.enqueue_bulk(entries.data(), REMOTE_BUFFER_COUNT);

        SPDK_NOTICELOG("SPDK backend started%s\n", resilient ? " (resilient mode)" : "");
        return 0;
    }

    int SPDKBackend::stop() {
        // Set up promise for subsystem fini (reactor thread will use this before exiting)
        std::promise<int> fini_promise;
        auto fini_future = fini_promise.get_future();
        g_subsys_fini_promise = &fini_promise;

        // Signal the reactor thread to stop
        // The reactor loop will: cleanup pipeline -> subsystem fini -> unregister thread
        g_reactor_running.store(false, std::memory_order_release);
        if (g_reactor_thread.joinable()) {
            g_reactor_thread.join();
        }
        g_subsys_fini_promise = nullptr;

        // Cleanup SPDK thread library
        spdk_thread_lib_fini();

        // TODO(joz): Ensure all buffers are freed
        std::array<BufferEntry, REMOTE_BUFFER_COUNT> entries{};
        available_.buffers.wait_dequeue_bulk(entries.data(), REMOTE_BUFFER_COUNT);
        for (size_t i = 0; i < REMOTE_BUFFER_COUNT; i++) {
            spdk::pipeline::free_buffer(entries[i].buffer);
        }

        SPDK_NOTICELOG("SPDK backend stopped\n");
        return 0;
    }

    SPDKHandle SPDKBackend::acquire(const size_t n) {
        // Register this thread with SPDK if not already done
        register_thread();
        std::vector<BufferEntry> entries(n);
        const size_t acquired = available_.buffers.wait_dequeue_bulk(entries.data(), n);
        assert(acquired == n);

        std::vector<void *> buffers(n);
        std::vector<size_t> indices(n);
        for (size_t i = 0; i < n; ++i) {
            buffers[i] = entries[i].buffer;
            indices[i] = entries[i].index;
        }

        return SPDKHandle{std::move(buffers), std::move(indices)};
    }

    void SPDKBackend::release(SPDKHandle &&handle) {
        std::vector<BufferEntry> entries(handle.buffers_.size());
        for (size_t i = 0; i < handle.buffers_.size(); ++i) {
            entries[i] = {handle.buffers_[i], handle.buffer_indices_[i]};
            // Clear the metadata for this buffer index
            spdk::pipeline::instance().clear_metadata(handle.buffer_indices_[i]);
        }
        available_.buffers.enqueue_bulk(entries.data(), entries.size());
        handle.buffers_.clear();
        handle.buffer_indices_.clear();
    }

    bool SPDKBackend::register_thread(const char *name) {
        if (t_spdk_thread) {
            return true; // Already registered
        }

        char thread_name[32];
        if (!name) {
            snprintf(thread_name, sizeof(thread_name), "worker_%lu",
                     std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
            name = thread_name;
        }

        return register_spdk_thread(name) != nullptr;
    }

    void SPDKBackend::unregister_thread() { unregister_spdk_thread(); }

    // SPDKHandle I/O implementations
    bool SPDKHandle::write(const size_t index, const UnifiedHandle &handle) {
        assert(spdk_get_thread() != nullptr);
        if (index >= buffers_.size()) {
            SPDK_ERRLOG("Write index %zu out of range (size=%zu)\n", index, buffers_.size());
            return false;
        }

        const size_t size = get_regions_size(handle.regions);
        if (size == 0) {
            return true;
        }

        void *dma_buffer = buffers_[index];
        size_t buffer_index = buffer_indices_[index];

        // Copy data from the ring buffer regions to our DMA buffer
        copy_regions_to_buffer(dma_buffer, handle.regions);

        // Record the write in metadata and get assigned LBA
        auto &pipeline = spdk::pipeline::instance();
        uint64_t lba = pipeline.record_write(buffer_index, size);
        if (lba == UINT64_MAX) {
            SPDK_ERRLOG("Failed to record write metadata\n");
            return false;
        }

        // Calculate LBA count
        uint32_t lba_count = pipeline.bytes_to_lba_count(size);

        // Perform synchronous write to NVMe
        if (pipeline.write_sync(dma_buffer, lba, lba_count) != 0) {
            SPDK_ERRLOG("Failed to write to NVMe at LBA %lu\n", lba);
            return false;
        }

        return true;
    }

    std::optional<UnifiedHandle> SPDKHandle::read(const size_t index, UnifiedBuffer &partition,
                                                  const size_t start) {
        assert(spdk_get_thread() != nullptr);
        if (index >= buffers_.size()) {
            SPDK_ERRLOG("Read index %zu out of range (size=%zu)\n", index, buffers_.size());
            return std::nullopt;
        }

        size_t buffer_index = buffer_indices_[index];
        auto &pipeline = spdk::pipeline::instance();

        // Look up the chunk metadata for this read
        auto chunk_meta = pipeline.get_chunk_metadata(buffer_index, start);
        if (!chunk_meta.has_value()) {
            // No more chunks to read
            return std::nullopt;
        }

        uint32_t size = chunk_meta->size;

        // Acquire space in the partition buffer for the data
        std::optional<UnifiedHandle> handle = std::nullopt;
        while (!handle.has_value()) { handle = partition.acquire_produce(size); }

        void *dma_buffer = buffers_[index];

        // Read data from NVMe into our DMA buffer
        if (pipeline.read_sync(dma_buffer, chunk_meta->lba, chunk_meta->lba_count) != 0) {
            SPDK_ERRLOG("Failed to read from NVMe at LBA %lu\n", chunk_meta->lba);
            partition.release(std::move(handle.value()));
            return std::nullopt;
        }

        // Copy data from DMA buffer to the partition ring buffer regions
        copy_buffer_to_regions(dma_buffer, size, handle.value().regions);

        return handle;
    }

} // namespace fdl
