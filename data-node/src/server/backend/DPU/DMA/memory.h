//
// Created by Felix Zhang on 2025-10-03.
//

#pragma once

#include <deque>

#include "pipelines/DOCA/doca.h"
#include "pipelines/DOCA/DMA/dma.h"
#include "pipelines/DOCA/EC/ec.h"
#include "server/backend/IBackend.h"
#include "utils/concurrent_queue.h"
#include "utils/debug.h"

namespace fdl {

// All DOCA buffers are allocated per-pipeline
static struct {
    std::array<std::unique_ptr<std::vector<doca_buf*>[]>, DMA_PIPELINE_COUNT> send;
    std::array<std::unique_ptr<std::vector<doca_buf*>[]>, DMA_PIPELINE_COUNT> recv;
} local_buffers;
static struct {
    std::array<std::vector<doca_buf*>, DMA_PIPELINE_COUNT> send;
    std::array<std::vector<doca_buf*>, DMA_PIPELINE_COUNT> recv;
    std::array<std::vector<size_t>, HOST_BUFFER_COUNT> chunks;    // Metadata is global as it should have an owner
    std::array<std::mutex, HOST_BUFFER_COUNT> mutexes;
} remote_buffers;

class DMAMemoryHandle : IHandle<DMAMemoryHandle> {
public:
    DMAMemoryHandle(std::vector<dma::dma_resources>& resources, std::vector<size_t>&& buffers) :
        resources_(resources), buffers_(std::move(buffers))
    {}
    ~DMAMemoryHandle() {
        if (!buffers_.empty()) {
            FDL_WARN("[DMAMemoryHandle] Buffers were not released before destruction");
        }
    }

    /* Disable copying & enable moving */
    DMAMemoryHandle(const DMAMemoryHandle&) = delete;
    DMAMemoryHandle& operator=(const DMAMemoryHandle&) = delete;
    DMAMemoryHandle(DMAMemoryHandle&& other) = default;
    DMAMemoryHandle& operator=(DMAMemoryHandle&& other) noexcept {
        if (this != &other) {
            resources_ = other.resources_;
            buffers_ = std::move(other.buffers_);
        }
        return *this;
    }

    /**
     * Will copy the entire buffer to remote
     *
     * @return: whether data was successfully written to the backend
     */
    bool write(size_t index, const UnifiedHandle& handle, ssize_t length = -1);

    std::optional<UnifiedHandle> read(size_t index, UnifiedBuffer& partition, size_t start);

    /* Define getters */
    size_t size() const { return buffers_.size(); }

private:
    friend class DMAMemoryBackend;

    std::pair<ssize_t, size_t> setup_chain(
        const std::vector<doca_buf*>& bufs, const UnifiedHandle& handle, ssize_t length
    );
    doca_error_t teardown_chain(const std::vector<doca_buf*>& bufs, size_t length);

    std::vector<dma::dma_resources>& resources_;
    std::vector<size_t> buffers_;
};

class DMAMemoryBackend final : public IBackend<DMAMemoryBackend, DMAMemoryHandle> {
public:
    /* Disable copying & moving */
    DMAMemoryBackend(const DMAMemoryBackend&) = delete;
    DMAMemoryBackend& operator=(const DMAMemoryBackend&) = delete;
    DMAMemoryBackend(DMAMemoryBackend&& other) = delete;
    DMAMemoryBackend& operator=(DMAMemoryBackend&& other) = delete;

    int start(bool resilient);
    int stop() { return 1; }    // TODO: properly teardown resources

    DMAMemoryHandle acquire(size_t n);
    void release(DMAMemoryHandle&& handle);

private:
    friend class IBackend;

    DMAMemoryBackend() = default;

    // Individual, per-context resources
    struct {
        std::vector<dma::dma_resources> dma{};
    } resources_{};

    struct {
        ConcurrentQueue<size_t> buffers{HOST_BUFFER_COUNT};
    } available_;

    char* data_ = nullptr;
};

}
