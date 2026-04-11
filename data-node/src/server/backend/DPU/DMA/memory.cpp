//
// Created by Felix Zhang on 2025-10-03.
//

#include <doca_argp.h>
#include <doca_buf.h>
#include <doca_dma.h>
#include <doca_log.h>

#include "memory.h"

#include "pipelines/DOCA/config.h"
#include "pipelines/DOCA/DMA/dma.h"
#include "pipelines/DOCA/EC/ec.h"
#include "utils/types.h"

DOCA_LOG_REGISTER(SERVER_DMA_MEMORY);

namespace fdl {

/**
 * Will write the data starting from `handle.region.data` (or `handle.region.first.data`)
 */
bool DMAMemoryHandle::write(const size_t index, const UnifiedHandle& handle, const ssize_t length) {
    // Actual index of the buffers to use
    const auto index_ = buffers_.at(index);

    /* Reserve a pipeline to use */
    size_t pipeline = dma::acquire();

    /* Setup DOCA buffers for DMA operation */
    auto& buffers = local_buffers.send[pipeline][index_];
    auto [size, count] = setup_chain(buffers, handle, length);
    if (size <= 0) {
        teardown_chain(buffers, count);
        dma::release(pipeline);
        return size == 0;
    }

    /* Copy data from local buffer(s) to remote buffer */
    doca_error_t result = dma::copy(
        core_doca_objects::instance().pipelines.dma[pipeline],
        resources_[pipeline].dma_context,
        buffers[0],
        remote_buffers.recv[pipeline][index_]
    );
    if (result != DOCA_SUCCESS) {
        teardown_chain(buffers, count);
        dma::release(pipeline);
        return false;
    }

    /* Update remote buffer metadata */
    {
        std::scoped_lock lock(remote_buffers.mutexes[index_]);
        remote_buffers.chunks[index_].push_back(remote_buffers.chunks[index_].back() + size);
    }

    /* Reset and release all acquired resources */
    result = teardown_chain(buffers, count);
    dma::release(pipeline);

    return result == DOCA_SUCCESS;
}

std::optional<UnifiedHandle> DMAMemoryHandle::read(const size_t index, UnifiedBuffer& partition, const size_t start) {
    // Actual index of the buffers to use
    const auto index_ = buffers_.at(index);

    /* Acquire section in unified buffer to produce data */
    size_t size;
    {
        std::unique_lock lock(remote_buffers.mutexes[index_]);
        if (start + 1 >= remote_buffers.chunks[index_].size()) return std::nullopt;
        size = remote_buffers.chunks[index_][start + 1] - remote_buffers.chunks[index_][start];
    }
    std::optional<UnifiedHandle> handle = std::nullopt;
    while (!handle.has_value()) { handle = partition.acquire_produce(size); }

    /* Reserve a pipeline to use */
    size_t pipeline = dma::acquire();

    /* Setup DOCA buffers for DMA operation */
    auto& buffers = local_buffers.recv[pipeline][index_];
    auto [size_, count] = setup_chain(buffers, handle.value(), -1);
    if (size_ <= 0) {
        teardown_chain(buffers, count);
        dma::release(pipeline);
        partition.release(std::move(handle.value()));
        return std::nullopt;
    }

    /* Copy data from remote buffer to local buffer(s) */
    const doca_error_t result = dma::copy(
        core_doca_objects::instance().pipelines.dma[pipeline],
        resources_[pipeline].dma_context,
        remote_buffers.send[pipeline][index_],
        buffers[0]
    );
    if (result != DOCA_SUCCESS) {
        teardown_chain(buffers, count);
        dma::release(pipeline);
        partition.release(std::move(handle.value()));
        return std::nullopt;
    }

    /* Reset and release acquired resources except buffer handle */
    dma::release(pipeline);
    if (teardown_chain(buffers, count) != DOCA_SUCCESS) {
        partition.release(std::move(handle.value()));
        return std::nullopt;
    }

    return handle;
}

std::pair<ssize_t, size_t> DMAMemoryHandle::setup_chain(
    const std::vector<doca_buf*>& bufs, const UnifiedHandle& handle, const ssize_t length
) {
    auto [regions, count] = std::visit(overloaded{
        [&](const UnifiedRegion& region) -> std::pair<const UnifiedRegion*, size_t> {
            return {&region, 1};
        },
        [&](const std::array<UnifiedRegion, 2>& pair) -> std::pair<const UnifiedRegion*, size_t> {
            return {pair.data(), 2};
        }
    }, handle.regions);

    size_t total = length == -1 ? 0 : length;
    if (length == -1) {
        for(size_t i = 0; i < count; ++i) {
            total += regions[i].size;
        }
    } else {
        ssize_t available = 0;
        for (size_t i = 0; i < count; ++i) available += regions[i].size;
        assert(length <= available);
    }

    size_t index = 0;
    size_t remaining = total;
    doca_buf* current = nullptr;

    for (size_t i = 0; i < count && remaining > 0; ++i) {
        char* data = regions[i].data;
        size_t available = std::min<size_t>(regions[i].size, remaining);
        while (available > 0) {
            if (index >= bufs.size()) {
                DOCA_LOG_ERR("[DMA] Out of pre-allocated DOCA buffers for chaining");
                return {-1, 0};
            }

            doca_buf* chunk = bufs[index];
            const size_t size  = std::min<size_t>(available, MAX_MEMCPY_BUF_SIZE);
            doca_buf_set_data(chunk, data, size);

            if (index > 0) {
                // Chain the newly created chunk to the previous one
                const doca_error_t response = doca_buf_chain_list(current, chunk);
                if (response != DOCA_SUCCESS) {
                    DOCA_LOG_ERR("[DMA] Failed to chain local buffers: %s", doca_error_get_descr(response));
                    return {-1, index};
                }
            }

            current = chunk;
            data += size;
            available -= size;
            remaining -= size;
            index++;
        }
    }

    return {total, index};
}

doca_error_t DMAMemoryHandle::teardown_chain(const std::vector<doca_buf*>& bufs, const size_t length) {
    doca_error_t response = DOCA_SUCCESS;
    for (size_t i = 0; i < length - 1; ++i) {
        const doca_error_t result = doca_buf_unchain_list(bufs[i], bufs[i + 1]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to unchain local buffers: %s", doca_error_get_descr(result));
            response = result;
        }
    }
    return response;
}

int DMAMemoryBackend::start(const bool resilient) {
    auto& state = core_doca_objects::instance();
    state.register_logger();

#ifndef DOCA_ARCH_DPU
    DOCA_LOG_ERR("DMA memory backend can run only on the DPU");
    return EXIT_FAILURE;
#endif

    DOCA_TRY(
        state.open_device(DOCA_CONF.pcie_address, [resilient](doca_devinfo* devinfo) -> doca_error_t {
            auto result = dma::capability_is_supported(devinfo);
            if (resilient && result == DOCA_SUCCESS) {
                result = ec::capability_is_supported(devinfo);
            }
            return result;
        }),
        "Failed to open DOCA device"
    );

    /* Initialize variables for DOCA buffer pre-allocation */
    const auto& local_addr = BufferPools::unified().data();
    const auto& local_len = BufferPools::unified().size_of();

    char* remote_addr = nullptr;
    size_t remote_len = 0;

    DOCA_TRY(
        state.initialize(2 * PARTITION_BUFFER_COUNT * MAX_CHAIN_LENGTH + 2 * HOST_BUFFER_COUNT, true, resilient),
        "Failed to create DOCA core objects"
    );

    /* Create a DOCA memory mapping for the local & remote buffers */
    state.create_local_mmap(state.local_mmap, local_addr, local_len);
    dma::create_remote_mmap(state, remote_addr, remote_len);

    /* Allocate resources for the pipelines */
    DOCA_TRY(dma::allocate_resources(state, resources_.dma), "Failed to allocate DMA resources");
    if (resilient) {
        DOCA_TRY(ec::pipelines::instance().allocate(), "Failed to allocate EC resources");
    }

    /* Allocate DOCA buffers from all the DMA memory maps */
    for (size_t i = 0; i < state.pipelines.dma.size(); ++i) {
        auto& pipeline = state.pipelines.dma[i];

        local_buffers.send[i] = std::make_unique<std::vector<doca_buf*>[]>(PARTITION_BUFFER_COUNT);
        local_buffers.recv[i] = std::make_unique<std::vector<doca_buf*>[]>(PARTITION_BUFFER_COUNT);

        for (size_t j = 0; j < PARTITION_BUFFER_COUNT; ++j) {
            DOCA_TRY(
                allocate_doca_buffers(
                    pipeline.buf_inv, state.local_mmap, local_addr, local_len, MAX_CHAIN_LENGTH, local_buffers.send[i][j]
                ), "Failed to acquire DOCA buffers representing local source buffers"
            );
            DOCA_TRY(
                allocate_doca_buffers(
                    pipeline.buf_inv, state.local_mmap, local_addr, local_len, MAX_CHAIN_LENGTH, local_buffers.recv[i][j]
                ), "Failed to acquire DOCA buffers representing local destination buffers"
            );
        }

        DOCA_TRY(
            allocate_doca_buffers(
                pipeline.buf_inv, state.remote_mmap, remote_addr, remote_len, HOST_BUFFER_COUNT, remote_buffers.send[i]
            ), "Failed to acquire DOCA buffers representing remote source buffers"
        );
        DOCA_TRY(
            allocate_doca_buffers(
                pipeline.buf_inv, state.remote_mmap, remote_addr, remote_len, HOST_BUFFER_COUNT, remote_buffers.recv[i]
            ), "Failed to acquire DOCA buffers representing remote destination buffers"
        );
    }

    /* Initializes metadata */
    for (auto& vec : remote_buffers.chunks) {
        vec.push_back(0);
    }

    /* Enqueue all remote buffers as unused */
    std::array<size_t, HOST_BUFFER_COUNT> free{};
    for (size_t i = 0; i < HOST_BUFFER_COUNT; i++) { free[i] = i; }
    available_.buffers.enqueue_bulk(free.data(), HOST_BUFFER_COUNT);

    return EXIT_SUCCESS;
}

DMAMemoryHandle DMAMemoryBackend::acquire(const size_t n) {
    std::vector<size_t> buffers(n);
    const size_t acquired = available_.buffers.wait_dequeue_bulk(buffers.data(), n);
    assert(acquired == n);

    return DMAMemoryHandle{resources_.dma, std::move(buffers)};
}

void DMAMemoryBackend::release(DMAMemoryHandle&& handle) {
    for (const auto& index : handle.buffers_) {
        for (size_t pipeline = 0; pipeline < DMA_PIPELINE_COUNT; pipeline++) {
            doca_buf_reset_data_len(remote_buffers.recv[pipeline][index]);
        }

        std::unique_lock lock(remote_buffers.mutexes[index]);
        remote_buffers.chunks[index] = {0};
    }

    available_.buffers.enqueue_bulk(handle.buffers_.data(), handle.buffers_.size());
    handle.buffers_.clear();
}

}
