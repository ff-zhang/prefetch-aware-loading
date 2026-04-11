//
// Created by Felix Zhang on 12/18/25.
//

#include "job.h"

#include <cassert>
#include <list>

namespace fdl {

Job::Job(const partition_id_t num_partitions, const bool remotes) :
    buffers_(BufferPools::unified().acquire(num_partitions)),
    backend_(fdl::backend().acquire(num_partitions)),
    sinks_(num_partitions)
{
    assert(buffers_.size() == backend_.size());

    if (remotes) {
        for (auto& remote : views_) {
            remote = BufferPools::remote().acquire(num_partitions);
        }
    }
}

Job::~Job() {
    for (auto& sink : sinks_) {
        if (sink.view != nullptr) {
            BufferPools::remote().release(std::vector{reinterpret_cast<rdma::ServerUnifiedBuffer*>(sink.view)});
            sink.view = nullptr;
        }
    }
}

bool Job::add_server(const size_t index, rdma::ibv_qp_info&& remote, const std::vector<std::pair<uint64_t, uint64_t>>& regions) {
    if (index >= views_.size() || regions.size() != buffers_.size()) {
        FDL_WARN("Unexpected remote (%lu) registration", index);
        return false;
    }

    for (size_t i = 0; i < regions.size(); ++i) {
        views_[index][i]->setup(&queue_pairs_[index], regions[i].first, regions[i].second, std::make_pair(&wrapper, this));
    }

    if (queue_pairs_[index].bringup(remote) != 0) {
        FDL_INFO("[Job] Failed to bring up QPs");
    }

    view_count_.fetch_add(1, std::memory_order_acq_rel);

    return true;
}

std::pair<size_t, rdma::QueuePair&> Job::add_client() {
    std::scoped_lock lock(clients_mutex_);
    auto& qp = *clients_.emplace_back(std::make_unique<rdma::QueuePair>()).get();
    contexts_.emplace_back();
    return {clients_.size() - 1, qp};
}

void Job::add_sink(const partition_id_t partition, const uint64_t client_id, const std::array<uint64_t, 2>& regions) {
    if (partition >= sinks_.size()) {
        FDL_WARN("[Job] add_sink: partition %u out of range", partition);
        return;
    }

    std::scoped_lock lock(clients_mutex_);

    if (client_id >= clients_.size()) {
        FDL_WARN("[Job] add_sink: client ID %lu out of range", client_id);
        return;
    }
    auto& qp = client(client_id);

    auto* view = reinterpret_cast<rdma::ClientUnifiedBuffer*>(BufferPools::remote().acquire(1)[0]);
    view->reset();
    view->setup(&qp, regions[0], regions[1], std::make_pair(&wrapper, this));

    sinks_[partition] = {
        .qp = &qp,
        .client_id = client_id,
        .view = view,
        .pending = std::nullopt
    };
}

void Job::close_client(const uint64_t client_id) {
    std::scoped_lock lock(clients_mutex_);

    if (client_id >= clients_.size() || !clients_[client_id]) {
        FDL_WARN("[Job] close_client: invalid or already-closed client %lu", client_id);
        return;
    }

    // Release any in-flight operations to sinks on this client
    for (size_t partition = 0; partition < sinks_.size(); ++partition) {
        auto& sink = sinks_[partition];
        if (sink.client_id != client_id) continue;

        // Return any handle that was waiting to be retried on the next flush
        if (sink.pending.has_value()) {
            buffers_[partition]->release(std::move(sink.pending.value()));
            sink.pending = std::nullopt;
        }

        // Return the remote view buffer to the pool
        if (sink.view != nullptr) {
            BufferPools::remote().release(std::vector{reinterpret_cast<rdma::ServerUnifiedBuffer*>(sink.view)});
            sink.view = nullptr;
        }

        sink.qp = nullptr;
        sink.client_id = std::numeric_limits<size_t>::max();
    }

    // Tear down the QP
    clients_[client_id]->teardown();

    FDL_LOG("[Job] client %lu disconnected and resources released", client_id);
}

bool Job::send(const partition_id_t partition) {
    std::scoped_lock lock(clients_mutex_);

    auto& sink = sinks_[partition];
    if (sink.qp == nullptr) {
        FDL_WARN("[Job] flush: sink is missing QP");
        return false;
    }

    // Acquire a handle if we don't already have one waiting to be sent
    if (!sink.pending.has_value()) {
        auto handle = buffers_[partition]->acquire_flush();
        if (!handle.has_value()) {
            return false;
        }
        sink.pending = std::move(handle);
    }

    // Build segment descriptors while the handle is still pending
    std::array<std::pair<char*, uint32_t>, 2> b_segments{};
    std::visit(overloaded{
        [&](const UnifiedRegion& region) {
            b_segments[0] = {region.data, region.size};
        },
        [&](const std::array<UnifiedRegion, 2>& pair) {
            b_segments[0] = {pair[0].data, pair[0].size};
            b_segments[1] = {pair[1].data, pair[1].size};
        }
    }, sink.pending.value().regions);

    std::array<std::pair<uint64_t*, uint32_t>, 2> m_segments{};
    std::visit(overloaded{
        [&](const std::span<uint64_t>& region) {
            m_segments[0] = {region.data(), static_cast<uint32_t>(region.size())};
        },
        [&](const std::array<std::span<uint64_t>, 2>& pair) {
            m_segments[0] = {pair[0].data(), static_cast<uint32_t>(pair[0].size())};
            m_segments[1] = {pair[1].data(), static_cast<uint32_t>(pair[1].size())};
        }
    }, sink.pending.value().metadata);

    const uint32_t m_size = sink.pending.value().m_size;

    // Transfer ownership of the handle into the context and, if submission fails, recover it
    SinkContext context = std::make_optional(
        std::make_pair(partition, std::move(sink.pending.value()))
    );
    sink.pending = std::nullopt;

    const auto ticket = sink.view->produce_async(
        b_segments, m_segments, m_size, rdma::UNIFIED, contexts_[sink.client_id], context
    );
    if (!ticket.has_value()) {
        // Remote ring buffer is full so reclaim the handle for the next attempt
        sink.pending = std::move(context.value().second);
        return false;
    }

    return true;
}

void Job::poll(const bool unsafe) {
    std::unique_lock lock(clients_mutex_, std::defer_lock);
    if (!unsafe) { lock.lock(); }

    if (contexts_.empty()) {
        if (!unsafe) { lock.unlock(); }
        return;
    }

    for (const auto& sink : sinks_) {
        if (sink.qp == nullptr) { continue; }

        std::array<size_t, rdma::POLL_BATCH_SIZE> tickets{};
        std::array<void*, rdma::POLL_BATCH_SIZE> contexts{};
        const size_t count = sink.qp->poll(tickets, contexts);

        for (size_t j = 0; j < count; ++j) {
            auto* context = static_cast<SinkContext*>(contexts[j]);
            if (!context || !context->has_value()) { continue; }
            auto& [partition, handle] = context->value();
            buffers_[partition]->release(std::move(handle));
            *context = std::nullopt;
        }

        sink.qp->reclaim(tickets, count);
    }

    if (!unsafe) { lock.unlock(); }
}

UnifiedHandle Job::acquire_produce(const size_t partition, const size_t size, const size_t block) const {
    assert(size < UnifiedBuffer::capacity());
    auto handle = buffers_[partition]->acquire_produce(size, block);
    while (!handle.has_value()) {
        handle = buffers_[partition]->acquire_produce(size, block);
    }
    return std::move(handle.value());
}

std::pair<std::optional<UnifiedHandle>, size_t> Job::acquire_consume(const size_t partition, const size_t start) {
    std::optional<UnifiedHandle> handle = buffers_[partition]->acquire_consume();
    if (handle.has_value()) {
        return {std::move(handle), 0};
    }

    handle = backend_.read(partition, *buffers_[partition], start);
    if (handle.has_value()) {
        return {std::move(handle), 1};
    }

    return {std::nullopt, 0};
}

void Job::release(const size_t partition, UnifiedHandle&& handle) const {
    buffers_[partition]->release(std::move(handle));
}

void Job::flush(const size_t partition) {
    auto handle = buffers_[partition]->acquire_consume();
    if (handle.has_value()) {
        backend_.write(partition, handle.value());
        buffers_[partition]->release(std::move(handle.value()));
    }
}

// TODO: proper error handling
InflightOperation Job::encode(const size_t pipeline, const UnifiedHandle& handle) {
    auto& pipelines = ec::pipelines::instance();
    auto [data_bufs, parity_bufs] = pipelines.buffers[pipeline];

    // Setup operation details
    InflightOperation operation{InflightOperation::EC, {}, {}, 0, pipeline, false};
    auto& [_1, data_ptrs, parity_ptrs, block_size, _2, _3] = operation;

    std::visit(overloaded{
        [&](const UnifiedRegion& region) -> void {
            /* Calculate the size of each block, round up, and align result to next multiple of 64 */
            block_size = (region.size + EC_DATA_BLOCK_COUNT - 1) / EC_DATA_BLOCK_COUNT + 63 & ~63;

            // Setup data buffer chain for encoding input data
            for (size_t i = 0; i < EC_DATA_BLOCK_COUNT; ++i) {
                doca_buf_set_data(data_bufs[i], region.data + i * block_size, block_size);
                data_ptrs[i] = region.data + i * block_size;
            }
        },
        [&](const std::array<UnifiedRegion, 2>& pair) -> void {
            /* Calculate the size of each block, round up, and align result to next multiple of 64 */
            block_size = ((pair[0].size + pair[1].size + EC_DATA_BLOCK_COUNT - 1) / EC_DATA_BLOCK_COUNT + 63) & ~63;

            // Setup data buffer chain for encoding input data
            size_t i = 0;
            for (; i < pair[0].size / block_size; ++i) {
                doca_buf_set_data(data_bufs[i], pair[0].data + i * block_size, block_size);
                data_ptrs[i] = pair[0].data + i * block_size;
            }
            for (; i < EC_DATA_BLOCK_COUNT; ++i) {
                doca_buf_set_data(data_bufs[i], pair[1].data + i * block_size, block_size);
                data_ptrs[i] = pair[1].data + i * block_size;
            }
        }
    }, handle.regions);

    // Setup parity buffer chain
    doca_buf_get_head(parity_bufs[0], reinterpret_cast<void**>(&parity_ptrs[0]));
    doca_buf_set_data(parity_bufs[0], parity_ptrs[0], 0);
    for (size_t i = 1; i < EC_PARITY_BLOCK_COUNT; ++i) {
        doca_buf_set_data(parity_bufs[i], parity_ptrs[0] + i * block_size, 0);
        parity_ptrs[i] = parity_ptrs[0] + i * block_size;
    }

    // Encode data blocks into the parity buffer
    pipelines.encode(pipeline, data_bufs[0], parity_bufs[0]);

    return operation;
}

void Job::scatter(const size_t partition, const InflightOperation& operation, context_t& completion) {
    auto& [_1, data, parity, block_size, _2, ready] = operation;
    assert(ready);

    // Scatter the rest of the blocks to the secondary servers
    completion.set(EC_DATA_BLOCK_COUNT + EC_PARITY_BLOCK_COUNT - 1);
    size_t i = 0;
    for (auto it = std::next(data.begin()); it != data.end(); ++it) {
        while (!view(i, partition).produce_async(
            std::array<std::pair<char*, uint32_t>, 2>{{{*it, block_size}, {nullptr, 0}}}, rdma::mr_t::UNIFIED, &completion
        ).has_value()) {}
        ++i;
    }
    for (const auto& it : parity) {
        while (!view(i, partition).produce_async(
            std::array<std::pair<char*, uint32_t>, 2>{{{it, block_size}, {nullptr, 0}}}, rdma::mr_t::SCRATCH, &completion
        ).has_value()) {}
        ++i;
    }
}

std::optional<UnifiedHandle> Job::decode(const size_t partition) {
    return std::nullopt;
}

} // namespace fdl