//
// Created by Felix Zhang on 3/19/26.
//

#include "agent.h"

#include <cstdlib>
#include <cstring>

#include "server/frontend/job.h"
#include "utils/debug.h"
#include "utils/functions.h"
#include "utils/network.h"
#include "utils/socket.h"

namespace fdl {

bool ShuffleJob::initialize(const job_id_t job_id, const partition_id_t num_partitions) {
    job_id_ = job_id;

    // Contact the resource broker
    sockaddr_in addr;
    pton(BROKER_ADDRESS.first, BROKER_ADDRESS.second, addr);
    int sockfd = connect(addr, false);
    if (sockfd < 0) {
        FDL_WARN("[Shuffle] Failed to connect to broker");
        return false;
    }

    // Get job metadata from the resource broker
    header::BrokerResponse response;
    {
        const header::BrokerRequest request{
            .job_id = job_id,
            .num_partitions = num_partitions,
        };
        send_all(sockfd, &request, sizeof(request));
        recv_all(sockfd, &response, sizeof(response));
    }
    servers_.resize(response.num_servers);
    recv_all(sockfd, servers_.data(), response.num_servers * sizeof(sockaddr_in));
    ::close(sockfd);

    // Allocation layout (all 64-byte aligned, required for RDMA atomics)
    const size_t staging_size = static_cast<size_t>(num_partitions) * UnifiedBuffer::size_of();
    const size_t dest_size = static_cast<size_t>(num_partitions) * UnifiedBuffer::size_of();
    const size_t views_size = static_cast<size_t>(num_partitions) * rdma::ServerUnifiedBuffer::size_of();
    const size_t total = staging_size + dest_size + views_size;

    void* raw = nullptr;
    if (posix_memalign(&raw, 64, total) != 0) {
        FDL_WARN("[Shuffle] posix_memalign failed");
        return false;
    }
    data_ = std::unique_ptr<char[]>(static_cast<char*>(raw));

    // Initialize the local staging and destination buffers
    staging_.reserve(num_partitions);
    destination_.reserve(num_partitions);
    for (partition_id_t i = 0; i < num_partitions; ++i) {
        auto buffer = std::make_unique<UnifiedBuffer>();
        buffer->initialize(data_.get() + static_cast<size_t>(i) * UnifiedBuffer::size_of());
        staging_.push_back(std::move(buffer));

        // Destination buffers sit in the second half of the UnifiedBuffer region
        buffer = std::make_unique<UnifiedBuffer>();
        buffer->initialize(data_.get() + staging_size + static_cast<size_t>(i) * UnifiedBuffer::size_of());
        destination_.push_back(std::move(buffer));
    }

    // Initialize produce-side tracking
    pending_.resize(num_partitions);
    sync_ = std::make_unique<std::atomic<uint64_t>[]>(num_partitions);
    for (partition_id_t i = 0; i < num_partitions; ++i) {
        sync_[i].store(0, std::memory_order_relaxed);
    }

    // Initialize per-partition sink request tracking
    client_ids_.resize(response.num_servers);
    requested_ = std::make_unique<std::atomic<uint64_t>[]>(num_partitions);
    for (partition_id_t i = 0; i < num_partitions; ++i) {
        requested_[i].store(0, std::memory_order_relaxed);
    }

    // Initialize the remote ring buffer views
    views_.reserve(num_partitions);
    for (partition_id_t i = 0; i < num_partitions; ++i) {
        auto buffer = std::make_unique<rdma::ServerUnifiedBuffer>();
        buffer->initialize(data_.get() + staging_size + dest_size + static_cast<size_t>(i) * rdma::ServerUnifiedBuffer::size_of());
        views_.push_back(std::move(buffer));
    }

    // Connect to each allocated shuffle server
    qps_.resize(response.num_servers);
    for (size_t i = 0; i < response.num_servers; ++i) {
        sockfd = connect(servers_[i], true);

        const header::ConnectionRequest request{
            .job_id = job_id, .kind = header::ConnectionRequest::SOURCE, .info = {}
        };
        if (!send_all(sockfd, &request, sizeof(request))) {
            FDL_WARN("[Shuffle] Failed to send connection request to server %zu", i);
            ::close(sockfd);
            return false;
        }

        // Register the entire allocation for all four MR slots such that all RDMA operations are valid
        qps_[i] = std::make_unique<rdma::QueuePair>();
        const std::array<std::pair<char*, size_t>, 3> mrs{{
            { data_.get(), total },
            { data_.get(), total },
            { data_.get(), total }
        }};
        if (qps_[i]->initialize(mrs) != EXIT_SUCCESS) {
            FDL_WARN("[Shuffle] Failed to initialise QP for server %zu", i);
            ::close(sockfd);
            return false;
        }

        // Send local QP information to remote
        const auto local_info = qps_[i]->materialize();
        if (!send_all(sockfd, &local_info, sizeof(local_info))) {
            FDL_WARN("[Shuffle] Failed to send local QP info to server %zu", i);
            ::close(sockfd);
            return false;
        }

        // Receive remote QP information and use it to bring up the QP
        rdma::ibv_qp_info remote_info{};
        if (!recv_all(sockfd, &remote_info, sizeof(remote_info))) {
            FDL_WARN("[Shuffle] Failed to receive remote QP info from server %zu", i);
            ::close(sockfd);
            return false;
        }
        if (qps_[i]->bringup(remote_info) != EXIT_SUCCESS) {
            FDL_WARN("[Shuffle] Failed to bring up QP for server %zu", i);
            ::close(sockfd);
            return false;
        }

        // Receive the ring buffer metadata for the partitions assigned to the job on the shuffle server
        header::ConnectionResponse info;
        recv_all(sockfd, &info, sizeof(info));
        client_ids_[i] = info.client_id;

        const size_t num_regions = items_at(response.num_servers, num_partitions, i);
        std::vector<std::pair<uint64_t, uint64_t>> regions(num_regions);
        if (!recv_all(sockfd, regions.data(), num_regions * sizeof(regions[0]))) {
            FDL_WARN("[Shuffle] Failed to receive region metadata from server %zu", i);
            ::close(sockfd);
            return false;
        }

        // Each partition is assigned to a remote shuffle server in a round-robin manner
        for (size_t j = 0; j < num_regions; ++j) {
            views_[i + j * response.num_servers]->setup(
                qps_[i].get(), regions[j].first, regions[j].second, std::make_optional(std::make_pair(&wrapper, this))
            );
        }

        ::close(sockfd);
    }

    // Start the polling and flushing worker
    contexts_.resize(response.num_servers);
    worker_ = std::thread(&ShuffleJob::worker, this);

    return true;
}

void ShuffleJob::send(const partition_id_t partition, const char* src, const size_t size) {
    assert(partition < views_.size());
    assert(size < CLIENT_BUFFER_SIZE);

    // Block until there is room in the staging ring buffer
    std::optional<UnifiedHandle> handle = staging_[partition]->acquire_produce(size);
    while (!handle.has_value()) {
        handle = staging_[partition]->acquire_produce(size);
    }

    // Copy data into the ring buffer
    std::visit(overloaded{
        [&](const UnifiedRegion& region) -> void {
            memcpy(region.data, src, size);
        },
        [&](const std::array<UnifiedRegion, 2>& pair) -> void {
            memcpy(pair[0].data, src, pair[0].size);
            memcpy(pair[1].data, src + pair[0].size, pair[1].size);
        }
    }, handle.value().regions);

    staging_[partition]->release(std::move(handle.value()));
}

void ShuffleJob::sync(const partition_id_t partition) {
    // Signal to the flushing thread that we are syncing this partition
    sync_[partition].store(1, std::memory_order_release);
    while (staging_[partition]->get_consumable(false)[2] != 0) {
        cpu_pause();
    }
}

UnifiedHandle ShuffleJob::receive(const partition_id_t partition) {
    assert(partition < views_.size());

    if (requested_[partition].fetch_or(1, std::memory_order_acq_rel) == 0) {
        const size_t rank = partition % servers_.size();
        int sockfd = connect(servers_[rank], false);
        if (sockfd < 0) {
            FDL_WARN("[Shuffle] Failed to connect to server %zu", rank);
            assert(false);
        }

        const header::ConnectionRequest request{
            .job_id = job_id_, .kind = header::ConnectionRequest::SINK, .info = {
                client_ids_[rank],
                partition,
                reinterpret_cast<uint64_t>(destination_[partition]->data()),
                reinterpret_cast<uint64_t>(destination_[partition]->state())
            }
        };
        if (!send_all(sockfd, &request, sizeof(request))) {
            FDL_WARN("[Shuffle] Failed to send connection request to server %zu", rank);
            ::close(sockfd);
            assert(false);
        }
        ::close(sockfd);
    }

    std::optional<UnifiedHandle> handle = std::nullopt;
    while (!handle.has_value()) {
        handle = destination_[partition]->acquire_consume();
    }
    return std::move(handle.value());
}

void ShuffleJob::close() {
    bool done = false;
    if (!done_.compare_exchange_strong(done, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return;
    }

    // Stop the background worker and wait for it to drain all in-flight operations
    if (worker_.joinable()) {
        worker_.join();
    }

    // Notify the servers that it can close the connections to this client
    for (size_t i = 0; i < servers_.size(); ++i) {
        const int sockfd = connect(servers_[i], false);
        if (sockfd < 0) {
            FDL_WARN("[Job] close: failed to connect to server %zu for teardown", i);
            continue;
        }
        const header::ConnectionRequest request{
            .job_id = job_id_,
            .kind = header::ConnectionRequest::CLOSE,
            .info = { client_ids_[i], 0, 0, 0 },
        };
        send_all(sockfd, &request, sizeof(request));
        ::close(sockfd);
    }

    // Tear down every QP
    for (auto& qp : qps_) {
        if (qp) {
            qp->teardown();
        }
    }
    qps_.clear();

    // Release the underlying allocation now that it is safe
    data_.reset();
}

void ShuffleJob::worker() {
    while (!done_.load(std::memory_order_acquire)) {
        bool pause = false;

        // Flush partitions
        for (partition_id_t i = 0; i < static_cast<partition_id_t>(staging_.size()); ++i) {
            const bool force = sync_[i].fetch_or(0, std::memory_order_acq_rel) == 1;
            if (try_flush(i, force)) { pause = true; }
        }

        // Drain the completion queues for all the QPs
        for (size_t i = 0; i < qps_.size(); ++i) {
            poll(qps_[i].get());
        }

        // Yield to avoid spinning when there is nothing to do
        if (pause) { cpu_pause(); }
    }

    // Gracefully shutdown by draining all staged data and in-flight operations
    for (partition_id_t partition = 0; partition < static_cast<partition_id_t>(staging_.size()); ++partition) {
        if (staging_[partition]->get_consumable(false)[2] == 0) { continue; }
        while (!try_flush(partition, true)) {}
    }
    for (size_t i = 0; i < qps_.size(); ++i) {
        while (qps_[i]->inflight() > 0) {
            poll(qps_[i].get());
            cpu_pause();
        }
    }
}

bool ShuffleJob::try_flush(const partition_id_t partition, const bool force) {
    // If there is no pending handle, decide whether to acquire one
    if (!pending_[partition].has_value()) {
        if (!force) {
            const auto [size, padding, slots] = staging_[partition]->get_consumable(true);
            if (size + padding < UnifiedBuffer::capacity() / 4 && slots < PARTITION_BUFFER_PACKETS / 2) {
                return false;
            }
        }

        auto handle = staging_[partition]->acquire_flush();
        if (!handle.has_value()) { return false; }
        pending_[partition] = std::move(handle);
    }

    // Build segment descriptors from the pending handle
    std::array<std::pair<char*, uint32_t>, 2> b_segments{};
    std::visit(overloaded{
        [&](const UnifiedRegion& region) {
            b_segments[0] = {region.data, region.size};
            b_segments[1] = {nullptr, 0};
        },
        [&](const std::array<UnifiedRegion, 2>& pair) {
            b_segments[0] = {pair[0].data, pair[0].size};
            b_segments[1] = {pair[1].data, pair[1].size};
        }
    }, pending_[partition].value().regions);

    std::array<std::pair<uint64_t*, uint32_t>, 2> m_segments{};
    std::visit(overloaded{
        [&](const std::span<uint64_t>& region) {
            m_segments[0] = {region.data(), static_cast<uint32_t>(region.size())};
            m_segments[1] = {nullptr, 0};
        },
        [&](const std::array<std::span<uint64_t>, 2>& pair) {
            m_segments[0] = {pair[0].data(), static_cast<uint32_t>(pair[0].size())};
            m_segments[1] = {pair[1].data(), static_cast<uint32_t>(pair[1].size())};
        }
    }, pending_[partition].value().metadata);

    // Move the handle into the per-QP context array so the completion callback can release it
    // If submission fails we move it straight back to pending
    const size_t remote = partition % qps_.size();
    const uint32_t m_size = pending_[partition].value().m_size;
    FlushContext context = std::make_optional(
        std::make_pair(partition, std::move(pending_[partition].value()))
    );
    pending_[partition] = std::nullopt;

    const auto ticket = views_[partition]->produce_async(
        b_segments, m_segments, m_size, rdma::UNIFIED, contexts_[remote], context
    );
    if (!ticket.has_value()) {
        // Remote ring buffer is full so reclaim the handle for the next attempt
        pending_[partition] = std::move(context.value().second);
        return false;
    }

    return true;
}

void ShuffleJob::poll(rdma::QueuePair* qp) {
    std::array<size_t, rdma::POLL_BATCH_SIZE> tickets{};
    std::array<void*, rdma::POLL_BATCH_SIZE> contexts{};
    const size_t count = qp->poll(tickets, contexts);

    for (size_t i = 0; i < count; ++i) {
        auto* ctx = static_cast<FlushContext*>(contexts[i]);
        if (ctx == nullptr || !ctx->has_value()) { continue; }

        // Release the staging buffer handle
        auto& [partition, handle] = ctx->value();
        staging_[partition]->release(std::move(handle));
        *ctx = std::nullopt;
    }

    qp->reclaim(tickets, count);
}

ShuffleJob* ShuffleClient::enroll(const job_id_t job_id, const partition_id_t num_partitions) {
    std::scoped_lock lock(mutex_);

    auto search = jobs_.find(job_id);
    if (search != jobs_.end()) {
        return &search->second;
    }

    bool success;
    std::tie(search, success) = jobs_.emplace(
        std::piecewise_construct, std::forward_as_tuple(job_id), std::forward_as_tuple()
    );
    if (!success) {
        return nullptr;
    }

    if (!search->second.initialize(job_id, num_partitions)) {
        jobs_.erase(search);
        return nullptr;
    }

    return &search->second;
}

}