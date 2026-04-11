//
// Created by Felix Zhang on 12/18/25.
//

#pragma once

#include <mutex>
#include <optional>
#include <vector>

#include "completion.h"
#include "pipelines/RDMA/ring_buffer_remote.h"
#include "server/backend/backend.h"
#include "utils/network.h"
#include "utils/rdma.h"

namespace fdl {

struct InflightOperation {
    enum { EC, RDMA } kind;

    std::array<char*, EC_DATA_BLOCK_COUNT> data_blocks{};
    std::array<char*, EC_PARITY_BLOCK_COUNT> parity_blocks{};
    size_t block_size = 0;

    size_t pipeline = 0;

    bool ready = false;
};

class Job {
public:
    // Context stored per in-flight sink write, indexed by the QP ticket
    using SinkContext = std::optional<std::pair<partition_id_t, UnifiedHandle>>;

    Job(partition_id_t num_partitions, bool remotes);
    ~Job();

    /* Disable copying & moving */
    Job(const Job&) = delete;
    Job& operator=(const Job&) = delete;
    Job(Job&& other) = delete;
    Job& operator=(Job&& other) = delete;

    bool add_server(size_t index, rdma::ibv_qp_info&& remote, const std::vector<std::pair<uint64_t, uint64_t>>& regions);

    // Returns the client index and QP connected to it
    std::pair<size_t, rdma::QueuePair&> add_client();

    // Register a client-side destination for the given partition
    void add_sink(partition_id_t partition, uint64_t client_id, const std::array<uint64_t, 2>& regions);

    // Tear down a client's QP and release all associated sink resources
    void close_client(uint64_t client_id);

    /* Reserve space in the requested unified buffer to produce data into */
    [[nodiscard]] UnifiedHandle acquire_produce(size_t partition, size_t size, size_t block = 1) const;

    /* Acquire region in the requested unified buffer to consume data from */
    [[nodiscard]] std::pair<std::optional<UnifiedHandle>, size_t> acquire_consume(size_t partition, size_t start);

    /* Release previously obtained region in a unified buffer */
    void release(size_t partition, UnifiedHandle&& handle) const;

    /* Flush all the data in the requested unified buffer into the disaggregated backend */
    void flush(size_t partition);

    /* Encode a data item in the requested unified buffer */
    InflightOperation encode(size_t pipeline, const UnifiedHandle& handle);

    void scatter(size_t partition, const InflightOperation& operation, context_t& completion);

    std::optional<UnifiedHandle> decode(size_t partition);

    /*
     * Try to flush all consumable data to its sink via RDMA
     * If the remote ring buffer is momentarily full the pending handle is held and retried on the next call
     * Returns true if a write was submitted, false otherwise
     */
    bool send(partition_id_t partition);

    // Poll every unique sink QP for completions and release the associated handles
    // The caller of this should hold the clients list mutex
    void poll(bool unsafe);
    static void wrapper(void* job, void* qp) {
        (void) qp;
        static_cast<Job*>(job)->poll(true);
    }

    /* Getters & setters */
    size_t size() const { return buffers_.size(); }

    UnifiedBuffer* buffer(const partition_id_t partition) const { return buffers_[partition]; }
    std::vector<UnifiedBuffer*>& buffers() { return buffers_; }
    BackendHandle& backend() { return backend_; }

    rdma::ServerUnifiedBuffer& view(const size_t index, const partition_id_t partition) const {
        return *views_[index][partition];
    }
    uint32_t view_count() const {
        return view_count_.load(std::memory_order_acquire);
    }

    rdma::ibv_qp_info& remote(const size_t i) { return remotes_[i]; }
    rdma::QueuePair& queue_pair(const size_t i) { return queue_pairs_[i]; }

    rdma::QueuePair& client(const size_t i) { return *clients_[i]; }

    bool has_sink(const partition_id_t partition) const {
        return partition < sinks_.size() && sinks_[partition].qp != nullptr;
    }

    void done(const bool value) { return done_.store(value, std::memory_order_release); }
    bool done() const { return done_.load(std::memory_order_acquire); }

private:
    // Per-partition sink descriptor
    struct Sink {
        rdma::QueuePair* qp = nullptr;
        size_t client_id = std::numeric_limits<size_t>::max();
        rdma::ClientUnifiedBuffer* view{};
        std::optional<UnifiedHandle> pending = std::nullopt;
    };

    // Ring buffers for holding incoming and outgoing per-partition data
    std::vector<UnifiedBuffer*> buffers_;

    // Handle for the job's log in the disaggregated backend
    BackendHandle backend_;

    // Views of the remote ring buffers (on other shuffler servers) assigned to this job
    std::array<std::vector<rdma::ServerUnifiedBuffer*>, SERVER_COUNT - 1> views_{};
    std::atomic<uint32_t> view_count_{0};

    // Queue pairs with other servers allocated
    std::array<rdma::ibv_qp_info, SERVER_COUNT - 1> remotes_{};
    std::array<rdma::QueuePair, SERVER_COUNT - 1> queue_pairs_{};

    // Queue pairs with clients
    // Guarded for concurrent access between the listener and the worker threads
    std::deque<std::unique_ptr<rdma::QueuePair>> clients_;
    std::mutex clients_mutex_;

    // Per-partition client-side sink descriptors
    std::vector<Sink> sinks_;

    // One context array per client QP, indexed by the ticket returned when writing
    std::vector<std::array<SinkContext, 2 * rdma::MAX_CQE>> contexts_;

    std::atomic<bool> done_{false};
};

} // namespace fdl