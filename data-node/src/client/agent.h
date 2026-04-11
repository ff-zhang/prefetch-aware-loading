//
// Created by Felix Zhang on 3/19/26.
//

#pragma once

#include <atomic>
#include <optional>
#include <thread>

#include "pipelines/RDMA/ring_buffer_remote.h"
#include "utils/buffer_pool.h"
#include "utils/rdma.h"
#include "utils/types.h"

namespace fdl {

class ShuffleJob {
public:
    // Context for one inflight RDMA write
    using FlushContext = std::optional<std::pair<partition_id_t, UnifiedHandle>>;

    ShuffleJob() = default;
    ~ShuffleJob() {
        close();
    }

    // Delete copy & move operators
    ShuffleJob(const ShuffleJob&) = delete;
    ShuffleJob& operator=(const ShuffleJob&) = delete;
    ShuffleJob(const ShuffleJob&&) = delete;
    ShuffleJob&& operator=(const ShuffleJob&&) = delete;

    void send(partition_id_t partition, const char* src, size_t size);

    // Blocks until all data in the local buffer has been moved to remote
    void sync(partition_id_t partition);

    // Blocks until a data item is acquired for consumption, which must be released after being used
    [[nodiscard]] UnifiedHandle receive(partition_id_t partition);
    void release(const partition_id_t partition, UnifiedHandle&& handle) {
        destination_[partition]->release(std::move(handle));
    }

    void close();

private:
    friend class ShuffleClient;
    bool initialize(job_id_t job_id, partition_id_t num_partitions);

    void worker();

    // Try to flush a staging buffer and push it to the remote ring-buffer.
    // If the remote ring buffer is full the handle is stored in pending_[partition]
    // and will be retried on the next call. Returns true if a write was submitted.
    bool try_flush(partition_id_t partition, bool force);

    // Poll for write completions and release the associated staging-buffer handles
    void poll(rdma::QueuePair* qp);
    static void wrapper(void* job, void* qp) {
        static_cast<ShuffleJob*>(job)->poll(static_cast<rdma::QueuePair*>(qp));
    }

    job_id_t job_id_;

    // 64-byte aligned contiguous allocation for local buffers and remote views
    std::unique_ptr<char[]> data_{};

    // One QP per allocated shuffle server
    std::vector<sockaddr_in> servers_;
    std::vector<std::unique_ptr<rdma::QueuePair>> qps_{};
    std::vector<uint64_t> client_ids_;

    /* Data structures for sending data */

    // Per-partition local staging buffers and views of the remote ring-buffers on the shuffle server(s)
    std::vector<std::unique_ptr<UnifiedBuffer>> staging_{};
    std::vector<std::unique_ptr<rdma::ServerUnifiedBuffer>> views_{};

    // Per-partition handles consumed from a ring buffer but not yet submitted to remote
    std::vector<std::optional<UnifiedHandle>> pending_{};
    std::unique_ptr<std::atomic<uint64_t>[]> sync_{};

    // Background thread for polling RQs and flushing ring buffers
    std::thread worker_{};
    std::atomic<bool> done_{false};

    // Per-QP context pools indexed by the ticket returned from writing
    std::vector<std::array<FlushContext, 2 * rdma::MAX_CQE>> contexts_{};

    /* Data structures for receiving data */

    // Destinations for data items from the shuffle service
    std::vector<std::unique_ptr<UnifiedBuffer>> destination_{};
    std::unique_ptr<std::atomic<uint64_t>[]> requested_{};
};

class ShuffleClient {
public:
    ShuffleJob* enroll(job_id_t job_id, partition_id_t num_partitions);
    void unregister(const job_id_t job_id) { jobs_.erase(job_id); }

private:
    std::unordered_map<job_id_t, ShuffleJob> jobs_;
    std::mutex mutex_;
};

} // namespace fdl