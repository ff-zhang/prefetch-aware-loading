//
// Created by Felix Zhang on 2025-09-05.
//

#pragma once

#include <atomic>
#include <cassert>
#include <ranges>
#include <unordered_map>

#include "job.h"
#include "utils/socket.h"
#include "utils/rdma.h"
#include "utils/types.h"

namespace fdl {

class Tracker {
public:
    // Delete copy & move operators
    Tracker(const Tracker&) = delete;
    Tracker& operator=(const Tracker&) = delete;
    Tracker(Tracker&&) = delete;
    Tracker& operator=(Tracker&&) = delete;

    static Tracker& instance() {
        static Tracker instance_;
        return instance_;
    }

    Job* get_job(job_id_t job_id);

    template<RangeOf<int> Range>
    Job& getsert_job(const job_id_t job_id, const partition_id_t num_partitions, const size_t rank, const Range& secondary_fds) {
        std::unique_lock lock(mutex_);

        if (const auto search = jobs_.find(job_id); search != jobs_.end()) {
            lock.unlock();
            while (search->second.view_count() < SERVER_COUNT - 1) {
                std::this_thread::yield();
            }
            return search->second;
        }

        // If the job does not exist yet, set it up
        auto [iter, success] = jobs_.try_emplace(job_id, num_partitions, true);
        assert(success && "Failed to place buffers into hash map\n");

        auto& job = iter->second;

        header::JobSetup request = {
            header::JobSetup::SYN, job_id, num_partitions, rank, {}
        };
        for (size_t i = 0; i < std::ranges::size(secondary_fds); i++) {
            const std::array<std::pair<char*, size_t>, 3> regions = {
                std::pair{BufferPools::unified().data(), BufferPools::unified().size_of()},
                std::pair{BufferPools::scratch().data(), BufferPools::scratch().size_of()},
                std::pair{ BufferPools::remote().data(), BufferPools::remote().size_of()},
            };

            // Initialize the new QPs
            job.queue_pair(i).initialize(regions);
            request.qp_info = job.queue_pair(i).materialize();

            send_all(secondary_fds[i], &request, sizeof(header::JobSetup));
        }

        lock.unlock();

        // Wait for all the remote ring buffers to be registered
        while (job.view_count() < std::ranges::size(secondary_fds) - 1) {
            std::this_thread::yield();
        }

        FDL_LOG("Created job %u", job_id);
        return job;
    }

    Job& getsert_job(job_id_t job_id, partition_id_t num_partitions);

private:
    Tracker() = default;

    std::unordered_map<job_id_t, Job> jobs_ = {};
    std::mutex mutex_{};
};

}
