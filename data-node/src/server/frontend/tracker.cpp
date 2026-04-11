//
// Created by Felix Zhang on 2025-09-05.
//


#include "tracker.h"

namespace fdl {

Job* Tracker::get_job(const job_id_t job_id) {
    std::unique_lock lock(mutex_);

    if (const auto search = jobs_.find(job_id); search != jobs_.end()) {
        lock.unlock();
        return &search->second;
    }

    return nullptr;
}

Job& Tracker::getsert_job(const job_id_t job_id, const partition_id_t num_partitions) {
    std::unique_lock lock(mutex_);

    if (const auto search = jobs_.find(job_id); search != jobs_.end()) {
        lock.unlock();
        while (search->second.view_count() < SERVER_COUNT - 1) {
            std::this_thread::yield();
        }
        return search->second;
    }

    // If the job does not exist yet, set it up
    auto [iter, success] = jobs_.try_emplace(job_id, num_partitions, false);
    assert(success && "Failed to place buffers into hash map\n");

    FDL_LOG("Created job %u", job_id);
    return iter->second;
}

}
