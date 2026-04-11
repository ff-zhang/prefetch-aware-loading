//
// Created by Felix Zhang on 2025-09-10.
//


#include <cassert>
#include <list>
#include <sys/socket.h>

#include "worker.h"
#include "server/frontend/tracker.h"

namespace fdl::rdma {

template<RangeOf<int> Range>
void worker(const job_id_t job_id, const partition_id_t num_partitions, const size_t rank, Range&& secondary_fds) {
    // Blocks until all the resources within the shuffle service are set up (including the remote ones)
    auto& job = Tracker::instance().getsert_job(job_id, num_partitions, rank, secondary_fds);

    // TODO: swap to ring buffer of some sort
    std::list<std::tuple<size_t, UnifiedHandle, InflightOperation, context_t>> inflight;
    while (!job.done()) {
        // For each partition with a registered sink, copy all consumable data from the local buffer to the remote destination
        // If the remote buffer is full, it stores a pending handle and retries on the next iteration.
        for (partition_id_t i = 0; i < num_partitions; ++i) {
            if (job.has_sink(i)) {
                job.send(i);
            }
        }

        // Drain completions and release source handles back to their buffers
        job.poll(false);

        // TODO: re-enable EC and test correctness
        continue;

        // TODO: skip buffers which are no longer being written to
        for (size_t i = 0; i < job.size(); ++i) {
            // Attempt to encode data item(s) in partition buffers
            std::optional<UnifiedHandle> handle = job.buffer(i)->acquire_consume();
            if (!handle.has_value()) {
                // No data available to be consumed
                job.buffer(i)->update_pointers();
                continue;
            }

            const size_t pipeline = ec::pipelines::instance().acquire();
            InflightOperation operation = job.encode(pipeline, handle.value());
            inflight.emplace_back(i, std::move(handle.value()), std::move(operation), context_t{});
        }

        for (auto it = inflight.begin(); it != inflight.end(); ) {
            auto& [partition, handle, operation, completion] = *it;
            if (operation.kind == InflightOperation::EC) {
                if (!operation.ready) {
                    // Drive the pipeline if the operation is still incomplete
                    operation.ready = ec::pipelines::instance().poll(operation.pipeline);
                }
                if (operation.ready) {
                    job.scatter(partition, operation, completion);
                    operation.kind = InflightOperation::RDMA;
                }
                ++it;
            } else if (operation.kind == InflightOperation::RDMA) {
                if (completion.count_.load(std::memory_order_acquire) == 0) {
                    ec::pipelines::instance().release(operation.pipeline);
                    job.buffer(partition)->release(std::move(handle));
                    it = inflight.erase(it);
                } else {
                    ++it;
                }
            } else {
                assert(false);
            }
        }
    }

    // TODO: drain in-flight operations for graceful shutdown
}

// Explicitly instantiate template types
template void worker<std::vector<int>>(job_id_t, partition_id_t, size_t, std::vector<int>&&);

}