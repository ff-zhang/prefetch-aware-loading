//
// Created by Felix Zhang on 12/4/25.
//

#pragma once

#include <array>
#include <vector>

#include <doca_buf.h>
#include <doca_ctx.h>
#include <doca_erasure_coding.h>
#include <doca_error.h>

#include "../doca.h"
#include "utils/parameters/configuration.h"

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "concurrentqueue/concurrentqueue.h"

namespace fdl::ec {

/* Buffers available to a single pipeline */
struct buffers_t {
    std::array<doca_buf*, EC_DATA_BLOCK_COUNT>& data;
    std::array<doca_buf*, EC_PARITY_BLOCK_COUNT>& parity;
};

/* User-supplied data for EC tasks */
struct user_data_t {
    std::atomic<doca_error_t> result{DOCA_SUCCESS};
    std::atomic_uint8_t count{0};
};

/* EC resources required per pipeline */
struct resources_t {
    doca_ec* ec_context = nullptr;              /* DOCA EC context */
    doca_ec_matrix* encoding_matrix = nullptr;  /* EC encoding matrix */
    doca_ec_matrix* recovery_matrix  = nullptr; /* Only used for in-flight decode o-perations */
    bool run_pe_progress = false;               /* Should the PE keep on progressing? */
    user_data_t completion = {};
};

class pipelines {
public:
    static pipelines& instance() {
        static pipelines instance;
        return instance;
    }

    /*
     * Allocate or free DOCA EC resources
     */
    doca_error_t allocate();
    doca_error_t free();

    /*
     * Acquire or release a pipeline (i.e., a chain of buffers for the data and parity blocks)
     */
    size_t acquire();
    void release(size_t pipeline);

    /*
     * Submit an EC task for asynchonous execution
     */
    doca_error_t encode(size_t pipeline, doca_buf*& source, doca_buf*& destination);
    doca_error_t decode(size_t pipeline, doca_buf*& source, doca_buf*& destination, std::vector<uint32_t> missing);

    /*
     * Advance the DOCA progress engine for this pipeline by one step
     */
    bool poll(size_t pipeline);

    /* Retrieve the outcome of the completed task on this pipeline */
    [[nodiscard]] doca_error_t result(const size_t pipeline) const {
        return resources[pipeline].completion.result.load(std::memory_order_acquire);
    }

    /* Pre-allocated DOCA buffers for EC */
    struct {
        std::array<std::array<doca_buf*, EC_DATA_BLOCK_COUNT>, EC_PIPELINE_COUNT> data;
        std::array<std::array<doca_buf*, EC_PARITY_BLOCK_COUNT>, EC_PIPELINE_COUNT> parity;

        buffers_t operator[] (const size_t index) {
            return buffers_t{data[index], parity[index]};
        }
    } buffers{};

    std::vector<resources_t> resources{EC_PIPELINE_COUNT};

    /* Tracks currently unused pipelines */
    moodycamel::BlockingConcurrentQueue<size_t> available{EC_PIPELINE_COUNT};

private:
    pipelines() = default;
    ~pipelines() { free(); }
};

/*
 * Check if the given device has the necessary capabilities to execute erasure coding
 * @devinfo [in]: The DOCA device information
 * @return: DOCA_SUCCESS if the device supports DMA memcpy task and DOCA_ERROR otherwise.
 */
doca_error_t capability_is_supported(doca_devinfo* devinfo);

}
