//
// Created by Felix Zhang on 10/8/25.
//

#pragma once

#include <atomic>

#include <doca_dma.h>
#include <doca_ctx.h>
#include <doca_error.h>

#include "pipelines/DOCA/doca.h"
#include "utils/parameters/configuration.h"

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "concurrentqueue/concurrentqueue.h"

namespace fdl::dma {

/* DMA resources per pipeline */
struct dma_resources {
	doca_dma* dma_context = nullptr;	/* DOCA DMA context */
	bool run_pe_progress = false;		/* Should the PE keep on progressing? */
};

/* User-supplied data for DMA tasks */
struct dma_user_data {
	std::atomic<doca_error_t> result{DOCA_SUCCESS};
	std::atomic_uint8_t count{0};
};

/* Tracks currently unused pipelines */
static moodycamel::BlockingConcurrentQueue<size_t> available{DMA_PIPELINE_COUNT + EC_PIPELINE_COUNT};

/*
 * Allocate DOCA DMA resources
 *
 * @pcie_addr [in]: PCIe address of device to open
 * @max_num_buf [in]: Number of doca_buf to allocate
 * @resources [out]: Structure containing all DMA resources
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t allocate_resources(core_doca_objects& state, std::vector<dma_resources>& resources);

/*
 * Destroy DOCA DMA resources
 *
 * @resources [out]: Structure containing all DMA resources
 * @dma_ctx [in]: DOCA DMA context
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t destroy_resources(core_doca_objects& state, std::vector<dma_resources>& resources);

doca_error_t create_remote_mmap(core_doca_objects& state, char*& addr, size_t& len);

/*
 * Acquire a set of local, remote, and wrap buffers
 */
size_t acquire();

void release(size_t pipeline);

doca_error_t copy(const pipeline_doca_objects& state, doca_dma*& context, doca_buf*& source, doca_buf*& destination);

/*
 * Check if the given device has the necessary capabilities to execute the DMA backend
 * @devinfo [in]: The DOCA device information
 * @return: DOCA_SUCCESS if the device supports DMA memcpy task and DOCA_ERROR otherwise.
 */
doca_error_t capability_is_supported(doca_devinfo* devinfo);

}
