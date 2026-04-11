//
// Created by Felix Zhang on 10/8/25.
//

#include <fstream>
#include <doca_mmap.h>

#include "dma.h"
#include "../config.h"

DOCA_LOG_REGISTER(SERVER_PIPELINES_DPU_DMA);

namespace fdl::dma {

/*
 * DMA Memcpy task completed callback
 *
 * @dma_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dma_memcpy_completed_callback(
	doca_dma_task_memcpy* dma_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)ctx_user_data;

	auto* user_data = static_cast<dma_user_data*>(task_user_data.ptr);

	/* Free task */
	doca_task_free(doca_dma_task_memcpy_as_task(dma_task));

	/* Update user-supplied data */
	user_data->count.fetch_sub(1);
	DOCA_LOG_INFO("DMA task was completed successfully");
}

/*
 * DMA Memcpy task error callback
 *
 * @dma_task [in]: failed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dma_memcpy_error_callback(
	doca_dma_task_memcpy* dma_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)ctx_user_data;

	auto* user_data = static_cast<dma_user_data*>(task_user_data.ptr);

	/* Get the result of the task */
	doca_task* task = doca_dma_task_memcpy_as_task(dma_task);
	const doca_error_t result = doca_task_get_status(task);
	DOCA_LOG_ERR("DMA task failed: %s", doca_error_get_descr(result));

	/* Free task */
	doca_task_free(task);

	/* Update user-supplied data saving the first encountered error */
	doca_error_t temp = DOCA_SUCCESS;
	user_data->result.compare_exchange_strong(temp, result, std::memory_order_acq_rel, std::memory_order_relaxed);
	user_data->count.fetch_sub(1);
}

/**
 * Callback triggered whenever DMA context state changes
 *
 * @user_data [in]: User data associated with the DMA context which holds `dma_resources*`
 * @ctx [in]: The DMA context that had a state change
 * @prev_state [in]: Previous context state
 * @next_state [in]: Next context state (context is already in this state when the callback is called)
 */
static void dma_state_changed_callback(
	const doca_data user_data, doca_ctx* ctx, doca_ctx_states prev_state, doca_ctx_states next_state
) {
	(void)ctx;
	(void)prev_state;

	auto* resources = static_cast<dma_resources*>(user_data.ptr);

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		DOCA_LOG_INFO("DMA context has been stopped");
		/* We can stop progressing the PE */
		resources->run_pe_progress = false;
		break;
	case DOCA_CTX_STATE_STARTING:
		/* The context is in starting state, this is unexpected for DMA */
		DOCA_LOG_ERR("DMA context entered into starting state. Unexpected transition");
		break;
	case DOCA_CTX_STATE_RUNNING:
		DOCA_LOG_INFO("DMA context is running");
		break;
	case DOCA_CTX_STATE_STOPPING:
		/**
		 * doca_ctx_stop() has been called.
		 * This happens either due to a failure encountered, in which case doca_pe_progress() will cause any inflight
		 * task to be flushed, or due to the successful compilation of the flow.
		 * In both cases, doca_pe_progress() will eventually transition the context to idle state.
		 */
		DOCA_LOG_INFO("DMA context entered into stopping state. Any inflight tasks will be flushed");
		break;
	default:
		break;
	}
}

/*
 * Loads export descriptor and buffer information content
 *
 * @export_desc_file_path [in]: Export descriptor file path
 * @buffer_info_file_path [in]: Buffer information file path
 * @export_desc [out]: Export descriptor buffer
 * @export_desc_len [out]: Export descriptor buffer length
 * @remote_addr [out]: Remote buffer address
 * @remote_addr_len [out]: Remote buffer total length
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t load_config_info(
	const char* export_desc_path, const char* buffer_info_path, char* export_desc, size_t& export_desc_len, char*& remote_addr, size_t& remote_addr_len
) {
	std::ifstream file(export_desc_path, std::ios::in);
	if (!file.is_open()) {
		DOCA_LOG_ERR("Failed to open %s", export_desc_path);
		return DOCA_ERROR_IO_FAILED;
	}

	/* Calculate export description file size */
	ssize_t file_size = file.tellg();
	if (!file.seekg(0, std::ios_base::end)) {
		DOCA_LOG_ERR("Failed to calculate file size");
		return DOCA_ERROR_IO_FAILED;
	}
	file_size = file.tellg() - file_size;
	if (file_size == -1) {
		DOCA_LOG_ERR("Failed to calculate file size");
		return DOCA_ERROR_IO_FAILED;
	}
	if (static_cast<size_t>(file_size) > RECV_BUF_SIZE) {
		file_size = RECV_BUF_SIZE;
	}

	export_desc_len = file_size;
	if (!file.seekg(0, std::ios_base::beg)) {
		DOCA_LOG_ERR("Failed to calculate file size");
		return DOCA_ERROR_IO_FAILED;
	}

	if (!file.read(export_desc, file_size)) {
		DOCA_LOG_ERR("Failed to allocate memory for source buffer");
		return DOCA_ERROR_IO_FAILED;
	}
	file.close();

	/* Read source buffer information from file */
	file.open(buffer_info_path, std::ios::in);
	if (!file.is_open()) {
		DOCA_LOG_ERR("Failed to open %s", buffer_info_path);
		return DOCA_ERROR_IO_FAILED;
	}

	/* Get source buffer address */
	char buffer[RECV_BUF_SIZE];
	if (!file.getline(buffer, RECV_BUF_SIZE)) {
		DOCA_LOG_ERR("Failed to read the source (host) buffer address");
		return DOCA_ERROR_IO_FAILED;
	}

	size_t convert_value = strtoull(buffer, nullptr, 0);
	if (convert_value == ULLONG_MAX) {
		DOCA_LOG_ERR("Failed to read the source (host) buffer address. Data is corrupted");
		return DOCA_ERROR_IO_FAILED;
	}
	remote_addr = reinterpret_cast<char *>(convert_value);

	/* Get source buffer length */
	if (!file.getline(buffer, RECV_BUF_SIZE)) {
		DOCA_LOG_ERR("Failed to read the source (host) buffer length");
		return DOCA_ERROR_IO_FAILED;
	}

	convert_value = strtoull(buffer, nullptr, 0);
	if (convert_value == ULLONG_MAX) {
		DOCA_LOG_ERR("Failed to read the source (host) buffer length. Data is corrupted");
		return DOCA_ERROR_IO_FAILED;
	}
	remote_addr_len = convert_value;

	return DOCA_SUCCESS;
}

doca_error_t allocate_resources(core_doca_objects& state, std::vector<dma_resources>& resources) {
	resources.resize(state.pipelines.dma.size());

	// Setup DMA-specific objects
	for (size_t i = 0; i < resources.size(); i++) {
		dma_resources& resource = resources[i];
		assert(resource.dma_context == nullptr);

		pipeline_doca_objects& pipeline = state.pipelines.dma[i];
		assert(pipeline.doca_context == nullptr);

		DOCA_TRY(doca_dma_create(state.device, &resource.dma_context), "Failed to create DMA context");

		pipeline.doca_context = doca_dma_as_ctx(resource.dma_context);
		DOCA_TRY(
			doca_ctx_set_state_changed_cb(pipeline.doca_context, dma_state_changed_callback),
			"Unable to set DMA state change callback"
		);
		DOCA_TRY(
			doca_dma_task_memcpy_set_conf(resource.dma_context, dma_memcpy_completed_callback, dma_memcpy_error_callback, NUM_DMA_TASKS),
			"Failed to set configurations for DMA memcpy task"
		);

		/* Include resources in user data of context to be used in callbacks */
		const doca_data ctx_user_data = { .ptr = &resource };
		doca_ctx_set_user_data(pipeline.doca_context, ctx_user_data);

		resource.run_pe_progress = true;

		/* Connect DMA context to progress engine */
		DOCA_TRY(
			doca_pe_connect_ctx(pipeline.prog_eng, pipeline.doca_context),
			"Failed to connect context to progress engine"
		);
		DOCA_TRY(doca_ctx_start(pipeline.doca_context), "Failed to start context");
	}

	/* Enqueue all pipelines as unused */
	std::array<size_t, DMA_PIPELINE_COUNT> free{};
	for (size_t i = 0; i < DMA_PIPELINE_COUNT; i++) { free[i] = i; }
	available.enqueue_bulk(free.data(), DMA_PIPELINE_COUNT);

	return DOCA_SUCCESS;
}

doca_error_t destroy_resources(core_doca_objects& state, std::vector<dma_resources>& resources) {
	doca_error_t result = DOCA_SUCCESS;
	for (size_t i = 0; i < resources.size(); i++) {
		dma_resources& resource = resources[i];
		pipeline_doca_objects& pipeline = state.pipelines.dma[i];

		if (resource.dma_context != nullptr) {
			const doca_error_t temp = doca_dma_destroy(resource.dma_context);
			DOCA_ERROR_PROPAGATE(result, temp);
			if (temp != DOCA_SUCCESS) {
				DOCA_LOG_ERR("Failed to destroy DOCA DMA context: %s", doca_error_get_descr(temp));
			}
			resource.dma_context = nullptr;
		}
		pipeline.doca_context = nullptr;
	}

	return result;
}

doca_error_t create_remote_mmap(core_doca_objects& state, char*& addr, size_t& len) {
	/* Load all relevant information */
	char export_desc[RECV_BUF_SIZE];
	size_t export_desc_len;
	DOCA_TRY(
	    load_config_info(DOCA_CONF.dma.export_desc_path, DOCA_CONF.dma.buf_info_path, export_desc, export_desc_len, addr, len),
	    "Failed to read memory configuration from file"
	);
	if (len == 0) {
	    DOCA_LOG_ERR("Remote buffer from host has length zero: failed to parse the buffer information file");
	    return DOCA_ERROR_EMPTY;
	}

	/* Create a DOCA memory mapping from the exported data */
	DOCA_TRY(
		doca_mmap_create_from_export(nullptr, export_desc, export_desc_len, state.device, &state.remote_mmap),
		"Failed to create remote memory map from exported description"
	);
	DOCA_TRY(doca_mmap_start(state.remote_mmap), "Failed to start remote memory map");

	return DOCA_SUCCESS;
}

size_t acquire() {
	/* Reserve a pipeline to use */
	size_t pipeline;
	available.wait_dequeue(pipeline);
	return pipeline;
}

void release(const size_t pipeline) {
	/* Release previously acquired pipeline */
	available.enqueue(pipeline);
}

doca_error_t copy(const pipeline_doca_objects& state, doca_dma*& context, doca_buf*& source, doca_buf*& destination) {
	/* Submit DMA task */
	doca_dma_task_memcpy* dma_task;
	dma_user_data user_data{ .count = 1 };
	DOCA_TRY(
		doca_dma_task_memcpy_alloc_init(context, source, destination, { .ptr = &user_data }, &dma_task),
		"Failed to allocate DMA task"
	);
	doca_task* task = doca_dma_task_memcpy_as_task(dma_task);
	const doca_error_t result = doca_task_submit(task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to submit DMA task: %s", doca_error_get_descr(result));
		doca_task_free(task);
		return result;
	}

	/* Wait for the task to complete */
	while (user_data.count == 1) {
		doca_pe_progress(state.prog_eng);
	}
	if (user_data.result != DOCA_SUCCESS) {
		return user_data.result;
	}

	return DOCA_SUCCESS;
}

doca_error_t capability_is_supported(doca_devinfo* devinfo) {
#ifdef DOCA_ARCH_HOST
	uint8_t from_export = 0;
	doca_error_t result = doca_mmap_cap_is_create_from_export_pci_supported(devinfo, &from_export);
	if (result != DOCA_SUCCESS) {
		return result;
	}
	if (from_export == 0) {
		return DOCA_ERROR_NOT_SUPPORTED;
	}
#endif

#ifdef DOCA_ARCH_DPU
	uint8_t from_export;
	DOCA_TRY(doca_mmap_cap_is_create_from_export_pci_supported(devinfo, &from_export), "Failed to get create from export PCI capability");
	if (from_export != 1) {
		doca_error_t result = DOCA_ERROR_NOT_SUPPORTED;
		DOCA_LOG_ERR(
			"Unable to create memory map from an exported one created via the PCI API: %s", doca_error_get_descr(result)
		);
		return result;
	}

	doca_error_t result = doca_dma_cap_task_memcpy_is_supported(devinfo);
	if (result != DOCA_SUCCESS) {
		return result;
	}

	uint64_t max_buf_size;
	DOCA_TRY(
		doca_dma_cap_task_memcpy_get_max_buf_size(devinfo, &max_buf_size), "Failed to get memcpy task max_buf_size capability for DMA"
	);
	if (max_buf_size < MAX_MEMCPY_BUF_SIZE) {
		doca_error_t result = DOCA_ERROR_NOT_SUPPORTED;
		DOCA_LOG_ERR(
			"Insufficient maximum buffer size (%ld): %s", max_buf_size, doca_error_get_descr(result)
		);
		return result;
	}

	uint32_t max_buf_list_len;
	DOCA_TRY(
		doca_dma_cap_task_memcpy_get_max_buf_list_len(devinfo, &max_buf_list_len),
		"Failed to get memcpy task max_buf_list_len capability for DMA"
	);
	if (max_buf_list_len < 3) {
		result = DOCA_ERROR_INVALID_VALUE;
		DOCA_LOG_ERR(
			"Insufficient maximum number of elements (%d) in linked list: %s", max_buf_list_len, doca_error_get_descr(result)
		);
		return result;
	}
#endif

	return result;
}

}
