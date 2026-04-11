//
// Created by Felix Zhang on 12/4/25.
//

#include <atomic>

#include "ec.h"
#include "../config.h"
#include "utils/buffer_pool.h"

DOCA_LOG_REGISTER(SERVER_PIPELINES_DPU_EC);

namespace fdl::ec {

/*
 * EC Create task completed callback
 *
 * @ec_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void ec_create_completed_callback(
	doca_ec_task_create* ec_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)(ctx_user_data);

	auto* user_data = static_cast<user_data_t*>(task_user_data.ptr);

	/* Free task */
	doca_task_free(doca_ec_task_create_as_task(ec_task));

	/* Update user-supplied data */
	user_data->count.fetch_sub(1);
	DOCA_LOG_INFO("EC create task was completed successfully");
}


/*
 * EC Create task error callback
 *
 * @ec_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void ec_create_error_callback(
	doca_ec_task_create* ec_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)(ctx_user_data);

	auto* user_data = static_cast<user_data_t*>(task_user_data.ptr);

	/* Get the result of the task */
	doca_task* task = doca_ec_task_create_as_task(ec_task);
	const doca_error_t result = doca_task_get_status(task);
	DOCA_LOG_ERR("EC create task failed: %s", doca_error_get_descr(result));

	/* Free task */
	doca_task_free(task);

	/* Update user-supplied data saving the first encountered error */
	doca_error_t temp = DOCA_SUCCESS;
	user_data->result.compare_exchange_strong(temp, result, std::memory_order_acq_rel, std::memory_order_relaxed);
	user_data->count.fetch_sub(1);
}

/*
 * EC Recover task completed callback
 *
 * @ec_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void ec_recover_completed_callback(
	doca_ec_task_recover* ec_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)(ctx_user_data);

	auto* user_data = static_cast<user_data_t*>(task_user_data.ptr);

	/* Free task */
	doca_task_free(doca_ec_task_recover_as_task(ec_task));

	/* Update user-supplied data */
	user_data->count.fetch_sub(1);
	DOCA_LOG_INFO("EC recover task was completed successfully");
}


/*
 * EC Recover task error callback
 *
 * @ec_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void ec_recover_error_callback(
	doca_ec_task_recover* ec_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)(ctx_user_data);

	auto* user_data = static_cast<user_data_t*>(task_user_data.ptr);

	/* Get the result of the task */
	doca_task* task = doca_ec_task_recover_as_task(ec_task);
	const doca_error_t result = doca_task_get_status(task);
	DOCA_LOG_ERR("EC recover task failed: %s", doca_error_get_descr(result));

	/* Free task */
	doca_task_free(task);

	/* Update user-supplied data saving the first encountered error */
	doca_error_t temp = DOCA_SUCCESS;
	user_data->result.compare_exchange_strong(temp, result, std::memory_order_acq_rel, std::memory_order_relaxed);
	user_data->count.fetch_sub(1);
}

/**
 * Callback triggered whenever EC context state changes
 *
 * @user_data [in]: User data associated with the EC context which holds `ec_resources*`
 * @ctx [in]: The EC context that had a state change
 * @prev_state [in]: Previous context state
 * @next_state [in]: Next context state (context is already in this state when the callback is called)
 */
static void ec_state_changed_callback(
	const doca_data user_data, doca_ctx* ctx, doca_ctx_states prev_state, doca_ctx_states next_state
) {
	(void)ctx;
	(void)prev_state;

	auto* resources = static_cast<resources_t*>(user_data.ptr);

	switch (next_state) {
		case DOCA_CTX_STATE_IDLE:
			DOCA_LOG_INFO("EC context has been stopped");
			/* We can stop progressing the PE */
			resources->run_pe_progress = false;
			break;
		case DOCA_CTX_STATE_STARTING:
			/* The context is in starting state, this is unexpected for EC */
			DOCA_LOG_ERR("EC context entered into starting state. Unexpected transition");
			break;
		case DOCA_CTX_STATE_RUNNING:
			DOCA_LOG_INFO("EC context is running");
			/**
			 * doca_ctx_stop() has been called.
			 * This happens either due to a failure encountered, in which case doca_pe_progress() will cause any inflight
			 * task to be flushed, or due to the successful compilation of the flow.
			 * In both cases, doca_pe_progress() will eventually transition the context to idle state.
			 */
			DOCA_LOG_INFO("EC context entered into stopping state. Any inflight tasks will be flushed");
			break;
		default:
			break;
	}
}

doca_error_t pipelines::allocate() {
	// Setup EC-specific objects
	auto& state = core_doca_objects::instance();
	for (size_t i = 0; i < resources.size(); i++) {
		resources_t& resource = resources[i];
		pipeline_doca_objects& pipeline = state.pipelines.ec[i];

		assert(resource.ec_context == nullptr);
		assert(pipeline.doca_context == nullptr);

		DOCA_TRY(doca_ec_create(state.device, &resource.ec_context), "Failed to create EC context");
		pipeline.doca_context = doca_ec_as_ctx(resource.ec_context);

		DOCA_TRY(
			doca_ctx_set_state_changed_cb(pipeline.doca_context, ec_state_changed_callback),
			"Unable to set EC state change callback"
		);
		DOCA_TRY(
			doca_ec_task_create_set_conf(resource.ec_context, ec_create_completed_callback, ec_create_error_callback, NUM_EC_TASKS),
			"Failed to set configuration for EC create task"
			);
		DOCA_TRY(
			doca_ec_task_recover_set_conf(resource.ec_context, ec_recover_completed_callback, ec_recover_error_callback, NUM_EC_TASKS),
			"Failed to set configuration for EC recover task"
		);

		/* Include resources in user data of context to be used in callbacks */
		const doca_data ctx_user_data = { .ptr = &resource };
		doca_ctx_set_user_data(pipeline.doca_context, ctx_user_data);

		DOCA_TRY(doca_ec_matrix_create(
			resource.ec_context, DOCA_EC_MATRIX_TYPE_CAUCHY, EC_DATA_BLOCK_COUNT, EC_PARITY_BLOCK_COUNT, &resource.encoding_matrix
		), "Unable to create EC encoding matrix");

		resource.run_pe_progress = true;

		/* Connect EC context to progress engine */
		DOCA_TRY(
			doca_pe_connect_ctx(pipeline.prog_eng, pipeline.doca_context),
			"Failed to connect context to progress engine"
		);
		DOCA_TRY(doca_ctx_start(pipeline.doca_context), "Failed to start context");
	}

	/* Create memory map for the EC buffer(s) */
	const auto& ec_addr = BufferPools::scratch().data();
	const auto& ec_len = BufferPools::scratch().size_of();
	state.create_local_mmap(state.scratch_mmap, ec_addr, ec_len);

	/* For each pipeline, pre-allocate DOCA buffers from the EC memory map */
	for (size_t i = 0; i < state.pipelines.ec.size(); ++i) {
		const auto& pipeline = state.pipelines.ec[i];
		auto [data, parity] = buffers[i];

		// Allocate chain of DOCA buffers for encoding
		doca_buf*& d_head = data[0];
		for (size_t block = 0; block < EC_DATA_BLOCK_COUNT; block++) {
			doca_buf*& current = data[block];
			DOCA_TRY(
				doca_buf_inventory_buf_get_by_addr(pipeline.buf_inv, state.local_mmap, ec_addr, ec_len, &current),
				"Unable to acquire DOCA buffer"
			);
			if (block > 0) {
				DOCA_TRY(doca_buf_chain_list_tail(d_head, *(&current - 1), current), "Failed to chain buffers");
			}
		}

		// Allocate chain of DOCA buffers for decoding
		doca_buf*& p_head = parity[0];
		for (size_t block = 0; block < EC_PARITY_BLOCK_COUNT; ++block) {
			doca_buf*& current = parity[block];
			DOCA_TRY(
				doca_buf_inventory_buf_get_by_addr(pipeline.buf_inv, state.scratch_mmap, ec_addr, ec_len, &current),
				"Unable to acquire DOCA buffer"
			);
			if (block > 0) {
				DOCA_TRY(doca_buf_chain_list_tail(p_head, *(&current - 1), current), "Failed to chain buffers");
			}
		}
	}

	/* Enqueue all pipelines as unused */
	std::array<size_t, EC_PIPELINE_COUNT> free{};
	for (size_t i = 0; i < EC_PIPELINE_COUNT; i++) { free[i] = i; }
	available.enqueue_bulk(free.data(), EC_PIPELINE_COUNT);

	return DOCA_SUCCESS;
}

doca_error_t pipelines::free() {
	doca_error_t result = DOCA_SUCCESS;
	doca_error_t temp;
	for (size_t i = 0; i < resources.size(); i++) {
		resources_t& resource = resources[i];
		pipeline_doca_objects& pipeline = core_doca_objects::instance().pipelines.ec[i];

		resource.run_pe_progress = false;

		if (resource.recovery_matrix != nullptr) {
			const doca_error_t tmp = doca_ec_matrix_destroy(resource.recovery_matrix);
			DOCA_ERROR_PROPAGATE(result, tmp);
			resource.recovery_matrix = nullptr;
		}
		if (resource.encoding_matrix != nullptr) {
			temp = doca_ec_matrix_destroy(resource.encoding_matrix);
			DOCA_ERROR_PROPAGATE(result, temp);
			if (temp != DOCA_SUCCESS) {
				DOCA_LOG_ERR("Failed to destroy DOCA EC matrix: %s", doca_error_get_descr(temp));
			}
			resource.encoding_matrix = nullptr;
		}

		if (resource.ec_context != nullptr) {
			temp = doca_ec_destroy(resource.ec_context);
			DOCA_ERROR_PROPAGATE(result, temp);
			if (temp != DOCA_SUCCESS) {
				DOCA_LOG_ERR("Failed to destroy DOCA EC context: %s", doca_error_get_descr(temp));
			}
			resource.ec_context = nullptr;
		}
		pipeline.doca_context = nullptr;
	}

	return result;
}

size_t pipelines::acquire() {
	/* Reserve a pipeline to use */
	size_t pipeline;
	available.wait_dequeue(pipeline);

	return pipeline;
}

void pipelines::release(const size_t pipeline) {
	/* Release previously acquired pipeline */
	available.enqueue(pipeline);
}

doca_error_t pipelines::encode(const size_t pipeline, doca_buf*& source, doca_buf*& destination) {
	auto& [context, encoding_matrix, _1, _2, user_data] = resources[pipeline];

	user_data.result.store(DOCA_SUCCESS, std::memory_order_relaxed);
	user_data.count.store(1, std::memory_order_release);

	/* Submit EC create task */
	doca_ec_task_create* ec_task = nullptr;
	doca_error_t result = doca_ec_task_create_allocate_init(
		context, encoding_matrix, source, destination, { .ptr = &user_data }, &ec_task
	);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate EC create task: %s", doca_error_get_descr(result));
		user_data.count.store(0, std::memory_order_release);
		return result;
	}

	doca_task* task = doca_ec_task_create_as_task(ec_task);
	result = doca_task_submit(task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to submit EC create task: %s", doca_error_get_descr(result));
		doca_task_free(task);
        user_data.count.store(0, std::memory_order_release);
		return result;
	}

	return DOCA_SUCCESS;
}

doca_error_t pipelines::decode(const size_t pipeline, doca_buf*& source, doca_buf*& destination, std::vector<uint32_t> missing) {
	auto& [context, encoding_matrix, recovery_matrix, _, user_data] = resources[pipeline];

	/* Create recovery matrix */
	DOCA_TRY(
		doca_ec_matrix_create_recover(context, encoding_matrix, missing.data(), missing.size(), &recovery_matrix),
		"Failed to create EC recovery matrix"
	);

	user_data.result.store(DOCA_SUCCESS, std::memory_order_relaxed);
	user_data.count.store(1, std::memory_order_release);

	/* Submit EC recover task */
	doca_ec_task_recover* ec_task = nullptr;
	doca_error_t result = doca_ec_task_recover_allocate_init(
		context, encoding_matrix, source, destination, { .ptr = &user_data }, &ec_task
	);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate EC create task: %s", doca_error_get_descr(result));
		doca_ec_matrix_destroy(recovery_matrix);
		recovery_matrix = nullptr;
		user_data.count.store(0, std::memory_order_release);
		return result;
	}


	doca_task* task = doca_ec_task_recover_as_task(ec_task);
	result = doca_task_submit(task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to submit EC create task: %s", doca_error_get_descr(result));
		doca_task_free(task);
		doca_ec_matrix_destroy(recovery_matrix);
		recovery_matrix = nullptr;
		user_data.count.store(0, std::memory_order_release);
		return result;
	}

	return DOCA_SUCCESS;
}

bool pipelines::poll(const size_t pipeline) {
	const auto& state = core_doca_objects::instance().pipelines.ec[pipeline];
	doca_pe_progress(state.prog_eng);

	auto& [context, encoding_matrix, recovery_matrix, _, user_data] = resources[pipeline];
	if (user_data.count.load(std::memory_order_acquire) != 0) {
		return false;
	}

	/* Tear down any state now that the task is done */
	if (recovery_matrix != nullptr) {
		const doca_error_t result = doca_ec_matrix_destroy(recovery_matrix);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to destroy EC recovery matrix: %s", doca_error_get_descr(result));
			/* Propagate into the inflight result so the caller can observe it */
			doca_error_t expected = DOCA_SUCCESS;
			user_data.result.compare_exchange_strong(expected, result, std::memory_order_acq_rel, std::memory_order_relaxed);
		}
		recovery_matrix = nullptr;
	}

	return true;
}

doca_error_t capability_is_supported(doca_devinfo* devinfo) {
	auto result = doca_ec_cap_task_galois_mul_is_supported(devinfo);
	if (result != DOCA_SUCCESS) {
		return result;
	}

	result = doca_ec_cap_task_create_is_supported(devinfo);
	if (result != DOCA_SUCCESS) {
		return result;
	}

	result = doca_ec_cap_task_recover_is_supported(devinfo);
	if (result != DOCA_SUCCESS) {
		return result;
	}

	uint32_t max_buf_list_len;
	DOCA_TRY(
		doca_ec_cap_get_max_buf_list_len(devinfo, &max_buf_list_len),
		"Failed to get task max_buf_list_len capability for error coding"
	);
	if (max_buf_list_len < 6) {
		result = DOCA_ERROR_INVALID_VALUE;
		DOCA_LOG_ERR(
			"Insufficient maximum number of elements (%d) in linked list: %s", max_buf_list_len, doca_error_get_descr(result)
		);
		return result;
	}

	return result;
}

}
