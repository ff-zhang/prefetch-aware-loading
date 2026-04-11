//
// Created by Felix Zhang on 10/5/25.
//

#include <doca_dma.h>
#include <doca_mmap.h>

#include "doca.h"
#include "utils/parameters/configuration.h"

DOCA_LOG_REGISTER(SERVER_PIPELINES_DPU_DOCA);

namespace fdl {

doca_error_t core_doca_objects::register_logger() {
	/* Register logger backends for DOCA errors and warnings */
#ifndef NDEBUG
	DOCA_TRY(doca_log_backend_create_with_file_sdk(stdout, &log), "Failed to create DOCA log backend");
#endif
	DOCA_TRY(doca_log_backend_create_with_file_sdk(stderr, &log), "Failed to create DOCA log backend");
	DOCA_TRY(doca_log_backend_set_sdk_level(log, DOCA_LOG_LEVEL_WARNING), "Failed to set DOCA log backend level limit");

	return DOCA_SUCCESS;
}

doca_error_t core_doca_objects::open_device(
	const char* pcie_addr, const std::function<doca_error_t(doca_devinfo*)>& check_device_capabilities
) {
	doca_devinfo** devlist;
	uint32_t num_devices;
	doca_error_t result = doca_devinfo_create_list(&devlist, &num_devices);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to open list of DOCA devices: %s", doca_error_get_name(result));
		return result;
	}

	doca_devinfo* devinfo = nullptr;
	for (size_t it = 0; it < num_devices; ++it) {
		uint8_t is_equal = 0;
		result = doca_devinfo_is_equal_pci_addr(devlist[it], pcie_addr, &is_equal);
		if (result != DOCA_SUCCESS || is_equal == 0) {
			continue;
		}

		result = check_device_capabilities(devlist[it]);
		if (result != DOCA_SUCCESS) {
			continue;
		}

		devinfo = devlist[it];
		break;
	}
	if (devinfo == nullptr) {
		DOCA_LOG_ERR("Failed to find device with PCI address");
		return DOCA_ERROR_INVALID_VALUE;
	}

	result = doca_dev_open(devinfo, &device);
	if (result != DOCA_SUCCESS){
		DOCA_LOG_ERR("Failed to open queried device: %s", doca_error_get_name(result));
		return result;
	}

	result = doca_devinfo_destroy_list(devlist);
	if (result != DOCA_SUCCESS){
		DOCA_LOG_ERR("Failed to destroy list of DOCA devices: %s", doca_error_get_name(result));
		return result;
	}

	return result;
}

doca_error_t core_doca_objects::initialize(const size_t max_num_buf, const bool dma, const bool ec) {
	assert(device != nullptr && "Device has not been created\n");

	/* Initialize data structures for DMA and EC pipelines if requested */
	if (dma) {
		pipelines.dma.resize(DMA_PIPELINE_COUNT);
	}
	if (ec) {
		pipelines.ec.resize(EC_PIPELINE_COUNT);
	}

	const auto views = pipelines.all();
	for (auto& pipeline : std::views::join(views)) {
		if (max_num_buf != 0) {
			DOCA_TRY(doca_buf_inventory_create(max_num_buf, &pipeline.buf_inv), "Unable to create buffer inventory");
			DOCA_TRY(doca_buf_inventory_start(pipeline.buf_inv), "Unable to start buffer inventory");
		}

		DOCA_TRY(doca_pe_create(&pipeline.prog_eng), "Unable to create progress engine");
	}

	return DOCA_SUCCESS;
}

doca_error_t core_doca_objects::create_local_mmap(doca_mmap*& mmap, char* addr, const size_t len) const {
	DOCA_TRY(doca_mmap_create(&mmap), "Unable to create local mmap");
	DOCA_TRY(doca_mmap_add_dev(mmap, device), "Unable to add device to local memory map");

	constexpr int flags =
		DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_RDMA_READ | DOCA_ACCESS_FLAG_RDMA_WRITE | DOCA_ACCESS_FLAG_RDMA_ATOMIC | DOCA_ACCESS_FLAG_PCI_READ_WRITE;
	DOCA_TRY(
		doca_mmap_set_permissions(mmap, flags),
		"Failed to set mmap permissions"
	);
	DOCA_TRY(doca_mmap_set_memrange(mmap, addr, len), "Failed to set memory range for local memory map");

	DOCA_TRY(doca_mmap_start(mmap), "Failed to start local memory map");

	return DOCA_SUCCESS;
}

doca_error_t core_doca_objects::terminate() {
	/* Objects should be destroyed in the reverse-order of which they were created */
	doca_error_t result = DOCA_SUCCESS;
	doca_error_t temp;

	/* Clean up the pipelines first */
	const auto views = pipelines.all();
	for (auto& pipeline : std::views::join(views)) {
		if (pipeline.prog_eng != nullptr) {
			temp = doca_pe_destroy(pipeline.prog_eng);
			if (temp != DOCA_SUCCESS) {
				DOCA_ERROR_PROPAGATE(result, temp);
				DOCA_LOG_ERR("Failed to destroy progress engine: %s", doca_error_get_descr(temp));
			}
			pipeline.prog_eng = nullptr;
		}
		if (pipeline.buf_inv != nullptr) {
			temp = doca_buf_inventory_destroy(pipeline.buf_inv);
			if (temp != DOCA_SUCCESS) {
				DOCA_ERROR_PROPAGATE(result, temp);
				DOCA_LOG_ERR("Failed to destroy buffer inventory: %s", doca_error_get_descr(temp));
			}
			pipeline.buf_inv = nullptr;
		}
	}

	/* Destroy all the memory maps created */
	if (remote_mmap != nullptr) {
		temp = doca_mmap_destroy(remote_mmap);
		if (temp != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, temp);
			DOCA_LOG_ERR("Failed to destroy remote mmap: %s", doca_error_get_descr(temp));
		}
		remote_mmap = nullptr;
	}
	if (scratch_mmap != nullptr) {
		temp = doca_mmap_destroy(scratch_mmap);
		if (temp != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, temp);
			DOCA_LOG_ERR("Failed to destroy staging mmap: %s", doca_error_get_descr(temp));
		}
		scratch_mmap = nullptr;
	}
	if (local_mmap != nullptr) {
		temp = doca_mmap_destroy(local_mmap);
		if (temp != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, temp);
			DOCA_LOG_ERR("Failed to destroy local mmap: %s", doca_error_get_descr(temp));
		}
		local_mmap = nullptr;
	}

	/* Finally, close the device */
	if (device != nullptr) {
		temp = doca_dev_close(device);
		if (temp != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, temp);
			DOCA_LOG_ERR("Failed to close device: %s", doca_error_get_descr(temp));
		}
		device = nullptr;
	}

	return result;
}

doca_error_t allocate_doca_buffers(
	doca_buf_inventory* buf_inv, doca_mmap* mmap, char* buf_addr, const size_t buf_len, const size_t num_buf, std::vector<doca_buf*>& buf_list
) {
	constexpr doca_error_t result = DOCA_ERROR_INVALID_VALUE;
	if (buf_inv == nullptr) {
		DOCA_LOG_ERR("Invalid value found, doca_buf_inventory is nullptr: %s", doca_error_get_descr(result));
		return result;
	}
	if (mmap == nullptr) {
		DOCA_LOG_ERR("Invalid value found, doca_mmap is nullptr: %s", doca_error_get_descr(result));
		return result;
	}
	if (buf_addr == nullptr) {
		DOCA_LOG_ERR("Invalid value found, buf_addr is nullptr: %s", doca_error_get_descr(result));
		return result;
	}
	if (buf_len == 0) {
		DOCA_LOG_ERR("Invalid value found, buf_len is 0: %s", doca_error_get_descr(result));
		return result;
	}
	if (num_buf == 0) {
		DOCA_LOG_ERR("Invalid value found, num_buf is 0: %s", doca_error_get_descr(result));
		return result;
	}
	if (!buf_list.empty()) {
		DOCA_LOG_ERR("Invalid value found, buf_list is not empty: %s", doca_error_get_descr(result));
		return result;
	}
	buf_list.resize(num_buf);

	// Initialize all buffers in the pool to cover the entire underlying memory map range
	for (auto*& doca_buf : buf_list) {
		DOCA_TRY(
			doca_buf_inventory_buf_get_by_addr(buf_inv, mmap, buf_addr, buf_len, &doca_buf),
			"Unable to acquire DOCA buffer"
		);
	}

	return DOCA_SUCCESS;
}

}
