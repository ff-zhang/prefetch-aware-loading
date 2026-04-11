//
// Created by Felix Zhang on 11/4/25.
//

#include <csignal>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

#include <doca_argp.h>
#include <doca_dma.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_mmap.h>

#include "pipelines/DOCA/config.h"
#include "pipelines/DOCA/DMA/dma.h"
#include "pipelines/DOCA/DMA/remote.h"
#include "utils/parameters/constants.h"
#include "utils/parameters/configuration.h"

DOCA_LOG_REGISTER(SERVER_DMA_HOST);

/*
 * Saves export descriptor and buffer information into two separate files
 *
 * @export_desc [in]: Export descriptor to write into a file
 * @export_desc_len [in]: Export descriptor length
 * @src_buffer [in]: Source buffer
 * @src_buffer_len [in]: Source buffer length
 * @export_desc_file_path [in]: Export descriptor file path
 * @buffer_info_file_path [in]: Buffer information file path
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t save_config_to_files(
	const char* export_desc_file_path,
	const char* buffer_info_file_path,
	const char* export_desc,
	const size_t export_desc_len,
	const char* src_buffer,
	const size_t src_buffer_len
) {
	/* Write export descriptor to file */
	std::ofstream file(export_desc_file_path, std::ios::out | std::ios::binary);
	if (!file.is_open()) {
		DOCA_LOG_ERR("Failed to create the DMA copy file");
		return DOCA_ERROR_IO_FAILED;
	}
	if (!file.write(export_desc, static_cast<std::streamsize>(export_desc_len))) {
		DOCA_LOG_ERR("Failed to write all data into the file");
		return DOCA_ERROR_IO_FAILED;
	}
	file.close();

	/* Save buffer information to file */
	file.open(buffer_info_file_path, std::ios::out);
	if (!file.is_open()) {
		DOCA_LOG_ERR("Failed to create the DMA copy file");
		return DOCA_ERROR_IO_FAILED;
	}
	file << static_cast<uint64_t>(reinterpret_cast<uintptr_t>(src_buffer)) << "\n" << static_cast<uint64_t>(src_buffer_len);
	file.close();

	return DOCA_SUCCESS;
}

/*
 * Run DOCA DMA Host copy sample
 *
 * @pcie_addr [in]: Device PCI address
 * @src_buffer [in]: Source buffer to copy
 * @src_buffer_len [in]: Buffer size
 * @export_desc_file_path [in]: Export descriptor file path
 * @buffer_info_file_name [in]: Buffer info file path
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t start_dma_host(
	const char* pcie_addr, char* src_buffer, const size_t src_buffer_len, char* export_desc_file_path, char* buffer_info_file_name
) {
    auto& state = fdl::core_doca_objects::instance();

	const void* export_desc;
	size_t export_desc_len;

	/* Allocate resources */
	DOCA_TRY(state.open_device(pcie_addr, &fdl::dma::capability_is_supported), "Failed to open DOCA device for DMA");
	DOCA_TRY(fdl::dma::allocate_host_resources(state), "Failed to allocate DMA host resources");

	/* Allow exporting the mmap to DPU for read and write operations */
	constexpr int flags =
		DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_RDMA_READ | DOCA_ACCESS_FLAG_RDMA_WRITE | DOCA_ACCESS_FLAG_RDMA_ATOMIC | DOCA_ACCESS_FLAG_PCI_READ_WRITE;
	DOCA_TRY(
		doca_mmap_set_permissions(state.remote_mmap, flags),
		"Failed to set mmap permissions"
	);

	/* Populate the memory map with the allocated memory */
	DOCA_TRY(
		doca_mmap_set_memrange(state.remote_mmap, src_buffer, src_buffer_len),
		"Failed to set memory range for source mmap"
	);

	DOCA_TRY(doca_mmap_start(state.remote_mmap), "Failed to start source mmap");

	/* Export DOCA mmap to enable DMA on Host*/
	DOCA_TRY(
		doca_mmap_export_pci(state.remote_mmap, state.device, &export_desc, &export_desc_len),
		"Failed to start export source mmap"
	);

	DOCA_LOG_INFO(
		"Please copy %s and %s to the DPU", export_desc_file_path, buffer_info_file_name
	);

	/* Saves the export desc and buffer info to files, it is the user responsibility to transfer them to the dpu */
	DOCA_TRY(
		save_config_to_files(export_desc_file_path, buffer_info_file_name, static_cast<const char*>(export_desc), export_desc_len, src_buffer, src_buffer_len),
		"Failed to save configuration information"
	);

	return DOCA_SUCCESS;
}

/*
 * Sample main function
 *
 * @argc [in]: command line arguments size
 * @argv [in]: array of command line arguments
 * @return: EXIT_SUCCESS on success and EXIT_FAILURE otherwise
 */
int main(int argc, char** argv) {
	(void) argc;
	(void) argv;

	/* Set the default configuration values */
	doca_config doca_conf = {
		.pcie_address = "21:00.0",
		.dma = { .export_desc_path = "/tmp/export_desc.txt", .buf_info_path = "/tmp/buffer_info.txt" },
	};

    auto& state = fdl::core_doca_objects::instance();
	doca_error_t result = state.register_logger();
	if (result != DOCA_SUCCESS) {
		return EXIT_FAILURE;
	}

#ifndef DOCA_ARCH_HOST
	DOCA_LOG_ERR("DMA host can only run on the host");
	return EXIT_FAILURE;
#endif

	std::cout << "Starting the host" << std::endl;

	constexpr size_t len = 2 * fdl::HOST_BUFFER_COUNT * fdl::HOST_BUFFER_SIZE;
	const auto remote_buf = new(std::align_val_t{ 4 * KiB }) char[len];

	result = start_dma_host(
		doca_conf.pcie_address, remote_buf, len, doca_conf.dma.export_desc_path, doca_conf.dma.buf_info_path
	);
	if (result != DOCA_SUCCESS) {
		doca_error_t temp = fdl::dma::destroy_host_resources(state);
		if (temp != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, temp);
			DOCA_LOG_ERR("Failed to destroy DMA host resources: %s", doca_error_get_descr(temp));
		}
		temp = state.terminate();
		if (temp != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, temp);
			DOCA_LOG_ERR("Failed to destroy DOCA core objects: %s", doca_error_get_descr(temp));
		}
		::operator delete(remote_buf, std::align_val_t{ 4 * KiB });

		return EXIT_FAILURE;
	}

	/* Wait for enter which means that the requester has finished reading */
	std::cout << "Wait till the DPU has finished and press enter" << std::endl;
	int enter = 0;
	while (enter != '\r' && enter != '\n') { enter = getchar(); }

	result = fdl::dma::destroy_host_resources(state);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to destroy DMA host resources: %s", doca_error_get_descr(result));
	}
	const doca_error_t temp = state.terminate();
	if (temp != DOCA_SUCCESS) {
		DOCA_ERROR_PROPAGATE(result, temp);
		DOCA_LOG_ERR("Failed to destroy DOCA core objects: %s", doca_error_get_descr(temp));
	}
	::operator delete(remote_buf, std::align_val_t{ 64 });

	std::cout << "Shut down host successfully" << std::endl;

	return EXIT_SUCCESS;
}