//
// Created by feFelix Zhang on 12/15/25.
//

#include <doca_mmap.h>

#include "remote.h"

DOCA_LOG_REGISTER(SERVER_PIPELINES_DPU_DMA_HOST)

namespace fdl::dma {

doca_error_t allocate_host_resources(core_doca_objects& state) {
    DOCA_TRY(doca_mmap_create(&state.remote_mmap), "Failed to create mmap");
    DOCA_TRY(doca_mmap_add_dev(state.remote_mmap, state.device), "Failed to add device to mmap");

    return DOCA_SUCCESS;
};

doca_error_t destroy_host_resources(core_doca_objects& state) {
    doca_error_t result = DOCA_SUCCESS;
    if (state.remote_mmap != nullptr) {
        result = doca_mmap_destroy(state.remote_mmap);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to destroy DOCA mmap: %s", doca_error_get_descr(result));
        }
        state.remote_mmap = nullptr;
    }

    return result;
};

}
