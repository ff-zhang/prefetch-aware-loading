//
// Created by Felix Zhang on 12/15/25.
//

#pragma once

#include "../doca.h"

namespace fdl::dma {

/*
 * Allocate DOCA DMA host resources
 *
 * @pcie_addr [in]: PCIe address of device to open
 * @state [out]: Structure containing all DOCA core structures
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t allocate_host_resources(core_doca_objects& state);

/*
 * Destroy DOCA DMA host resources
 *
 * @state [in]: Structure containing all DOCA core structures
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t destroy_host_resources(core_doca_objects& state);

}
