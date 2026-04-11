//
// Created by Felix Zhang on 2/16/26.
//

#pragma once

#include <doca_dev.h>

#ifdef DOCA_ARCH_DPU
constexpr char PCIE_ADDRESS[] = "0000:03:00.1";
constexpr uint8_t GID = 1;
#else
constexpr char PCIE_ADDRESS[] = "0000:21:00.1";
constexpr uint8_t GID = 3;
#endif
