//
// Created by Felix Zhang on 11/4/25.
//

#pragma once

#include <array>
#include <cstdint>

#include <doca_dev.h>

constexpr size_t MAX_USER_ARG_SIZE = 256;               /* Maximum size of user input argument */
constexpr size_t MAX_ARG_SIZE = MAX_USER_ARG_SIZE + 1;  /* Maximum size of input argument */
constexpr size_t MAX_USER_TXT_SIZE = 4096;              /* Maximum size of user input text */
constexpr size_t MAX_TXT_SIZE = MAX_USER_TXT_SIZE + 1;  /* Maximum size of input text */

constexpr size_t RECV_BUF_SIZE = 512;	/* Size of buffer which contains config information */
constexpr size_t NUM_DMA_TASKS = 256;	/* Number of memcpy tasks that the DMA can allocate */
constexpr size_t NUM_EC_TASKS = 128;	/* Number of create tasks that the EC can allocate */

constexpr size_t SLEEP_NS = 1 * 1000;   /* Sample DOCA tasks every 1 microsecond(s)  */

/* Configuration struct */
struct dma_config {
    char export_desc_path[MAX_ARG_SIZE];    /* Path to save/read the exported descriptor file */
    char buf_info_path[MAX_ARG_SIZE];       /* Path to save/read the buffer information file */
};

/* Set the default configuration values */
struct doca_config {
    char pcie_address[DOCA_DEVINFO_PCI_ADDR_SIZE];        /* PCIe device address */
    dma_config dma;
};

/* Set the default configuration values */
constexpr doca_config DOCA_CONF = {
#ifdef DOCA_ARCH_DPU
    .pcie_address = "0000:03:00.0",
#else
    .pcie_address = "0000:21:00.0",
#endif
    .dma = { .export_desc_path = "/tmp/export_desc.txt", .buf_info_path = "/tmp/buffer_info.txt" },
};