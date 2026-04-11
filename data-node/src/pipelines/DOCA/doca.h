//
// Created by Felix Zhang on 10/5/25.
//

#pragma once

#include <functional>
#include <utility>

#include <doca_argp.h>
#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_pe.h>

#include "utils/debug.h"
#include "utils/types.h"

#define DOCA_TRY(expr, msg) \
    do { \
        doca_error_t result = (expr); \
        if (result != DOCA_SUCCESS) { \
            FDL_WARN("%s: %s", (msg), doca_error_get_descr(result)); \
            return result; \
        } \
    } while (0)

namespace fdl {

struct pipeline_doca_objects {
    doca_buf_inventory* buf_inv = nullptr;  /* DOCA buffer inventory */
    doca_ctx* doca_context = nullptr;       /* DOCA context */
    doca_pe* prog_eng = nullptr;            /* DOCA progress engine */
};

/* DOCA core objects used within the server */
class core_doca_objects {
public:
    static core_doca_objects& instance() {
        static core_doca_objects instance;
        return instance;
    }

    /* Disable copying & enable moving */
    core_doca_objects(const core_doca_objects& other) = delete;
    core_doca_objects& operator=(const core_doca_objects& other) = delete;
    core_doca_objects(core_doca_objects&& other) noexcept :
        log(std::exchange(other.log, nullptr)),
        device(std::exchange(other.device, nullptr)),
        local_mmap(std::exchange(other.local_mmap, nullptr)),
        scratch_mmap(std::exchange(other.scratch_mmap, nullptr)),
        remote_mmap(std::exchange(other.remote_mmap, nullptr)),
        pipelines(std::move(other.pipelines))
    {}
    core_doca_objects& operator=(core_doca_objects&& other) noexcept {
        if (this != &other) {
            log = std::exchange(other.log, nullptr);
            device = std::exchange(other.device, nullptr);
            local_mmap = std::exchange(other.local_mmap, nullptr);
            scratch_mmap = std::exchange(other.scratch_mmap, nullptr);
            remote_mmap = std::exchange(other.remote_mmap, nullptr);
            pipelines = std::move(other.pipelines);
        }
        return *this;
    }

    doca_error_t register_logger();

    /*
     * Open a DOCA device according to a given PCI address
     *
     * @pcie_addr [in]: PCIe address
     * @check_device_capabilities [in]: function that checks if the device have some (task) capabilities
     * @device [out]: pointer to doca_dev struct, NULL if not found
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t open_device(
        const char* pcie_addr, const std::function<doca_error_t(doca_devinfo*)>& check_device_capabilities
    );

    /*
     * Initialize a series of DOCA Core objects needed for the program's execution
     *
     * @state [in]: struct containing the set of initialized DOCA Core objects
     * @max_bufs [in]: maximum number of buffers for DOCA Inventory
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t initialize(size_t max_num_buf, bool dma, bool ec);

    doca_error_t create_local_mmap(doca_mmap*& mmap, char* addr, size_t len) const;

    /*
     * Clean up the series of DOCA Core objects created during initialization
     *
     * @state [in]: struct containing the set of initialized DOCA Core objects
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t terminate();

    doca_log_backend* log = nullptr;
    doca_dev* device = nullptr;         /* DOCA (PCI) device */
    doca_mmap* local_mmap = nullptr;    /* DOCA memory mapping for local buffers */
    doca_mmap* scratch_mmap = nullptr;  /* DOCA memory mapping for staging buffers */
    doca_mmap* remote_mmap = nullptr;   /* DOCA memory mapping for remote buffers */

    struct {
        std::vector<pipeline_doca_objects> dma{};
        std::vector<pipeline_doca_objects> ec{};
        std::vector<pipeline_doca_objects> rdma{};
        std::vector<pipeline_doca_objects> spdk{};

        auto all() {
            return std::array{
                std::views::all(dma), std::views::all(ec), std::views::all(rdma), std::views::all(spdk)
            };
        }
    } pipelines = {};

private:
    core_doca_objects() = default;
    ~core_doca_objects() {
        terminate();
    }
};

/*
 * Allocate a linked list of DOCA buffers from a contiguous memory region
 *
 * @buf_inv [in]: DOCA buffer inventory used to acquire DOCA buffers
 * @mmap [in]: DOCA memory map associated with the target buffer region
 * @buf_addr [in]: Starting address of the contiguous memory region to segment
 * @buf_len [in]: Total length (in bytes) of the buffer region
 * @num_buf [in]: Number of buffers to divide the region into
 * @set_data_pos [in]: Flag indicating whether to set the data pointer for each buffer
 * @dbuf [out]: Pointer to the head of the resulting DOCA buffer chain
 *
 * @return: DOCA_SUCCESS on success, or an appropriate DOCA_ERROR code on failure
 */
doca_error_t allocate_doca_buffers(
    doca_buf_inventory* buf_inv, doca_mmap* mmap, char* buf_addr, size_t buf_len, size_t num_buf, std::vector<doca_buf*>& buf_list
);

}
