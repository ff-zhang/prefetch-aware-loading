//
// Created by Felix Zhang on 4/6/26.
//

#include <filesystem>

#include "device.h"

namespace fdl::rdma {

static bool pci_addr_matches(ibv_device* device, const char* pci_addr) {
    const char* name = ibv_get_device_name(device);
    const auto path  = std::filesystem::path("/sys/class/infiniband") / name / "device";
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) return false;
    const auto full = std::filesystem::canonical(path, ec).string();
    return full.find(pci_addr) != std::string::npos;
}

static bool capability_is_supported(ibv_context* context) {
    ibv_device_attr_ex attr{};
    if (ibv_query_device_ex(context, nullptr, &attr)) return false;
    return attr.orig_attr.atomic_cap == IBV_ATOMIC_HCA;
}

Device::~Device() {
    if (pd_) { ibv_dealloc_pd(pd_); }
    if (ctx_) { ibv_close_device(ctx_); }
}

int Device::open() {
    std::scoped_lock lock(mutex_);
    if (ctx_) return 0;

    int num_devices = 0;
    ibv_device** dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices == 0) {
        FDL_WARN("[Device] no IB devices found");
        ibv_free_device_list(dev_list);
        return -1;
    }

    for (int i = 0; i < num_devices; ++i) {
        ibv_context* ctx = ibv_open_device(dev_list[i]);
        if (!ctx) continue;

        ibv_device_attr attr{};
        if (ibv_query_device(ctx, &attr)) {
            ibv_close_device(ctx);
            continue;
        }

        if (!capability_is_supported(ctx) ||
            !pci_addr_matches(dev_list[i], PCIE_ADDRESS) ||
            attr.phys_port_cnt == 255) {
            ibv_close_device(ctx);
            continue;
        }

        ibv_pd* pd = ibv_alloc_pd(ctx);
        if (!pd) {
            FDL_WARN("[Device] ibv_alloc_pd failed: %s", strerror(errno));
            ibv_close_device(ctx);
            ibv_free_device_list(dev_list);
            return -1;
        }

        ctx_ = ctx;
        pd_ = pd;
        attr_ = attr;
        FDL_LOG("[Device] opened device '%s'", ibv_get_device_name(dev_list[i]));
        ibv_free_device_list(dev_list);
        return 0;
    }

    FDL_WARN("[Device] no device matched PCI address %s", PCIE_ADDRESS);
    ibv_free_device_list(dev_list);
    return -1;
}

uint8_t Device::next_port_num() {
    return (port_counter_.fetch_add(1, std::memory_order_relaxed) % attr_.phys_port_cnt) + 1;
}

}