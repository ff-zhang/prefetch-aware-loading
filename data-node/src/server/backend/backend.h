//
// Created by Felix Zhang on 2025-09-19.
//

#pragma once

#ifdef DOCA_ENABLED
#include "DPU/DMA/memory.h"
#else
#include "host/memory.h"
#endif

namespace fdl {

#ifdef DOCA_ENABLED
    using Backend = DMAMemoryBackend;
    using BackendHandle = DMAMemoryHandle;
#else
    using Backend = HostMemoryBackend;
    using BackendHandle = HostMemoryHandle;
#endif

static auto& backend() {
    return Backend::instance();
}

}
