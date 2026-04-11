//
// Created by Felix Zhang on 2025-09-10.
//

#pragma once

#include <optional>

#include "utils/buffer_pool.h"

namespace fdl {

// Forward declarations
class DMAMemoryBackend;
class HostMemoryBackend;

template <typename Handle>
class IHandle {
public:
    bool write(const size_t index, const UnifiedHandle& handle) {
        return static_cast<Handle*>(this)->write(index, handle);
    }
    std::optional<UnifiedHandle> read(const size_t index, UnifiedBuffer& partition, const size_t start) {
        return static_cast<Handle*>(this)->read(index, partition, start);
    }

    size_t size() const { return static_cast<Handle*>(this)->size(); }
};

template <typename Backend, typename Handle>
class IBackend {
public:
    static Backend& instance() {
        static Backend instance;
        return instance;
    }

    int start(const bool resilient) { return static_cast<Backend*>(this)->start(resilient); }
    int stop() { return static_cast<Backend*>(this)->stop(); }

    Handle acquire(const size_t n) { return static_cast<Backend*>(this)->acquire(n); }
    void release(Handle&& handle) { return static_cast<Backend*>(this)->release(std::move(handle)); }
};

}
