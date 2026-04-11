//
// Created by Felix Zhang on 2025-09-10.
//

#pragma once

#include <deque>

#include "../IBackend.h"
#include "utils/debug.h"

namespace fdl {

// TODO: update to use refactored buffer pool
class HostMemoryHandle final : public IHandle<HostMemoryHandle> {
public:
    explicit HostMemoryHandle(std::vector<MemoryBuffer*>&& buffers) : buffers_(std::move(buffers)) {
        chunks_.resize(buffers_.size());
        for (auto& vec : chunks_) {
            vec.push_back(0);
        }
        mutexes_.resize(buffers_.size());
    }
    ~HostMemoryHandle() {
        if (!buffers_.empty()) {
            FDL_WARN("[HostMemoryHandle] Buffers were not released before destruction");
            BufferPools::memory().release(std::move(buffers_));
        }
    }

    /* Disable copying & define moving */
    HostMemoryHandle(const HostMemoryHandle&) = delete;
    HostMemoryHandle& operator=(const HostMemoryHandle&) = delete;
    HostMemoryHandle(HostMemoryHandle&& other) noexcept :
        buffers_(std::move(other.buffers_)), chunks_(std::move(other.chunks_)), mutexes_(std::move(other.mutexes_))
    {}
    HostMemoryHandle& operator=(HostMemoryHandle&& other) noexcept {
        if (this != &other) {
            buffers_ = std::move(other.buffers_);
            chunks_ = std::move(other.chunks_);
            mutexes_ = std::move(other.mutexes_);
        }
        return *this;
    }

    bool write(size_t index, const UnifiedHandle& handle);
    std::optional<UnifiedHandle> read(size_t index, UnifiedBuffer& partition, size_t start);

    /* Define getters */
    size_t size() const { return buffers_.size(); }

private:
    friend class HostMemoryBackend;

    std::vector<MemoryBuffer*> buffers_;
    std::vector<std::vector<size_t>> chunks_;
    std::deque<std::mutex> mutexes_;
};

class HostMemoryBackend final : public IBackend<HostMemoryBackend, HostMemoryHandle> {
public:
    /* Delete copy & move operators */
    HostMemoryBackend(const HostMemoryBackend& other) = delete;
    HostMemoryBackend& operator=(const HostMemoryBackend& other) = delete;

    int start(const bool resilient) {
        BufferPools::memory();
        return 0;
    }
    int stop() { return 0; }

    HostMemoryHandle acquire(size_t n);
    void release(HostMemoryHandle&& handle);

private:
    friend class IBackend;

    HostMemoryBackend() = default;
};

}
