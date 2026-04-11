//
// Created by Felix Zhang on 2025-09-10.
//

#include <mutex>

#include "memory.h"
#include "utils/buffer_pool.h"

namespace fdl {

bool HostMemoryHandle::write(const size_t index, const UnifiedHandle& handle) {
    std::visit(overloaded{
        [&](const UnifiedRegion& region) -> void {
            {
                std::unique_lock lock(mutexes_[index]);
                chunks_[index].push_back(chunks_[index].back() + region.size);
            }
            buffers_[index]->produce(region.data, region.size);
        },
        [&](const std::array<UnifiedRegion, 2>& pair) -> void {
            {
                std::unique_lock lock(mutexes_[index]);
                chunks_[index].push_back(chunks_[index].back() + pair[0].size + pair[1].size);
            }
            buffers_[index]->produce(pair[0].data, pair[1].data, pair[0].size, pair[1].size);
        }
    }, handle.regions);

    return true;
}

std::optional<UnifiedHandle> HostMemoryHandle::read(const size_t index, UnifiedBuffer& partition, const size_t start) {
    size_t consume_index, size;
    {
        std::unique_lock lock(mutexes_[index]);
        if (start + 1 >= chunks_[index].size()) return std::nullopt;
        consume_index = chunks_[index][start];
        size = chunks_[index][start + 1] - consume_index;
    }

    auto [data, len] = buffers_[index]->read(consume_index);
    if (len < size) { return std::nullopt; }

    std::optional<UnifiedHandle> handle = std::nullopt;
    while (!handle.has_value()) { handle = partition.acquire_produce(size); }

    std::visit(overloaded{
        [&](const UnifiedRegion& region) -> void {
            std::memcpy(region.data, data, region.size);
        },
        [&](const std::array<UnifiedRegion, 2>& pair) -> void {
            std::memcpy(pair[0].data, data, pair[0].size);
            std::memcpy(pair[1].data, data + pair[0].size, pair[1].size);
        }
    }, handle.value().regions);

    return handle;
}

HostMemoryHandle HostMemoryBackend::acquire(const size_t n) {
    return HostMemoryHandle(BufferPools::memory().acquire(n));
}

void HostMemoryBackend::release(HostMemoryHandle&& handle) {
    BufferPools::memory().release(std::move(handle.buffers_));
    handle.buffers_.clear();
}

}
