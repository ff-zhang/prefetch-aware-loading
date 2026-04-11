//
// Created by joz on 2/16/26.
//

#pragma once
#include <cstdint>

#include "server/backend/IBackend.h"

namespace fdl {
    class SPDKHandle final : public IHandle<SPDKHandle> {
    public:
        SPDKHandle(std::vector<void *> &&buffers, std::vector<size_t> &&buffer_indices) :
            buffers_(std::move(buffers)), buffer_indices_(std::move(buffer_indices)) {}

        ~SPDKHandle() {
            if (!buffers_.empty()) {
                FDL_WARN("[SPDKHandle] Buffers were not released before destruction");
            }
        }
        // Move-only semantics
        SPDKHandle(SPDKHandle &&) = default;
        SPDKHandle &operator=(SPDKHandle &&) = default;
        SPDKHandle(const SPDKHandle &) = delete;
        SPDKHandle &operator=(const SPDKHandle &) = delete;

        bool write(size_t index, const UnifiedHandle &handle);
        std::optional<UnifiedHandle> read(size_t index, UnifiedBuffer &partition, size_t start);

        [[nodiscard]] size_t size() const { return buffers_.size(); }

    private:
        friend class SPDKBackend;
        std::vector<void *> buffers_; // SPDK DMA buffers
        std::vector<size_t> buffer_indices_; // Index into pipeline metadata
    };

    class SPDKBackend final : public IBackend<SPDKBackend, SPDKHandle> {
    public:
        /* Disable copying & moving */
        SPDKBackend(const SPDKBackend &) = delete;
        SPDKBackend &operator=(const SPDKBackend &) = delete;
        SPDKBackend(SPDKBackend &&other) = delete;
        SPDKBackend &operator=(SPDKBackend &&other) = delete;

        int start(bool resilient);
        int stop();

        SPDKHandle acquire(size_t n);
        void release(SPDKHandle &&handle);

        // Register the calling thread with SPDK (must be called before I/O)
        static bool register_thread(const char *name = nullptr);

        // Unregister the calling thread from SPDK
        static void unregister_thread();

    private:
        friend class IBackend;

        SPDKBackend() = default;

        // Buffer entry with pointer and metadata index
        struct BufferEntry {
            void *buffer;
            size_t index;
        };

        struct {
            ConcurrentQueue<BufferEntry> buffers{HOST_BUFFER_COUNT};
        } available_;
    };
} // namespace fdl
