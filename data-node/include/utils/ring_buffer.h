//
// Created by Felix Zhang on 10/16/25.
//

#pragma once

#include <atomic>
#include <bit>
#include <optional>
#include <span>
#include <utility>
#include <variant>

#include "debug.h"
#include "parameters/configuration.h"
#include "pause.h"
#include "types.h"

namespace fdl {

enum opcode_t { SET_DATA, SET_META, SET_BOTH, CLEAR };

struct RingBufferRegion {
    char* data;
    uint32_t size;

private:
    template<std::uint32_t N, std::uint32_t M>
    friend class RingBuffer;

    template<std::uint32_t N>
    friend class RingBufferHandle;

    RingBufferRegion(char* data, const uint32_t size, const uint32_t start_index) : data(data), size(size), start_index(start_index) {};

    uint32_t start_index;
};

template<std::uint32_t N>
class RingBufferHandle {
public:
    /* Diable copying & enable moving */
    RingBufferHandle(const RingBufferHandle&) = delete;
    RingBufferHandle& operator=(const RingBufferHandle&) = delete;
    RingBufferHandle(RingBufferHandle&&) = default;
    RingBufferHandle& operator=(RingBufferHandle&&) = default;

    using regions_t = std::variant<RingBufferRegion, std::array<RingBufferRegion, 2>>;
    regions_t regions;

    std::variant<std::span<uint64_t>, std::array<std::span<uint64_t>, 2>> metadata;

    uint32_t m_index;
    uint32_t m_size;

private:
    template<std::uint32_t N_, std::uint32_t M_>
    friend class RingBuffer;

    RingBufferHandle(
        const bool is_produce, const bool is_consume, char* b_data, const uint32_t b_index, const uint32_t b_size, uint64_t* m_data, const uint32_t m_index, const uint32_t m_size
    ) : regions(
            b_index < (b_index + b_size) % N || b_index + b_size == N
                ? regions_t(RingBufferRegion(b_data + b_index, b_size, b_index))
                : regions_t(std::array{
                    RingBufferRegion(b_data + b_index, N - b_index, b_index), RingBufferRegion(b_data, b_size + b_index - N, 0)
                })
        ),
        metadata(
            m_index < (m_index + m_size) % N || m_index + m_size == N
                ? std::variant<std::span<uint64_t>, std::array<std::span<uint64_t>, 2>>(std::span(m_data + m_index, m_size))
                : std::variant<std::span<uint64_t>, std::array<std::span<uint64_t>, 2>>(std::array{
                    std::span(m_data + m_index, N - m_index), std::span(m_data, m_size + m_index - N)
                })
        ),
        m_index(m_index), m_size(m_size), is_produce(is_produce), is_consume(is_consume)
    {}

    bool is_produce;
    bool is_consume;
};

/*
 * This is the core ring buffer used to send and receive data between servers (using RDMA).
 * As such, it is designed to be viewed and updated remotely.
 * To provide I/O-tization, we separate data items and their metadata (i.e. sizes) into distinct buffers.
 *
 * The process to produce a data item is:
 *     1. check there is room in the data and metadata buffers (i.e. between the produce tail and consume head)
 *     2. atomically update the produce tail, acquiring room in the data buffer and slot in the metadata buffer
 *     3. write the data into the data buffer
 *     4. atomically write the item size into the acquired metadata slot and set the (packed) consume bit to 1
 * The final step signals that the data item has been written and can now be consumed.
 *
 * The process to consume data is:
 *     1. check the slot in the metadata buffer at the consume tail is non-zero
 *     2. atomically update the consume tail, acquiring a data item
 *     3. read the item from the ring buffer
 *     4. atomically set the consume bit in the metadata slot to 0
 *     5. point the consume head to the first item being consumed
 *
 * By design, this ring buffer can never be full
 */
template<std::uint32_t N, std::uint32_t M>
class RingBuffer {
public:
    static constexpr uint8_t PADDING_OFFSET = 31;

    static constexpr uint64_t CONSUME_BIT = static_cast<uint64_t>(0b1) << 63;
    // static constexpr uint64_t PROD_CONS_BIT = static_cast<uint64_t>(0b1) << 62;
    static constexpr uint64_t PADDING_SPAN = static_cast<uint64_t>(0x3FFFFFFF) << PADDING_OFFSET;
    static constexpr uint64_t SIZE_SPAN = 0xFFFFFFFF;

    struct HostIndex {
        uint32_t b_index;
        uint32_t m_index;

        bool operator==(const HostIndex& other) const {
            return this->b_index == other.b_index && this->m_index == other.m_index;
        }
        bool operator!=(const HostIndex& other) const {
            return !(*this == other);
        }

        HostIndex operator+(const HostIndex& other) const {
            return HostIndex{(this->b_index + other.b_index) % N, (this->m_index + other.m_index) % M};
        }
        HostIndex& operator+=(const HostIndex& other) {
            this->b_index = (this->b_index + other.b_index) % N;
            this->m_index = (this->m_index + other.m_index) % M;
            return *this;
        }

        explicit operator uint64_t() const {
            return std::bit_cast<uint64_t>(*this);
        }
    };
    static_assert(sizeof(HostIndex) == sizeof(uint64_t));
    static_assert(std::is_trivially_copyable_v<HostIndex>);

    /*
     * This is stored in network-byte order to enable RDMA atomic operations
     */
    class NetworkIndex {
    public:
        NetworkIndex() = default;

        explicit NetworkIndex(const HostIndex& index) :
            data_(htobe64(std::bit_cast<uint64_t>(index)))
        {}

        bool operator==(const NetworkIndex& other) const {
            return this->data_ == other.data_;
        }
        bool operator!=(const NetworkIndex& other) const {
            return !(*this == other);
        }

        HostIndex n64toh() const {
            // This assumes the host is a little-endian machine
            return std::bit_cast<HostIndex>(be64toh(data_));
        }

    private:
        uint64_t data_;
    };
    static_assert(sizeof(NetworkIndex) == sizeof(uint64_t));
    static_assert(std::is_trivially_copyable_v<NetworkIndex>);
    static_assert(std::atomic<NetworkIndex>::is_always_lock_free); // GCC is bad 🤣

    struct alignas(64) State {
        enum kind_t { METADATA, PROD_TAILS, CONS_HEADS, CONS_TAILS };

        std::array<std::atomic<uint64_t>, M> metadata_{};

        alignas(64) std::atomic<NetworkIndex> produce_tails_{};  // First index at which data can be produced to
        alignas(64) std::atomic<NetworkIndex> consume_heads_{}; // First index at which data can be consumed from
        alignas(64) std::atomic<NetworkIndex> consume_tails_{}; // First index where data is currently being consumed
    };

    RingBuffer() = default;

    /* Disable copying & moving */
    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&& other) = delete;
    RingBuffer& operator=(RingBuffer&& other) = delete;

    void initialize(char* data) {
        assert(data_ == nullptr && "Buffer has been previously set");
        data_ = data;

        state_ = reinterpret_cast<State*>(data + capacity());
        memset(state_, 0, sizeof(State));
    }

    /**
     * Acquires room in the buffer to produce data into
     */
    std::optional<RingBufferHandle<N>> acquire_produce(const uint32_t size, const uint32_t block = 1) {
        if (size == 0 || size >= N) {
            return std::nullopt;
        }

        NetworkIndex network_tails = state_->produce_tails_.load(std::memory_order_acquire);
        HostIndex produce_tails;

        // Update the producer index
        uint64_t padding = 0;
        HostIndex offset;
        do {
            produce_tails = network_tails.n64toh();
            HostIndex consume_heads = state_->consume_heads_.load(std::memory_order_acquire).n64toh();

            // Check if there is enough space available
            auto [b_size, m_size] = get_producible(produce_tails, consume_heads);
            if (b_size <= size || m_size <= 1) {
                update_pointers();
                return std::nullopt;
            }

            // If block size is provided, account for the potential padding needed
            if (block <= 1 || produce_tails.b_index < consume_heads.b_index) {
                offset = HostIndex{size, 1};
            } else {
                const uint32_t count = (N - produce_tails.b_index) / block;
                padding = (N - produce_tails.b_index) - count * block;
                offset = HostIndex{size + static_cast<uint32_t>(padding), 1};
            }
        } while (!state_->produce_tails_.compare_exchange_weak(
            network_tails,   // Updated on failure
            NetworkIndex(produce_tails + offset),
            std::memory_order_acq_rel,
            std::memory_order_relaxed
        ));

        uint64_t slot = size;
        if (padding) {
            assert(padding < PADDING_SPAN >> PADDING_OFFSET);
            slot |= padding << PADDING_OFFSET;
        }
        state_->metadata_[produce_tails.m_index].store(slot, std::memory_order_release);

        // Return pointer(s) to where data should be written to
        auto m_data = reinterpret_cast<uint64_t*>(state_->metadata_.data());
        return RingBufferHandle<N>(
            true, false, data_, produce_tails.b_index, size, m_data, produce_tails.m_index, 1
        );
    }

    /**
     * Claims data in the buffer to be consumed
     */
    [[nodiscard]] std::optional<RingBufferHandle<N>> acquire_consume() {
        NetworkIndex network_tails = state_->consume_tails_.load(std::memory_order_acquire);
        HostIndex consume_tails = network_tails.n64toh();
        std::array<uint32_t, 3> size;
        do {
            // Check if there is anything to consume
            size = get_consumable(consume_tails, false);
            if (size[2] == 0) {
                return std::nullopt;
            }
        } while (!state_->consume_tails_.compare_exchange_weak(
            network_tails,    // Updated on failure
            NetworkIndex(consume_tails + HostIndex{size[0] + size[1], 1}),
            std::memory_order_acq_rel,
            std::memory_order_relaxed
        ));

        // Return pointer(s) to where data should be read from
        auto m_data = reinterpret_cast<uint64_t*>(state_->metadata_.data());
        return RingBufferHandle<N>(
            false, true, data_, consume_tails.b_index, size[0], m_data, consume_tails.m_index, 1
        );
    }

    /**
     * Claims all data in the buffer to be consumed
     */
    [[nodiscard]] std::optional<RingBufferHandle<N>> acquire_flush() {
        NetworkIndex network_tails = state_->consume_tails_.load(std::memory_order_acquire);
        HostIndex consume_tails = network_tails.n64toh();
        std::array<uint32_t, 3> size;
        do {
            // Check if there is anything to consume
            size = get_consumable(consume_tails, true);
            if (size[2] == 0) {
                return std::nullopt;
            }
        } while (!state_->consume_tails_.compare_exchange_weak(
            network_tails,    // Updated on failure
            NetworkIndex(consume_tails + HostIndex{size[0] + size[1], size[2]}),
            std::memory_order_acq_rel,
            std::memory_order_relaxed
        ));
        assert(size[1] == 0);

        // Return pointer(s) to where data should be read from
        auto m_data = reinterpret_cast<uint64_t*>(state_->metadata_.data());
        return RingBufferHandle<N>(
            false, true, data_, consume_tails.b_index, size[0], m_data, consume_tails.m_index, size[2]
        );
    }

    /**
     * Releases handle acquired to produce data into and/or consume data from
     */
    void release(RingBufferHandle<N>&& handle) {
        auto size = std::visit(overloaded{
            [&](const RingBufferRegion& region) -> uint32_t { return region.size; },
            [&](const std::array<RingBufferRegion, 2>& pair) -> uint32_t { return pair[0].size + pair[1].size; }
        }, handle.regions);

        if (handle.is_produce) {
            assert(handle.m_size == 1);

            // Write the item size into the metadata array and mark it as consumable
            state_->metadata_[handle.m_index].fetch_or(CONSUME_BIT, std::memory_order_acq_rel);
        }
        if (handle.is_consume) {
            for (size_t i = 0; i < handle.m_size; i++) {
                // Clear the consume bit to mark the item as consumed
                state_->metadata_[(handle.m_index + i) % M].fetch_and(~CONSUME_BIT, std::memory_order_acq_rel);
            }
            update_pointers();
        }
    }

    /*
     * Amount of space available to produce data into
     */
    static std::array<uint32_t, 2> get_producible(const HostIndex& start, const HostIndex& end) {
        std::array<uint32_t, 2> size = distance(start, end);
        if (size[0] == 0) { size[0] = N; }
        if (size[1] == 0) { size[1] = M; }

        return size;
    }
    std::array<uint32_t, 2> get_producible() const {
        return get_producible(
            state_->produce_tails_.load(std::memory_order_acquire).n64toh(),
            state_->consume_heads_.load(std::memory_order_acquire).n64toh()
        );
    }

    /*
     * Amount of data available to be consumed
     */
    std::array<uint32_t, 3> get_consumable(const HostIndex& start, const bool all) const {
        std::array<uint32_t, 3> size{};
        while (size[0] < CLIENT_BUFFER_COUNT / 2) {
            const uint64_t slot = state_->metadata_[(start.m_index + size[2]) % M];
            if (~slot & CONSUME_BIT) {
                break;
            }
            size[0] += static_cast<uint32_t>(slot & SIZE_SPAN);
            size[1] += static_cast<uint32_t>((slot & PADDING_SPAN) >> PADDING_OFFSET);
            ++size[2];

            if (!all) { break; }
        }
        return size;
    }
    std::array<uint32_t, 3> get_consumable(const bool all) const {
        const HostIndex index = state_->consume_tails_.load(std::memory_order_acquire).n64toh();
        return get_consumable(index, all);
    }

    void update_pointers() {
        // Calculate how far the consume head can be moved
        HostIndex tail = state_->consume_tails_.load(std::memory_order_acquire).n64toh();
        NetworkIndex expected = state_->consume_heads_.load(std::memory_order_acquire);
        HostIndex head = expected.n64toh();

        HostIndex offset{};
        while (head + offset != tail) {
            const uint64_t slot = state_->metadata_[(head.m_index + offset.m_index) % M].load(std::memory_order_acquire);
            if (slot & CONSUME_BIT) {
                break;
            }
            auto b_offset = static_cast<uint32_t>(slot & SIZE_SPAN) + static_cast<uint32_t>((slot & PADDING_SPAN) >> PADDING_OFFSET);
            offset += HostIndex{b_offset, 1};
        }

        // If this fails, then someone else has already updated the consume head for us
        if (offset.b_index > 0) {
            state_->consume_heads_.compare_exchange_strong(
                expected, NetworkIndex(head + offset), std::memory_order_acq_rel, std::memory_order_relaxed
            );
        }
    }

    /* Define getters & setters */
    char* data() const noexcept { return data_; }
    State* state() const noexcept { return state_; }

    static size_t capacity() { return N; }
    static size_t size_of() { return capacity() + sizeof(State); }

private:
    /*
     * Calculates the distance between from the start index to the end
     * Note for size calculations that, if two values are equal, their distance is 0
     */
    static std::array<uint32_t, 2> distance(const HostIndex& start, const HostIndex& end) {
        return {
            start.b_index <= end.b_index ? end.b_index - start.b_index : N - start.b_index + end.b_index,
            start.m_index <= end.m_index ? end.m_index - start.m_index : M - start.m_index + end.m_index
        };
    }

    char* data_{};
    State* state_;
};

}
