//
// Created by Felix Zhang on 1/14/26.
//

#pragma once

#include "queue_pair.h"
#include "utils/debug.h"
#include "utils/parameters/configuration.h"
#include "utils/ring_buffer.h"

namespace fdl::rdma {

constexpr size_t CAS_INFLIGHT_COUNT = 32;

template<std::uint32_t N, std::uint32_t M>
class RingBufferRemote {
    using HostIndex = RingBuffer<N, M>::HostIndex;
    using NetworkIndex = RingBuffer<N, M>::NetworkIndex;
    using State = RingBuffer<N, M>::State;
    using kind_t = RingBuffer<N, M>::State::kind_t;

public:
    struct alignas(64) view_t {
        alignas(64) std::atomic<NetworkIndex> produce_tails_{}; // First index at which data can be produced to
        alignas(64) std::atomic<NetworkIndex> consume_heads_{}; // First index at which data can be consumed from
        alignas(64) std::atomic<NetworkIndex> consume_tails_{}; // First index where data is currently being consumed

        // Used to store the values retrieved when performing compare and swap with RDMA
        std::array<NetworkIndex, CAS_INFLIGHT_COUNT> buffers_{};
    };

    RingBufferRemote() = default;

    /* Disable copying & enable moving */
    RingBufferRemote(const RingBufferRemote&) = delete;
    RingBufferRemote& operator=(const RingBufferRemote&) = delete;
    RingBufferRemote(RingBufferRemote&& other) noexcept :
        qp_(other.qp_),
        data_(other.data_),
        state_(other.state_),
        view_(other.view_)
    {}
    RingBufferRemote& operator=(RingBufferRemote&& other) noexcept {
        if (this != &other) {
            qp_ = other.qp_;
            data_ = other.data_;
            state_ = other.state_;
            view_ = other.view_;
        }
        return *this;
    }

    void initialize(char* buffer) noexcept {
        if (view_ != nullptr) {
            assert(false && "Buffer has been previously set");
        }

        view_ = reinterpret_cast<view_t*>(buffer);
        memset(view_, 0, sizeof(view_t));
    }

    void setup(QueuePair* qp, const uint64_t data, const uint64_t state, std::optional<std::pair<void(*)(void*, void*), void*>> progress) {
        qp_ = qp;
        data_ = data;
        state_ = state;
        progress_ = std::move(progress);
    }

    void reset() noexcept {
        assert(view_ != nullptr);
        memset(view_, 0, sizeof(view_t));

        qp_ = nullptr;
        data_ = 0;
        state_ = 0;
        progress_ = std::nullopt;

        pipeline_.store(0, std::memory_order_release);
    }

    template <typename T>
    std::optional<uint64_t> produce_async(
        const std::array<std::pair<char*, uint32_t>, 2>& b_segments,
        const std::array<std::pair<uint64_t*, uint32_t>, 2>& m_segments,
        const uint32_t m_size,
        const mr_t local,
        std::array<T, 2 * MAX_CQE>& contexts,
        T& context
    ) {
        auto operations = produce_setup(b_segments, m_segments, m_size, local);
        if (operations.empty()) {
            return std::nullopt;
        }

        const uint64_t ticket = qp_->write(std::move(operations), true, contexts, context);
        assert(ticket != std::numeric_limits<uint64_t>::max());
        return ticket;
    }

    // TODO: re-enable when needed
    std::optional<uint64_t> produce_async(const std::array<std::pair<char*, uint32_t>, 2>& segments, const mr_t local, void* context) {
        // auto operations = produce_setup(segments, local, slot);
        std::vector<ibv_write_t> operations = {};
        if (operations.empty()) {
            return std::nullopt;
        }

        const uint64_t ticket = qp_->write(std::move(operations), true, context);
        assert(ticket != std::numeric_limits<uint64_t>::max());
        return ticket;
    }

    void await(const uint64_t ticket) {
        while (qp_->completion(ticket).count.load(std::memory_order_acquire) != 0) {
            cpu_pause();
        }
    }

    static size_t size_of() { return sizeof(view_t); }

    /* Should only be used for testing purposes */
    [[nodiscard]] QueuePair* qp() const { return qp_; }
    [[nodiscard]] uint64_t data() const { return data_; }
    [[nodiscard]] view_t* view() const { return view_; }

private:
    std::vector<ibv_write_t> produce_setup(
        const std::array<std::pair<char*, uint32_t>, 2>& b_segments,
        const std::array<std::pair<uint64_t*, uint32_t>, 2>& m_segments,
        const uint32_t m_size,
        const mr_t local
    ) {
        uint32_t b_size = 0;
        for (const auto& [data, length] : b_segments) {
            if (data == nullptr || length == 0) { break; }
            b_size += length;
        }
        if (b_size == 0 || b_size > N) {
            return {};
        }

        // Atomically update the remote producer tail
        std::optional<HostIndex> start = update_remote({b_size, m_size});
        if (!start.has_value()) {
            return {};
        }

        // Set up the chain of writes to be submitted to the queue pair
        // The order of operations is extremely important here to ensure correctness
        std::vector<ibv_write_t> operations;
        operations.reserve(8);

        // Map local data segments to regions in the remote buffer
        uint32_t offset = start.value().b_index;
        for (const auto& [data, length] : b_segments) {
            if (data == nullptr || length == 0) { break; }

            if (offset + length <= N) {
                operations.emplace_back(data, length, data_ + offset, local, UNIFIED);
                offset = (offset + length) % N;
            } else {
                const uint32_t size1 = N - offset;
                const uint32_t size2 = length - size1;
                operations.emplace_back(data, size1, data_ + offset, local, UNIFIED);
                operations.emplace_back(data + size1, size2, data_, local, UNIFIED);
                offset = size2;
            }
        }

        // Map local metadata segments to regions in the remote buffer
        offset = start.value().m_index;
        for (const auto& [buffer, count] : m_segments) {
            if (buffer == nullptr || count == 0) { break; }

            auto raw = reinterpret_cast<const char*>(buffer);
            if (offset + count <= M) {
                operations.emplace_back(raw, count * sizeof(uint64_t), state_ + offset * sizeof(uint64_t), local, UNIFIED);
                offset = (offset + count) % M;
            } else {
                const uint32_t size1 = M - offset;
                const uint32_t size_2 = count - size1;
                operations.emplace_back(raw, size1 * sizeof(uint64_t), state_ + offset * sizeof(uint64_t), local, UNIFIED);
                operations.emplace_back(raw + size1 * sizeof(uint64_t), size_2 * sizeof(uint64_t), state_, local, UNIFIED);
                offset = size_2;
            }
        }

        return operations;
    }

    /*
     * Move the remote produce tail forward by the given amount
     */
    std::optional<HostIndex> update_remote(const HostIndex& size) {
        NetworkIndex network_tails;
        HostIndex produce_tails;
        do {
            network_tails = view_->produce_tails_.load(std::memory_order_acquire);
            produce_tails = network_tails.n64toh();

            HostIndex consume_heads = view_->consume_heads_.load(std::memory_order_acquire).n64toh();

            // Check if there is enough space available
            while (true) {
                auto [b_avail, m_avail] = RingBuffer<N, M>::get_producible(produce_tails, consume_heads);

                // If there is insufficient room, poll the consumer head's remote value
                if (b_avail <= size.b_index || m_avail <= size.m_index) {
                    const uint64_t ticket = read(kind_t::CONS_HEADS, true);
                    if (progress_.has_value()) {
                        auto& [function, context] = progress_.value();
                        qp_->await(ticket, [&](QueuePair* qp) { function(context, qp); });
                    } else {
                        qp_->await(ticket, [](QueuePair*) {});
                    }

                    HostIndex updated = view_->consume_heads_.load(std::memory_order_acquire).n64toh();
                    if (updated == consume_heads) {
                        // No room in the remote ring buffer has been freed up
                        return std::nullopt;
                    }

                    // Re-compute the available space if the consumer head has moved
                    consume_heads = updated;
                    continue;
                }

                // There is space in the ring buffer to produce the data into
                break;
            }
        } while (!compare_exchange(
            kind_t::PROD_TAILS, network_tails, NetworkIndex(produce_tails + size)
        ));

        return produce_tails;
    }

    /*
     * Updated on failure (and success)
     */
    bool compare_exchange(const kind_t kind, const NetworkIndex expected, const NetworkIndex desired) {
        NetworkIndex old;
        std::atomic<NetworkIndex>* laddr;
        uint64_t raddr;
        switch (kind) {
            case kind_t::PROD_TAILS: {
                old = view_->produce_tails_.load(std::memory_order_acquire);
                raddr = state_ + offsetof(State, produce_tails_);
                laddr = &view_->produce_tails_;
                break;
            }
            case kind_t::CONS_HEADS: {
                old = view_->consume_heads_.load(std::memory_order_acquire);
                raddr = state_ + offsetof(State, consume_heads_);
                laddr = &view_->consume_heads_;
                break;
            }
            case kind_t::CONS_TAILS: {
                old = view_->consume_tails_.load(std::memory_order_acquire);
                raddr = state_ + offsetof(State, consume_tails_);
                laddr = &view_->consume_tails_;
                break;
            }
            default: {
                FDL_WARN("Unexpected destination type for compare & swap");
                 return false;
            }
        }

        const uint64_t pipeline = pipeline_.fetch_add(1, std::memory_order_acq_rel) % CAS_INFLIGHT_COUNT;
        const auto ticket = qp_->compare_exchange(
            reinterpret_cast<uint64_t*>(&view_->buffers_[pipeline]),
            raddr,
            std::bit_cast<uint64_t>(expected),
            std::bit_cast<uint64_t>(desired),
            JOB,
            UNIFIED,
            true
        );
        if (progress_.has_value()) {
            auto& [function, context] = progress_.value();
            qp_->await(ticket, [&](QueuePair* qp){ function(context, qp); });
        } else {
            qp_->await(ticket, [](QueuePair*) {});
        }

        if (view_->buffers_[pipeline] == old) {
            // Must update the local pointer since we successfully modified the remote one
            NetworkIndex updated = old;
            while (!laddr->compare_exchange_weak(updated, desired, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                updated = old;
            }
            return true;
        } else {
            // Try to update the local value to the one fetched from
            laddr->compare_exchange_strong(old, view_->buffers_[pipeline], std::memory_order_acq_rel, std::memory_order_relaxed);

            // Failed to update the remote pointer
            return false;
        }
    }

    /*
     * Read from remote unified state metadata
     */
    uint64_t read(const kind_t kind, const bool signal) {
        uint64_t target;
        std::atomic<NetworkIndex>* destination;
        switch (kind) {
            case kind_t::PROD_TAILS: {
                target = state_ + offsetof(State, produce_tails_);
                destination = &view_->produce_tails_;
                break;
            }
            case kind_t::CONS_HEADS: {
                target = state_ + offsetof(State, consume_heads_);
                destination = &view_->consume_heads_;
                break;
            }
            case kind_t::CONS_TAILS: {
                target = state_ + offsetof(State, consume_tails_);
                destination = &view_->consume_tails_;
                break;
            }
            default: {
                FDL_WARN("Unexpected destination type for compare & swap");
                return false;
            }
        }

        const uint64_t ticket = qp_->read<std::atomic<NetworkIndex>>(target, destination, JOB, UNIFIED, signal, nullptr);
        assert(signal || ticket == std::numeric_limits<uint64_t>::max());
        return ticket;
    }

    QueuePair* qp_;

    /* Remote addresses of the data and metadata for the ring buffer */
    uint64_t data_;
    uint64_t state_;

    /* Local copy of the ring buffer's state */
    view_t* view_;

    std::atomic<uint64_t> pipeline_;

    /* If used, these are used to drive progress when awaiting completions */
    std::optional<std::pair<void (*)(void*, void*), void*>> progress_;
};

using ClientUnifiedBuffer = RingBufferRemote<CLIENT_BUFFER_SIZE, CLIENT_BUFFER_PACKETS>;

}