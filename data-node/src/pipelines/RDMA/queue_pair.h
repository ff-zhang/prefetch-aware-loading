//
// Created by Felix Zhang on 1/8/26.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <random>
#include <infiniband/verbs.h>

#include "config.h"
#include "request.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "utils/debug.h"
#include "utils/parameters/configuration.h"
#include "utils/pause.h"
#include "utils/rdma.h"
#include "utils/types.h"

namespace fdl::rdma {

inline const char *ibv_wc_opcode_str(const ibv_wc_opcode opcode) {
    switch (opcode) {
        case IBV_WC_SEND:
            return "SEND";
        case IBV_WC_RDMA_WRITE:
            return "RDMA_WRITE";
        case IBV_WC_RDMA_READ:
            return "RDMA_READ";
        case IBV_WC_COMP_SWAP:
            return "COMP_SWAP";
        case IBV_WC_FETCH_ADD:
            return "FETCH_ADD";
        case IBV_WC_BIND_MW:
            return "BIND_MW";
        case IBV_WC_LOCAL_INV:
            return "LOCAL_INV";
        case IBV_WC_RECV:
            return "RECV";
        case IBV_WC_RECV_RDMA_WITH_IMM:
            return "RECV_RDMA_WITH_IMM";
        default:
            return "UNKNOWN";
    }
}

alignas(64) static std::atomic<uint8_t> port_num = 0;

class QueuePair {
public:
    struct completion_t {
        alignas(64) std::atomic<size_t> count;  // The number of WRs this completion is responsible for
        void* context;
    };

    QueuePair() {
        std::array<size_t, 2 * MAX_CQE> indexes{};
        std::iota (std::begin(indexes), std::end(indexes), 0);
        available_.enqueue_bulk(&indexes[0], 2 * MAX_CQE);
    }
    ~QueuePair() {
        teardown();
    }
    
    /* Disable copying & moving */
    QueuePair(const QueuePair& other) = delete;
    QueuePair& operator=(const QueuePair& other) = delete;
    QueuePair(QueuePair&& other) = delete;
    QueuePair& operator=(QueuePair&& other) = delete;

    template<RangeOf<std::pair<char*, size_t>> Range>
    int initialize(const Range& regions);
    int bringup(const ibv_qp_info& remote);

    /* Closes QP (if open) and tears down any resources in use */
    void teardown();

    /* Materializes information required to connect to initialized QP */
    [[nodiscard]] ibv_qp_info materialize() const;

    template <typename T>
    uint64_t write(const std::vector<ibv_write_t>& operations, bool signal, std::array<T, 2 * MAX_CQE>& contexts, T& context) {
        if (context) { assert(signal); }

        // Wait for space
        const uint64_t ticket = reserve_slot(signal);
        contexts[ticket] = std::move(context);
        completions_[ticket].context = &contexts[ticket];

        return write_(ticket, operations, signal);
    }
    uint64_t write(const std::vector<ibv_write_t>& operations, bool signal, void* context);

    uint64_t compare_exchange(uint64_t* laddr, uint64_t raddr, uint64_t expected, uint64_t desired, mr_t l_mr, mr_t r_mr, bool signal);

    template <typename T>
    uint64_t read(const uint64_t target, T* destination, const mr_t local, const mr_t remote, bool signal, void* context) {
        if (context) { assert(signal); }

        auto [lkey, rkey] = get_keys(local, remote);
        ibv_sge sge = {
            .addr = reinterpret_cast<uint64_t>(destination),
            .length = sizeof(T),
            .lkey = lkey
        };

        // Wait for space
        const size_t ticket = reserve_slot(signal);
        completions_[ticket].context = context;

        ibv_send_wr wr{};
        wr.wr_id = ticket;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = signal ? IBV_SEND_SIGNALED : 0;
        wr.wr.rdma.remote_addr = target;
        wr.wr.rdma.rkey = rkey;
        wr.next = nullptr;

        ibv_send_wr *bad_wr;
        if (const int ret = ibv_post_send(qp_, &wr, &bad_wr); ret) {
            FDL_WARN("ibv_post_send failed @ read: %s (%d)", strerror(ret), ret);
            throw std::runtime_error("");
        }

        return wr.wr_id;
    }

    size_t poll(std::array<size_t, POLL_BATCH_SIZE>& tickets, std::array<void*, POLL_BATCH_SIZE>& contexts);
    void reclaim(const std::array<size_t, POLL_BATCH_SIZE>& tickets, const size_t count) {
        if (count > 0) {
            available_.enqueue_bulk(tickets.data(), count);
        }
    }

    void poll_reclaim() {
        std::array<size_t, POLL_BATCH_SIZE> tickets{};
        std::array<void*, POLL_BATCH_SIZE> contexts{};
        const size_t count = poll(tickets, contexts);
        reclaim(tickets, count);
    }

    template<typename F>
    void await(const uint64_t ticket, F&& poll) {
        while (completion(ticket).count.load(std::memory_order_acquire) != 0) {
            poll(this);
            cpu_pause();
        }
    }

    /* Getters */
    [[nodiscard]] bool ready() const { return initialized_ && connected_; }

    const completion_t& completion(const size_t ticket) const {
        return completions_[ticket];
    }

    uint64_t inflight() const {
        return inflight_.load(std::memory_order_acquire);
    }

private:
    uint64_t write_(uint64_t ticket, const std::vector<ibv_write_t>& operations, bool signal);

    static uint32_t create_psn() {
        std::random_device dev;
        std::uniform_int_distribution<uint32_t> dist(0, 0xffffff);
        return dist(dev);
    }

    /*
     * If a completion slot was reserved, the corresponding index is returned.
     * Otherwise, the max. value possible is returned
     */
    [[nodiscard]] uint64_t reserve_slot(bool& signal) {
        uint64_t ticket = std::numeric_limits<uint64_t>::max();
        if (signal) {
            available_.wait_dequeue(ticket);
        }

        auto inflight = inflight_.load(std::memory_order_acquire);
        while (true) {
            if (inflight >= MAX_INFLIGHT) {
                cpu_pause();
                inflight = inflight_.load(std::memory_order_acquire);
                continue;
            }

            if (inflight_.compare_exchange_weak(
                inflight, inflight + 1, std::memory_order_acq_rel, std::memory_order_relaxed
            )) {
                break;
            }
        }

        signal = signal || (unsignaled_.load(std::memory_order_relaxed) * 3 >= MAX_INFLIGHT * 4);
        if (signal) {
            const size_t unsignaled = unsignaled_.fetch_and(0,  std::memory_order_acq_rel);
            completions_[ticket].count.store(1 + unsignaled, std::memory_order_release);
        } else {
            unsignaled_.fetch_add(1, std::memory_order_acq_rel);
        }

        return ticket;
    }

    [[nodiscard]] std::array<uint32_t, 2> get_keys(const mr_t local, const mr_t remote) const {
        std::array<uint32_t, 2> keys{};
        keys[0] = mrs_[local]->lkey;
        keys[1] = remote_.rkey[remote];
        return keys;
    }

    bool initialized_ = false;
    bool connected_ = false;

    uint8_t gid_ = 0;
    uint8_t port_num_ = 0;
    uint32_t psn_ = 0;
    int max_rd_atomic_ = 0;

    ibv_cq* cq_ = nullptr;
    std::array<ibv_mr*, MAX_NUM_MR> mrs_{};

    ibv_qp_info remote_{};
    ibv_qp* qp_ = nullptr;

    std::atomic<size_t> unsignaled_{};
    std::atomic<size_t> inflight_{};

    moodycamel::BlockingConcurrentQueue<size_t> available_{2 * MAX_CQE * moodycamel::ConcurrentQueue<size_t>::BLOCK_SIZE};
    std::array<completion_t, 2 * MAX_CQE> completions_{};

    std::vector<DecodeRequest*> requests_{};
};

}
