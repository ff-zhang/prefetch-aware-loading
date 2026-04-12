//
// Created by Felix Zhang on 4/12/26.
//

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include <getopt.h>
#include <unistd.h>

#include "pipelines/RDMA/device.h"
#include "pipelines/RDMA/queue_pair.h"
#include "pipelines/RDMA/ring_buffer_remote.h"
#include "utils/debug.h"
#include "utils/network.h"
#include "utils/parameters/configuration.h"
#include "utils/pause.h"
#include "utils/ring_buffer.h"
#include "utils/socket.h"

using RingBuffer = fdl::RingBuffer<fdl::CLIENT_BUFFER_SIZE, fdl::CLIENT_BUFFER_PACKETS>;

// Message exchanged during TCP handshake so both sides agree on batch dimensions
struct BatchConfig {
    uint32_t batch_size;
    uint32_t sample_size;
};

// Progress callback used to drive QP completions when waiting for RDMA operations to complete
static void progress_fn(void* context, void* qp) {
    static_cast<fdl::rdma::QueuePair*>(qp)->poll_reclaim();
}

// Align up to the given power-of-two
static constexpr size_t align_up(const size_t n, const size_t a) {
    return (n + a - 1) & ~(a - 1);
}

int main(int argc, char** argv) {
    uint16_t port = 9000;
    BatchConfig config{32, 4096};

    static constexpr option options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"batch-size", required_argument, nullptr, 'b'},
        {"sample-size", required_argument, nullptr, 's'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "", options, nullptr)) != -1) {
        switch (c) {
            case 'p': {
                port = static_cast<uint16_t>(std::stoul(optarg));
                break;
            }
            case 'b': {
                config.batch_size = static_cast<uint32_t>(std::stoul(optarg));
                break;
            }
            case 's': {
                config.sample_size = static_cast<uint32_t>(std::stoul(optarg));
                break;
            }
            default: {
                fprintf(stderr, "Usage: data_server --port PORT [--batch-size N] [--sample-size M]\n");
                return EXIT_FAILURE;
            }
        }
    }
    FDL_INFO("[Server] batch_size=%u sample_size=%u", config.batch_size, config.sample_size);

    const size_t length = config.batch_size * config.sample_size;
    if (length == 0 || length >= fdl::CLIENT_BUFFER_SIZE) {
        FDL_WARN("[Server] total batch length %lu out of valid range (0, %zu)", length, fdl::CLIENT_BUFFER_SIZE);
        return EXIT_FAILURE;
    }

    // Setup memory allocation for the ring buffer and remote view
    struct {
        size_t unified = 0;
        size_t scratch = 64;
        size_t view = align_up(sizeof(fdl::rdma::ClientUnifiedBuffer::view_t), 64);
    } sizes;
    sizes.unified = align_up(length + sizeof(uint64_t), 64);

    const size_t total = sizes.unified + sizes.scratch + sizes.view;
    const std::unique_ptr<char, decltype(&free)> memory(
        static_cast<char*>(aligned_alloc(64, total)), free
    );
    if (!memory) {
        perror("aligned_alloc");
        return 1;
    }
    memset(memory.get(), 0, total);

    char* ring_buf = memory.get();
    auto* metadata = reinterpret_cast<uint64_t*>(ring_buf + length);
    char* scratch_buf = memory.get() + sizes.unified;
    char* view_buf = scratch_buf + sizes.scratch;

    // Pre-fill the metadata slot that accompanies every batch
    *metadata = static_cast<uint64_t>(length) | RingBuffer::CONSUME_BIT;

    sockaddr_in listen_addr{};
    fdl::pton("0.0.0.0", port, listen_addr);
    const int listen_fd = fdl::listen(listen_addr, 1);
    if (listen_fd < 0) { return 1; }

    FDL_LOG("[Server] Listening on 0.0.0.0:%u", port);

    while (true) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        const int client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &addrlen);
        if (client_fd < 0) { perror("accept"); continue; }

        FDL_LOG("[Server] Client connected from %s", fdl::ntop(client_addr).c_str());

        // Send the client the batch dimensions
        if (!fdl::send_all(client_fd, &config, sizeof(config))) {
            close(client_fd); continue;
        }

        // Create QP and register memory regions.
        fdl::rdma::QueuePair qp;
        const std::vector<std::pair<char*, size_t>> regions = {
            {ring_buf, sizes.unified},
            {scratch_buf, sizes.scratch},
            {view_buf, sizes.view},
        };
        if (qp.initialize(regions) != 0) {
            FDL_WARN("[Server] QP initialize failed");
            close(client_fd);
            continue;
        }

        // Receive the client's QP info
        fdl::rdma::ibv_qp_info client_info{};
        if (!fdl::recv_all(client_fd, &client_info, sizeof(client_info))) {
            close(client_fd);
            continue;
        }

        // Send the server's QP info.
        const fdl::rdma::ibv_qp_info server_info = qp.materialize();
        if (!fdl::send_all(client_fd, &server_info, sizeof(server_info))) {
            close(client_fd);
            continue;
        }

        // Bring up the QP connection
        if (qp.bringup(client_info) != 0) {
            FDL_WARN("[Server] QP bringup failed");
            close(client_fd);
            continue;
        }

        // Derive remote ring buffer addresses from the client's information
        // The client registers its ring buffer as its unified MR and RingBuffer<N,M>::initialize() places its state at offset N
        const uint64_t data_addr  = client_info.addr[fdl::rdma::UNIFIED];
        const uint64_t state_addr = data_addr + fdl::CLIENT_BUFFER_SIZE;

        // Set up a local view of the client's ring buffer
        fdl::rdma::ClientUnifiedBuffer view;
        view.initialize(view_buf);
        view.setup(&qp, data_addr, state_addr, std::make_optional(std::make_pair(&progress_fn, nullptr)));

        // Producer segment descriptors
        const std::array<std::pair<char*, uint32_t>, 2> b_segments{{
            {ring_buf, length}, {nullptr,    0u}
        }};
        const std::array<std::pair<uint64_t*, uint32_t>, 2> m_segments{{
            {metadata, 1u}, {nullptr,   0u}
        }};

        std::array<bool, 2 * fdl::rdma::MAX_CQE> contexts{};
        bool context = false;

        FDL_LOG("[Server] Starting producer loop");
        uint32_t counter = 0;
        auto t0 = std::chrono::steady_clock::now();
        while (true) {
            // Write a distinguishable dummy pattern into the batch buffer
            memset(ring_buf, static_cast<int>(counter & 0xFF), length);

            // Push the batch into the client's ring buffer via one-sided RDMA
            std::optional<uint64_t> ticket;
            do {
                context = false;
                ticket = view.produce_async(b_segments, m_segments, 1, fdl::rdma::UNIFIED, contexts, context);
                if (!ticket) { qp.poll_reclaim(); }
            } while (!ticket);

            // Wait for all RDMA writes for this batch to complete.
            qp.await(*ticket, [](fdl::rdma::QueuePair* qp_) -> void {
                qp_->poll_reclaim();
            });

            ++counter;
            if (counter % 100 == 0) {
                const auto t1 = std::chrono::steady_clock::now();
                const double secs = std::chrono::duration<double>(t1 - t0).count();
                const double gbps = (100.0 * length) / secs / 1e9;
                FDL_LOG("[Server] batch=%-8u %.3f GB/s  (%.0f batches/s)", counter, gbps, 100.0 / secs);
                t0 = t1;
            }
        }

        close(client_fd);
    }

    close(listen_fd);
    return 0;
}
