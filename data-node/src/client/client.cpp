//
// Created by Felix Zhang on 4/12/26.
//

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <getopt.h>
#include <unistd.h>

#include "pipelines/RDMA/queue_pair.h"
#include "utils/debug.h"
#include "utils/network.h"
#include "utils/parameters/configuration.h"
#include "utils/pause.h"
#include "utils/ring_buffer.h"
#include "utils/socket.h"
#include "utils/types.h"

constexpr size_t BATCH_COUNT = 100;

using RingBuffer = fdl::RingBuffer<fdl::CLIENT_BUFFER_SIZE, fdl::CLIENT_BUFFER_PACKETS>;
using BufferHandle = fdl::RingBufferHandle<fdl::CLIENT_BUFFER_SIZE>;

// Message exchanged during TCP handshake so both sides agree on batch dimensions
struct BatchConfig {
    uint32_t batch_size;
    uint32_t sample_size;
};

int main(int argc, char** argv) {
    const char* ip = nullptr;
    uint16_t port = 9000;
    std::string tmpfs = "/dev/shm";
    uint32_t lookahead = 0;

    static constexpr option options[] = {
        {"ip", required_argument, nullptr, 'i'},
        {"port", required_argument, nullptr, 'p'},
        {"tmpfs", required_argument, nullptr, 't'},
        {"lookahead", required_argument, nullptr, 'l'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "", options, nullptr)) != -1) {
        switch (c) {
            case 'i': {
                ip = optarg;
                break;
            }
            case 'p': {
                port = static_cast<uint16_t>(std::stoul(optarg));
                break;
            }
            case 't': {
                tmpfs = optarg;
                break;
            }
            case 'l': {
                lookahead = std::stoul(optarg);
                break;
            }
            default: {
                FDL_WARN("Usage: agent --ip SERVER_IP --port PORT [--tmpfs PATH] [--lookahead N]");
                return EXIT_FAILURE;
            }
        }
    }
    if (!ip) {
        fprintf(stderr, "[agent] --ip is required\n");
        return 1;
    }

    // Allocate the ring buffer that the server will write batches into using RDMA
    const std::unique_ptr<char, decltype(&free)> memory(
        static_cast<char*>(aligned_alloc(64, RingBuffer::size_of())), free
    );
    if (!memory) {
        perror("aligned_alloc");
        return EXIT_FAILURE;
    }
    memset(memory.get(), 0, RingBuffer::size_of());

    RingBuffer buffer;
    buffer.initialize(memory.get());

    // Connect to the server via TCP
    sockaddr_in address{};
    if (!fdl::pton(ip, port, address)) {
        FDL_WARN("[Agent] Invalid server address: %s:%u\n", ip, port);
        return EXIT_FAILURE;
    }
    const int server_fd = fdl::connect(address, true);
    if (server_fd < 0) { return EXIT_FAILURE; }

    FDL_LOG("[Agent] Connected to %s:%u", ip, port);

    // Receive batch dimensions from the server
    BatchConfig config{};
    if (!fdl::recv_all(server_fd, &config, sizeof(config))) {
        close(server_fd);
        return EXIT_FAILURE;
    }
    FDL_INFO("[Agent] batch_size=%u sample_size=%u", config.batch_size, config.sample_size);

    const uint32_t length = config.batch_size * config.sample_size;
    if (length == 0 || length >= fdl::CLIENT_BUFFER_SIZE) {
        FDL_WARN("[Agent] total batch length %u out of valid range (0, %zu)", length, fdl::CLIENT_BUFFER_SIZE);
        close(server_fd);
        return EXIT_FAILURE;
    }

    // Setup client-side QP
    fdl::rdma::QueuePair qp;
    const std::vector<std::pair<char*, size_t>> regions = {
        {memory.get(), RingBuffer::size_of()}
    };
    if (qp.initialize(regions) != 0) {
        FDL_WARN("[Agent] QP initialize failed");
        close(server_fd);
        return EXIT_FAILURE;
    }

    // Send client QP info to the server
    const fdl::rdma::ibv_qp_info client_info = qp.materialize();
    if (!fdl::send_all(server_fd, &client_info, sizeof(client_info))) {
        close(server_fd);
        return EXIT_FAILURE;
    }

    // Bring up the RDMA QP using the remote information
    fdl::rdma::ibv_qp_info server_info{};
    if (!fdl::recv_all(server_fd, &server_info, sizeof(server_info))) {
        close(server_fd);
        return EXIT_FAILURE;
    }
    if (qp.bringup(server_info) != 0) {
        FDL_WARN("[agent] QP bringup failed");
        close(server_fd); return 1;
    }

    FDL_LOG("[Agent] RDMA connection established");
    
    FDL_LOG("[Agent] Initializing tmpfs at %s", tmpfs.c_str());
    try {
        if (std::filesystem::exists(tmpfs) && std::filesystem::is_directory(tmpfs)) {
            uint32_t count = 0;
            for (const auto& entry : std::filesystem::directory_iterator(tmpfs)) {
                const std::string filename = entry.path().filename().string();
            
                // Remove the DONE signal or any shard files
                if (filename == "DONE" || (filename.find("shard_") == 0 && (filename.ends_with(".pt") || filename.ends_with(".pt.tmp")))) {
                    std::filesystem::remove(entry.path());
                    count++;
                }
            }
            if (count > 0) {
                FDL_LOG("[Agent] Purged %u leftover files from previous session", count);
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        FDL_WARN("[Agent] Error during startup cleanup: %s", e.what());
    }
    
    FDL_LOG("[Agent] Starting consumer loop");

    // Continuously spin on the ring buffer, copying each batch to tmpfs
    uint32_t counter = 0;
    auto t0 = std::chrono::steady_clock::now();
    while (counter < BATCH_COUNT) {
        // Busy-wait until the server has written a batch into our buffer
        std::optional<BufferHandle> handle;
        do {
            handle = buffer.acquire_consume();
            if (!handle) { cpu_pause(); }
        } while (!handle);

        // Check if we are at the file limit
        if (lookahead != 0 && counter >= lookahead) {
            char temp[512];
            snprintf(temp, sizeof(temp), "%s/shard_%u.pt", tmpfs.c_str(), counter - lookahead);

            char final[512];
            snprintf(final, sizeof(final), "%s/shard_%u.pt", tmpfs.c_str(), counter - lookahead);

            // Wait until the external process unlinks the oldest file
            while (::access(temp, F_OK) == 0 || ::access(final, F_OK) == 0) {
                cpu_pause();
            }
        }

        // Determine the destination file for this batch
        char temp[512];
        snprintf(temp, sizeof(temp), "%s/shard_%u.pt.tmp", tmpfs.c_str(), counter);

        // Write the batch into the tmpfs file
        const int fd = open(temp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            perror("open");
        } else {
            std::visit(fdl::overloaded{
                [&](const fdl::RingBufferRegion& r) {
                    if (::write(fd, r.data, r.size) < 0) { perror("write"); }
                },
                [&](const std::array<fdl::RingBufferRegion, 2>& pair) {
                    if (::write(fd, pair[0].data, pair[0].size) < 0) { perror("write"); }
                    if (::write(fd, pair[1].data, pair[1].size) < 0) { perror("write"); }
                }
            }, handle->regions);
            ::close(fd);
        }

        char final[512];
        snprintf(final, sizeof(final), "%s/shard_%u.pt", tmpfs.c_str(), counter);
        ::rename(temp, final);

        // Release the consumed slot back to the ring buffer.
        buffer.release(std::move(*handle));
        ++counter;

        if (counter % 10 == 0) {
            const auto t1 = std::chrono::steady_clock::now();
            const double secs = std::chrono::duration<double>(t1 - t0).count();
            const double gbps = (100.0 * length) / secs / 1e9;
            FDL_LOG("[Agent] batch=%-8u %.3f GB/s (%.0f batches/s)", counter, gbps, 100.0 / secs);
            t0 = t1;
        }
    }

    close(server_fd);

    // Check if the producer has signaled completion
    auto done = tmpfs + "/DONE";
    const int fd = open(done.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("open");
    }

    return 0;
}
