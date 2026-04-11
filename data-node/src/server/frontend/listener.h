//
// Created by Felix Zhang on 2025-09-03.
//

#pragma once

#include "utils/buffer_pool.h"
#include "utils/functions.h"
#include "utils/parameters/derived.h"

namespace fdl {

// Memory regions shared by every QP on this server and the layout matches `mr_t`
static std::array<std::pair<char*, size_t>, 3> server_regions() {
    return {{
        { BufferPools::unified().data(), BufferPools::unified().size_of() },
        { BufferPools::scratch().data(), BufferPools::scratch().size_of() },
        { BufferPools::remote().data(), BufferPools::remote().size_of() },
    }};
}

class Listener {
public:
    static constexpr int MAX_EVENTS = 16;

    // Delete copy & move operators
    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;
    Listener(const Listener&&) = delete;
    Listener&& operator=(const Listener&&) = delete;

    static Listener& instance() {
        static Listener instance_;
        return instance_;
    }

    void run();
    void cleanup() {
        if (broker_fd_ >= 0) {
            ::close(broker_fd_);
            broker_fd_ = -1;
        }
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (poll_fd_ >= 0) {
            ::close(poll_fd_);
            poll_fd_ = -1;
        }
    }

    int server_fd(const size_t remote) const {
        return server_fds_[remote];
    }

private:
    Listener() = default;
    ~Listener() { cleanup(); }

    bool bringup();

    // Used when bringing up the OOB mesh be
    bool server(size_t local);
    bool client(size_t local, size_t remote);

    void handle_broker_message(std::vector<std::thread>& workers) const;
    void handle_client_message() const;
    void handle_server_message(int server_fd) const;

    // Rank of the server in the shuffle cl
    size_t rank_ = std::numeric_limits<size_t>::max();

    int broker_fd_ = -1;
    std::array<int, SERVER_COUNT - 1> server_fds_ = repeat<int, SERVER_COUNT - 1>(-1);

    int listen_fd_ = -1;
    int poll_fd_ = -1;
};

}
