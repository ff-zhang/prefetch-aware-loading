//
// Created by Felix Zhang on 2025-09-03.
//

#include <cassert>
#include <cstring>
#include <thread>
#include <vector>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "listener.h"
#include "tracker.h"
#include "RDMA/worker.h"
#include "utils/debug.h"
#include "utils/rdma.h"
#include "utils/socket.h"

namespace fdl {

void Listener::handle_broker_message(std::vector<std::thread>& workers) const {
    // Incoming request from the broker to acquire resources for a new shuffle job
    header::ResourceRequest notification;
    std::vector<uint64_t> servers;
    recv_all(broker_fd_, &notification, sizeof(notification));
    servers.resize(notification.num_servers);
    recv_all(broker_fd_, servers.data(), sizeof(uint64_t) * notification.num_servers);

    // Collect OOB socket for all secondary servers in this job.
    std::vector<int> secondary_fds;
    secondary_fds.reserve(notification.num_servers - 1);
    for (const auto& rank : servers) {
        if (rank == rank_) { continue; }
        const uint64_t index = rank > rank_ ? rank - 1 : rank;
        secondary_fds.emplace_back(server_fds_[index]);
    }

    workers.emplace_back(
        rdma::worker<std::vector<int>>, notification.job_id, notification.num_partitions, rank_, std::move(secondary_fds)
    );
}

void Listener::handle_client_message() const {
    const int client_fd = ::accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) return;

    FDL_INFO("[Listener] Setting up connection with client");

    header::ConnectionRequest request;
    if (!recv_all(client_fd, &request, sizeof(request))) {
        ::close(client_fd);
        return;
    }

    // Look up the job — it should have been created before the client connects.
    Job* job = Tracker::instance().get_job(request.job_id);
    if (!job) {
        FDL_WARN("[Listener] client: unknown job ID %u", request.job_id);
        ::close(client_fd);
        return;
    }
    if (request.kind == header::ConnectionRequest::SOURCE) {
        // Allocate and initialize a QP for this client for this job
        auto [client_id, qp] = job->add_client();
        if (qp.initialize(server_regions()) != EXIT_SUCCESS) {
            FDL_WARN("[Listener] (Job %u) Failed to initialize client %lu QP", request.job_id, client_id);
            ::close(client_fd);
            return;
        }

        rdma::ibv_qp_info remote_info{};
        if (!recv_all(client_fd, &remote_info, sizeof(remote_info))) {
            ::close(client_fd);
            return;
        }
        const auto local_info = qp.materialize();
        if (!send_all(client_fd, &local_info, sizeof(local_info))) {
            ::close(client_fd);
            return;
        }

        // Bring up the new QP
        if (qp.bringup(remote_info) != EXIT_SUCCESS) {
            FDL_WARN("[Listener] (Job %u) Failed to bring up client %lu QP ", request.job_id, client_id);
            ::close(client_fd);
            return;
        }

        // Send buffer region metadata for all partitions on this server
        const header::ConnectionResponse response = { .job_id = request.job_id, .client_id = client_id };
        send_all(client_fd, &response, sizeof(response));

        std::vector<std::pair<uint64_t, uint64_t>> regions;
        regions.reserve(job->size());
        for (const auto* buffer : job->buffers()) {
            regions.emplace_back(
                reinterpret_cast<uint64_t>(buffer->data()), reinterpret_cast<uint64_t>(buffer->state())
            );
        }
        send_all(client_fd, regions.data(), regions.size() * sizeof(std::pair<uint64_t, uint64_t>));

        FDL_INFO("[Listener] (Job %u) Client %lu QP ready", request.job_id, client_id);
    } else if (request.kind == header::ConnectionRequest::SINK) {
        job->add_sink(request.info.partition, request.info.client_id, {request.info.data, request.info.state});
    } else if (request.kind == header::ConnectionRequest::CLOSE) {
        job->close_client(request.info.client_id);
    } else {
        FDL_WARN("[Listener] (Job %u) Unexpected connection request type %d ", request.job_id, request.kind);
    }

    ::close(client_fd);
}

void Listener::handle_server_message(const int server_fd) const {
    header::JobSetup request;
    if (!recv_all(server_fd, &request, sizeof(header::JobSetup))) {
        return;
    }

    switch (request.kind) {
        case header::JobSetup::SYN: {
            // We are a secondary server so create the job and bring up a QP with the primary node
            auto& job = Tracker::instance().getsert_job(request.job_id, request.num_partitions);
            assert(request.num_partitions == job.size());

            // Calculate the index of the QP from the primary's rank.
            const size_t index = request.rank < rank_ ? request.rank : request.rank - 1;

            // Initialize and bring up a QP using the info that arrived in the packet
            if (job.queue_pair(index).initialize(server_regions()) != EXIT_SUCCESS) {
                FDL_WARN("[Listener] Secondary QP init failed (job %u)", request.job_id);
                return;
            }
            if (job.queue_pair(index).bringup(request.qp_info) != EXIT_SUCCESS) {
                FDL_WARN("[Listener] Secondary QP bringup failed (job %u)", request.job_id);
                return;
            }

            // Build the buffer-region list so the primary can scatter into us
            std::vector<std::pair<uint64_t, uint64_t>> regions;
            regions.reserve(request.num_partitions);
            for (const auto* buffer : job.buffers()) {
                regions.emplace_back(
                    reinterpret_cast<uint64_t>(buffer->data()), reinterpret_cast<uint64_t>(buffer->state())
                );
            }

            // Reply with ACK carrying this server's QP info and buffer regions
            request.kind = header::JobSetup::ACK;
            request.rank = rank_;
            request.qp_info = job.queue_pair(index).materialize();
            send_all(server_fd, &request, sizeof(header::JobSetup));
            send_all(server_fd, regions.data(), regions.size() * sizeof(std::pair<uint64_t, uint64_t>));

            break;
        }
        case header::JobSetup::ACK: {
            // We are the primary server so receive the secondary's QP info and finish bringup
            auto* job = Tracker::instance().get_job(request.job_id);
            assert(job != nullptr && request.num_partitions == job->size());

            // Receive remote regions
            std::vector<std::pair<uint64_t, uint64_t>> regions(request.num_partitions);
            recv_all(server_fd, regions.data(), regions.size() * sizeof(std::pair<uint64_t, uint64_t>));

            // Map the secondary's absolute rank to the QP-based index
            if (request.rank > rank_) { --request.rank; }
            job->add_server(request.rank, std::move(request.qp_info), regions);

            if (job->view_count() == SERVER_COUNT - 1) {
                // All the remotes have been set up so send the completion notification to the broker
                const header::ResourceResponse completion = { request.job_id };
                send_all(broker_fd_, &completion, sizeof(completion));
            }

            break;
        }
    }
}

void Listener::run() {
    bringup();
    FDL_LOG("[Listener] Bring up completed successfully");

    // Start polling the OOB socket for messages
    std::array<epoll_event, MAX_EVENTS> events{};
    std::vector<std::thread> workers;
    while (true) {
        const int n = epoll_wait(poll_fd_, events.data(), MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) { continue; }
            perror("[Listener] epoll_wait");
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (!(events[i].events & EPOLLIN)) { continue; }

            if (events[i].data.fd == broker_fd_) {
                handle_broker_message(workers);
            } else if (events[i].data.fd == listen_fd_) {
                handle_client_message();
            } else {
                handle_server_message(events[i].data.fd);
            }
        }
    }
}

bool Listener::bringup() {
    const auto ip = get_local_ip();

    // Determine the local system's rank
    for (size_t i = 0; i < SERVER_COUNT; ++i) {
        if (strcmp(SERVER_IPS[i], ip.c_str()) == 0) {
            rank_ = i;
            break;
        }
    }
    if (rank_ == std::numeric_limits<size_t>::max()) {
        FDL_WARN("IP address not found: %s", ip.c_str());
        return false;
    }

    // Start listening on the OOB port
    sockaddr_in address;
    pton(ip.c_str(), OOB_PORT, address);
    listen_fd_ = listen(address, 16);

    FDL_LOG("[Listener] Listening on %s:%u", ip.c_str(), OOB_PORT);

    // Bring up the out-of-band network mesh
    auto worker = std::thread(&Listener::server, this, rank_);
    for (size_t i = rank_ + 1; i < SERVER_COUNT; ++i) {
        if (!client(rank_, i)) { return false; }
    }
    worker.join();

    // This connection should always be from the resource broker
    broker_fd_ = ::accept(listen_fd_, nullptr, nullptr);

    FDL_INFO("[Listener] Connected to the resource broker");

    poll_fd_ = epoll_create(1);
    if (poll_fd_ < 0) {
        perror("[Listener] epoll_create");
        return false;
    }

    auto add_sockfd = [&](const int sockfd) {
        epoll_event event = { .events = EPOLLIN, .data = { .fd = sockfd } };
        if (epoll_ctl(poll_fd_, EPOLL_CTL_ADD, sockfd, &event) < 0) {
            perror("[Listener] epoll_ctl");
            return false;
        }
        return true;
    };

    if (!add_sockfd(listen_fd_) || !add_sockfd(broker_fd_)) {
        return false;
    }
    for (const int server_fd : server_fds_) {
        if (server_fd != -1 && !add_sockfd(server_fd)) {
            return false;
        }
    }

    return true;
}

bool Listener::server(const size_t local) {
    bool success = true;

    // Accept connections from all lower-ranked peers
    for (size_t i = 0; i < local; i++) {
        const int sockfd = ::accept(listen_fd_, nullptr, nullptr);
        if (sockfd < 0) {
            success = false;
            continue;
        }

        size_t remote;
        if (!recv_all(sockfd, &remote, sizeof(remote))) {
            success = false;
            continue;
        }

        server_fds_[remote < local ? remote : remote - 1] = sockfd;
        FDL_LOG("[Listener] Accepted connection from node %lu", remote);
    }

    return success;
}

bool Listener::client(const size_t local, const size_t remote) {
    FDL_LOG("[Listener] Connecting to %s:%u", SERVER_IPS[remote], OOB_PORT);

    sockaddr_in address;
    pton(SERVER_IPS[remote], OOB_PORT, address);
    const int sockfd = connect(address, true);

    server_fds_[remote < local ? remote : remote - 1] = sockfd;
    FDL_LOG("[Listener] Connected to node %lu", remote);

    return send_all(sockfd, &local, sizeof(size_t));
}

} // namespace fdl