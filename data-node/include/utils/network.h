//
// Created by Felix Zhang on 2025-09-08.
//

#pragma once

#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

#include "debug.h"
#include "types.h"

namespace fdl::header {

struct BrokerRequest {
    job_id_t job_id;
    partition_id_t num_partitions;
};

struct BrokerResponse {
    job_id_t job_id;
    uint32_t num_servers;
    // This should be immediately followed by an array of IP addresses for each of the shuffle servers
    // std::array<sockaddr_in, num_servers>
};

struct ResourceRequest {
    job_id_t job_id;
    partition_id_t num_partitions;
    uint32_t num_servers;
    // This should be immediately followed by an array of IP addresses for each of the shuffle servers
    // std::array<sockaddr_in, num_servers>
};

struct ResourceResponse {
    job_id_t job_id;
};

struct ConnectionRequest {
    job_id_t job_id;
    enum { SOURCE = 0, SINK = 1, CLOSE = 2 } kind;
    struct {
        uint64_t client_id;
        partition_id_t partition;
        uint64_t data;
        uint64_t state;
    } info; // This is only used by connections requests from sinks and when closing
};

struct ConnectionResponse {
    job_id_t job_id;
    uint64_t client_id;
};

}

namespace fdl {

inline bool pton(const char* ip, const int port, sockaddr_in& addr) {
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        return false;
    }
    addr.sin_port = htons(port);

    return true;
}

inline std::string ntop(const sockaddr_in& addr) {
    char buffer[INET_ADDRSTRLEN];
    const char* result = inet_ntop(AF_INET, &addr.sin_addr, buffer, INET_ADDRSTRLEN);
    return result ? std::string(buffer) : "";
}

[[nodiscard]] inline int listen(const sockaddr_in& addr, const int backlog) {
    const int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    constexpr int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY,  &opt, sizeof(opt));

    while (true) {
        if (::bind(sockfd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0) {
            break;
        }
        sleep(2);
    }

    if (::listen(sockfd, backlog) < 0) {
        perror("listen");
        return -1;
    }

    return sockfd;
}

[[nodiscard]] inline int connect(const sockaddr_in& addr, const bool block) {
    const int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    constexpr int flag = 1;
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    if (block) {
        while (::connect(sockfd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {}
    } else {
        if (::connect(sockfd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("connect");
            return -1;
        }
    }

    return sockfd;
}

}