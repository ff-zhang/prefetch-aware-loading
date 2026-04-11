//
// Created by Felix Zhang on 2025-09-18.
//

#pragma once

#include <cstring>
#include <string>

#include <ifaddrs.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include "parameters/configuration.h"

namespace fdl {

constexpr int IFACE_MTU = 9000;

inline std::string get_local_ip() {
    const int sock = socket(AF_INET, SOCK_DGRAM, 0);
    ifaddrs* ifas = nullptr;
    if (sock < 0 || getifaddrs(&ifas) == -1) {
        if (sock >= 0) close(sock);
        return "";
    }

    std::string result;
    for (ifaddrs const* ifa = ifas; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }

        if (ifa->ifa_addr->sa_family == AF_INET) {
            // Skip loopback interfaces
            if (strcmp(ifa->ifa_name, "lo") == 0) {
                continue;
            }

            auto const* addr = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);
            char ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &addr->sin_addr, ip, INET_ADDRSTRLEN);
            for (auto& other : SERVER_IPS) {
                if (strcmp(ip, other) == 0) {
                    result = ip;
                }
            }
            if (!result.empty()) {
                break;
            }

            // TODO: re-enable this once cluster network issue is solved
            // Get the MTU for this interface
            // ifreq ifr;
            // strncpy(ifr.ifr_name, ifa->ifa_name, IFNAMSIZ - 1);
            // ifr.ifr_name[IFNAMSIZ - 1] = '\0';
            //
            // if (ioctl(sock, SIOCGIFMTU, &ifr) == 0) {
            //     if (ifr.ifr_mtu == IFACE_MTU) {
            //         auto const* addr = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);
            //         char ip[INET_ADDRSTRLEN];
            //         inet_ntop(AF_INET, &addr->sin_addr, ip, INET_ADDRSTRLEN);
            //
            //         result = ip;
            //         break;
            //     }
            // }
        }
    }

    freeifaddrs(ifas);
    close(sock);

    return result;
}

inline bool send_all(const int sockfd, const void* buffer, size_t size) {
    auto data = static_cast<const char*>(buffer);
    while (size > 0) {
        const ssize_t n = ::send(sockfd, data, size, 0);
        if (n < 0) {
            perror("send_all");
            return false;
        }

        data += n;
        size -= n;
    }
    return true;
}

inline bool recv_all(const int sockfd, void* buffer, size_t size) {
    auto data = static_cast<char*>(buffer);
    while (size > 0) {
        const ssize_t n = ::recv(sockfd, data, size, 0);
        if (n <= 0) {
            if (n < 0) { perror("recv_all"); }
            return false;
        }
        data += n;
        size -= n;
    }
    return true;
}

}
