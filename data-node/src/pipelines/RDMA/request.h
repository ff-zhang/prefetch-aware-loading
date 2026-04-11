//
// Created by Felix Zhang on 2/12/26.
//

#pragma once

#include <cstdint>

namespace fdl::rdma {
    struct DecodeRequest {
        struct data_t {
            uint64_t partition;
            uint64_t raddr;
        };
        data_t* data;

        void initialize(char* buffer) {
            data = reinterpret_cast<data_t*>(buffer);
        }

        static constexpr size_t size_of() {
            return sizeof(data_t);
        }
    };
}
