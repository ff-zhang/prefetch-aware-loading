//
// Created by Felix Zhang on 2025-09-04.
//

#pragma once

#include <cstdint>

constexpr size_t KiB = 1024;
constexpr size_t MiB = 1024 * KiB;
constexpr size_t GiB = 1024 * MiB;

namespace fdl {

#if defined(__cpp_lib_hardware_interference_size)
    static constexpr size_t CACHE_LINE_SIZE_DESS = std::hardware_destructive_interference_size;
#else
    // Fallback value
    static constexpr size_t CACHE_LINE_SIZE_DESS = 64;
#endif

constexpr size_t MAX_MEMCPY_BUF_SIZE = 2 * MiB;

}
