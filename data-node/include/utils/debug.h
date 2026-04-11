//
// Created by Felix Zhang on 2025-09-18.
//

#pragma once

#include <cstdio>
#include <ranges>
#include <string>
#include <sstream>

#ifndef NDEBUG
#define FDL_INFO(fmt, ...) \
    do { std::fprintf(stdout, fmt "\n", ##__VA_ARGS__); } while (0)
#else
#define FDL_INFO(fmt, ...) do {} while(0)
#endif

#define FDL_LOG(fmt, ...) \
    do { std::fprintf(stdout, fmt "\n", ##__VA_ARGS__); } while (0)

#define FDL_WARN(fmt, ...) \
    do { std::fprintf(stderr, fmt "\n", ##__VA_ARGS__); } while (0)

namespace fdl {

template<std::ranges::input_range Range>
std::string join(const Range& range, const std::string& delimiter) {
    std::ostringstream oss;

    auto it = std::ranges::begin(range);
    if (it != std::ranges::end(range)) {
        oss << *it;
        ++it;
    }
    for (; it != std::ranges::end(range); ++it) {
        oss << delimiter << *it;
    }

    return oss.str();
}

}
