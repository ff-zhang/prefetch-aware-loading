//
// Created by Felix Zhang on 2025-09-17.
//

#pragma once

#include <ranges>
#include <unordered_map>
#include <vector>

#include "parameters/constants.h"

namespace fdl {

template <typename R, typename V>
concept RangeOf = std::ranges::range<R> && std::same_as<std::ranges::range_value_t<R>, V>;

template<class... Ts>
struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

template <typename T>
struct alignas(CACHE_LINE_SIZE_DESS) CacheAligned {
    static_assert(sizeof(T) < CACHE_LINE_SIZE_DESS, "Type must be smaller than CACHE_LINE_SIZE_DESS");
    T value;
    char pad[CACHE_LINE_SIZE_DESS - sizeof(T)] = {};
};

using job_id_t = uint32_t;
using partition_id_t = uint32_t;

using response_map_t = std::unordered_map<partition_id_t, std::pair<std::vector<char>, size_t>>;

}
