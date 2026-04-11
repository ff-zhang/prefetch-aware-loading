//
// Created by Felix Zhang on 3/21/26.
//

#pragma once

#include <array>
#include <cstddef>

namespace fdl {

/**
 * @param slots: total number of slots
 * @param items: total number of items distributed
 * @param index: location being queried
 * @return number of items at `index` when `items` are distributed across `slots` in a round-robin manner
 */
inline size_t items_at(const size_t slots, const size_t items, const size_t index) {
    if (slots == 0 || index >= slots) return 0;

    size_t count = items / slots;
    if (index < items % slots) {
        ++count;
    }
    return count;
}

template<std::semiregular T, std::size_t N>
constexpr auto repeat(const T& value) -> std::array<T, N> {
    std::array<T, N> ret;
    ret.fill(value);
    return ret;
}

}
