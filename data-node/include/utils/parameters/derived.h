//
// Created by Felix Zhang on 4/6/26.
//

#pragma once

#include "configuration.h"

namespace fdl {

constexpr size_t SERVER_COUNT = SERVER_IPS.size();
static_assert(EC_DATA_BLOCK_COUNT + EC_PARITY_BLOCK_COUNT <= SERVER_COUNT);

// Number of DOCA buffers needed to cover one PARTITION_BUFFER_SIZE region
constexpr size_t MAX_CHAIN_LENGTH = PARTITION_BUFFER_SIZE / MAX_MEMCPY_BUF_SIZE + 2;

}
