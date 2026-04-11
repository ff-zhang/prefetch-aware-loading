//
// Created by Felix Zhang on 2/6/26.
//

#pragma once

#include "utils/network.h"

namespace fdl::rdma {

template<RangeOf<int> Range>
void worker(job_id_t job_id, partition_id_t num_partitions, size_t rank, Range&& secondary_fds);

}
