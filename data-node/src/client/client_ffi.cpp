//
// Flat C API implementation for Panama FFM interop.
// Wraps the C++ ISource/ISink client behind extern "C" functions.
//
#include "client_ffi.h"

#include "agent.h"

static fdl::ShuffleClient* as_client(void* h) { return static_cast<fdl::ShuffleClient*>(h); }
static fdl::ShuffleJob* as_job(void* h) { return static_cast<fdl::ShuffleJob*>(h); }

void* fdl_client_create() {
    try {
        return new fdl::ShuffleClient();
    } catch (...) {
        return nullptr;
    }
}

void* fdl_client_enroll(void* client, const int job_id, const int num_partitions) {
    try {
        return as_client(client)->enroll(static_cast<fdl::job_id_t>(job_id), static_cast<fdl::partition_id_t>(num_partitions));
    } catch (...) {
        return nullptr;
    }
}

void fdl_client_unregister(void *client, const int job_id){
    if (client) as_client(client)->unregister(static_cast<fdl::job_id_t>(job_id));
}

void fdl_client_destroy(void *client){
    if (client) delete as_client(client);
}

void fdl_job_send(void *job, const int partition, const char *data, const int len){
    if (job) as_job(job)->send(static_cast<fdl::partition_id_t>(partition), data, static_cast<size_t>(len));
}

void* fdl_job_receive(void *job, const int partition){
    if (!job) return nullptr;
    auto result = as_job(job)->receive(static_cast<fdl::partition_id_t>(partition));
    return new fdl::UnifiedHandle(std::move(result));
}

int fdl_handle_get_regions(void* handle, void** out_data, int* out_sizes) {
    if (!handle) return 0;
    const auto h = static_cast<fdl::UnifiedHandle*>(handle);
    const int num_regions = std::visit(fdl::overloaded {
    [&](const fdl::RingBufferRegion& r) {
        out_data[0] = static_cast<void*>(r.data);
        out_sizes[0] = static_cast<int>(r.size);
        return 1;
    },
    [&](const std::array<fdl::RingBufferRegion, 2>& regions) {
        out_data[0] = static_cast<void*>(regions[0].data);
        out_sizes[0] = static_cast<int>(regions[0].size);
        out_data[1] = static_cast<void*>(regions[1].data);
        out_sizes[1] = static_cast<int>(regions[1].size);
        return 2;
    }
    }, h->regions);
    return num_regions;
}

long long fdl_handle_get_item_size(void* handle) {
    if (!handle) return 0;
    const auto h = static_cast<fdl::UnifiedHandle*>(handle);
    const long long item_size = std::visit(fdl::overloaded {
        [&](const std::span<uint64_t>& m) -> long long {
            return m.empty() ? 0 : m[0] & 0xFFFFFFFFULL;
        },
        [&](const std::array<std::span<uint64_t>, 2>& m) -> long long {
            if (!m[0].empty()) return m[0][0] & 0xFFFFFFFFULL;
            if (!m[1].empty()) return m[1][0] & 0xFFFFFFFFULL;
            return 0;
        }
    }, h->metadata);
    return item_size;
}

void fdl_job_sync(void* job, const int partition) {
    if (!job) return;
    as_job(job)->sync(static_cast<fdl::partition_id_t>(partition));
}

void fdl_job_release(void* job, const int partition, void *handle){
    if (job && handle) {
        const auto h = static_cast<fdl::UnifiedHandle*>(handle);
        as_job(job)->release(partition, std::move(*h));
        delete h;
    }
}

void fdl_job_close(void* job){
    if (job) as_job(job)->close();
}
