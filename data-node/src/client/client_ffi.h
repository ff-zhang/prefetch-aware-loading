//
// Flat C API for Panama FFM interop.
// Wraps the C++ ISource/ISink client interface.
//

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

void* fdl_client_create(void);

void* fdl_client_enroll(void* client, int job_id, int num_partitions);

void fdl_client_unregister(void* client, int job_id);

void fdl_client_destroy(void* client);

void fdl_job_send(void* job, int partition, const char* data, int len);

// Receive data for the requested partition.
//
//   partition   – partition ID to fetch
//
// On return:
//   returns an opaque handle to the received data, or NULL on failure.
//
// The caller is responsible for calling fdl_job_release on the handle.
void* fdl_job_receive(void* job, int partition);

// Get the regions from a received handle.
//
//   handle      - handle returned by fdl_job_receive
//   out_data    - array of size 2 to hold the data pointers
//   out_sizes   - array of size 2 to hold the region sizes
//
// Returns the number of regions (1 or 2).
int fdl_handle_get_regions(void* handle, void** out_data, int* out_sizes);

// Get the item size from a received handle.
//
//   handle      - handle returned by fdl_job_receive
//
// Returns the item size.
long long fdl_handle_get_item_size(void* handle);

void fdl_job_sync(void* job, int partition);

void fdl_job_release(void* job, int partition, void *handle);

void fdl_job_close(void* job);

#ifdef __cplusplus
}
#endif

