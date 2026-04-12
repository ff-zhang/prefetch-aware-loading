add_library(pipelines STATIC
  RDMA/device.cpp
  RDMA/queue_pair.cpp
)
target_link_libraries(pipelines PUBLIC
  ibverbs
)
