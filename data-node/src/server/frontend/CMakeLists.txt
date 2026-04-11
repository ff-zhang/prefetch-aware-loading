
add_library(frontend STATIC
  job.cpp
  listener.cpp
  tracker.cpp
  RDMA/worker.cpp
  ${PROTO_SRCS}
)
target_link_libraries(frontend PUBLIC atomic backend)
target_include_directories(frontend PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${BRPC_INCLUDE_DIR}
  ${EZPWD_INCLUDE_DIR}
)
