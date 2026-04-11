# Base sources for pipelines library
set(PIPELINES_SOURCES
  DOCA/doca.cpp
  DOCA/DMA/dma.cpp
  DOCA/EC/ec.cpp
  RDMA/device.cpp
  RDMA/queue_pair.cpp
)

# Add SPDK sources when using storage backend
if(NOT USE_MEMORY_BACKEND)
  list(APPEND PIPELINES_SOURCES SPDK/spdk.cpp)
endif()

add_library(pipelines STATIC ${PIPELINES_SOURCES})

# Compile definitions
target_compile_definitions(pipelines PUBLIC DOCA_ENABLED)
if(NOT USE_MEMORY_BACKEND)
  target_compile_definitions(pipelines PUBLIC SPDK_ENABLED)
else()
  target_compile_definitions(pipelines PUBLIC MEMORY_BACKEND_ENABLED)
endif()

# Include directories
target_include_directories(pipelines PUBLIC ${DOCA_INCLUDE_DIRS})
if(NOT USE_MEMORY_BACKEND)
  target_include_directories(pipelines PUBLIC ${SPDK_INCLUDE_DIRS})
endif()

# Link directories
target_link_directories(pipelines PUBLIC ${DOCA_LIBRARY_DIRS})
if(NOT USE_MEMORY_BACKEND)
  target_link_directories(pipelines PUBLIC ${SPDK_LIBRARY_DIRS})
endif()

# Link libraries
target_link_libraries(pipelines PUBLIC
  ibverbs
  ${DOCA_LIBRARIES}
)
if(NOT USE_MEMORY_BACKEND)
  target_link_libraries(pipelines PUBLIC
    ${SPDK_LIBRARIES}
    ${DPDK_LIBRARIES}
    ${ISAL_LIBRARIES}
    ${SPDK_SYSLIBS}
  )
endif()

add_library(remote STATIC
  DOCA/doca.cpp
  DOCA/DMA/dma.cpp
  DOCA/DMA/remote.cpp
)
target_compile_definitions(remote PUBLIC DOCA_ENABLED)
target_include_directories(remote PUBLIC ${DOCA_INCLUDE_DIRS})
target_link_libraries(remote PUBLIC ${DOCA_LIBRARIES})
target_link_directories(remote PUBLIC ${DOCA_LIBRARY_DIRS})