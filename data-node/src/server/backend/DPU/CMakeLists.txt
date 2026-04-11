add_library(backend STATIC
  DMA/memory.cpp
)
target_compile_definitions(backend PUBLIC DOCA_ENABLED)
target_include_directories(backend PUBLIC ${DOCA_INCLUDE_DIRS})
target_link_libraries(backend PUBLIC
  pipelines
  ${DOCA_LIBRARIES}
)
target_link_directories(backend PUBLIC ${DOCA_LIBRARY_DIRS})

add_executable(host
  DMA/host.cpp
)
target_compile_definitions(host PUBLIC DOCA_ENABLED)
target_include_directories(host PUBLIC ${DOCA_INCLUDE_DIRS})
target_link_libraries(host PUBLIC
  remote
  ${DOCA_LIBRARIES}
)
target_link_directories(host PUBLIC ${DOCA_LIBRARY_DIRS})