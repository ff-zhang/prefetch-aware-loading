if (DOCA_FOUND)
  add_subdirectory(DPU)

  # Add SPDK backend sources when using storage backend
  if(NOT USE_MEMORY_BACKEND)
    target_sources(backend PRIVATE
      SPDK/spdk.cpp
    )
  endif()
else()
  add_library(backend STATIC
    host/memory.cpp
  )
endif()