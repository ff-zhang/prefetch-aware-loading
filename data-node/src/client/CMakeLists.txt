add_library(client STATIC
  agent.cpp
)
target_link_libraries(client PUBLIC
  pipelines
  ibverbs
)

# Panama FFM shared library (replaces the old JNI-based fdl target)
add_library(fdl SHARED
  client_ffi.cpp
)
target_link_libraries(fdl PRIVATE client)
set_target_properties(fdl PROPERTIES OUTPUT_NAME "fdl")
