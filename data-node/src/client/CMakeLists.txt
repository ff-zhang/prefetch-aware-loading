add_library(client STATIC
  agent.cpp
)
target_link_libraries(client PUBLIC
  pipelines
  ibverbs
)
