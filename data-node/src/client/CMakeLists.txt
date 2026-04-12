add_executable(agent agent.cpp)
target_link_libraries(agent PRIVATE
  pipelines
  ibverbs
)
