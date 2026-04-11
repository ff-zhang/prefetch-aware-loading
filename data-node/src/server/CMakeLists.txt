add_subdirectory(backend)
add_subdirectory(frontend)

add_executable(server main.cpp)
target_link_directories(server PRIVATE
  ${BRPC_LIBRARY_DIR}
  ${EZPWD_INCLUDE_DIR}
)
target_link_libraries(server PRIVATE
  backend
  frontend
  pipelines
  protobuf
  glog
  gflags
  ssl
  crypto
)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    target_compile_options(server PRIVATE -mcx16)
endif()
