cmake_minimum_required(VERSION 3.22 FATAL_ERROR)
project(prefetch-aware-loading VERSION 0.1.0)

# Enable compilation database
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Add additional warnings
if (MSVC)
  add_compile_options(/W4)
else()
  # Ignore DOCA deprecated warnings
  add_compile_options(-Wall -Wextra -Wpedantic -Wno-deprecated -Wno-deprecated-declarations -Wno-interference-size)
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/third_party
)

add_subdirectory(src)
