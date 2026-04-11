cmake_minimum_required(VERSION 3.22 FATAL_ERROR)
project(dess VERSION 0.1.0)

option(USE_MEMORY_BACKEND "Use in-memory backend rather than storage backend" ON)

# Enable compilation database
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(PkgConfig REQUIRED)
set(ENV{PKG_CONFIG_PATH} "/opt/mellanox/doca/lib/x86_64-linux-gnu/pkgconfig:$ENV{PKG_CONFIG_PATH}")
pkg_check_modules(DOCA
  doca-argp doca-common doca-dma doca-erasure-coding doca-rdma
)

set(EZPWD_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/third_party/ezpwd-reed-solomon/c++" CACHE PATH "Path to EZPWD headers")

# Add additional warnings
if (MSVC)
  add_compile_options(/W4)
else()
  # Ignore DOCA deprecated warnings
  add_compile_options(-Wall -Wextra -Wpedantic -Wno-deprecated -Wno-deprecated-declarations -Wno-interference-size)
endif()

# SPDK (i.e., storage backend) configuration
if(NOT USE_MEMORY_BACKEND)
  # Detect machine's architecture
  execute_process(
    COMMAND dpkg-architecture -qDEB_HOST_MULTIARCH
    OUTPUT_VARIABLE HOST_MULTIARCH
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )

  # SPDK paths (using DOCA's DPDK to minimize conflicts)
  set(SPDK_ROOT ${CMAKE_SOURCE_DIR}/third_party/spdk)
  set(SPDK_BUILD_DIR ${SPDK_ROOT}/build)

  # DOCA's DPDK paths
  set(DOCA_DPDK_DIR "/opt/mellanox/dpdk")
  set(DOCA_DPDK_LIB_DIR "${DOCA_DPDK_DIR}/lib/${HOST_MULTIARCH}")
  set(DOCA_DPDK_INC_DIR "${DOCA_DPDK_DIR}/include/dpdk")
  set(DOCA_DPDK_INC_DIR_ARCH "${DOCA_DPDK_DIR}/include/${HOST_MULTIARCH}/dpdk")

  # SPDK include directories
  set(SPDK_INCLUDE_DIRS
    ${SPDK_ROOT}/include
    ${DOCA_DPDK_INC_DIR}
    ${DOCA_DPDK_INC_DIR_ARCH}
  )

  # SPDK library directories
  set(SPDK_LIBRARY_DIRS
    ${SPDK_BUILD_DIR}/lib
    ${DOCA_DPDK_LIB_DIR}
  )

  # Core SPDK libraries
  set(SPDK_LIBRARIES
    -Wl,--whole-archive
    spdk_event
    spdk_event_iobuf
    spdk_event_accel
    spdk_event_vmd
    spdk_event_keyring
    spdk_event_sock
    spdk_event_bdev
    spdk_bdev
    spdk_bdev_malloc
    spdk_bdev_nvme
    spdk_nvme
    spdk_rdma_provider
    spdk_rdma_utils
    spdk_accel
    spdk_dma
    spdk_vmd
    spdk_keyring
    spdk_thread
    spdk_env_dpdk
    spdk_log
    spdk_util
    spdk_json
    spdk_rpc
    spdk_jsonrpc
    spdk_sock
    spdk_sock_posix
    spdk_trace
    spdk_notify
    spdk_init
    -Wl,--no-whole-archive
  )

  # Additional DPDK libraries needed by SPDK
  set(DPDK_LIBRARIES
    rte_eal
    rte_mempool
    rte_ring
    rte_mbuf
    rte_bus_pci
    rte_pci
    rte_mempool_ring
    rte_telemetry
    rte_kvargs
    rte_rcu
    rte_power
    rte_ethdev
    rte_net
    rte_ip_frag
    rte_vhost
    rte_hash
    rte_dmadev
    rte_cryptodev
  )

  # ISA-L libraries (built with SPDK)
  set(ISAL_LIBRARIES
    ${SPDK_ROOT}/isa-l/.libs/libisal.a
    ${SPDK_ROOT}/isa-l-crypto/.libs/libisal_crypto.a
  )

  # System libraries required by SPDK
  set(SPDK_SYSLIBS
    rdmacm
    ibverbs
    numa
    pthread
    rt
    uuid
    ssl
    crypto
    m
    lz4
    fuse3
    keyutils
    aio
    bsd
    dl
  )
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/third_party
)

add_subdirectory(src)
add_subdirectory(tests EXCLUDE_FROM_ALL)
add_subdirectory(third_party)
