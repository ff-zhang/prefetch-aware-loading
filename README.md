# PADL
Pronounced *paddle*, this is (a proof-of-concept for) a **P**CIe-**A**ware **D**ata-**L**oader.
It targets ML training workloads within PyTorch.

## Getting Started

Begin by cloning the repository.
If the submodules were not included, they can be fetched with the command:
```bash
git submodule update --init --recursive
```

### Prerequisites

- Linux host with RDMA-capable NIC and the `libibverbs` development headers
- tmpfs mount on the compute node
- CMake ≥ 3.22 and a C++20-capable compiler

### Building

To build all the applications, use the following commands:
```bash
cd ./data-node
cmake -B cmake-build-release -S . -DCMAKE_BUILD_TYPE=Release -Wno-deprecated -Wno-deprecated-declarations
cmake --build cmake-build-release
```

The two data-loader binaries are `server` and `client`, found under `cmake-build-release/src/`.

### Running

Begin by starting the server on the data node:

```bash
./cmake-build-release/src/server/server \
    --port 9000 \
    --batch-size 32 \
    --sample-size 4096
```

The server listens for one client at a time and sends the batch configuration to the connecting agent before pushing batches.
Note that `batch-size × sample-size` must be less than 1 GiB, the hard-coded ring buffer capacity.

Start the client on the compute node:

```bash
./cmake-build-release/src/client/client \
    --ip <DATA_NODE_IP> \
    --port 9000 \
    --tmpfs /dev/shm
```

Each received batch is written to `<tmpfs>/shard_<N>.pt`, starting from 0 and incrementing each time.
Both processes print throughput statistics every 100 batches.
