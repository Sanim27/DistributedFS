# DistributedFS

A distributed file system implementation in Go, featuring P2P networking, content-addressable storage, and encryption.

## Prerequisites

- Go (version 1.22 or higher recommended)

## Getting Started

### Running the Project

To build and run the simulation:

```sh
make run
```

This will compile the project and execute the `main.go` entry point, which demonstrates spinning up multiple nodes and performing file operations (Store, Delete, Get).

### Building

To build the binary without running:

```sh
make build
```

The executable will be located at `bin/fs`.

### Testing

To run the test suite:

```sh
make test
```

## Features

- **P2P Communication**: Custom TCP transport layer for node-to-node communication.
- **Distributed Storage**: Nodes can store and retrieve files across the network.
- **Bootstrapping**: Nodes can discover peers via bootstrap nodes.
- **Encryption**: Data is encrypted at rest.
- **Content Addressable Storage (CAS)**: Files are stored based on their content hash.
