# DistributedFS

A peer-to-peer distributed file storage system written in Go. Stores files across multiple nodes with automatic replication, AES-256 encryption, and fault tolerance. Perfect for decentralized backup and P2P file sharing without central servers.

## Core Features

- **Automatic Replication**: Store a file once, replicated to all peers for redundancy and availability.
- **AES-256 Encryption**: All files encrypted at rest with AES-256-CTR mode before storage on disk.
- **Content-Addressable Storage**: Files indexed by SHA-1 hash in nested directory structure for deduplication.
- **P2P Communication**: Nodes communicate via TCP with custom protocol for metadata and file streaming.
- **Bootstrap Nodes**: New nodes join network via bootstrap entry points for easy horizontal scaling.
- **Fault Tolerance**: Files available on multiple nodes; system survives node failures without data loss.

---

## Installation & Setup

### Prerequisites
- **Go** 1.22.2 or higher
- **git**

### Clone the Repository
```bash
git clone -b srniraula https://github.com/Sanim27/DistributedFS.git
cd DistributedFS
```

### Build the Project
```bash
make build
```
This compiles the binary to `bin/fs`

---

## Usage

### 1. Upload a File

Upload a file to the distributed network:

```bash
make upload FILE=hello.txt
```

**What happens:**
- Starts 3 demo nodes on ports 3000, 4000, and 5000
- Uploads `hello.txt` from your project directory
- Automatically replicates to all nodes
- Files stored in encrypted format in respective `*_network/` directories


### 2. Run Demo

Run the default demo that shows store and retrieve operations:

```bash
make run
```

**What happens:**
- Starts 3 nodes
- Stores a test file
- Deletes it locally on the origin node
- Retrieves it from the network (demonstrates P2P retrieval)
- Prints the file contents

### 3. Run Tests

```bash
make test
```

### 4. Clean Up

```bash
make clean
```
