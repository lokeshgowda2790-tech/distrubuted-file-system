# Mini Distributed File System (DFS)

A simplified **Distributed File System (DFS)** inspired by Google File System (GFS) and Hadoop Distributed File System (HDFS). This project demonstrates the core concepts of distributed storage, file chunking, replication, fault tolerance, and metadata management using Java.

## Overview

The system is designed to store large files across multiple storage nodes while ensuring data availability and reliability through replication. A central Master Server manages metadata and coordinates file storage operations.

## Architecture

### Master Server

The Master Server acts as the coordinator of the distributed file system.

Responsibilities:

* Maintains metadata for files and chunks
* Tracks chunk locations across storage nodes
* Allocates unique chunk IDs
* Decides replica placement for chunks
* Monitors Storage Nodes through heartbeats
* Detects node failures and initiates re-replication
* Persists metadata for recovery after restart

### Storage Node

Storage Nodes are responsible for storing actual file chunks.

Responsibilities:

* Register with the Master Server
* Store chunk files on local disk
* Serve chunk upload and download requests
* Send periodic heartbeat messages
* Report available storage capacity
* Participate in chunk replication

### DFS Client

The DFS Client provides the user interface for interacting with the file system.

Supported Operations:

* `put <file>` – Upload a file to DFS
* `get <file>` – Download a file from DFS
* Automatic file chunking and reassembly
* Replica-aware chunk retrieval

## Features

### Phase 1 Features

#### File Chunking

* Splits files into fixed-size chunks
* Supports files of any size
* Uses streaming I/O to avoid loading entire files into memory

#### Replication

* Configurable replication factor (default = 2)
* Stores each chunk on multiple Storage Nodes
* Improves fault tolerance and availability

#### Metadata Management

* Maintains mappings:

  * File → Chunks
  * Chunk → Storage Nodes

#### Streaming I/O

* Efficient memory usage
* Supports large file uploads and downloads

### Phase 2 Features

#### Heartbeat Mechanism

* Storage Nodes periodically send heartbeat messages
* Master tracks node health and availability

#### Failure Detection

* Detects failed nodes based on heartbeat timeout
* Removes failed nodes from future allocations

#### Automatic Re-Replication

* Restores replication factor when a node fails
* Copies chunks to healthy nodes automatically

#### Checksum Validation

* SHA-256 based chunk integrity verification
* Detects data corruption during transfer and retrieval

#### Metadata Persistence

* Saves metadata to disk
* Restores metadata after Master restart

#### Node Recovery

* Supports rejoining Storage Nodes
* Synchronizes metadata after node recovery

## Project Structure

```text
MiniDFS/
├── master/
│   └── MasterServer.java
│
├── storage/
│   └── StorageNode.java
│
├── client/
│   └── DFSClient.java
│
├── metadata/
│   └── metadata.json
│
├── chunks/
│   └── chunk files
│
├── pom.xml
└── README.md
```

## Technologies Used

* Java 17
* Maven
* Java Sockets
* Multithreading
* File I/O
* SHA-256 Checksums
* JSON Metadata Storage

## How It Works

### Upload (PUT)

1. Client selects a file.
2. File is split into chunks.
3. Client requests chunk allocation from Master.
4. Master selects Storage Nodes for replicas.
5. Client uploads chunks to assigned nodes.
6. Master updates metadata.

### Download (GET)

1. Client requests file metadata from Master.
2. Master returns chunk locations.
3. Client fetches chunks from Storage Nodes.
4. Checksums are verified.
5. Chunks are reassembled into the original file.

### Failure Handling

1. Storage Nodes send heartbeats periodically.
2. Master detects node failures.
3. Replication count is checked.
4. Missing replicas are recreated automatically.
5. Data remains available despite node failures.

## Example Commands

```bash
# Upload a file
put sample.txt

# Download a file
get sample.txt
```

## Future Enhancements

* Leader election for Master redundancy
* Load balancing across Storage Nodes
* Data compression
* Encryption for secure data transfer
* Web-based monitoring dashboard
* Rack-aware replica placement
* Distributed metadata management

## Learning Outcomes

This project helped in understanding:

* Distributed System Fundamentals
* File Chunking Strategies
* Replication Mechanisms
* Fault Tolerance Techniques
* Metadata Management
* Heartbeat-Based Failure Detection
* Client-Server Communication
* Storage System Design

## Inspiration

This project is inspired by:

* Google File System (GFS)
* Hadoop Distributed File System (HDFS)

It is intended for educational purposes and demonstrates the core concepts behind modern distributed storage systems.
