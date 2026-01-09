# Predastore Distributed Object Storage Architecture

Predastore is a distributed, S3-compatible, erasure-coded object store designed for high throughput, strong consistency, and fault tolerance. This document serves as the authoritative implementation guide for developers.

---

## Table of Contents

1. [Core Goals](#1-core-goals)
2. [Quick Start](#2-quick-start)
3. [System Architecture](#3-system-architecture)
4. [Logical Data Model](#4-logical-data-model)
5. [Global Metadata (s3db)](#5-global-metadata-s3db)
6. [Local Shard Storage](#6-local-shard-storage)
7. [Reed-Solomon Encoding](#7-reed-solomon-encoding)
8. [Hash Ring Placement](#8-hash-ring-placement)
9. [QUIC Protocol](#9-quic-protocol)
10. [S3 Operations](#10-s3-operations)
11. [Failure Handling & Repair](#11-failure-handling--repair)
12. [Cluster Evolution](#12-cluster-evolution)
13. [Security](#13-security)
14. [Deployment Modes](#14-deployment-modes)
15. [Configuration Reference](#15-configuration-reference)
16. [Developer Reference](#16-developer-reference)
17. [Tunable Parameters](#17-tunable-parameters)
18. [Future Work](#18-future-work)

---

# 1. Core Goals

- High throughput read/write performance
- Strong consistency for metadata via Raft consensus
- Efficient disk layout for large objects
- Ability to scale from 3 nodes (RS(2,1)) to 32+ nodes (RS(3,2))
- Node-to-node QUIC streaming for partial reads and reconstruction
- Local rebuildability if Badger DBs are lost
- Smooth cluster evolution as nodes are added or RS scheme changes

---

# 2. Quick Start

### Starting a Cluster

```bash
# Run full predastore - Simulate 3 multi-node database, and 5 shard instances locally.
./bin/s3d -backend distributed -config s3/tests/config/cluster.toml

# Run full predastore with distributed backend (auto-launches DB and QUIC servers) as node 1
./bin/s3d -backend distributed -config ./s3/tests/config/cluster.toml -node 1

# Run only the database server
./bin/s3d -db -config ./s3/tests/config/cluster.toml -db-node 1

# Run specific QUIC shard node
./bin/s3d -backend distributed -config ./s3/tests/config/cluster.toml -node 2
```

### CLI Flags

| Flag | Environment | Description |
|------|-------------|-------------|
| `-config` | `CONFIG` | Path to configuration file (default: config/server.toml) |
| `-backend` | `BACKEND` | Storage backend: filesystem or distributed |
| `-node` | `NODE` | QUIC shard node ID to run (-1 = dev mode, runs all locally) |
| `-db` | `DB_ONLY=true` | Run only the distributed database server |
| `-db-node` | `DB_NODE` | Database node ID to run (-1 = auto-detect or run all locally) |
| `-port` | `PORT` | S3 API server port (default: 443) |
| `-tls-cert` | - | Path to TLS certificate |
| `-tls-key` | - | Path to TLS private key |

See `s3/tests/config/cluster.toml` for a complete example configuration.

---

# 3. System Architecture

## Overall Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            S3 Client (AWS CLI/SDK)                      │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Predastore S3D (HTTP/Fiber)                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────────┐   │
│  │ Auth/SigV4  │  │  Routing    │  │  Backend    │  │ GlobalState   │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └───────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
        │                                                    │
        │                                                    ▼
        │                               ┌─────────────────────────────────┐
        │                               │   Distributed s3db Cluster      │
        │                               │  ┌─────────┐  ┌─────────┐       │
        │                               │  │ Leader  │  │Follower │  ...  │
        │                               │  │         │  │         │       │
        │                               │  │ BoltDB  │  │ BoltDB  │       │
        │                               │  │(Raft)   │  │(Raft)   │       │
        │                               │  │         │  │         │       │
        │                               │  │ Badger  │  │ Badger  │       │
        │                               │  │(FSM)    │  │(FSM)    │       │
        │                               │  └─────────┘  └─────────┘       │
        │                               │        Raft Consensus           │
        │                               └─────────────────────────────────┘
        │
        │  ┌─────────────────┼─────────────────┐
        ▼  ▼                 ▼                 ▼
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │  QUIC Node  │   │  QUIC Node  │   │  QUIC Node  │
    │  (Shard 0)  │   │  (Shard 1)  │   │  (Shard 2)  │
    ├─────────────┤   ├─────────────┤   ├─────────────┤
    │ Local WAL   │   │ Local WAL   │   │ Local WAL   │
    │ Local Badger│   │ Local Badger│   │ Local Badger│
    └─────────────┘   └─────────────┘   └─────────────┘
```

## Component Overview

| Component | Purpose |
|-----------|---------|
| **S3D (HTTP Server)** | Handles S3 API requests, authentication, routing |
| **s3db Cluster** | Distributed Raft-based metadata store |
| **QUIC Shard Nodes** | Store erasure-coded object shards in WAL files |
| **GlobalState** | Abstraction layer for metadata access (local or distributed) |

---

# 4. Logical Data Model

## Objects → Chunks → Shards → Segments → Blocks

```
Object
 └── Chunks (logical 32 MB units)
      └── Shards (per-node RS slices, 32 MB each)
           └── Segments (compressed 64 KB units, note compression OPTIONAL)
                └── Blocks (8 KB logical)
```

### Size Reference

| Unit | Size | Description |
|------|------|-------------|
| Block | 8 KB | Smallest addressable unit |
| Segment | 64 KB | 8 blocks, independently compressed |
| Shard | 32 MB | Per-node RS slice (4096 blocks / 512 segments) |
| Chunk | 32 MB | Logical unit for RS encoding |

---

# 5. Global Metadata (s3db)

Predastore uses a distributed Raft-based database (s3db) to store global state including bucket ownership, object metadata, and shard location mappings.

## s3db Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Predastore S3D Binary                            │
│  ┌───────────────────────────────┐   ┌───────────────────────────────┐  │
│  │      S3 API Server            │   │     s3db Server(s)            │  │
│  │   (Fiber HTTP/HTTPS)          │   │   (Fiber HTTPS + Raft)        │  │
│  │                               │   │                               │  │
│  │  ┌──────────┐ ┌──────────┐    │   │  ┌────────┐  ┌────────────┐   │  │
│  │  │ Handlers │ │ Backend  │────┼───┼─▶│ Client │  │ Raft Node  │   │  │
│  │  └──────────┘ └──────────┘    │   │  └────────┘  └────────────┘   │  │
│  └───────────────────────────────┘   └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Storage Architecture: BoltDB vs BadgerDB

The s3db cluster uses **two different databases** for distinct purposes:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        s3db Node Storage                                │
│                                                                         │
│  ┌─────────────────────────────┐    ┌─────────────────────────────┐    │
│  │         BoltDB              │    │         BadgerDB            │    │
│  │     (raft.db file)          │    │     (badger/ directory)     │    │
│  │                             │    │                             │    │
│  │  ┌───────────────────────┐  │    │  ┌───────────────────────┐  │    │
│  │  │ Raft Log Store        │  │    │  │ Application Data      │  │    │
│  │  │ - Committed log       │  │    │  │ - Object metadata     │  │    │
│  │  │   entries             │  │    │  │ - Bucket info         │  │    │
│  │  │ - Term/vote state     │  │    │  │ - ARN mappings        │  │    │
│  │  └───────────────────────┘  │    │  └───────────────────────┘  │    │
│  │                             │    │                             │    │
│  │  ┌───────────────────────┐  │    │  FSM (Finite State         │    │
│  │  │ Stable Store          │  │    │  Machine) applies          │    │
│  │  │ - Current term        │  │    │  committed commands        │    │
│  │  │ - Voted for           │  │    │  here                      │    │
│  │  │ - Cluster config      │  │    │                             │    │
│  │  └───────────────────────┘  │    └─────────────────────────────┘    │
│  └─────────────────────────────┘                                        │
│                                                                         │
│  ┌─────────────────────────────┐                                        │
│  │      Snapshot Store         │                                        │
│  │    (snapshots/ directory)   │                                        │
│  │  - Point-in-time FSM state  │                                        │
│  │  - Used for log compaction  │                                        │
│  │  - New node catch-up        │                                        │
│  └─────────────────────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why Two Databases?

| Component | Database | Purpose |
|-----------|----------|---------|
| **Raft Log Store** | BoltDB | Stores the ordered sequence of commands (log entries) that Raft replicates. Required by HashiCorp Raft library. |
| **Stable Store** | BoltDB | Stores Raft metadata: current term, voted-for candidate, cluster configuration. |
| **FSM State** | BadgerDB | Stores actual application data (objects, buckets). Commands from the log are applied here. |
| **Snapshots** | File-based | Periodic snapshots of BadgerDB state for log compaction and new node bootstrap. |

**Key Insight**: BoltDB stores *how to build state* (the Raft log), while BadgerDB stores *the actual state* (application data). This separation is required by the Raft consensus protocol.

### Directory Structure

Each database node creates the following directory structure:

```
<path>/node-<id>/
├── raft.db           # BoltDB: Raft log + stable store
├── badger/           # BadgerDB: Application data (FSM state)
│   ├── 000001.vlog
│   ├── 000001.sst
│   └── MANIFEST
└── snapshots/        # Raft snapshots for recovery
    └── <snapshot-id>/
```

## Raft Consensus

Leveraging the HashiCorp Raft library: https://github.com/hashicorp/raft

### Leader Election Process

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Raft Leader Election                                │
│                                                                         │
│  1. Nodes start as Followers                                            │
│     - Wait for heartbeats from leader                                   │
│     - Timeout: ElectionTimeout (default: 1000ms)                        │
│                                                                         │
│  2. On heartbeat timeout, node becomes Candidate                        │
│     - Increments term                                                   │
│     - Votes for itself                                                  │
│     - Requests votes from other nodes                                   │
│                                                                         │
│  3. Node becomes Leader if it receives majority votes                   │
│     - For 3 nodes: needs 2 votes (including self)                       │
│     - For 5 nodes: needs 3 votes                                        │
│                                                                         │
│  4. Leader sends heartbeats to maintain authority                       │
│     - Interval: HeartbeatTimeout (default: 1000ms)                      │
│     - Followers reset election timer on each heartbeat                  │
│                                                                         │
│  5. If leader fails, followers timeout and new election begins          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Write Path (Requires Leader)

```
Client ──▶ Any Node ──▶ Redirect to Leader ──▶ Leader
                                                  │
                                                  ▼
                                          ┌───────────────┐
                                          │ 1. Create     │
                                          │    Command    │
                                          └───────┬───────┘
                                                  │
                                          ┌───────▼───────┐
                                          │ 2. Append to  │
                                          │    Raft Log   │
                                          └───────┬───────┘
                                                  │
                                          ┌───────▼───────┐
                                          │ 3. Replicate  │
                                          │    to Followers│
                                          └───────┬───────┘
                                                  │
                                          ┌───────▼───────┐
                                          │ 4. Wait for   │
                                          │    Majority   │
                                          └───────┬───────┘
                                                  │
                                          ┌───────▼───────┐
                                          │ 5. Apply to   │
                                          │    FSM/Badger │
                                          └───────┬───────┘
                                                  │
                                          ┌───────▼───────┐
                                          │ 6. Return     │
                                          │    Success    │
                                          └───────────────┘
```

### Read Path (Any Node)

Reads can go to any node but may return stale data on followers:

```
Client ──▶ Any Node ──▶ Read from local BadgerDB ──▶ Return
```

## Command Structure

Commands are serialized as JSON and replicated through Raft:

```go
type Command struct {
    Type  CommandType `json:"type"`   // CommandPut or CommandDelete
    Table string      `json:"table"`  // e.g., "objects", "buckets"
    Key   []byte      `json:"key"`    // Binary key (base64 encoded in JSON)
    Value []byte      `json:"value"`  // Binary value (base64 encoded in JSON)
}
```

**Important**: The `Key` field uses `[]byte` instead of `string` to safely handle binary keys containing arbitrary bytes. JSON automatically base64-encodes `[]byte` fields, preventing corruption of binary data during serialization.

## GlobalState Interface

The distributed backend uses a `GlobalState` interface to abstract storage operations:

```go
type GlobalState interface {
    Get(table string, key []byte) ([]byte, error)
    Set(table string, key []byte, value []byte) error
    Delete(table string, key []byte) error
    Exists(table string, key []byte) (bool, error)
    ListKeys(table string, prefix []byte) ([][]byte, error)
    Scan(table string, prefix []byte, fn func(key, value []byte) error) error
    Close() error
}
```

Two implementations:
- **LocalState**: Wraps a local Badger DB (for single-node or testing)
- **DistributedState**: Wraps an s3db.Client (for production clusters)

## Global State Data Model

| Table | Key Pattern | Value | Purpose |
|-------|-------------|-------|---------|
| objects | `arn:aws:s3:::<bucket>/<key>` | object hash (32 bytes) | Object listing |
| objects | `<object-hash>` | ObjectToShardNodes (gob) | Shard location map |
| objects | `deleted:<bucket>/<key>` | DeletedObjectInfo (gob) | Compaction tracking |
| buckets | `<bucket-name>` | bucket metadata | Bucket ownership |

## ChunkLayout Metadata

```go
type RSConfig struct {
    DataShards   uint8
    ParityShards uint8
}

type ChunkLayout struct {
    ObjectID   [32]byte
    ChunkIndex uint32
    Size       uint64
    RS         RSConfig
    RingEpoch  uint32
}
```

Key format: `[ 32 byte objectID | 4 byte chunk index ]`

## s3db REST API

The s3db service provides a REST API with AWS Signature V4 authentication:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/put/{table}/{hex-key}` | Store a key-value pair |
| GET | `/v1/get/{table}/{hex-key}` | Retrieve a value |
| DELETE | `/v1/delete/{table}/{hex-key}` | Delete a key |
| GET | `/v1/scan/{table}?prefix=X&limit=N` | Scan keys with prefix |
| GET | `/v1/leader` | Get current leader info |
| POST | `/v1/join` | Join a new node to cluster |
| GET | `/status` | Node status and Raft state |
| GET | `/health` | Health check |

**Note**: Keys in the URL path are hex-encoded to safely transport binary data.

---

# 6. Local Shard Storage

Each QUIC shard node stores data locally and maintains its own Badger index.

## Local Badger (Per-QUIC-Node)

| Key Pattern | Value | Purpose |
|-------------|-------|---------|
| `<object-hash>` | WAL file + offset | Locate shard data in WAL |

## Shard File Format

```
[ Header 8KB ]
[ Compressed segments ... ]
[ Footer or .idx index ]
```

### Segments

- 64 KB logical
- Independently LZ4/Snappy compressed (OPTIONAL)
- Each segment contains:
  - segmentIndex
  - logicalLength
  - compressedLength
  - crc32

### Index

Maps segmentIndex → fileOffset, compressedLength, logicalLength.

### Local KV Rebuild

If local Badger is lost:
1. Read shard headers
2. Load footer / `.idx` file
3. Recreate all local KV entries

---

# 7. Reed-Solomon Encoding

Supports RS(2,1) as the default, RS(3,2) and user configurable.

### Configuration

```toml
[rs]
data = 3
parity = 2
```

Defines RS(3,2): 3 data shards, 2 parity shards. Can tolerate loss of any 2 nodes.

### Encoding Workflow

1. Split 32 MB chunk into 4096 blocks (8 KB)
2. Pack into 512 segments (64 KB)
3. RS encode per segment
4. Compress and store per-node

### Decoding

- Attempt data shards
- If missing/corrupt → fetch parity
- Decompress → RS decode
- Return chunk bytes

---

# 8. Hash Ring Placement

Predastore uses a consistent hash ring with virtual nodes.

### Placement Algorithm

1. Compute `hash64 = first8(SHA256(bucket/path:chunkIndex))`
2. Locate ring position ≥ hash64
3. Select next K+M distinct nodes based on RSConfig

---

# 9. QUIC Protocol

Node-to-node communication uses QUIC for shard fetching.

### Request

```
MSG_FETCH_SHARD {
  ObjectID    [32]
  ChunkIndex  u32
  ShardRole   u8
  SegmentIndex u32
  SegmentCount u16
}
```

### Response

```
MSG_FETCH_SHARD_RESP {
  Status u8
  Crc32 u32
  TotalLength u32
}
[ compressed bytes ]
```

---

# 10. S3 Operations

## List Buckets

```
┌──────────────┐     ┌─────────────┐     ┌─────────────────┐
│  S3 Client   │────▶│    S3D      │────▶│  s3db Cluster   │
│  aws s3 ls   │     │  (HTTP)     │     │   (via Client)  │
└──────────────┘     └─────────────┘     └─────────────────┘
                            │                    │
                            │  1. Extract account ID from auth
                            │                    │
                            │  2. Query buckets table
                            │                    │
                            ▼                    ▼
                     ┌─────────────────────────────┐
                     │  Return XML bucket list     │
                     └─────────────────────────────┘
```

## PUT Object

```
┌───────────────────────────┐
│         S3 Client         │
│ aws s3 cp file s3://b/k   │
└─────────────┬─────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                         S3D (HTTP)                          │
│  1. Authenticate request (SigV4)                            │
│  2. Verify bucket access                                    │
│  3. Handle chunked transfer encoding (aws-chunked)          │
│  4. Write body to temp file                                 │
│  5. Generate object hash from bucket + key                  │
│  6. Determine shard nodes via consistent hash ring          │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Reed-Solomon Encoding                      │
│  - Split object into rsDataShard pieces                     │
│  - Generate rsParityShard parity pieces                     │
│  - Total shards = data + parity (e.g., 3 + 2 = 5)           │
└─────────────┬───────────────────────────────────────────────┘
              │
              │  Parallel QUIC requests to all shard nodes
              ▼
┌─────────────────┐  ┌─────────────────┐       ┌─────────────────┐
│   QUIC Node 0   │  │   QUIC Node 1   │  ...  │   QUIC Node 4   │
│   (Data 0)      │  │   (Data 1)      │       │   (Parity 1)    │
│  ┌───────────┐  │  │  ┌───────────┐  │       │  ┌───────────┐  │
│  │ WAL Write │  │  │  │ WAL Write │  │       │  │ WAL Write │  │
│  │  Shard    │  │  │  │  Shard    │  │       │  │  Shard    │  │
│  └─────┬─────┘  │  │  └─────┬─────┘  │       │  └─────┬─────┘  │
│        ▼        │  │        ▼        │       │        ▼        │
│  ┌───────────┐  │  │  ┌───────────┐  │       │  ┌───────────┐  │
│  │LocalBadger│  │  │  │LocalBadger│  │       │  │LocalBadger│  │
│  │ hash→WAL  │  │  │  │ hash→WAL  │  │       │  │ hash→WAL  │  │
│  └───────────┘  │  │  └───────────┘  │       │  └───────────┘  │
└─────────────────┘  └─────────────────┘       └─────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│              s3db Cluster Update (via Client)               │
│  1. Write to leader node                                    │
│  2. Leader replicates to followers (Raft)                   │
│  3. Store ARN key → object hash mapping                     │
│  4. Store object hash → ObjectToShardNodes metadata         │
│  5. Return ETag to client on majority commit                │
└─────────────────────────────────────────────────────────────┘
```

## GET Object

```
┌───────────────────────────┐
│         S3 Client         │
│ aws s3 cp s3://bucket/key │
└─────────────┬─────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                         S3D (HTTP)                          │
│  1. Authenticate request (SigV4)                            │
│  2. Query s3db cluster for object metadata                  │
│  3. Get ObjectToShardNodes via object hash                  │
│  4. Determine shard nodes via consistent hash ring          │
└─────────────┬───────────────────────────────────────────────┘
              │
              │  Parallel QUIC requests to data shard nodes
              ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   QUIC Node 0   │  │   QUIC Node 1   │  │   QUIC Node 2   │
│  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │
│  │LocalBadger│  │  │  │LocalBadger│  │  │  │LocalBadger│  │
│  │ hash→WAL  │  │  │  │ hash→WAL  │  │  │  │ hash→WAL  │  │
│  └─────┬─────┘  │  │  └─────┬─────┘  │  │  └─────┬─────┘  │
│        ▼        │  │        ▼        │  │        ▼        │
│  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │
│  │  WAL Read │  │  │  │  WAL Read │  │  │  │  WAL Read │  │
│  │  Shard 0  │  │  │  │  Shard 1  │  │  │  │  Shard 2  │  │
│  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    S3D Reassembly                           │
│  - Collect data shards (minimum: rsDataShard count)         │
│  - If shard missing/corrupt: fetch parity, reconstruct      │
│  - Reed-Solomon decode to original object                   │
│  - Stream response to client                                │
└─────────────────────────────────────────────────────────────┘
```

## DELETE Object

```
┌────────────────────────────┐
│          S3 Client         │
│ aws s3 rm s3://bucket/key  │
└─────────────┬──────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                         S3D (HTTP)                          │
│  1. Authenticate request (SigV4)                            │
│  2. Query s3db for object metadata                          │
│  3. Get ObjectToShardNodes to find all shard locations      │
│  4. Determine nodes via consistent hash ring                │
└─────────────┬───────────────────────────────────────────────┘
              │
              │  Parallel QUIC DELETE requests to all nodes
              ▼
┌─────────────────┐  ┌─────────────────┐       ┌─────────────────┐
│   QUIC Node 0   │  │   QUIC Node 1   │  ...  │   QUIC Node 4   │
│  ┌───────────┐  │  │  ┌───────────┐  │       │  ┌───────────┐  │
│  │LocalBadger│  │  │  │LocalBadger│  │       │  │LocalBadger│  │
│  │Deletehash │  │  │  │Deletehash │  │       │  │Deletehash │  │
│  └───────────┘  │  │  └───────────┘  │       │  └───────────┘  │
└─────────────────┘  └─────────────────┘       └─────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│              s3db Cluster Cleanup (via Client)              │
│  1. Write DeletedObjectInfo for compaction tracking         │
│  2. Delete ARN key → object hash mapping                    │
│  3. Delete object hash → ObjectToShardNodes metadata        │
│  4. Return 204 No Content on majority commit                │
└─────────────────────────────────────────────────────────────┘
```

---

# 11. Failure Handling & Repair

- CRC mismatch → fallback to parity shards
- Node timeout → fallback to parity shards
- If < DataShards available → GET fails
- After degraded read → schedule background read-repair

---

# 12. Cluster Evolution

### Adding Nodes

- Update hash ring → bump ringEpoch
- New writes use new epoch
- Old objects remain on old epoch
- Background rebalancer optional

### Changing RS Configuration

- Mixed RS(2,1) and RS(3,2) allowed as per design
- RS config encoded into Badger KV to determine configuration
- New objects can switch RSConfig
- Old objects can be migrated in background

---

# 13. Security

## TLS/HTTPS

All communication uses TLS:
- s3db server uses HTTPS with configurable certificates
- Client connections skip certificate verification for self-signed certs (configurable)
- Certificate paths: `-tls-cert` and `-tls-key` flags

## Authentication

- S3 API uses AWS Signature V4 authentication
- s3db uses AWS Signature V4 authentication
- Credentials defined in `[[db]]` or `[[auth]]` sections
- All database operations require valid signatures

---

# 14. Deployment Modes

## Development Mode (Single Host)

When all nodes have the same host address, s3d runs everything locally:

```bash
# Launches embedded DB + all QUIC nodes as goroutines
./bin/s3d -backend distributed -config ./s3/tests/config/cluster.toml
```

## Production Mode (Multi-Host)

For production, run separate processes on each host:

```bash
# Host 1: Database leader + QUIC node 0
./bin/s3d -backend distributed -config cluster.toml -node 0 -db-node 1

# Host 2: Database follower + QUIC node 1
./bin/s3d -backend distributed -config cluster.toml -node 1 -db-node 2

# Host 3: Database follower + QUIC node 2
./bin/s3d -backend distributed -config cluster.toml -node 2 -db-node 3
```

Or run database nodes separately:

```bash
# DB-only nodes (no QUIC shards)
./bin/s3d -db -config cluster.toml -db-node 1

# QUIC-only nodes (connect to DB cluster)
./bin/s3d -backend distributed -config cluster.toml -node 0
```

## Default Embedded Database

When no `[[db]]` configuration is provided and the distributed backend is used, s3d automatically launches a default embedded database:
- Listens on `127.0.0.1:6660`
- Data stored in `<base_path>/db/` or `data/db/`
- Uses credentials from first `[[auth]]` entry or defaults to `predastore:predastore`

---

# 15. Configuration Reference

## Database Node Configuration

```toml
# Specify distributed database nodes
[[db]]
id = 1
host = "192.168.1.10"      # Bind address (can be 0.0.0.0)
port = 6660                # HTTPS API port
raft_port = 7660           # Optional, defaults to port + 1000
advertise_host = ""        # Optional: address to advertise (see below)
path = "/data/db/node-1/"
leader = true              # Bootstrap leader hint
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[[db]]
id = 2
host = "192.168.1.11"
port = 6660
path = "/data/db/node-2/"
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[[db]]
id = 3
host = "192.168.1.12"
port = 6660
path = "/data/db/node-3/"
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### Port Configuration

Each database node uses two ports:

| Port | Purpose | Default |
|------|---------|---------|
| `port` | HTTPS REST API for client requests | 6660 |
| `raft_port` | TCP for Raft consensus (leader election, log replication) | `port + 1000` (e.g., 7660) |

### Bind vs Advertise Address

Raft requires two addresses:
- **Bind Address**: Where the node listens (can be `0.0.0.0` to accept connections on any interface)
- **Advertise Address**: What the node tells other nodes to connect to (must be reachable)

```toml
[[db]]
id = 1
host = "0.0.0.0"              # Bind to all interfaces
advertise_host = "10.0.1.5"   # Tell other nodes to connect here
port = 6660
```

**Important**: If `host` is `0.0.0.0` and `advertise_host` is not set, it defaults to `127.0.0.1`. For multi-machine clusters, always set `advertise_host` to the reachable IP address.

## Shard Node Configuration

```toml
[[nodes]]
id = 0
host = "192.168.1.20"
port = 9990
path = "/data/shards/node-0/"

[[nodes]]
id = 1
host = "192.168.1.21"
port = 9991
path = "/data/shards/node-1/"

[[nodes]]
id = 2
host = "192.168.1.22"
port = 9992
path = "/data/shards/node-2/"
```

## Reed-Solomon Configuration

```toml
[rs]
data = 3
parity = 2
```

Requires 5 nodes, providing 3 data, and 2 parity bits for recovery.

---

# 16. Developer Reference

## Key Files

| File | Purpose |
|------|---------|
| `s3db/config.go` | Cluster configuration, port calculation, advertise addresses |
| `s3db/raft.go` | RaftNode implementation, leader election, consensus |
| `s3db/fsm.go` | Finite State Machine, applies commands to BadgerDB |
| `s3db/server.go` | HTTPS REST API, routes, authentication middleware |
| `s3db/client.go` | Client for connecting to s3db cluster |
| `backend/distributed/globalstate.go` | GlobalState interface, LocalState, DistributedState |
| `backend/distributed/put.go` | PUT object implementation |
| `backend/distributed/get.go` | GET object implementation |
| `backend/distributed/list.go` | LIST operations implementation |
| `backend/distributed/delete.go` | DELETE object implementation |

## Raft Tuning Parameters

```go
// Default configuration in s3db/config.go
HeartbeatTimeout:   1000 * time.Millisecond  // Leader heartbeat interval
ElectionTimeout:    1000 * time.Millisecond  // Follower timeout before election
CommitTimeout:      50 * time.Millisecond    // Max time to wait for commit
SnapshotInterval:   120 * time.Second        // How often to check for snapshot
SnapshotThreshold:  8192                     // Log entries before snapshot
TrailingLogs:       10240                    // Logs to keep after snapshot
LeaderLeaseTimeout: 500 * time.Millisecond   // Leader step-down if no contact
```

## Common Patterns

```go
// Creating a distributed state client
state, err := distributed.NewDistributedState(&distributed.DBClientConfig{
    Nodes:           []string{"127.0.0.1:6660", "127.0.0.1:6661"},
    AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
    SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    Region:          "us-east-1",
})

// Storing object metadata
objectHash := s3db.GenObjectHash(bucket, key)
err := state.Set("objects", objectHash[:], metadataBytes)

// Listing objects by prefix
err := state.Scan("objects", []byte("arn:aws:s3:::mybucket/"), func(key, value []byte) error {
    // Process each object
    return nil
})
```

---

# 17. Tunable Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| Block size | 8 KB | Smallest addressable unit |
| Segment size | 64 KB | Compression unit |
| Shard size | 32 MB | Per-node slice |
| RS schemes | RS(2,1) / RS(3,2) | Erasure coding configuration |
| Hash ring vnodes | 64–256 | Virtual nodes for distribution |
| QUIC stream limit | 32–256 | Concurrent streams |
| In-flight chunks | 16–32 | Parallel chunk operations |

---

# 18. Future Work

- **Gossip Protocol**: Replace shared `cluster.toml` with dynamic node discovery
- **Bucket Operations**: Dynamic bucket creation/deletion stored in s3db
- **Compaction**: WAL file compaction and garbage collection using DeletedObjectInfo
- **Rebalancing**: Automatic shard redistribution when nodes join/leave
- **Read Replicas**: Allow reads from any s3db follower for better read scaling
- **Background Repair**: Automated healing of degraded objects

---

This file is the authoritative reference for Predastore implementation.
