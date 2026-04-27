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
# Run full predastore - dev mode simulates a 3-node DB cluster and 5 QUIC shard nodes locally.
./bin/s3d -config s3/tests/config/cluster.toml

# Run as a specific node (auto-launches local DB and QUIC servers for this node).
./bin/s3d -config ./s3/tests/config/cluster.toml -node 1

# Run only the database server.
./bin/s3d -db -config ./s3/tests/config/cluster.toml -db-node 1

# Run a specific QUIC shard node.
./bin/s3d -config ./s3/tests/config/cluster.toml -node 2
```

### CLI Flags

| Flag | Environment | Description |
|------|-------------|-------------|
| `-config` | `CONFIG` | Path to configuration file (default: config/server.toml) |
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

## Objects → Shards → Fragments

```
Object (arbitrary size, RS-encoded as a whole)
 └── Shards (K data + M parity slices, one per node)
      └── Fragments (fixed 8 KB payload + 32 B header, stored in WAL files)
```

An object is Reed-Solomon encoded end-to-end into `K` data shards and `M` parity shards
without any intermediate chunking. Each shard is streamed to one node, where it is
stored as a sequence of fixed-size fragments inside one or more WAL files.

### Size Reference

| Unit | Size | Description |
|------|------|-------------|
| Fragment payload | 8 KB | Fixed on-disk unit, zero-padded when logical length is shorter |
| Fragment header | 32 B | Metadata + CRC32 covering header + padded payload |
| Shard | `⌈object_size / K⌉` | Per-node RS slice, variable size |
| WAL file | configurable | Container for one or more fragments; may hold fragments from multiple shards |

A WAL file has no fixed relationship to a shard. Small shards pack into a single file
together with other small shards; large shards span multiple files. Fragments from
different shards may be interleaved within the same file but never overlap.

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

Each QUIC shard node stores data locally in append-style WAL files and maintains
its own Badger index mapping shard identifiers to WAL file locations.

## Local Badger (Per-QUIC-Node)

| Key Pattern | Value | Purpose |
|-------------|-------|---------|
| `<object-hash>` | `{WALFiles: [{WALNum, Offset, Size}, ...], ShardNum, TotalSize}` | Locate all fragments of a shard |

A shard that spans multiple WAL files produces a `WALFiles` list with one entry per
file, each recording the starting fragment offset and the number of bytes from this
shard stored in that file.

**Commit rule**: a Badger entry for a shard exists if and only if all of that shard's
fragments are fsync-durable on disk. The absence of a Badger entry means the shard is
unreadable, regardless of what bytes happen to be on disk.

## WAL File On-Disk Layout

```
┌──────────────────────────────────────────────────────────────┐
│ WAL Header (14 B)                                            │
│ ┌──────────┬─────────┬────────────┬────────────┐             │
│ │ Magic(4) │ Ver(2)  │ ShardSize(4)│ChunkSize(4)│             │
│ └──────────┴─────────┴────────────┴────────────┘             │
├──────────────────────────────────────────────────────────────┤
│ Fragment 0 (8224 B = 32 B header + 8192 B payload)           │
├──────────────────────────────────────────────────────────────┤
│ Fragment 1 (8224 B)                                          │
├──────────────────────────────────────────────────────────────┤
│ ... fragments may belong to different shards ...             │
├──────────────────────────────────────────────────────────────┤
│ Fragment N (8224 B)                                          │
└──────────────────────────────────────────────────────────────┘
```

Every fragment on disk is exactly `FragmentHeaderBytes + ChunkSize = 32 + 8192 = 8224`
bytes. Fragments whose logical payload is shorter than `ChunkSize` are zero-padded.
The padding bytes are included in the CRC calculation.

### Fragment Header (32 B)

```
┌─────────────────────────────────────────────────────────────────┐
│                         SeqNum (8)                              │
├─────────────────────────────────────────────────────────────────┤
│                        ShardNum (8)                             │
├───────────────────────────┬─────────────────────────────────────┤
│     ShardFragment (4)     │          Length (4)                 │
├───────────────────────────┼─────────────────────────────────────┤
│        Flags (4)          │         CRC32 (4)                   │
└───────────────────────────┴─────────────────────────────────────┘
```

| Field | Size | Purpose |
|-------|------|---------|
| `SeqNum` | 8 B | Global monotonic sequence number (optional; reserved for future replay) |
| `ShardNum` | 8 B | Identifier of the shard this fragment belongs to |
| `ShardFragment` | 4 B | Index of this fragment within the shard (0..N-1) |
| `Length` | 4 B | Logical payload length (≤ 8192); remaining payload bytes are zero-padding |
| `Flags` | 4 B | Bit flags; `FlagEndOfShard` marks the final fragment of a shard |
| `CRC32` | 4 B | IEEE CRC32 of the full 32 B header (with CRC field zeroed) + full 8192 B padded payload |

`ShardNum` + `ShardFragment` uniquely identify a fragment's role. The CRC covers both
header and payload, so a reader can independently verify every 8 KB unit without any
external metadata.

## Fragment Slot Allocation

A shard of size `S` requires `⌈S / ChunkSize⌉` fragments. Fragments for a given shard
are allocated contiguously within each WAL file they touch but may span multiple files
when the current file's free slots are exhausted.

**Reservation protocol** (executed under a short `wal.mu` critical section):

1. Compute the fragment count for the incoming shard from the body length on the QUIC
   request header.
2. Walk the set of open WAL files, allocating contiguous slot ranges. When a file's
   free slots are exhausted, create a new WAL file via `os.File.Truncate(maxFileSize)`.
   Pre-creating full-size files ensures unwritten slots read as zero bytes and makes
   room for concurrent reservations to begin allocation immediately.
3. For each file touched, increment that file's in-memory `refCount`.
4. Assign `SeqNum` values atomically (one per fragment).
5. Mark a file "full" once its final slot is allocated so no further reservations may
   claim slots in it.
6. Return the allocation as a list of `(WALNum, baseFragmentIndex, fragmentCount)`
   tuples. Release the lock.

The reservation step is the only shared-state critical section on the write path. No
I/O is performed while holding `wal.mu`.

## Lock-Free Fragment Writes

Once a reservation is issued, the writer goroutine owns a disjoint set of fragment
slots across one or more WAL files. Slot offsets are deterministic:

```
fileOffset(walNum, fragmentIndex) = WALHeaderSize + fragmentIndex * 8224
```

The writer goroutine drains fragment-sized batches from its QUIC stream, assembles
each fragment (header + payload + zero padding + CRC), and issues
`(*os.File).WriteAt(fragmentBytes, fileOffset)`. Multiple writer goroutines may issue
concurrent `WriteAt` calls to the same WAL file as long as their slot allocations are
disjoint. POSIX `pwrite` guarantees atomicity for non-overlapping regions, and Go's
`WriteAt` is explicitly safe for concurrent use.

A writer may coalesce up to `N` adjacent fragments into a single `WriteAt` call to
amortise syscall overhead. `N` is a tunable (see §17) that trades memory for syscall
count; it has no effect on correctness or the observable state of the WAL.

## Commit Sequence

For each WAL file touched by a reservation, the writer goroutine performs the
following steps in order:

1. **Write** all owned fragments via `WriteAt` (batched per the tunable above).
2. **Fsync** the WAL file.
3. **Commit** by writing the Badger entry (`<object-hash>` → `WALFiles` list).
4. **Decrement** the file's `refCount` atomically. If the decremented value is zero
   and the file is marked full, close the file.

Step 3 is the linearisation point for readers: a shard is readable if and only if its
Badger entry is present. Steps 1-2 may produce fragments on disk that are never
followed by a commit (aborted writes, crashes); those fragments are dead space
reclaimable by the compactor.

## Reference Counting & File Lifetime

Each open WAL file carries an atomic `refCount` tracking the number of in-flight
reservations that hold at least one slot in the file. The counter is:

- Incremented under `wal.mu` during reservation, for every file touched.
- Decremented after a reservation's commit or abort completes for that file.
- Checked for zero immediately after the decrement returns (via the return value of
  `atomic.AddInt32`). A caller that observes zero and the file is marked full is the
  sole closer.

Marking a file "full" under `wal.mu` before its final `refCount` increment returns
ensures no late reservation can arrive between a decrement-to-zero and the close.

## Abort Semantics

A reservation is aborted when the server-side writer fails mid-shard (network error,
client disconnect, disk error). On abort:

- The writer skips step 3 (Badger commit). The shard becomes unreadable.
- The writer still performs step 4 (refCount decrement) so the file can be closed
  when no other in-flight reservations remain.
- Any fragments already written to disk sit as dead space until the compactor
  reclaims them.

There is no distinct on-disk or in-Badger `failed` state. The absence of a Badger
entry is the authoritative signal that a shard is unreadable; the read path treats
"Badger miss" identically to "shard never existed" and falls back to parity.

## Background Compaction & Cold Storage

Committed, fully-written WAL files are candidates for background compaction. The
compactor:

- Scans closed WAL files for regions not referenced by any live Badger entry (dead
  fragments from aborted reservations or deleted shards).
- Rewrites live fragments into a new compacted file, updates Badger entries to point
  at the new location, and removes the old file.
- Optionally migrates aged, rarely-accessed shards to long-term cold storage. On a
  subsequent GET, the shard is rehydrated from cold storage into a local WAL file
  and served; the rehydration may complete asynchronously with the GET response.

Compaction is not on the critical write path and is not required for correctness of
PUT or GET operations.

## Local KV Rebuild

If the local Badger index is lost, it can be reconstructed from WAL files:

1. Scan each WAL file from offset `WALHeaderSize` to EOF, one fragment at a time.
2. Verify each fragment's CRC32. Corrupt fragments are discarded.
3. Group surviving fragments by `ShardNum`.
4. A `ShardNum` group is complete when it contains contiguous `ShardFragment` values
   0 through N-1 and the fragment at index N-1 has `FlagEndOfShard` set.
5. Insert a Badger entry for each complete shard, pointing at the (WALNum, offset)
   list where its fragments live.

Incomplete shard groups (missing fragments, missing `FlagEndOfShard`) are discarded.
They correspond to reservations whose commits never landed.

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

1. Read the complete object body into memory on the S3D process.
2. RS encode the object as a whole into `K` data shards + `M` parity shards using
   `reedsolomon.NewStream(K, M)` followed by `enc.Split(body, dataWriters, fileSize)`.
   Each shard is approximately `⌈object_size / K⌉` bytes.
3. Ship each shard to its assigned node over QUIC (see §9). The node stores the shard
   as a sequence of 8 KB fragments inside its WAL files (see §6).

No intermediate chunking or segmentation occurs. Compression is not performed on the
write path; it is optionally applied to closed WAL files by the background compactor.

### Decoding

- Request data shards in parallel from their assigned nodes.
- If a data shard is missing (Badger miss) or any fragment fails CRC validation, fetch
  a parity shard instead.
- Once `K` valid shards have been collected, RS decode to reconstruct the object.
- Fewer than `K` valid shards → GET fails.

---

# 8. Hash Ring Placement

Predastore uses a consistent hash ring with virtual nodes.

### Placement Algorithm

1. Compute `hash64 = first8(SHA256(bucket/path:chunkIndex))`
2. Locate ring position ≥ hash64
3. Select next K+M distinct nodes based on RSConfig

---

# 9. QUIC Protocol

Node-to-node communication uses QUIC for shard storage and retrieval. QUIC was chosen specifically to avoid TLS handshakes on every request while maintaining encryption.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           CLIENT SIDE                                    │
│                                                                          │
│  Distributed Backend                                                     │
│       │                                                                  │
│       ▼                                                                  │
│  DialPooled("node1:9991")                                               │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Connection Pool (DefaultPool)                                   │    │
│  │  ┌─────────────────────────────────────────────────────────┐    │    │
│  │  │  map[addr]*pooledConn                                   │    │    │
│  │  │                                                          │    │    │
│  │  │  "node1:9991" → *Client{conn: quic.Conn} ─┐             │    │    │
│  │  │  "node2:9991" → *Client{conn: quic.Conn}  │ REUSED!     │    │    │
│  │  │  "node3:9991" → *Client{conn: quic.Conn}  │ (no TLS)    │    │    │
│  │  └──────────────────────────────────────────┼──────────────┘    │    │
│  └─────────────────────────────────────────────┼───────────────────┘    │
│                                                │                         │
│       ▼                                        │                         │
│  client.Put(ctx, putReq, shardData)            │                         │
│       │                                        │                         │
│       ▼                                        │                         │
│  conn.OpenStreamSync() ←───────────────────────┘                        │
│       │         ▲                                                        │
│       │         │ Multiplexed streams on SINGLE connection              │
│       │         │ (stream IDs: 0, 4, 8, 12, ...)                        │
│       ▼         │                                                        │
│  ┌──────────────┴──────────────────────────────────────────────┐        │
│  │  Stream (per request - lightweight!)                         │        │
│  │  - Write: header + request JSON + shard data                │        │
│  │  - Read: response header + response JSON                    │        │
│  │  - Close: CancelRead(0) + Close() → releases stream ID      │        │
│  └──────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘

                              │ QUIC/UDP │
                              │ (single  │
                              │  socket) │
                              ▼          ▼

┌─────────────────────────────────────────────────────────────────────────┐
│                           SERVER SIDE                                    │
│                                                                          │
│  listener.Accept() → quic.Conn (TLS handshake HERE, ONCE per client)   │
│       │                                                                  │
│       ▼                                                                  │
│  serveConn(conn) - runs forever for this connection                     │
│       │                                                                  │
│       ▼ (loop)                                                          │
│  conn.AcceptStream() → gets next stream from client                     │
│       │                                                                  │
│       ▼                                                                  │
│  go handleStream(stream)                                                │
│       │                                                                  │
│       ├─→ Read request header + body                                    │
│       ├─→ Process (WAL write, read, etc.)                               │
│       ├─→ Write response                                                │
│       └─→ Close stream (CancelRead + Close)                             │
│                                                                          │
│  [Loop back to AcceptStream for next request on same connection]        │
└─────────────────────────────────────────────────────────────────────────┘
```

## Connection vs Stream Lifecycle

| Layer | Lifecycle | Cost | When Created |
|-------|-----------|------|--------------|
| **Connection** | Long-lived, pooled | TLS handshake (~50-100ms) | First request to node |
| **Stream** | Per-request | ~0 (stream ID allocation) | Every PUT/GET/DELETE |

### What Happens Per Shard Write

| Step | Operation | Cost |
|------|-----------|------|
| 1 | `DialPooled("node:9991")` | **O(1) map lookup** - no TLS! |
| 2 | Check connection alive | `conn.Context().Err() == nil` |
| 3 | `conn.OpenStreamSync()` | **Very fast** - just assigns stream ID |
| 4 | Write request + data | Network I/O |
| 5 | Read response | Network I/O |
| 6 | Close stream | Releases stream ID |

**Connection stays in pool, NOT closed!**

### TLS Handshake Occurs Only When

1. **First connection to a node** - unavoidable
2. **Connection died** (idle timeout, network error) - re-establishes
3. **Pool cleanup** evicted idle connection (>2 min idle)

## Connection Pool Configuration

```go
// pool.go
&quic.Config{
    HandshakeIdleTimeout: 5 * time.Second,
    KeepAlivePeriod:      15 * time.Second,
    MaxIdleTimeout:       120 * time.Second,  // Connection stays alive
}

// Cleanup: every 30s, evict connections idle > 2 minutes
```

## Stream Closing (Critical for Connection Reuse)

QUIC streams have two independent half-connections. Both must be closed:

```go
// After reading all response data:
s.CancelRead(0)  // Close read side (tells peer we're done reading)
s.Close()        // Close write side (sends FIN)
```

**Warning**: Failing to close streams causes stream exhaustion. With ~100 concurrent stream limit, unclosed streams will block `OpenStreamSync()`.

## Protocol Format

### Request Header (32 bytes)

```
┌────────────────────────────────────────────────────────────────┐
│ Version (1) │ Method (1) │ Status (1) │ Reserved (1)          │
├────────────────────────────────────────────────────────────────┤
│                         ReqID (8)                              │
├────────────────────────────────────────────────────────────────┤
│         KeyLen (4)        │        MetaLen (4)                 │
├────────────────────────────────────────────────────────────────┤
│                        BodyLen (8)                             │
└────────────────────────────────────────────────────────────────┘
```

### Methods

| Method | Value | Description |
|--------|-------|-------------|
| GET | 1 | Retrieve shard data |
| PUT | 2 | Store shard data |
| DELETE | 3 | Delete shard metadata |

### Request/Response Flow

**PUT Request:**
```
[Header 32B] [PutRequest JSON] [Shard Data...]
```

**PUT Response:**
```
[Header 32B] [PutResponse JSON]
```

**GET Request:**
```
[Header 32B] [ObjectRequest JSON]
```

**GET Response:**
```
[Header 32B] [Shard Data...]
```

## Key Files

| File | Purpose |
|------|---------|
| `quic/quicclient/pool.go` | Connection pooling, reuse logic |
| `quic/quicclient/quicclient.go` | Client operations (Put, Get, Delete) |
| `quic/quicserver/server.go` | Server accept loop, stream handling |
| `quic/quicproto/proto.go` | Header format, read/write helpers |

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
│  4. Read body into memory                                   │
│  5. Generate object hash from bucket + key                  │
│  6. Determine shard nodes via consistent hash ring          │
└─────────────┬───────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Reed-Solomon Encoding                      │
│  - RS encode whole object into K data + M parity shards     │
│  - Shard size ≈ object_size / K                             │
└─────────────┬───────────────────────────────────────────────┘
              │
              │  Parallel QUIC streams, one per shard
              ▼
┌─────────────────┐  ┌─────────────────┐       ┌─────────────────┐
│   QUIC Node 0   │  │   QUIC Node 1   │  ...  │   QUIC Node K+M │
│   (Data 0)      │  │   (Data 1)      │       │   (Parity M-1)  │
│                 │  │                 │       │                 │
│  Per-stream goroutine per shard:                              │
│  1. Read body length from protocol header                     │
│  2. Reserve fragment slots under short wal.mu lock            │
│     - Allocate contiguous slots across 1+ WAL files           │
│     - Pre-create files via Truncate if needed                 │
│     - Increment refCount for each file touched                │
│  3. Lock-free loop: batch N fragments → WriteAt(bytes, offset)│
│  4. fsync each WAL file                                       │
│  5. Badger put: object-hash → WALFiles list                   │
│  6. Atomic refCount decrement; close file if zero and full    │
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

### Write-Path Concurrency Invariants

- `wal.mu` is held only during reservation and is released before any I/O begins. It
  covers fragment slot allocation, WAL file creation, SeqNum assignment, and refCount
  bookkeeping.
- Fragment writes to disk are lock-free. Multiple concurrent writer goroutines may
  issue `WriteAt` against the same WAL file provided their reserved slots are
  disjoint.
- A shard is observable to readers only after its Badger entry has been committed.
  Fragments fsynced to disk without a corresponding Badger put are invisible to GET
  and are reclaimable by compaction.
- File close happens exactly once, driven by the reservation whose refCount
  decrement returns zero (observed via `atomic.AddInt32`'s return value) after the
  file has been marked full.

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

## Read-Path Failure Modes

A GET fetches shards in parallel from their placement nodes. Each node either returns
a valid shard or fails; failures degrade to parity fetches until `K` valid shards are
collected.

| Failure | Detection | Response |
|---------|-----------|----------|
| Badger miss | Local lookup returns not-found | Node returns "absent"; S3D fetches parity |
| Fragment CRC mismatch | Per-fragment CRC32 during read | Node discards corrupt fragment; treated as absent; S3D fetches parity |
| Node timeout | QUIC request deadline exceeded | S3D fetches parity |
| Fewer than `K` valid shards | Aggregate check after all fetches | GET returns 500/503 to the client |

A shard that was written to disk but never committed to Badger is indistinguishable
from one that was never attempted — both surface as "absent." The read path does not
distinguish between them, and the commit invariant (§6) ensures readers cannot
observe partial writes.

## Background Repair

A node-local healing process periodically reconciles local state against the hash
ring:

1. Query s3db for the set of shards the hash ring places on this node.
2. For each expected shard, look up the local Badger index.
3. If absent, reconstruct the shard by fetching `K` valid shards from peers and RS
   decoding, then write the result as a new reservation locally.

The healer uses pull-based reconciliation against the metadata plane, so it repairs
both shards that failed mid-write and shards that never arrived (e.g. the node was
offline during the original PUT). A per-node in-memory recent-failures queue may
optionally drive faster repair for known-bad shards without requiring a full ring
scan.

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
./bin/s3d -config ./s3/tests/config/cluster.toml
```

## Production Mode (Multi-Host)

For production, run separate processes on each host:

```bash
# Host 1: Database leader + QUIC node 0
./bin/s3d -config cluster.toml -node 0 -db-node 1

# Host 2: Database follower + QUIC node 1
./bin/s3d -config cluster.toml -node 1 -db-node 2

# Host 3: Database follower + QUIC node 2
./bin/s3d -config cluster.toml -node 2 -db-node 3
```

Or run database nodes separately:

```bash
# DB-only nodes (no QUIC shards)
./bin/s3d -db -config cluster.toml -db-node 1

# QUIC-only nodes (connect to DB cluster)
./bin/s3d -config cluster.toml -node 0
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
| `ChunkSize` | 8 KB | Fragment payload size on disk |
| `FragmentHeaderBytes` | 32 B | Per-fragment header (SeqNum, ShardNum, ShardFragment, Length, Flags, CRC32) |
| `ShardSize` | 32 MB | Maximum WAL file payload before rollover (per-file capacity, not per-shard) |
| Fragment batch size `N` | tunable | Number of fragments coalesced per `WriteAt` by a writer goroutine; memory-vs-syscall tradeoff |
| RS schemes | RS(2,1) / RS(3,2) | Erasure coding configuration |
| Hash ring vnodes | 64–256 | Virtual nodes for distribution |
| QUIC concurrent streams | 32–256 | Per-connection stream limit |
| QUIC flow-control windows | see `quic/quicconf` | Receive window sizes (stream and connection) |

Fragment batch size `N` has no effect on the observable state of the WAL; it tunes
memory usage and syscall count on the write path. A production node serving many
concurrent shards per WAL file will typically set `N` higher; a memory-constrained
node will set it lower.

---

This file covers the engineering rationale and architecture of Predastore. For
gaps between this design and the current codebase, and for longer-horizon work,
see [TODO.md](./TODO.md).
