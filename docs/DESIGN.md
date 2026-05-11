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
| `-encryption-key-file` | `ENCRYPTION_KEY_FILE` | Path to 32-byte AES-256 master key (required, mode `0600`) |

See `s3/tests/config/cluster.toml` for a complete example configuration.

`quicd` (the standalone shard-node binary) accepts the same
`-encryption-key-file` / `ENCRYPTION_KEY_FILE` and refuses to start without
it. Every node in a cluster MUST be given the same master key file —
fragments sealed under one master cannot be opened under another.

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
    │ Store       │   │ Store       │   │ Store       │
    │ (segments + │   │ (segments + │   │ (segments + │
    │  index)     │   │  index)     │   │  index)     │
    └─────────────┘   └─────────────┘   └─────────────┘
```

## Component Overview

| Component | Purpose |
|-----------|---------|
| **S3D (HTTP Server)** | Handles S3 API requests, authentication, routing |
| **s3db Cluster** | Distributed Raft-based metadata store |
| **QUIC Shard Nodes** | Store erasure-coded object shards in append-only segment files |
| **GlobalState** | Abstraction layer for metadata access (local or distributed) |

---

# 4. Logical Data Model

## Objects → Shards → Fragments

```
Object (arbitrary size, RS-encoded as a whole)
 └── Shards (K data + M parity slices, one per node)
      └── Fragments (fixed 8 KB payload + 32 B header, stored in segment files)
```

An object is Reed-Solomon encoded end-to-end into `K` data shards and `M` parity shards
without any intermediate chunking. Each shard is streamed to one node, where it is
stored as a contiguous **extent** of fixed-size fragments inside an append-only
segment file.

### Size Reference

| Unit | Size | Description |
|------|------|-------------|
| Fragment body | 8 KiB | AES-256-GCM ciphertext, zero-padded when logical length is shorter |
| Fragment header | 32 B | `fragNum`, `shardNum`, `size`, `flags`, two reserved slots |
| Fragment tag | 16 B | GCM authentication tag; binds ciphertext + AAD |
| Fragment total | 8240 B | header + body + tag, fixed on-disk unit |
| Shard | `⌈object_size / K⌉` | Per-node RS slice, variable size; occupies a contiguous extent |
| Segment file | up to 4 GiB | Append-only container; rolls when full. Holds extents from multiple shards |

Each shard is allocated a contiguous extent of fragments within a single segment.
Multiple shards may share a segment but their extents are disjoint and never
interleave. When a segment fills, its `flagFull` bit is set in the header and the
store rolls forward to the next segment number.

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

Each QUIC shard node stores data locally in append-only segment files and maintains
its own Badger index mapping shard identifiers to the on-disk extent that holds
the shard's fragments. The implementation lives in the `store/` package.

## Local Badger (Per-QUIC-Node)

| Key Pattern | Value | Purpose |
|-------------|-------|---------|
| `<32 B object hash \|\| 4 B shard index>` | encoded extent record | Locate a shard's fragments on disk |

The extent record is:

| Field | Size | Purpose |
|-------|------|---------|
| `SegNum` | 8 B | Segment file number containing the shard |
| `Off` | 8 B | Byte offset of the shard's first fragment within the segment |
| `PSize` | 8 B | Physical size on disk (fragment-aligned, includes per-fragment headers) |
| `LSize` | 8 B | Logical (user) size of the shard |

**Commit rule**: a Badger entry for a shard exists if and only if all of that
shard's fragments are fsync-durable on disk. The absence of a Badger entry means
the shard is unreadable, regardless of what bytes happen to be on disk.

## Segment File On-Disk Layout

Segments are append-only files containing a 14 B header followed by a sequence of
fixed-size fragments:

```
┌──────────────────────────────────────────────────────────────┐
│ Segment Header (14 B)                                        │
│ ┌──────────┬─────────┬───────────┬──────────────┐            │
│ │ Magic(4) │ Ver(2)  │ Flags(4)  │ Reserved(4)  │            │
│ │ "S3SE"   │   1     │ flagFull  │              │            │
│ └──────────┴─────────┴───────────┴──────────────┘            │
├──────────────────────────────────────────────────────────────┤
│ Fragment 0 (8240 B = 32 B header + 8192 B body + 16 B tag)   │
├──────────────────────────────────────────────────────────────┤
│ Fragment 1 (8240 B)                                          │
├──────────────────────────────────────────────────────────────┤
│ ... fragments belonging to one or more shards ...            │
├──────────────────────────────────────────────────────────────┤
│ Fragment N (8240 B)                                          │
└──────────────────────────────────────────────────────────────┘
```

Every fragment on disk is exactly `fragHeaderSize + fragBodySize + fragTagSize
= 32 + 8192 + 16 = 8240` bytes. Bodies are zero-padded to `fragBodySize`
before encryption; GCM is a stream cipher so the ciphertext is the same length
as the plaintext (8192 B), and the authentication tag follows.

The magic `'S','3','S','E'` (segment version `1`) identifies the
encryption-at-rest format. The previous pre-encryption magic (`'S','3','S','F'`)
is rejected outright by `openSegment` — there is no in-place migration, the
operator must start with a fresh data dir.

A segment grows up to `maxSegSize` (4 GiB). When full, `flagFull` is set in the
header and the store rolls forward to a new segment number. Within a segment,
each shard occupies a contiguous extent of fragments; extents from different
shards are disjoint.

### Fragment Header (32 B)

```
┌─────────────────────────────────────────────────────────────────┐
│                         FragNum (8)                             │
├─────────────────────────────────────────────────────────────────┤
│                        ShardNum (8)                             │
├───────────────────────────┬─────────────────────────────────────┤
│       Reserved (4)        │          Size (4)                   │
├───────────────────────────┼─────────────────────────────────────┤
│         Flags (4)         │         Reserved (4)                │
└───────────────────────────┴─────────────────────────────────────┘
```

| Field | Size | Purpose |
|-------|------|---------|
| `FragNum` | 8 B | Global monotonic fragment counter (across segments). Feeds the GCM nonce, so it must be unique for the lifetime of the master key (see Persistent State below). |
| `ShardNum` | 8 B | Identifier of the shard this fragment belongs to. Bound into AAD. |
| `Reserved` | 4 B | Reserved for future use |
| `Size` | 4 B | Logical payload length (≤ 8192); the body bytes past `Size` are zero-padding inside the ciphertext |
| `Flags` | 4 B | Bit flags; `flagEndOfShard` marks the final fragment of a shard |
| `Reserved` | 4 B | Formerly CRC32 — see "Encryption-at-Rest" below; not reclaimed for a different field while the current segment magic is in use |

The fragment header is plaintext on disk — readers need `FragNum` and
`ShardNum` *before* decryption to reconstruct the AAD and nonce. The header
fields are protected by GCM authentication, not by encryption: any tamper
that changes `FragNum`, `ShardNum`, or `Size` makes the reconstructed AAD
differ from what was bound at seal time and the tag check fails.

### Encryption-at-Rest

Every fragment is sealed independently under AES-256-GCM. The 12-byte GCM
nonce and 52-byte AAD are deterministic and reconstructable at read time
from the on-disk header + per-data-dir state:

```
nonce[0:8]   = BE(fragNum)    // from fragment header
nonce[8:12]  = BE(storeID)    // from state.json (4 random bytes, per data dir)

aad[0:32]    = objectHash     // SHA-256, already part of the shard index key
aad[32:36]   = BE(shardIndex) // already part of the shard index key
aad[36:44]   = BE(shardNum)   // from fragment header
aad[44:52]   = BE(fragNum)    // from fragment header
```

Properties:

- **Confidentiality.** Disk-level access yields only ciphertext + tag;
  plaintext is never written to disk.
- **Authenticated integrity.** GCM is the sole integrity authority — there is
  no separate CRC. A failed tag check returns `ErrIntegrity`; "disk
  corruption", "tamper", and "wrong master key" all surface as one error.
- **Position binding.** AAD binds each fragment to its
  `(objectHash, shardIndex, shardNum, fragNum)` slot, so swapping fragments
  between shards or rewriting the on-disk header to claim a different slot
  fails the tag.
- **Cross-data-dir defence.** `storeID` enters the nonce, not the AAD —
  splicing a fragment from data dir A into data dir B's segment yields a
  different nonce at read time and the tag fails.
- **Mandatory.** `store.Open` errors without `WithAEAD`; the operator-layer
  daemons (`s3d`, `quicd`) refuse to start without `-encryption-key-file`.
  There is no unencrypted code path.

The master key is loaded once at daemon startup by the `internal/keyfile`
package (raw 32 bytes, mode `0600` — group/other-readable rejected outright),
turned into a `cipher.AEAD`, and handed to `store.Open` via `WithAEAD`. The
store never sees the raw key bytes.

## Extent Reservation

A shard of logical size `S` requires `⌈S / fragBodySize⌉` fragments occupying a
contiguous extent of `n × totalFragSize` bytes within a segment.

**Reservation protocol** (executed under a short `store.mutex` critical section):

1. Compute the fragment count from the body length on the QUIC request header.
2. Get the current segment. If full, mark it via `markFull()` and roll to the
   next segment number (up to 100 attempts).
3. `Truncate(off + extentSize)` the segment file to reserve the extent up front.
   Pre-allocating ensures every subsequent `WriteAt` lands within file bounds
   without extending the file concurrently.
4. Increment the segment's atomic `refs` counter.
5. Allocate monotonic `fragNum`/`shardNum` values for the writer.
6. Build and return a `shardWriter` bound to the reserved extent. Release the
   lock.

The reservation is the only shared-state critical section on the write path. No
data I/O happens while holding `store.mutex` — only the `Truncate` syscall, which
extends the file's size metadata without writing any blocks.

## Lock-Free Fragment Writes

Once a reservation is issued, the writer goroutine owns a disjoint byte range
within the segment file. Fragment offsets are deterministic:

```
fileOffset(extent, frag) = extent.Off + frag * totalFragSize
```

The writer assembles fragments (header + zero-padded body + GCM tag) into an
in-memory window buffer, seals each body in place under the shared
`cipher.AEAD`, and issues `(*os.File).WriteAt(buf, fileOffset)` for the whole
window in one syscall. Multiple writer goroutines may issue concurrent
`WriteAt` calls against the same segment file because their reserved extents
are disjoint. POSIX `pwrite` guarantees atomicity for non-overlapping regions,
and Go's `WriteAt` is explicitly safe for concurrent use. The AEAD itself is
constructed once at `Store.Open` and shared — the stdlib's GCM is safe for
concurrent `Seal` / `Open`.

## Commit Sequence

`shardWriter.Close()` performs the following steps in order:

1. **Final flush** of any remaining buffered fragments via `WriteAt`.
2. **Fsync** the segment file.
3. **Commit** by writing the Badger index entry: `<object hash || shard index>`
   → encoded extent.
4. **Decrement** the segment's `refs` counter atomically.

Step 3 is the linearisation point for readers: a shard is readable if and only
if its Badger entry is present. Steps 1-2 may produce fragments on disk that are
never followed by a commit (aborted writes, crashes); those fragments are dead
space reclaimable by the compactor.

## Reference Counting & Segment Lifetime

Each cached segment carries an atomic `refs` counter tracking the number of
in-flight readers and writers that hold the segment open. The counter is:

- Incremented under `store.mutex` when a reservation succeeds (writers) or when
  `Lookup` returns a reader.
- Decremented when the writer closes (after step 3 above) or the reader closes.

`Store.Close()` waits for all `refs` to drain (spinning with `runtime.Gosched`)
before closing segment file descriptors and the index.

## Abort Semantics

A reservation is aborted when the server-side writer fails mid-shard (network
error, client disconnect, disk error). On abort:

- The writer skips step 3 (Badger commit). The shard becomes unreadable.
- The writer still performs step 4 (`refs` decrement) so the segment ref count
  drains correctly.
- Any fragments already written to disk sit as dead space until the compactor
  reclaims them.

There is no distinct on-disk or in-Badger `failed` state. The absence of a
Badger entry is the authoritative signal that a shard is unreadable; the read
path treats "Badger miss" identically to "shard never existed" and falls back to
parity.

## Persistent State

The Store persists monotonic counters and the per-data-dir crypto identity
in `state.json`:

| Field | Purpose |
|-------|---------|
| `segNum` | Current segment number (advances when a segment is marked full) |
| `shardNum` | Next shard identifier to assign |
| `fragNum` | Next global fragment counter to assign (bound into the GCM nonce) |
| `fragNumHighWater` | Durably-reserved upper bound on `fragNum`; `Append` extends it (and fsyncs) only when an allocation would cross it |
| `storeID` | 4 random bytes generated on first `Open` and held for the lifetime of the data dir; bound into the GCM nonce |

`state.json` is written atomically and durably: write `state.json.tmp`,
fsync the file, rename to `state.json`, fsync the parent directory. The
first save (after `storeID` generation, before any fragment can be sealed)
MUST complete before `Open` returns — otherwise a crash + restart could
generate a different `storeID` and orphan data written under the old one.

`fragNum` uniqueness across crashes is preserved by **batched high-water
reservation**, the standard pattern for crash-safe monotonic-counter
allocators. On `Open`, the store advances `fragNumHighWater` by
`fragNumReservation` (= 1 048 576) and fsyncs `state.json`. `Append` then
hands out `fragNum` values freely below the high-water without touching
disk; only an allocation that would cross the high-water triggers another
fsync. On crash recovery, `Open` resumes `fragNum = fragNumHighWater` —
the unflushed reservation window from before the crash is sacrificed to
guarantee nonce uniqueness. At most `fragNumReservation` fragNums are
"wasted" per crash; against the 2⁶⁴ budget this is negligible.

## Background Compaction & Cold Storage

Closed, fully-written segments are candidates for background compaction. The
compactor:

- Scans full segments for extents not referenced by any live Badger entry (dead
  extents from aborted reservations or deleted shards).
- Rewrites live extents into a new compacted segment, updates Badger entries to
  point at the new location, and removes the old segment.
- Optionally migrates aged, rarely-accessed shards to long-term cold storage. On
  a subsequent GET, the shard is rehydrated from cold storage into a local
  segment and served; rehydration may complete asynchronously with the GET
  response.

Compaction is not on the critical write path and is not required for
correctness of PUT or GET operations.

## Local KV Rebuild

If the local Badger index is lost, fragments alone are **not** sufficient to
rebuild it. The AAD that authenticates each fragment is keyed on
`(objectHash, shardIndex)` — both of which live only in the lost Badger
key, not on disk. A scan-and-decrypt rebuild would need an exhaustive search
over every known shard-key candidate per fragment, which is intractable.

The supported recovery path for a lost local index is **read-repair from
peers**: the hash ring identifies which shards this node should hold;
missing shards are reconstructed by fetching `K` valid shards from peers
and RS-decoding (see §11). The on-disk fragments from the lost index
become dead space reclaimable by compaction.

This is a deliberate trade-off introduced with the encryption-at-rest
format: the per-fragment AAD binding that defends against fragment
shuffling also makes "rebuild from segments alone" infeasible without
storing the shard key in plaintext on disk, which would weaken the
position-binding guarantee.

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
   as a contiguous extent of 8 KiB fragments inside a segment file, sealing each
   fragment under AES-256-GCM (see §6).

No intermediate chunking or segmentation occurs. Compression is not performed on the
write path; it is optionally applied to closed segments by the background compactor.

### Decoding

- Request data shards in parallel from their assigned nodes.
- If a data shard is missing (Badger miss) or any fragment fails GCM
  authentication (`ErrIntegrity`), fetch a parity shard instead.
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
│       ├─→ Process (store write, read, etc.)                            │
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
│  2. Reserve a contiguous extent under short store.mutex lock  │
│     - Roll to next segment if current is full                 │
│     - Truncate segment to pre-allocate the extent             │
│     - Increment segment refs                                  │
│  3. Lock-free WriteAt of fragment buffers within the extent   │
│  4. fsync the segment file                                    │
│  5. Badger put: <object-hash || shard-index> → extent record  │
│  6. Atomic segment refs decrement                             │
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

- `store.mutex` is held only during reservation and is released before any data
  I/O begins. It covers extent allocation, segment rotation, segment-file
  `Truncate`, `fragNum`/`shardNum` assignment, and `state.json` persistence.
- Fragment writes to disk are lock-free. Multiple concurrent writer goroutines
  may issue `WriteAt` against the same segment file provided their reserved
  extents are disjoint.
- A shard is observable to readers only after its Badger entry has been
  committed. Fragments fsynced to disk without a corresponding Badger put are
  invisible to GET and are reclaimable by compaction.
- Segment file descriptors are kept open in a per-store cache and are closed
  only on `Store.Close()`, after waiting for all outstanding writer/reader
  references to drain.

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
│  │key→extent │  │  │  │key→extent │  │  │  │key→extent │  │
│  └─────┬─────┘  │  │  └─────┬─────┘  │  │  └─────┬─────┘  │
│        ▼        │  │        ▼        │  │        ▼        │
│  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │
│  │ Segment   │  │  │  │ Segment   │  │  │  │ Segment   │  │
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
| Fragment integrity failure | GCM tag check fails during read (returns `ErrIntegrity`) | Node treats the shard as unreadable; S3D fetches parity. Covers disk corruption, tamper, fragment swap, and wrong master key indistinguishably. |
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

## Encryption at Rest

Every shard fragment is sealed under AES-256-GCM before it touches disk —
see §6 ("Encryption-at-Rest") for the on-disk format, AAD, and nonce
construction. Operationally:

- **Master key.** One 32-byte AES-256 key per cluster, loaded from a file
  path supplied via `-encryption-key-file` / `ENCRYPTION_KEY_FILE`. The
  loader is fail-closed on permissions: any group/other-readable mode
  (`mode & 0077 != 0`) is rejected with no override — `chmod 600` is
  mandatory. The same key MUST be configured on every node; rotation is
  out of scope for the current implementation.
- **No plaintext on disk.** Both `s3d` and `quicd` refuse to start without
  a key path; `store.Open` errors without `WithAEAD`. There is no
  unencrypted code path to fall back to.
- **Logging.** The raw key is never logged. Operators identify a key by
  its fingerprint (`hex(sha256(key)[:8])`, 16 hex chars), logged once at
  `Server.init` and at QUIC server startup.
- **In scope.** Confidentiality and integrity against an attacker reading
  or modifying segment files; detection of fragment shuffling within or
  across shards / data dirs on the same master key.
- **Out of scope.** Compromise of a node host with the live master key in
  memory; per-bucket / per-tenant keys; KMS integration; key rotation;
  migration of existing unencrypted data (clusters must start fresh against
  the new segment magic); encryption of the s3db (BadgerDB) index.

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
| `store/store.go` | Store: segment cache, extent reservation, fragNum high-water reservation, index commits, `WithAEAD` option |
| `store/segment.go` | Segment file format, header (magic `S3SE`), `flagFull` rollover |
| `store/fragment.go` | Per-fragment layout, AES-256-GCM `seal` / `open`, AAD + nonce construction |
| `store/writer.go` | Per-shard writer: buffered fragment assembly, in-place seal, lock-free `WriteAt`, fsync, commit |
| `store/reader.go` | Per-shard reader: extent → batched seg.ReadAt → in-place GCM Open, `ErrIntegrity` on tag failure |
| `store/state.go` | Persistent counters + `storeID` (`state.json`, atomic + fsync'd) |
| `store/extent.go` | Extent record + encoding for the per-node Badger index |
| `internal/keyfile/keyfile.go` | Master-key loader (32-byte, fail-closed on loose perms) + `Fingerprint` for log-safe key identification |
| `backend/distributed/globalstate.go` | GlobalState interface, LocalState, DistributedState |
| `backend/distributed/put.go` | PUT object implementation |
| `backend/distributed/get.go` | GET object implementation |
| `backend/distributed/list.go` | LIST operations implementation |
| `backend/distributed/delete.go` | DELETE object implementation |
| `quic/quicserver/put.go` | QUIC handler for shard PUT (calls `store.Append` + writer) |
| `quic/quicserver/get.go` | QUIC handler for shard GET (calls `store.Lookup` + reader) |
| `quic/quicserver/delete.go` | QUIC handler for shard DELETE (calls `store.Delete`) |

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
| `fragBodySize` | 8 KiB | Fragment body size on disk (GCM ciphertext, same length as plaintext) |
| `fragHeaderSize` | 32 B | Per-fragment header (`FragNum`, `ShardNum`, two reserved slots, `Size`, `Flags`) |
| `fragTagSize` | 16 B | AES-256-GCM authentication tag per fragment |
| `totalFragSize` | 8240 B | header + body + tag, fixed on-disk fragment unit |
| `maxSegSize` | 4 GiB | Maximum segment file size before rollover |
| `fragNumReservation` | 1 048 576 | fragNum batch durably reserved in `state.json` per fsync; bounds wasted nonce space per crash |
| `bufLen` | 32 fragments | Per-shard writer/reader window: amortises `WriteAt` / `ReadAt` syscalls (≈ 256 KiB RAM per active stream) |
| RS schemes | RS(2,1) / RS(3,2) | Erasure coding configuration |
| Hash ring vnodes | 64–256 | Virtual nodes for distribution |
| QUIC concurrent streams | 32–256 | Per-connection stream limit |
| QUIC flow-control windows | see `quic/quicconf` | Receive window sizes (stream and connection) |

---

This file covers the engineering rationale and architecture of Predastore. For
gaps between this design and the current codebase, and for longer-horizon work,
see [TODO.md](./TODO.md).
