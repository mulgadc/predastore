# Predastore Implementation Status

This document tracks the implementation status of Predastore's pluggable storage backend architecture. For the full architecture design, see [DESIGN.md](./DESIGN.md).

---

## Table of Contents

1. [Implementation Overview](#1-implementation-overview)
2. [Completed Components](#2-completed-components)
3. [Backend Interface](#3-backend-interface)
4. [Filesystem Backend](#4-filesystem-backend)
5. [Distributed Backend](#5-distributed-backend)
6. [Global Metadata (s3db)](#6-global-metadata-s3db)
7. [QUIC Transport](#7-quic-transport)
8. [Reed-Solomon Encoding](#8-reed-solomon-encoding)
9. [Remaining Work](#9-remaining-work)
10. [Testing](#10-testing)

---

## 1. Implementation Overview

Predastore has successfully implemented a pluggable backend architecture supporting two storage modes:

| Backend | Status | Description |
|---------|--------|-------------|
| **Filesystem** | Complete | Local file storage for development and single-node deployments |
| **Distributed** | Complete | Clustered storage with Raft consensus, erasure coding, and QUIC transport |

### Key Design Decisions (Implemented)

| Decision | Implementation | Location |
|----------|---------------|----------|
| Interface granularity | Object-level operations | `backend/backend.go` |
| Backend selection | Configuration-driven via `-backend` flag | `cmd/s3d/main.go` |
| Metadata storage | Raft + BadgerDB for distributed; local BadgerDB for filesystem | `s3db/`, `backend/distributed/globalstate.go` |
| Erasure coding | Per-object Reed-Solomon | `backend/distributed/put.go`, `backend/distributed/get.go` |
| Node transport | QUIC streams | `quic/quicclient/`, `quic/quicserver/` |
| Consensus | HashiCorp Raft | `s3db/raft.go` |

---

## 2. Completed Components

### Core Infrastructure

| Component | Files | Status |
|-----------|-------|--------|
| Backend Interface | `backend/backend.go`, `backend/types.go` | Complete |
| Error Types | `backend/errors.go` | Complete |
| Filesystem Backend | `backend/filesystem/*.go` | Complete |
| Distributed Backend | `backend/distributed/*.go` | Complete |
| S3 API Handlers | `s3/routes.go` | Complete |
| AWS SigV4 Auth | `auth/sigv4.go`, `s3/authmiddleware.go` | Complete |

### Distributed Database (s3db)

| Component | Files | Status |
|-----------|-------|--------|
| Raft Consensus | `s3db/raft.go` | Complete |
| FSM (BadgerDB) | `s3db/fsm.go` | Complete |
| HTTPS REST API | `s3db/server.go` | Complete |
| Client Library | `s3db/client.go` | Complete |
| SigV4 Authentication | `s3db/auth.go` | Complete |
| Configuration | `s3db/config.go` | Complete |

### Shard Distribution

| Component | Files | Status |
|-----------|-------|--------|
| Hash Ring | `backend/distributed/distributed.go` (buraksezer/consistent) | Complete |
| Reed-Solomon Codec | Uses `klauspost/reedsolomon` | Complete |
| WAL Storage | `s3/wal/` | Complete |
| QUIC Client | `quic/quicclient/` | Complete |
| QUIC Server | `quic/quicserver/` | Complete |

---

## 3. Backend Interface

The backend interface is HTTP-layer agnostic and uses typed request/response structs:

```go
// backend/backend.go

type Backend interface {
    // Object operations
    GetObject(ctx context.Context, req *GetObjectRequest) (*GetObjectResponse, error)
    HeadObject(ctx context.Context, bucket, key string) (*HeadObjectResponse, error)
    PutObject(ctx context.Context, req *PutObjectRequest) (*PutObjectResponse, error)
    DeleteObject(ctx context.Context, req *DeleteObjectRequest) error

    // Bucket operations
    ListBuckets(ctx context.Context) (*ListBucketsResponse, error)
    ListObjects(ctx context.Context, req *ListObjectsRequest) (*ListObjectsResponse, error)

    // Multipart upload operations
    CreateMultipartUpload(ctx context.Context, req *CreateMultipartUploadRequest) (*CreateMultipartUploadResponse, error)
    UploadPart(ctx context.Context, req *UploadPartRequest) (*UploadPartResponse, error)
    CompleteMultipartUpload(ctx context.Context, req *CompleteMultipartUploadRequest) (*CompleteMultipartUploadResponse, error)
    AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error

    // Backend info
    Type() string
    Close() error
}
```

### Request/Response Types

All types are defined in `backend/types.go`:

- `GetObjectRequest`, `GetObjectResponse` - Object retrieval with range support
- `PutObjectRequest`, `PutObjectResponse` - Object storage with chunked encoding support
- `ListObjectsRequest`, `ListObjectsResponse` - S3 ListObjectsV2 compatible
- `CreateMultipartUploadRequest/Response`, `UploadPartRequest/Response`, `CompleteMultipartUploadRequest/Response` - Full multipart support

### Typed Errors

```go
// backend/errors.go

var (
    ErrNotFound       = &BackendError{Code: "NoSuchKey", Message: "The specified key does not exist", HTTPStatus: 404}
    ErrBucketNotFound = &BackendError{Code: "NoSuchBucket", Message: "The specified bucket does not exist", HTTPStatus: 404}
    ErrAccessDenied   = &BackendError{Code: "AccessDenied", Message: "Access Denied", HTTPStatus: 403}
    // ... more S3-compatible errors
)
```

---

## 4. Filesystem Backend

The filesystem backend wraps local file operations for single-node deployments.

### Implementation Files

```
backend/filesystem/
├── filesystem.go      # Backend initialization
├── get.go            # GetObject, HeadObject
├── put.go            # PutObject
├── delete.go         # DeleteObject
├── list.go           # ListObjects, ListBuckets
├── multipart.go      # Multipart upload operations
└── filesystem_test.go
```

### Usage

```bash
# Run with filesystem backend (default)
./bin/s3d -config config/server.toml

# Explicit filesystem backend
./bin/s3d -backend filesystem -config config/server.toml
```

---

## 5. Distributed Backend

The distributed backend implements erasure-coded storage across multiple nodes.

### Implementation Files

```
backend/distributed/
├── distributed.go     # Backend initialization, hash ring, RS encoding
├── globalstate.go     # GlobalState interface (LocalState, DistributedState)
├── get.go            # GetObject with RS decoding
├── put.go            # PutObject with RS encoding
├── delete.go         # DeleteObject across nodes
├── list.go           # ListObjects via s3db
├── multipart.go      # Multipart operations
└── distributed_test.go
```

### GlobalState Interface

```go
// backend/distributed/globalstate.go

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
- **LocalState**: Wraps local BadgerDB (for testing/single-node)
- **DistributedState**: Wraps s3db.Client (for production clusters)

### ObjectToShardNodes Metadata

```go
type ObjectToShardNodes struct {
    Object           [32]byte   // SHA-256 hash of bucket/key
    Size             int64      // Original object size
    DataShardNodes   []uint32   // Node IDs holding data shards
    ParityShardNodes []uint32   // Node IDs holding parity shards
}
```

### Usage

```bash
# Run distributed backend (development mode - all nodes local)
./bin/s3d -backend distributed -config s3/tests/config/cluster.toml

# Run specific node in production
./bin/s3d -backend distributed -config cluster.toml -node 0 -db-node 1
```

---

## 6. Global Metadata (s3db)

The s3db cluster provides strongly consistent metadata storage using Raft.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        s3db Node Storage                                │
│                                                                         │
│  ┌─────────────────────────────┐    ┌─────────────────────────────┐    │
│  │         BoltDB              │    │         BadgerDB            │    │
│  │     (raft.db file)          │    │     (badger/ directory)     │    │
│  │                             │    │                             │    │
│  │  - Raft Log Store           │    │  - Object metadata          │    │
│  │  - Stable Store             │    │  - Bucket info              │    │
│  │  - Term/vote state          │    │  - ARN mappings             │    │
│  └─────────────────────────────┘    └─────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Implementation Files

```
s3db/
├── config.go    # Cluster configuration, port calculation
├── raft.go      # RaftNode, leader election, consensus
├── fsm.go       # Finite State Machine (applies commands to BadgerDB)
├── server.go    # HTTPS REST API with SigV4 auth
├── client.go    # Client library for connecting to cluster
├── auth.go      # AWS SigV4 signing/verification
└── s3db.go      # Local BadgerDB wrapper
```

### REST API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/put/{table}/{hex-key}` | Store key-value (leader only) |
| GET | `/v1/get/{table}/{hex-key}` | Retrieve value (any node) |
| DELETE | `/v1/delete/{table}/{hex-key}` | Delete key (leader only) |
| GET | `/v1/scan/{table}?prefix=X&limit=N` | Scan keys with prefix |
| GET | `/v1/leader` | Get current leader info |
| GET | `/status` | Node status and Raft state |

---

## 7. QUIC Transport

QUIC provides efficient node-to-node communication for shard distribution.

### Implementation Files

```
quic/
├── quicclient/
│   └── client.go    # QUIC client for PUT/GET operations
└── quicserver/
    └── server.go    # QUIC server handling shard requests
```

### Protocol

**PUT Request:**
```go
type PutRequest struct {
    Bucket     string
    Object     string
    ObjectHash [32]byte
    ShardSize  int
}
```

**GET Request:**
```go
type ObjectRequest struct {
    Bucket string
    Object string
}
```

---

## 8. Reed-Solomon Encoding

Using `github.com/klauspost/reedsolomon` for erasure coding.

### Configuration

```toml
[rs]
data = 3    # Data shards
parity = 2  # Parity shards
```

### Encoding Flow (PUT)

1. Receive object body, write to temp file
2. Generate object hash from bucket/key
3. Determine target nodes via consistent hash ring
4. Split object into `data` shards using Reed-Solomon
5. Generate `parity` shards
6. Send each shard to assigned node via QUIC
7. Store metadata in s3db cluster

### Decoding Flow (GET)

1. Query s3db for object metadata (ObjectToShardNodes)
2. Fetch data shards from nodes via QUIC
3. If any shard missing/corrupt, fetch parity shards
4. Reconstruct original data using Reed-Solomon
5. Stream to client

---

## 9. Remaining Work

### High Priority

| Feature | Description | Complexity |
|---------|-------------|------------|
| **Node Failure Detection** | Detect unavailable nodes and trigger degraded reads | Medium |
| **Degraded Read Handling** | Automatic fallback to parity shards when data shards unavailable | Medium |
| **Background Repair** | Heal objects with missing/corrupt shards | High |
| **WAL Compaction** | Garbage collection for deleted objects using DeletedObjectInfo | High |

### Medium Priority

| Feature | Description | Complexity |
|---------|-------------|------------|
| **Dynamic Bucket Operations** | Create/delete buckets via s3db (currently config-only) | Medium |
| **Read Replicas** | Allow reads from s3db followers for better scaling | Low |
| **Cluster Rebalancing** | Redistribute shards when nodes join/leave | High |
| **Metrics & Observability** | Prometheus metrics, structured logging | Medium |

### Future Enhancements

| Feature | Description | Complexity |
|---------|-------------|------------|
| **Gossip Protocol** | Replace static `cluster.toml` with dynamic node discovery | High |
| **Storage Classes** | Per-object redundancy selection (like MinIO) | Medium |
| **Object Versioning** | S3-compatible versioning | High |
| **Lifecycle Policies** | Automatic object expiration | Medium |
| **Compression** | Optional LZ4/Snappy compression per segment | Low |
| **Event Notifications** | S3-compatible event hooks | Medium |

### Not Planned (Deferred)

| Feature | Reason |
|---------|--------|
| gRPC for metadata | HTTPS REST with SigV4 is sufficient |
| Volume-based storage | WAL approach working well |
| Multi-region replication | Focus on single-region first |

---

## 10. Testing

### Running Tests

```bash
# Unit tests
make test

# Integration tests (requires cluster)
go test -tags=integration ./...

# Specific package tests
go test ./backend/distributed/...
go test ./s3db/...
```

### Test Configuration

Example cluster configuration for testing: `s3/tests/config/cluster.toml`

### Test Coverage

| Package | Coverage | Notes |
|---------|----------|-------|
| `backend/distributed` | Partial | Needs more edge case coverage |
| `backend/filesystem` | Good | Core operations covered |
| `s3db` | Good | Raft, FSM, auth tests |
| `s3` | Good | S3 API handlers, SigV4 |
| `auth` | Good | Signature verification |

---

## References

- [DESIGN.md](./DESIGN.md) - Full architecture documentation
- [HashiCorp Raft](https://github.com/hashicorp/raft) - Consensus library
- [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) - Erasure coding
- [buraksezer/consistent](https://github.com/buraksezer/consistent) - Consistent hashing
- [quic-go](https://github.com/quic-go/quic-go) - QUIC implementation

---

*Last Updated: 2026-01-09*
