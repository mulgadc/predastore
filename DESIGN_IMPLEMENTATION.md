# Predastore Implementation Guide: Pluggable Storage Backend Architecture

This document provides a comprehensive implementation guide for evolving Predastore from its current filesystem-only backend to a configurable, pluggable storage architecture that supports both local filesystem and distributed storage modes.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Industry Analysis](#2-industry-analysis)
3. [Current State Analysis](#3-current-state-analysis)
4. [Proposed Architecture](#4-proposed-architecture)
5. [Storage Backend Interface](#5-storage-backend-interface)
6. [Implementation: Filesystem Backend](#6-implementation-filesystem-backend)
7. [Implementation: Distributed Backend](#7-implementation-distributed-backend)
8. [Configuration System](#8-configuration-system)
9. [Migration Strategy](#9-migration-strategy)
10. [Testing Strategy](#10-testing-strategy)
11. [Implementation Roadmap](#11-implementation-roadmap)
12. [Recommendations & Improvements](#12-recommendations--improvements)

---

## 1. Executive Summary

### Goals

1. **Pluggable Backend Architecture**: Introduce a storage interface abstraction allowing runtime selection of storage backends (filesystem, distributed)
2. **Backward Compatibility**: Preserve existing filesystem behavior for development and single-node deployments
3. **Forward Compatibility**: Lay the groundwork for the distributed architecture described in DESIGN.md
4. **Simplicity**: Keep the design simple and pragmatic, avoiding over-engineering

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Interface granularity | Object-level operations | Matches S3 API semantics; simpler than chunk-level |
| Backend selection | Configuration-driven | Runtime flexibility without recompilation |
| Metadata storage | Per-backend responsibility | Filesystem uses FS; distributed uses Raft+Badger |
| Erasure coding scope | Per-object (like MinIO) | Simpler healing, per-object storage class |

---

## 2. Industry Analysis

### MinIO Approach

MinIO's architecture provides valuable lessons for Predastore:

- **Erasure Sets**: Fixed groups of 4-16 drives as coding units
- **Object-Level Erasure**: Unlike volume-level RAID, enables per-object healing
- **Consistent Hashing**: SipHash for deterministic object placement
- **Storage Classes**: Per-object redundancy selection via headers

**Applicable to Predastore**: Object-level erasure coding, storage class headers, automatic erasure set sizing.

**Reference**: [MinIO Distributed Design](https://github.com/minio/minio/blob/master/docs/distributed/DESIGN.md)

### Ceph RADOS

Ceph's CRUSH algorithm and RADOS architecture offer insights:

- **CRUSH Algorithm**: Pseudo-random placement without central lookup
- **Placement Groups**: Indirection layer between objects and OSDs
- **Failure Domains**: Topology-aware placement (rack, datacenter)
- **Self-Healing**: Autonomous repair without central coordination

**Applicable to Predastore**: Hash ring concept aligns with CRUSH; placement groups map to chunk placement.

**Reference**: [Ceph Architecture](https://docs.ceph.com/en/reef/architecture/)

### AWS S3

S3's design principles, revealed at USENIX FAST '23:

- **ShardStore**: Rust-based LSM tree for storage nodes
- **Multi-AZ Redundancy**: Cross-datacenter erasure coding
- **Append-Only Design**: Immutability for consistency
- **300+ Microservices**: Extreme separation of concerns

**Applicable to Predastore**: Append-only shard files, separation of metadata from data path.

**Reference**: [Building and Operating S3](https://www.allthingsdistributed.com/2023/07/building-and-operating-a-pretty-big-storage-system.html)

### SeaweedFS

SeaweedFS demonstrates pragmatic design choices:

- **Volume-Based Storage**: Multiple files per volume reduces inode overhead
- **Pluggable Metadata**: Backend-agnostic metadata storage (Redis, MySQL, etc.)
- **Master/Volume Separation**: Clean split between coordination and storage
- **Facebook Haystack Inspiration**: Proven blob storage design

**Applicable to Predastore**: Pluggable backend pattern, volume/shard file concept.

**Reference**: [SeaweedFS Architecture](https://github.com/seaweedfs/seaweedfs)

---

## 3. Current State Analysis

### Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    S3 API Layer                         │
│  (routes.go, authmiddleware.go)                        │
├─────────────────────────────────────────────────────────┤
│                   Handler Layer                         │
│  (get.go, put.go, delete.go, list.go, multipart.go)   │
├─────────────────────────────────────────────────────────┤
│              Direct OS Filesystem Calls                 │
│  (os.Open, os.Create, os.ReadDir, os.Remove, etc.)    │
└─────────────────────────────────────────────────────────┘
```

### Key Files and Responsibilities

| File | Current Responsibility | Storage Coupling |
|------|----------------------|------------------|
| `s3/get.go` | GetObject, GetObjectHead | `os.Open()`, `os.Stat()` |
| `s3/put.go` | PutObject | `os.Create()`, `os.MkdirAll()` |
| `s3/delete.go` | DeleteObject | `os.Remove()` |
| `s3/list.go` | ListObjectsV2 | `os.ReadDir()` |
| `s3/multipartupload.go` | Multipart operations | `os.TempDir()`, file operations |
| `s3/config.go` | Bucket configuration | `Type: "fs"`, `Pathname` |

### Pain Points

1. **No Abstraction**: Handlers directly call `os.*` functions
2. **Hardcoded Paths**: Bucket pathname resolution embedded in handlers
3. **No Streaming Interface**: Files read entirely or in fixed chunks
4. **Multipart Temp Storage**: Uses OS temp directory, not distributed
5. **No Metadata Layer**: File metadata derived from filesystem stats

---

## 4. Proposed Architecture

### Layered Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    S3 API Layer                         │
│  (routes.go, authmiddleware.go)                        │
├─────────────────────────────────────────────────────────┤
│                   Handler Layer                         │
│  (get.go, put.go, delete.go, list.go, multipart.go)   │
├─────────────────────────────────────────────────────────┤
│               Storage Backend Interface                 │
│  (storage/backend.go)                                  │
├─────────────────┬───────────────────────────────────────┤
│   Filesystem    │          Distributed                  │
│    Backend      │           Backend                     │
│ (storage/fs/)   │     (storage/distributed/)           │
│                 │                                       │
│  - Local files  │  - Raft metadata                     │
│  - os.* calls   │  - Hash ring placement               │
│  - Single node  │  - Reed-Solomon encoding             │
│                 │  - QUIC node communication           │
└─────────────────┴───────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| **Storage Interface** | Defines contract for all backends |
| **Backend Registry** | Registers and instantiates backends by type |
| **Filesystem Backend** | Local file storage (existing behavior) |
| **Distributed Backend** | Clustered storage per DESIGN.md |
| **Config Loader** | Parses storage configuration |

---

## 5. Storage Backend Interface

### Core Interface Definition

```go
// storage/backend.go

package storage

import (
    "context"
    "io"
    "time"
)

// ObjectInfo contains metadata about a stored object
type ObjectInfo struct {
    Key          string
    Size         int64
    LastModified time.Time
    ETag         string
    ContentType  string
    Metadata     map[string]string
}

// ListOptions configures object listing behavior
type ListOptions struct {
    Prefix       string
    Delimiter    string
    StartAfter   string
    MaxKeys      int
    ContinuationToken string
}

// ListResult contains the results of a list operation
type ListResult struct {
    Contents       []ObjectInfo
    CommonPrefixes []string
    IsTruncated    bool
    NextContinuationToken string
}

// MultipartUpload represents an in-progress multipart upload
type MultipartUpload struct {
    UploadID    string
    Key         string
    Initiated   time.Time
}

// PartInfo describes an uploaded part
type PartInfo struct {
    PartNumber   int
    Size         int64
    ETag         string
    LastModified time.Time
}

// Backend defines the interface for storage backends
type Backend interface {
    // Object Operations
    GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error)
    GetObjectRange(ctx context.Context, bucket, key string, start, end int64) (io.ReadCloser, *ObjectInfo, error)
    PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, opts PutOptions) (*ObjectInfo, error)
    DeleteObject(ctx context.Context, bucket, key string) error
    HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error)

    // Listing
    ListObjects(ctx context.Context, bucket string, opts ListOptions) (*ListResult, error)

    // Multipart Upload
    CreateMultipartUpload(ctx context.Context, bucket, key string) (*MultipartUpload, error)
    UploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, reader io.Reader, size int64) (*PartInfo, error)
    CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []PartInfo) (*ObjectInfo, error)
    AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
    ListParts(ctx context.Context, bucket, key, uploadID string) ([]PartInfo, error)

    // Bucket Operations (optional, can be delegated to config)
    BucketExists(ctx context.Context, bucket string) (bool, error)

    // Lifecycle
    Close() error
}

// PutOptions configures object upload behavior
type PutOptions struct {
    ContentType  string
    Metadata     map[string]string
    StorageClass string // "STANDARD", "REDUCED_REDUNDANCY"
}

// BackendConfig contains backend-specific configuration
type BackendConfig struct {
    Type    string                 // "fs", "distributed"
    Options map[string]interface{} // Backend-specific options
}
```

### Backend Registry Pattern

```go
// storage/registry.go

package storage

import (
    "fmt"
    "sync"
)

// BackendFactory creates a backend instance from configuration
type BackendFactory func(config BackendConfig) (Backend, error)

var (
    registryMu sync.RWMutex
    registry   = make(map[string]BackendFactory)
)

// Register adds a backend factory to the registry
func Register(name string, factory BackendFactory) {
    registryMu.Lock()
    defer registryMu.Unlock()
    registry[name] = factory
}

// NewBackend creates a backend instance by type
func NewBackend(config BackendConfig) (Backend, error) {
    registryMu.RLock()
    factory, ok := registry[config.Type]
    registryMu.RUnlock()

    if !ok {
        return nil, fmt.Errorf("unknown storage backend type: %s", config.Type)
    }

    return factory(config)
}

// AvailableBackends returns registered backend types
func AvailableBackends() []string {
    registryMu.RLock()
    defer registryMu.RUnlock()

    types := make([]string, 0, len(registry))
    for t := range registry {
        types = append(types, t)
    }
    return types
}
```

### Error Types

```go
// storage/errors.go

package storage

import "errors"

var (
    ErrNotFound          = errors.New("object not found")
    ErrBucketNotFound    = errors.New("bucket not found")
    ErrAlreadyExists     = errors.New("object already exists")
    ErrInvalidRange      = errors.New("invalid range")
    ErrUploadNotFound    = errors.New("upload not found")
    ErrPartNotFound      = errors.New("part not found")
    ErrInvalidPartOrder  = errors.New("invalid part order")
    ErrBackendUnavailable = errors.New("backend unavailable")
    ErrQuorumNotMet      = errors.New("quorum not met")
)

// IsNotFound returns true if the error indicates object not found
func IsNotFound(err error) bool {
    return errors.Is(err, ErrNotFound)
}

// IsBucketNotFound returns true if the error indicates bucket not found
func IsBucketNotFound(err error) bool {
    return errors.Is(err, ErrBucketNotFound)
}
```

---

## 6. Implementation: Filesystem Backend

### Structure

```
storage/
├── backend.go          # Interface definitions
├── registry.go         # Backend registry
├── errors.go           # Error types
└── fs/
    ├── backend.go      # Filesystem backend implementation
    ├── multipart.go    # Multipart upload handling
    └── fs_test.go      # Unit tests
```

### Implementation

```go
// storage/fs/backend.go

package fs

import (
    "context"
    "crypto/md5"
    "encoding/hex"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "sync"
    "time"

    "predastore/storage"
)

func init() {
    storage.Register("fs", NewFilesystemBackend)
}

// Config holds filesystem backend configuration
type Config struct {
    BasePath string            // Root path for all buckets
    Buckets  map[string]string // bucket name -> path mapping
}

// FilesystemBackend implements storage.Backend using local filesystem
type FilesystemBackend struct {
    config   Config
    mu       sync.RWMutex
    uploads  map[string]*multipartState // uploadID -> state
}

// NewFilesystemBackend creates a new filesystem backend
func NewFilesystemBackend(cfg storage.BackendConfig) (storage.Backend, error) {
    config := Config{
        Buckets: make(map[string]string),
    }

    if basePath, ok := cfg.Options["base_path"].(string); ok {
        config.BasePath = basePath
    }

    if buckets, ok := cfg.Options["buckets"].(map[string]string); ok {
        config.Buckets = buckets
    }

    return &FilesystemBackend{
        config:  config,
        uploads: make(map[string]*multipartState),
    }, nil
}

// resolvePath converts bucket/key to filesystem path
func (b *FilesystemBackend) resolvePath(bucket, key string) (string, error) {
    bucketPath, ok := b.config.Buckets[bucket]
    if !ok {
        return "", storage.ErrBucketNotFound
    }

    // Prevent path traversal
    cleanKey := filepath.Clean(key)
    if strings.HasPrefix(cleanKey, "..") {
        return "", fmt.Errorf("invalid key: path traversal detected")
    }

    return filepath.Join(bucketPath, cleanKey), nil
}

// GetObject retrieves an object from the filesystem
func (b *FilesystemBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
    path, err := b.resolvePath(bucket, key)
    if err != nil {
        return nil, nil, err
    }

    file, err := os.Open(path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, nil, storage.ErrNotFound
        }
        return nil, nil, err
    }

    info, err := file.Stat()
    if err != nil {
        file.Close()
        return nil, nil, err
    }

    if info.IsDir() {
        file.Close()
        return nil, nil, storage.ErrNotFound
    }

    objInfo := &storage.ObjectInfo{
        Key:          key,
        Size:         info.Size(),
        LastModified: info.ModTime(),
        ETag:         generateETag(bucket, key, info.ModTime()),
    }

    return file, objInfo, nil
}

// GetObjectRange retrieves a byte range from an object
func (b *FilesystemBackend) GetObjectRange(ctx context.Context, bucket, key string, start, end int64) (io.ReadCloser, *storage.ObjectInfo, error) {
    path, err := b.resolvePath(bucket, key)
    if err != nil {
        return nil, nil, err
    }

    file, err := os.Open(path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, nil, storage.ErrNotFound
        }
        return nil, nil, err
    }

    info, err := file.Stat()
    if err != nil {
        file.Close()
        return nil, nil, err
    }

    // Validate range
    if start < 0 || start >= info.Size() {
        file.Close()
        return nil, nil, storage.ErrInvalidRange
    }

    if end < 0 || end >= info.Size() {
        end = info.Size() - 1
    }

    // Seek to start position
    if _, err := file.Seek(start, io.SeekStart); err != nil {
        file.Close()
        return nil, nil, err
    }

    objInfo := &storage.ObjectInfo{
        Key:          key,
        Size:         end - start + 1,
        LastModified: info.ModTime(),
        ETag:         generateETag(bucket, key, info.ModTime()),
    }

    // Wrap in limited reader
    return &limitedReadCloser{
        Reader: io.LimitReader(file, end-start+1),
        Closer: file,
    }, objInfo, nil
}

// PutObject stores an object to the filesystem
func (b *FilesystemBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, opts storage.PutOptions) (*storage.ObjectInfo, error) {
    path, err := b.resolvePath(bucket, key)
    if err != nil {
        return nil, err
    }

    // Create parent directories
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, err
    }

    // Create file
    file, err := os.Create(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    // Calculate MD5 while writing
    hash := md5.New()
    writer := io.MultiWriter(file, hash)

    written, err := io.Copy(writer, reader)
    if err != nil {
        os.Remove(path) // Clean up on error
        return nil, err
    }

    etag := hex.EncodeToString(hash.Sum(nil))

    return &storage.ObjectInfo{
        Key:          key,
        Size:         written,
        LastModified: time.Now(),
        ETag:         etag,
        ContentType:  opts.ContentType,
    }, nil
}

// DeleteObject removes an object from the filesystem
func (b *FilesystemBackend) DeleteObject(ctx context.Context, bucket, key string) error {
    path, err := b.resolvePath(bucket, key)
    if err != nil {
        return err
    }

    bucketPath := b.config.Buckets[bucket]

    err = os.Remove(path)
    if err != nil && !os.IsNotExist(err) {
        return err
    }

    // Clean up empty parent directories
    dir := filepath.Dir(path)
    for dir != bucketPath && dir != "." {
        if err := os.Remove(dir); err != nil {
            break // Directory not empty or other error
        }
        dir = filepath.Dir(dir)
    }

    return nil
}

// HeadObject retrieves object metadata without the body
func (b *FilesystemBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.ObjectInfo, error) {
    path, err := b.resolvePath(bucket, key)
    if err != nil {
        return nil, err
    }

    info, err := os.Stat(path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, storage.ErrNotFound
        }
        return nil, err
    }

    if info.IsDir() {
        return nil, storage.ErrNotFound
    }

    return &storage.ObjectInfo{
        Key:          key,
        Size:         info.Size(),
        LastModified: info.ModTime(),
        ETag:         generateETag(bucket, key, info.ModTime()),
    }, nil
}

// ListObjects lists objects in a bucket with optional prefix filtering
func (b *FilesystemBackend) ListObjects(ctx context.Context, bucket string, opts storage.ListOptions) (*storage.ListResult, error) {
    bucketPath, ok := b.config.Buckets[bucket]
    if !ok {
        return nil, storage.ErrBucketNotFound
    }

    result := &storage.ListResult{
        Contents:       make([]storage.ObjectInfo, 0),
        CommonPrefixes: make([]string, 0),
    }

    prefixDir := filepath.Join(bucketPath, opts.Prefix)

    // Check if prefix points to a directory
    prefixInfo, err := os.Stat(prefixDir)
    if err == nil && prefixInfo.IsDir() {
        // List directory contents
        entries, err := os.ReadDir(prefixDir)
        if err != nil {
            return nil, err
        }

        for _, entry := range entries {
            relPath := filepath.Join(opts.Prefix, entry.Name())

            if entry.IsDir() {
                if opts.Delimiter == "/" {
                    result.CommonPrefixes = append(result.CommonPrefixes, relPath+"/")
                }
            } else {
                info, err := entry.Info()
                if err != nil {
                    continue
                }
                result.Contents = append(result.Contents, storage.ObjectInfo{
                    Key:          relPath,
                    Size:         info.Size(),
                    LastModified: info.ModTime(),
                    ETag:         generateETag(bucket, relPath, info.ModTime()),
                })
            }
        }
    } else {
        // Walk and filter by prefix
        filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
            if err != nil {
                return nil
            }

            relPath, _ := filepath.Rel(bucketPath, path)
            if relPath == "." {
                return nil
            }

            if !strings.HasPrefix(relPath, opts.Prefix) {
                return nil
            }

            if info.IsDir() {
                return nil
            }

            result.Contents = append(result.Contents, storage.ObjectInfo{
                Key:          relPath,
                Size:         info.Size(),
                LastModified: info.ModTime(),
                ETag:         generateETag(bucket, relPath, info.ModTime()),
            })

            return nil
        })
    }

    // Sort by key
    sort.Slice(result.Contents, func(i, j int) bool {
        return result.Contents[i].Key < result.Contents[j].Key
    })

    // Apply MaxKeys limit
    if opts.MaxKeys > 0 && len(result.Contents) > opts.MaxKeys {
        result.Contents = result.Contents[:opts.MaxKeys]
        result.IsTruncated = true
    }

    return result, nil
}

// BucketExists checks if a bucket is configured
func (b *FilesystemBackend) BucketExists(ctx context.Context, bucket string) (bool, error) {
    _, ok := b.config.Buckets[bucket]
    return ok, nil
}

// Close releases resources
func (b *FilesystemBackend) Close() error {
    return nil
}

// Helper types and functions

type limitedReadCloser struct {
    io.Reader
    io.Closer
}

func generateETag(bucket, key string, modTime time.Time) string {
    h := md5.New()
    h.Write([]byte(fmt.Sprintf("%s/%s/%d", bucket, key, modTime.UnixNano())))
    return fmt.Sprintf("\"%s\"", hex.EncodeToString(h.Sum(nil)))
}
```

---

## 7. Implementation: Distributed Backend

### Architecture Overview

The distributed backend implements the architecture from DESIGN.md:

```
┌────────────────────────────────────────────────────────────────┐
│                    Distributed Backend                          │
├────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐│
│  │ Hash Ring   │  │  RS Codec   │  │   Node Manager          ││
│  │ Placement   │  │  Encoder/   │  │   - Health checks       ││
│  │             │  │  Decoder    │  │   - Connection pool     ││
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘│
│         │                │                      │              │
│  ┌──────▼────────────────▼──────────────────────▼─────────────┐│
│  │                  Chunk Manager                              ││
│  │  - Split objects into chunks                                ││
│  │  - Coordinate RS encoding                                   ││
│  │  - Manage shard distribution                                ││
│  └──────────────────────────┬─────────────────────────────────┘│
│                             │                                   │
│  ┌──────────────────────────▼─────────────────────────────────┐│
│  │                  Metadata Layer                             ││
│  │  - Raft consensus (global metadata)                         ││
│  │  - Badger KV (local + global stores)                        ││
│  └──────────────────────────┬─────────────────────────────────┘│
│                             │                                   │
│  ┌──────────────────────────▼─────────────────────────────────┐│
│  │                  Transport Layer                            ││
│  │  - QUIC streams for shard transfer                          ││
│  │  - gRPC for metadata operations                             ││
│  └────────────────────────────────────────────────────────────┘│
└────────────────────────────────────────────────────────────────┘
```

### Core Data Structures

```go
// storage/distributed/types.go

package distributed

import (
    "crypto/sha256"
    "encoding/binary"
    "time"
)

// RSConfig defines Reed-Solomon parameters
type RSConfig struct {
    DataShards   uint8 // Number of data shards (k)
    ParityShards uint8 // Number of parity shards (m)
}

// Common RS configurations
var (
    RS_2_1 = RSConfig{DataShards: 2, ParityShards: 1} // 3 nodes minimum
    RS_3_2 = RSConfig{DataShards: 3, ParityShards: 2} // 5 nodes minimum
    RS_4_2 = RSConfig{DataShards: 4, ParityShards: 2} // 6 nodes minimum
)

// ObjectID is a 32-byte identifier derived from bucket/key
type ObjectID [32]byte

// NewObjectID creates an ObjectID from bucket and key
func NewObjectID(bucket, key string) ObjectID {
    h := sha256.New()
    h.Write([]byte(bucket))
    h.Write([]byte{0})
    h.Write([]byte(key))
    var id ObjectID
    copy(id[:], h.Sum(nil))
    return id
}

// ChunkLayout describes how a chunk is stored
type ChunkLayout struct {
    ObjectID   ObjectID
    ChunkIndex uint32
    Size       uint64
    RS         RSConfig
    RingEpoch  uint32
    Checksum   uint32
    Created    time.Time
}

// ShardLocation identifies where a shard is stored
type ShardLocation struct {
    NodeID     string
    ShardRole  uint8 // 0..DataShards-1 for data, DataShards..total-1 for parity
    Offset     uint64
    Length     uint64
    Checksum   uint32
}

// NodeInfo describes a storage node
type NodeInfo struct {
    ID         string
    Address    string
    Port       uint16
    QuicPort   uint16
    Weight     uint32 // For weighted placement
    Zone       string // Failure domain
    State      NodeState
    LastSeen   time.Time
    Capacity   uint64
    Used       uint64
}

type NodeState int

const (
    NodeStateUnknown NodeState = iota
    NodeStateHealthy
    NodeStateDegraded
    NodeStateOffline
)

// Constants
const (
    BlockSize   = 8 * 1024        // 8 KB
    SegmentSize = 64 * 1024       // 64 KB (8 blocks)
    ShardSize   = 32 * 1024 * 1024 // 32 MB
    ChunkSize   = 32 * 1024 * 1024 // 32 MB
)
```

### Hash Ring Implementation

```go
// storage/distributed/ring.go

package distributed

import (
    "crypto/sha256"
    "encoding/binary"
    "sort"
    "sync"
)

// HashRing implements consistent hashing with virtual nodes
type HashRing struct {
    mu           sync.RWMutex
    vnodes       int
    ring         []uint64
    nodeMap      map[uint64]string // hash -> nodeID
    nodes        map[string]*NodeInfo
    epoch        uint32
}

// NewHashRing creates a new hash ring
func NewHashRing(vnodes int) *HashRing {
    return &HashRing{
        vnodes:  vnodes,
        ring:    make([]uint64, 0),
        nodeMap: make(map[uint64]string),
        nodes:   make(map[string]*NodeInfo),
    }
}

// AddNode adds a node to the ring
func (r *HashRing) AddNode(node *NodeInfo) {
    r.mu.Lock()
    defer r.mu.Unlock()

    r.nodes[node.ID] = node

    // Add virtual nodes
    for i := 0; i < r.vnodes; i++ {
        hash := r.hashKey(node.ID, i)
        r.ring = append(r.ring, hash)
        r.nodeMap[hash] = node.ID
    }

    sort.Slice(r.ring, func(i, j int) bool {
        return r.ring[i] < r.ring[j]
    })

    r.epoch++
}

// RemoveNode removes a node from the ring
func (r *HashRing) RemoveNode(nodeID string) {
    r.mu.Lock()
    defer r.mu.Unlock()

    delete(r.nodes, nodeID)

    // Remove virtual nodes
    newRing := make([]uint64, 0, len(r.ring)-r.vnodes)
    for _, hash := range r.ring {
        if r.nodeMap[hash] != nodeID {
            newRing = append(newRing, hash)
        } else {
            delete(r.nodeMap, hash)
        }
    }
    r.ring = newRing
    r.epoch++
}

// GetNodes returns the nodes responsible for a key
func (r *HashRing) GetNodes(bucket, key string, chunkIndex uint32, count int) []string {
    r.mu.RLock()
    defer r.mu.RUnlock()

    if len(r.ring) == 0 {
        return nil
    }

    hash := r.hashChunk(bucket, key, chunkIndex)

    // Find starting position
    idx := sort.Search(len(r.ring), func(i int) bool {
        return r.ring[i] >= hash
    })
    if idx == len(r.ring) {
        idx = 0
    }

    // Collect distinct nodes
    nodes := make([]string, 0, count)
    seen := make(map[string]bool)

    for i := 0; i < len(r.ring) && len(nodes) < count; i++ {
        pos := (idx + i) % len(r.ring)
        nodeID := r.nodeMap[r.ring[pos]]

        if !seen[nodeID] {
            seen[nodeID] = true
            nodes = append(nodes, nodeID)
        }
    }

    return nodes
}

// Epoch returns the current ring epoch
func (r *HashRing) Epoch() uint32 {
    r.mu.RLock()
    defer r.mu.RUnlock()
    return r.epoch
}

func (r *HashRing) hashKey(nodeID string, vnode int) uint64 {
    h := sha256.New()
    h.Write([]byte(nodeID))
    binary.Write(h, binary.BigEndian, uint32(vnode))
    sum := h.Sum(nil)
    return binary.BigEndian.Uint64(sum[:8])
}

func (r *HashRing) hashChunk(bucket, key string, chunkIndex uint32) uint64 {
    h := sha256.New()
    h.Write([]byte(bucket))
    h.Write([]byte("/"))
    h.Write([]byte(key))
    h.Write([]byte(":"))
    binary.Write(h, binary.BigEndian, chunkIndex)
    sum := h.Sum(nil)
    return binary.BigEndian.Uint64(sum[:8])
}
```

### Reed-Solomon Codec

```go
// storage/distributed/erasure.go

package distributed

import (
    "github.com/klauspost/reedsolomon"
)

// Encoder handles Reed-Solomon encoding/decoding
type Encoder struct {
    rs     reedsolomon.Encoder
    config RSConfig
}

// NewEncoder creates a new RS encoder
func NewEncoder(config RSConfig) (*Encoder, error) {
    rs, err := reedsolomon.New(int(config.DataShards), int(config.ParityShards))
    if err != nil {
        return nil, err
    }
    return &Encoder{rs: rs, config: config}, nil
}

// Encode splits data into shards with parity
func (e *Encoder) Encode(data []byte) ([][]byte, error) {
    shardSize := (len(data) + int(e.config.DataShards) - 1) / int(e.config.DataShards)

    // Create shards
    shards := make([][]byte, e.config.DataShards+e.config.ParityShards)
    for i := range shards {
        shards[i] = make([]byte, shardSize)
    }

    // Copy data into data shards
    for i := 0; i < int(e.config.DataShards); i++ {
        start := i * shardSize
        end := start + shardSize
        if end > len(data) {
            end = len(data)
        }
        if start < len(data) {
            copy(shards[i], data[start:end])
        }
    }

    // Generate parity shards
    if err := e.rs.Encode(shards); err != nil {
        return nil, err
    }

    return shards, nil
}

// Decode reconstructs data from available shards
func (e *Encoder) Decode(shards [][]byte, dataSize int) ([]byte, error) {
    // Reconstruct missing shards if possible
    if err := e.rs.Reconstruct(shards); err != nil {
        return nil, err
    }

    // Join data shards
    data := make([]byte, 0, dataSize)
    shardSize := len(shards[0])

    for i := 0; i < int(e.config.DataShards); i++ {
        remaining := dataSize - len(data)
        if remaining <= 0 {
            break
        }
        toAdd := shardSize
        if toAdd > remaining {
            toAdd = remaining
        }
        data = append(data, shards[i][:toAdd]...)
    }

    return data, nil
}

// Verify checks if all shards are consistent
func (e *Encoder) Verify(shards [][]byte) (bool, error) {
    return e.rs.Verify(shards)
}
```

### Distributed Backend Implementation (Skeleton)

```go
// storage/distributed/backend.go

package distributed

import (
    "context"
    "fmt"
    "io"
    "sync"

    "predastore/storage"
)

func init() {
    storage.Register("distributed", NewDistributedBackend)
}

// Config holds distributed backend configuration
type Config struct {
    NodeID      string
    ListenAddr  string
    RaftAddr    string
    QuicAddr    string
    DataDir     string
    JoinAddrs   []string
    RSConfig    RSConfig
    VirtualNodes int
}

// DistributedBackend implements storage.Backend for distributed storage
type DistributedBackend struct {
    config    Config
    ring      *HashRing
    metadata  *MetadataStore // Raft + Badger
    transport *Transport     // QUIC connections
    encoder   *Encoder
    mu        sync.RWMutex
}

// NewDistributedBackend creates a new distributed backend
func NewDistributedBackend(cfg storage.BackendConfig) (storage.Backend, error) {
    config := Config{
        RSConfig:     RS_2_1,
        VirtualNodes: 128,
    }

    // Parse configuration from options
    if nodeID, ok := cfg.Options["node_id"].(string); ok {
        config.NodeID = nodeID
    }
    if dataDir, ok := cfg.Options["data_dir"].(string); ok {
        config.DataDir = dataDir
    }
    // ... parse other options

    encoder, err := NewEncoder(config.RSConfig)
    if err != nil {
        return nil, fmt.Errorf("failed to create encoder: %w", err)
    }

    backend := &DistributedBackend{
        config:  config,
        ring:    NewHashRing(config.VirtualNodes),
        encoder: encoder,
    }

    // Initialize metadata store (Raft + Badger)
    // Initialize transport (QUIC)
    // Join cluster if JoinAddrs specified

    return backend, nil
}

// GetObject retrieves an object from the distributed store
func (b *DistributedBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
    // 1. Look up chunk layout from metadata
    objectID := NewObjectID(bucket, key)
    layout, err := b.metadata.GetChunkLayout(ctx, objectID)
    if err != nil {
        return nil, nil, err
    }

    // 2. For each chunk:
    //    a. Determine nodes from hash ring using stored ringEpoch
    //    b. Fetch shards from nodes via QUIC
    //    c. RS decode if necessary
    //    d. Stream to caller

    // 3. Return streaming reader
    reader := &distributedReader{
        backend: b,
        layout:  layout,
        ctx:     ctx,
    }

    info := &storage.ObjectInfo{
        Key:          key,
        Size:         int64(layout.Size),
        LastModified: layout.Created,
        // ETag computed from chunk checksums
    }

    return reader, info, nil
}

// PutObject stores an object to the distributed store
func (b *DistributedBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, opts storage.PutOptions) (*storage.ObjectInfo, error) {
    objectID := NewObjectID(bucket, key)

    // 1. Split object into chunks
    chunkIndex := uint32(0)

    for {
        // Read chunk
        chunk := make([]byte, ChunkSize)
        n, err := io.ReadFull(reader, chunk)
        if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
            return nil, err
        }
        if n == 0 {
            break
        }
        chunk = chunk[:n]

        // 2. RS encode chunk into shards
        shards, err := b.encoder.Encode(chunk)
        if err != nil {
            return nil, err
        }

        // 3. Determine target nodes via hash ring
        totalShards := int(b.config.RSConfig.DataShards + b.config.RSConfig.ParityShards)
        nodes := b.ring.GetNodes(bucket, key, chunkIndex, totalShards)

        if len(nodes) < totalShards {
            return nil, storage.ErrQuorumNotMet
        }

        // 4. Write shards to nodes via QUIC
        for i, shard := range shards {
            nodeID := nodes[i]
            if err := b.transport.WriteShard(ctx, nodeID, objectID, chunkIndex, uint8(i), shard); err != nil {
                // Handle partial failure - may need to rollback or retry
                return nil, err
            }
        }

        // 5. Record chunk layout in metadata (Raft)
        layout := ChunkLayout{
            ObjectID:   objectID,
            ChunkIndex: chunkIndex,
            Size:       uint64(n),
            RS:         b.config.RSConfig,
            RingEpoch:  b.ring.Epoch(),
        }
        if err := b.metadata.PutChunkLayout(ctx, layout); err != nil {
            return nil, err
        }

        chunkIndex++

        if n < ChunkSize {
            break
        }
    }

    return &storage.ObjectInfo{
        Key:  key,
        Size: size,
    }, nil
}

// DeleteObject removes an object from the distributed store
func (b *DistributedBackend) DeleteObject(ctx context.Context, bucket, key string) error {
    objectID := NewObjectID(bucket, key)

    // 1. Get all chunk layouts
    // 2. For each chunk, delete shards from all nodes
    // 3. Remove metadata entries

    return b.metadata.DeleteObject(ctx, objectID)
}

// HeadObject retrieves object metadata
func (b *DistributedBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.ObjectInfo, error) {
    objectID := NewObjectID(bucket, key)
    layout, err := b.metadata.GetChunkLayout(ctx, objectID)
    if err != nil {
        return nil, err
    }

    return &storage.ObjectInfo{
        Key:          key,
        Size:         int64(layout.Size),
        LastModified: layout.Created,
    }, nil
}

// ListObjects lists objects (requires metadata index)
func (b *DistributedBackend) ListObjects(ctx context.Context, bucket string, opts storage.ListOptions) (*storage.ListResult, error) {
    return b.metadata.ListObjects(ctx, bucket, opts)
}

// GetObjectRange retrieves a byte range
func (b *DistributedBackend) GetObjectRange(ctx context.Context, bucket, key string, start, end int64) (io.ReadCloser, *storage.ObjectInfo, error) {
    // Similar to GetObject but with range calculations
    // Determine which chunks/segments contain the range
    // Fetch only necessary data
    return nil, nil, fmt.Errorf("not implemented")
}

// Multipart operations
func (b *DistributedBackend) CreateMultipartUpload(ctx context.Context, bucket, key string) (*storage.MultipartUpload, error) {
    return nil, fmt.Errorf("not implemented")
}

func (b *DistributedBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, reader io.Reader, size int64) (*storage.PartInfo, error) {
    return nil, fmt.Errorf("not implemented")
}

func (b *DistributedBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.PartInfo) (*storage.ObjectInfo, error) {
    return nil, fmt.Errorf("not implemented")
}

func (b *DistributedBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
    return fmt.Errorf("not implemented")
}

func (b *DistributedBackend) ListParts(ctx context.Context, bucket, key, uploadID string) ([]storage.PartInfo, error) {
    return nil, fmt.Errorf("not implemented")
}

func (b *DistributedBackend) BucketExists(ctx context.Context, bucket string) (bool, error) {
    return b.metadata.BucketExists(ctx, bucket)
}

func (b *DistributedBackend) Close() error {
    // Close transport, metadata store, etc.
    return nil
}

// distributedReader streams object data from distributed storage
type distributedReader struct {
    backend *DistributedBackend
    layout  *ChunkLayout
    ctx     context.Context
    // ... additional streaming state
}

func (r *distributedReader) Read(p []byte) (n int, err error) {
    // Stream chunks, decode, and return data
    return 0, io.EOF
}

func (r *distributedReader) Close() error {
    return nil
}
```

---

## 8. Configuration System

### Updated Configuration Schema

```toml
# config/server.toml

version = "2.0"
region = "us-east-1"

# Storage backend configuration
[storage]
# Backend type: "fs" (filesystem) or "distributed"
type = "fs"

# Filesystem backend options (when type = "fs")
[storage.fs]
base_path = "/var/lib/predastore"

# Distributed backend options (when type = "distributed")
[storage.distributed]
node_id = "node-1"
data_dir = "/var/lib/predastore/data"
raft_addr = "127.0.0.1:7000"
quic_addr = "127.0.0.1:7001"
join_addrs = ["node-2:7000", "node-3:7000"]
virtual_nodes = 128

[storage.distributed.erasure]
data_shards = 2
parity_shards = 1

# Bucket definitions
[[buckets]]
name = "downloads"
region = "us-east-1"
public = true
# For filesystem backend, specify pathname
pathname = "/var/data/downloads"

[[buckets]]
name = "backups"
region = "us-east-1"
public = false
# Storage class for distributed backend
storage_class = "STANDARD"

# Authentication
[[auth]]
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
[[auth.policy]]
bucket = "downloads"
actions = ["s3:ListBucket", "s3:GetObject", "s3:PutObject"]
```

### Configuration Loader

```go
// s3/config.go (updated)

package s3

import (
    "predastore/storage"
    _ "predastore/storage/fs"          // Register filesystem backend
    _ "predastore/storage/distributed" // Register distributed backend
)

// StorageConfig holds storage backend configuration
type StorageConfig struct {
    Type        string                 `toml:"type"`
    FS          *FSConfig              `toml:"fs,omitempty"`
    Distributed *DistributedConfig     `toml:"distributed,omitempty"`
}

type FSConfig struct {
    BasePath string `toml:"base_path"`
}

type DistributedConfig struct {
    NodeID       string   `toml:"node_id"`
    DataDir      string   `toml:"data_dir"`
    RaftAddr     string   `toml:"raft_addr"`
    QuicAddr     string   `toml:"quic_addr"`
    JoinAddrs    []string `toml:"join_addrs"`
    VirtualNodes int      `toml:"virtual_nodes"`
    Erasure      struct {
        DataShards   int `toml:"data_shards"`
        ParityShards int `toml:"parity_shards"`
    } `toml:"erasure"`
}

// InitStorage initializes the storage backend from configuration
func InitStorage(cfg *Config) (storage.Backend, error) {
    backendCfg := storage.BackendConfig{
        Type:    cfg.Storage.Type,
        Options: make(map[string]interface{}),
    }

    switch cfg.Storage.Type {
    case "fs":
        if cfg.Storage.FS != nil {
            backendCfg.Options["base_path"] = cfg.Storage.FS.BasePath
        }
        // Build bucket path map from bucket configs
        buckets := make(map[string]string)
        for _, b := range cfg.Buckets {
            buckets[b.Name] = b.Pathname
        }
        backendCfg.Options["buckets"] = buckets

    case "distributed":
        if cfg.Storage.Distributed != nil {
            backendCfg.Options["node_id"] = cfg.Storage.Distributed.NodeID
            backendCfg.Options["data_dir"] = cfg.Storage.Distributed.DataDir
            backendCfg.Options["raft_addr"] = cfg.Storage.Distributed.RaftAddr
            backendCfg.Options["quic_addr"] = cfg.Storage.Distributed.QuicAddr
            backendCfg.Options["join_addrs"] = cfg.Storage.Distributed.JoinAddrs
            backendCfg.Options["virtual_nodes"] = cfg.Storage.Distributed.VirtualNodes
            backendCfg.Options["data_shards"] = cfg.Storage.Distributed.Erasure.DataShards
            backendCfg.Options["parity_shards"] = cfg.Storage.Distributed.Erasure.ParityShards
        }
    }

    return storage.NewBackend(backendCfg)
}
```

---

## 9. Migration Strategy

### Phase 1: Introduce Abstraction (Non-Breaking)

1. Create `storage/` package with interface definitions
2. Implement filesystem backend wrapping existing code
3. Update handlers to use backend interface
4. Ensure all existing tests pass
5. No configuration changes required

### Phase 2: Handler Refactoring

```go
// s3/s3.go (updated)

type S3Server struct {
    Config  *Config
    Backend storage.Backend // Add backend reference
}

// s3/get.go (refactored)

func (s *S3Server) GetObject(c *fiber.Ctx) error {
    bucket := c.Params("bucket")
    key := c.Params("*")

    ctx := c.Context()

    // Use backend interface instead of direct os calls
    reader, info, err := s.Backend.GetObject(ctx, bucket, key)
    if err != nil {
        if storage.IsNotFound(err) {
            return s.SendS3Error(c, "NoSuchKey", "The specified key does not exist", 404)
        }
        return s.SendS3Error(c, "InternalError", err.Error(), 500)
    }
    defer reader.Close()

    // Set headers
    c.Set("Content-Length", fmt.Sprintf("%d", info.Size))
    c.Set("ETag", info.ETag)
    c.Set("Last-Modified", info.LastModified.Format(time.RFC1123))

    // Stream response
    return c.SendStream(reader)
}
```

### Phase 3: Add Distributed Backend

1. Implement distributed backend components incrementally
2. Add integration tests with multi-node setup
3. Update configuration schema
4. Document deployment topology

---

## 10. Testing Strategy

### Unit Tests

```go
// storage/backend_test.go

package storage_test

import (
    "context"
    "io"
    "strings"
    "testing"

    "predastore/storage"
    _ "predastore/storage/fs"
)

func TestBackendInterface(t *testing.T) {
    // Test that all backends implement the interface correctly
    for _, backendType := range storage.AvailableBackends() {
        t.Run(backendType, func(t *testing.T) {
            cfg := getTestConfig(backendType)
            backend, err := storage.NewBackend(cfg)
            if err != nil {
                t.Fatalf("Failed to create backend: %v", err)
            }
            defer backend.Close()

            testPutGetDelete(t, backend)
            testListObjects(t, backend)
            testMultipartUpload(t, backend)
        })
    }
}

func testPutGetDelete(t *testing.T, backend storage.Backend) {
    ctx := context.Background()
    bucket := "test-bucket"
    key := "test-object"
    content := "Hello, World!"

    // Put
    info, err := backend.PutObject(ctx, bucket, key,
        strings.NewReader(content), int64(len(content)), storage.PutOptions{})
    if err != nil {
        t.Fatalf("PutObject failed: %v", err)
    }
    if info.Size != int64(len(content)) {
        t.Errorf("Size mismatch: got %d, want %d", info.Size, len(content))
    }

    // Get
    reader, info, err := backend.GetObject(ctx, bucket, key)
    if err != nil {
        t.Fatalf("GetObject failed: %v", err)
    }
    defer reader.Close()

    data, _ := io.ReadAll(reader)
    if string(data) != content {
        t.Errorf("Content mismatch: got %q, want %q", string(data), content)
    }

    // Delete
    if err := backend.DeleteObject(ctx, bucket, key); err != nil {
        t.Fatalf("DeleteObject failed: %v", err)
    }

    // Verify deleted
    _, _, err = backend.GetObject(ctx, bucket, key)
    if !storage.IsNotFound(err) {
        t.Errorf("Expected NotFound error, got: %v", err)
    }
}
```

### Integration Tests

```go
// storage/distributed/integration_test.go

// +build integration

package distributed_test

func TestThreeNodeCluster(t *testing.T) {
    // Spin up 3-node cluster
    // Test object operations with RS(2,1)
    // Simulate node failure and recovery
}

func TestFiveNodeCluster(t *testing.T) {
    // Spin up 5-node cluster
    // Test with RS(3,2)
    // Test concurrent read/write
}
```

### Benchmark Tests

```go
func BenchmarkPutObject(b *testing.B) {
    sizes := []int{1024, 1024*1024, 32*1024*1024}

    for _, size := range sizes {
        b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
            // Benchmark put operations
        })
    }
}
```

---

## 11. Implementation Roadmap

### Milestone 1: Storage Abstraction (1-2 weeks)

- [ ] Create `storage/` package structure
- [ ] Define `Backend` interface
- [ ] Implement backend registry
- [ ] Create filesystem backend (wrap existing code)
- [ ] Refactor handlers to use interface
- [ ] Update configuration loading
- [ ] Ensure backward compatibility
- [ ] All existing tests pass

### Milestone 2: Filesystem Backend Polish (1 week)

- [ ] Add context support throughout
- [ ] Improve error handling
- [ ] Add proper streaming for large files
- [ ] Implement missing multipart methods
- [ ] Add unit tests for filesystem backend
- [ ] Performance benchmarks

### Milestone 3: Distributed Infrastructure (2-3 weeks)

- [ ] Implement hash ring
- [ ] Integrate Reed-Solomon library
- [ ] Set up Raft + Badger metadata store
- [ ] Implement QUIC transport layer
- [ ] Basic node discovery/membership

### Milestone 4: Distributed Operations (2-3 weeks)

- [ ] Implement distributed PutObject
- [ ] Implement distributed GetObject
- [ ] Implement distributed DeleteObject
- [ ] Implement distributed ListObjects
- [ ] Integration tests with multi-node

### Milestone 5: Resilience & Recovery (2 weeks)

- [ ] Node failure detection
- [ ] Degraded read handling
- [ ] Background repair jobs
- [ ] Cluster rebalancing
- [ ] Operational monitoring

### Milestone 6: Production Readiness (2 weeks)

- [ ] Performance optimization
- [ ] Documentation
- [ ] Deployment guides
- [ ] Operational runbooks

---

## 12. Recommendations & Improvements

### Simplifications from DESIGN.md

1. **Defer Compression**: The DESIGN.md mentions optional LZ4/Snappy compression. Recommend deferring this to a later phase to reduce initial complexity.

2. **Start with RS(2,1)**: Begin with the simpler 3-node configuration before supporting RS(3,2) or higher schemes.

3. **Single Ring Epoch Initially**: While the design supports ring epoch for cluster evolution, the initial implementation can use a fixed epoch.

4. **Metadata Colocation**: Consider starting with local Badger per-node before implementing full Raft consensus for faster iteration.

### Additional Recommendations

#### 1. Object Versioning Support

Consider adding version support to the interface early:

```go
type ObjectInfo struct {
    // ... existing fields
    VersionID string // For future versioning support
}
```

#### 2. Storage Classes

Add storage class support for different redundancy levels:

```go
const (
    StorageClassStandard          = "STANDARD"           // RS(3,2)
    StorageClassReducedRedundancy = "REDUCED_REDUNDANCY" // RS(2,1)
    StorageClassOneZone           = "ONEZONE_IA"         // Single copy
)
```

#### 3. Metrics and Observability

Embed metrics collection in the interface:

```go
type Backend interface {
    // ... existing methods

    // Metrics returns backend-specific metrics
    Metrics() BackendMetrics
}

type BackendMetrics struct {
    ObjectCount     int64
    TotalBytes      int64
    ReadOperations  int64
    WriteOperations int64
    // ... more metrics
}
```

#### 4. Lifecycle Management

Plan for object lifecycle policies:

```go
type LifecycleRule struct {
    ID         string
    Prefix     string
    Expiration time.Duration
    Transition *TransitionRule
}
```

#### 5. Event Notifications

Consider event hooks for future integrations:

```go
type EventType string

const (
    EventObjectCreated = EventType("s3:ObjectCreated:*")
    EventObjectRemoved = EventType("s3:ObjectRemoved:*")
)

type EventSubscriber interface {
    OnEvent(event Event) error
}
```

### Architecture Decisions Summary

| Area | Recommendation | Rationale |
|------|---------------|-----------|
| Interface Granularity | Object-level | Matches S3 semantics |
| Metadata Storage | Raft + Badger | Strong consistency for cluster state |
| Data Transport | QUIC | Multiplexed streams, connection migration |
| Erasure Coding | Per-object (like MinIO) | Simpler healing, flexible storage classes |
| Chunk Size | 32 MB | Balance between latency and throughput |
| Virtual Nodes | 128-256 | Good distribution without excessive memory |

---

## References

- [MinIO Distributed Design](https://github.com/minio/minio/blob/master/docs/distributed/DESIGN.md)
- [MinIO Erasure Coding](https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html)
- [Ceph Architecture](https://docs.ceph.com/en/reef/architecture/)
- [Ceph CRUSH Maps](https://docs.ceph.com/en/reef/rados/operations/crush-map/)
- [AWS S3 Architecture Talk](https://www.allthingsdistributed.com/2023/07/building-and-operating-a-pretty-big-storage-system.html)
- [SeaweedFS Architecture](https://github.com/seaweedfs/seaweedfs)
- [Go Storage Abstraction Patterns](https://justin.azoff.dev/blog/implementing-pluggable-backends-in-go/)
- [ChartMuseum Storage Library](https://github.com/chartmuseum/storage)
- [go-storage (BeyondStorage)](https://github.com/beyondstorage/go-storage)
- [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) - Go RS implementation

---

*Document Version: 1.0*
*Last Updated: 2025-11-27*
