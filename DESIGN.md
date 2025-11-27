# Predastore Distributed Object Storage Architecture

For versions of Predastore < v0.1.5 the default storage layer was the local filesystem as a development POC.

This document outlines the required distributed object storage architecture for Predastore > v1.0 which is a work in progress. The improved design to meet v1.0 milestones will include:

- Global metadata (Raft + Badger)
- Local shard storage and per-node KV
- Reed–Solomon encoding (RS(2,1) / RS(3,2))
- Hash ring placement (scalable to 32+ nodes)
- QUIC-based node-to-node streaming
- Compressed block segments inside shard files
- Recovery mechanisms
- Cluster evolution (adding nodes, switching RS schemes)

This file serves as the implementation guide for developers.

---

# 1. Core Goals

Predastore is a distributed, S3-compatible, erasure-coded object store designed for:

- High throughput read/write performance
- Strong consistency for metadata
- Efficient disk layout for large objects
- Ability to scale from 3 nodes (RS(2,1)) to 32+ nodes (RS(3,2))
- Node-to-node QUIC streaming for partial reads and reconstruction
- Local rebuildability if Badger DBs are lost
- Smooth cluster evolution as nodes are added or RS scheme changes

---

# 2. Logical Data Model

## 2.1 Objects → Chunks → Shards → Segments → Blocks

```
Object
 └── Chunks (logical 32 MB units)
      └── Shards (per-node RS slices, 32 MB each)
           └── Segments (compressed 64 KB units, note compression OPTIONAL)
                └── Blocks (8 KB logical)
```

### Sizes

- Block: 8 KB
- Segment: 64 KB logical (8 blocks), compressed independently
- Shard: 32 MB logical per node (4096 blocks / 512 segments)
- Chunk: 32 MB logical unit for RS encoding

---

# 3. Hash Ring Placement

Predastore uses a consistent hash ring with virtual nodes.

### Steps

1. Compute `hash64 = first8(SHA256(bucket/path:chunkIndex))`
2. Locate ring position ≥ hash64
3. Select next K+M distinct nodes based on RSConfig

---

# 4. Global Metadata (Raft + Badger)

Tracks logical object/chunk layout.

### ChunkLayout

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

### Key Format

```
[ 32 byte objectID | 4 byte chunk index ]
```

### Recovery

- If Badger lost: Raft snapshot + log replay rebuilds state.

---

# 5. Local Node Storage

Each node stores shards and local mapping:

```
(objectID, chunkIndex, shardRole) → {ShardID, offset, length, crc}
```

Stored in a local Badger instance.

---

# 6. Shard File Format

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

1. Read shard headers
2. Load footer / `.idx` file
3. Recreate all local KV entries

---

# 7. Reed–Solomon Encoding

Supports RS(2,1) as the default, RS(3,2) and user configurable.

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

# 8. QUIC Protocol for Shard Fetching

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

# 9. awsgw Streaming Layer

- Accepts S3 commands
- Looks up metadata
- Computes node set via ringEpoch
- Fetches segments over QUIC from multiple nodes
- Performs RS decode if needed
- Streams ordered bytes to client

### Ordered Writer

Maintains next-to-write chunk and buffers out-of-order results.

---

# 10. Failure Handling & Repair

- CRC mismatch → fallback to parity
- Node timeout → fallback to parity
- If < DataShards available → GET fails
- After degraded read → schedule background read-repair

---

# 11. Cluster Evolution

### Adding nodes

- Update hash ring → bump ringEpoch
- New writes use new epoch
- Old objects remain on old epoch
- Background rebalancer optional

### Changing RS

- Mixed RS(2,1) and RS(3,2) allowed as per design, RS encoded into badger KV to determine configuration
- New objects can switch RSConfig
- Old objects can be migrated in background

---

# 12. Implementation Roadmap

1. Implement ring + ringEpoch
2. Define metadata serialization (protobuf/msgpack)
3. Implement Raft FSM + Badger global metadata layer
4. Implement shard file format
5. Implement shard index rebuild scanner
6. Implement QUIC FetchShard RPC
7. Implement awsgw chunk pipeline
8. Implement RS encoder/decoder
9. Implement background repair
10. Integrate cluster membership changes

---

# 13. Tunable Parameters

| Parameter         | Default           |
| ----------------- | ----------------- |
| Block size        | 8 KB              |
| Segment size      | 64 KB             |
| Shard size        | 32 MB             |
| RS schemes        | RS(2,1) / RS(3,2) |
| Hash ring vnodes  | 64–256            |
| QUIC stream limit | 32–256            |
| In-flight chunks  | 16–32             |

---

This file is the authoritative reference for Predastore implementation.
