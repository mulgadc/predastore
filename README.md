# Predastore

Predastore is a distributed, S3-compatible object storage system with Reed-Solomon erasure coding, built for bare-metal, edge, and on-premise deployments. It is the storage backend for [Hive](https://github.com/mulgadc/hive) — an AWS-compatible infrastructure stack for private clouds.

Predastore can run as a single-node server with local filesystem storage, or scale out to a multi-node cluster with erasure-coded shards, Raft-consensus metadata, and QUIC-based inter-node transport.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     S3 Client (AWS CLI/SDK)                      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Predastore S3D (HTTP/TLS)                     │
│         Auth (SigV4)  ·  Routing  ·  Backend Abstraction        │
└────────┬───────────────────────────────────┬────────────────────┘
         │                                   │
         ▼                                   ▼
┌─────────────────────┐       ┌──────────────────────────────────┐
│  s3db Cluster       │       │  QUIC Shard Nodes                │
│  (Raft Consensus)   │       │  ┌────────┐┌────────┐┌────────┐ │
│                     │       │  │ Node 0 ││ Node 1 ││ Node 2 │ │
│  BoltDB (Raft log)  │       │  │  WAL   ││  WAL   ││  WAL   │ │
│  BadgerDB (FSM)     │       │  │ Badger ││ Badger ││ Badger │ │
└─────────────────────┘       │  └────────┘└────────┘└────────┘ │
                              └──────────────────────────────────┘
```

**S3D** serves the S3 HTTP API with AWS Signature V4 authentication. The **s3db cluster** provides strongly consistent metadata via Raft (HashiCorp Raft + BoltDB + BadgerDB). **QUIC shard nodes** store erasure-coded object data in write-ahead logs, communicating over persistent QUIC connections with pooled, multiplexed streams — eliminating per-request TLS handshakes.

See [DESIGN.md](DESIGN.md) for the full architecture reference, including the data model, QUIC protocol format, Raft consensus details, hash ring placement, and failure handling.

## Key Design Decisions

- **Reed-Solomon erasure coding** — objects are split into data + parity shards (configurable, e.g. RS(3,2) tolerates loss of any 2 nodes). No full replication overhead.
- **Raft consensus for metadata** — bucket and object metadata is strongly consistent across the cluster. Reads can go to any node; writes go through the leader.
- **QUIC transport** — node-to-node shard I/O uses QUIC over UDP with connection pooling. A single long-lived connection per node pair carries multiplexed streams, so shard writes cost only a stream ID allocation, not a TLS handshake.
- **Write-ahead logs** — each shard node writes data to a local WAL for durability before acknowledging. WAL entries are indexed in a local BadgerDB for fast lookups.
- **Consistent hash ring** — shard placement is deterministic via a hash ring with virtual nodes. Adding nodes bumps a ring epoch; old objects stay on the old epoch, new writes use the new one.
- **Single binary** — `s3d` runs the S3 API server, database nodes, and QUIC shard nodes. In development mode everything runs in one process; in production each component can run separately.

## S3 API Compatibility

Predastore implements key S3 operations compatible with AWS CLI, SDKs, and existing S3 tools:

| Category | Operations |
|----------|------------|
| **Buckets** | CreateBucket, DeleteBucket, ListBuckets, HeadBucket |
| **Objects** | PutObject, GetObject, DeleteObject, HeadObject, ListObjects/V2 |
| **Multipart** | InitiateMultipartUpload, UploadPart, CompleteMultipartUpload |
| **Auth** | AWS Signature V4 |

## Quick Start

### Build and Run

```bash
make build
./bin/s3d
```

This starts a single-node server with the filesystem backend on `https://localhost:8443`.

### Configuration

Configure via environment variables or `config.yaml`:

```yaml
server:
  host: "0.0.0.0"
  port: 8443
  tls:
    cert_file: "config/server.pem"
    key_file: "config/server.key"

storage:
  backend: "file"            # "file" or "distributed"
  data_dir: "/var/lib/predastore"

auth:
  access_key: "your-access-key"
  secret_key: "your-secret-key"
```

### AWS CLI Examples

```bash
# Create a bucket
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 mb s3://my-bucket

# Upload a file
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 cp ./file.txt s3://my-bucket/

# List bucket contents
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 ls s3://my-bucket/

# Download a file
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 cp s3://my-bucket/file.txt ./downloaded.txt
```

## Storage Backends

### Filesystem (Default)

Local filesystem storage with a flat layout. Production-ready for single-node deployments — no external dependencies.

### Distributed

Multi-node storage with erasure coding, Raft-consensus metadata, and QUIC transport:

```bash
# Development mode — runs all DB and shard nodes in one process
./bin/s3d -backend distributed -config s3/tests/config/cluster.toml

# Production — separate processes per host
./bin/s3d -backend distributed -config cluster.toml -node 0 -db-node 1   # Host 1
./bin/s3d -backend distributed -config cluster.toml -node 1 -db-node 2   # Host 2
./bin/s3d -backend distributed -config cluster.toml -node 2 -db-node 3   # Host 3
```

The distributed backend's data model decomposes objects into chunks, shards, segments, and blocks:

| Unit | Size | Description |
|------|------|-------------|
| Block | 8 KB | Smallest addressable unit |
| Segment | 64 KB | Independently compressible (8 blocks) |
| Shard | 32 MB | Per-node RS slice (512 segments) |
| Chunk | 32 MB | Logical unit for RS encoding |

See [DESIGN.md](DESIGN.md) for full configuration reference, including database node setup, shard node setup, RS tuning, and deployment modes.

## Hive Integration

Predastore is the default S3 storage provider for [Hive](https://github.com/mulgadc/hive). When running as part of the Hive stack, Predastore integrates via NATS messaging and provides storage for:

- **EC2 AMI images** — machine images for VM launches
- **EBS volume snapshots** — via [Viperblock](https://github.com/mulgadc/viperblock), which uses Predastore as its S3-compatible backend
- **User data** — cloud-init configurations and system artifacts

Predastore subscribes to NATS topics (`s3.putobject`, `s3.getobject`, `s3.createbucket`, etc.) for seamless integration with the rest of the Hive control plane.

## Development

```bash
make build            # Build s3d binary
make test             # Run tests
make preflight        # Full CI checks (fmt, vet, gosec, staticcheck, govulncheck, tests)
make bench            # Benchmarks
make dev              # Hot reload with air
make clean            # Clean build artifacts
```

### Docker

```bash
make docker_s3d           # Build Docker image
make docker_compose_up    # Start with docker-compose
make docker_compose_down  # Stop services
```

### Performance Tuning

For distributed mode, increase system socket buffers for QUIC:

```bash
sudo sysctl -w net.core.rmem_max=7500000
sudo sysctl -w net.core.wmem_max=7500000
```

## Roadmap

- [x] S3 API core (buckets, objects, multipart)
- [x] AWS Signature V4 authentication
- [x] Distributed storage with Reed-Solomon erasure coding
- [x] Raft-consensus metadata (s3db)
- [x] QUIC transport with connection pooling
- [x] Consistent hash ring placement
- [ ] Gossip-based node discovery
- [ ] WAL compaction and garbage collection
- [ ] Automatic shard rebalancing
- [ ] Background read-repair
- [ ] Bucket versioning
- [ ] Lifecycle policies

## License

Apache 2.0 License. See [LICENSE](LICENSE) for details.
