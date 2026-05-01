# Predastore

Predastore developed by [Mulga Defense Corporation](https://mulgadc.com/) is a distributed, S3-compatible object storage system with Reed-Solomon erasure coding, built for bare-metal, edge, and on-premise deployments. It is the storage backend for [Spinifex](https://github.com/mulgadc/spinifex) — an AWS-compatible infrastructure stack for private clouds.

Predastore runs as a distributed cluster with erasure-coded shards, Raft-consensus metadata, and QUIC-based inter-node transport. For development, all nodes run in a single process on loopback.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     S3 Client (AWS CLI/SDK)                     │
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
│  (Raft Consensus)   │       │  ┌────────┐┌────────┐┌────────┐  │
│                     │       │  │ Node 0 ││ Node 1 ││ Node 2 │  │
│  BoltDB (Raft log)  │       │  │ Store  ││ Store  ││ Store  │  │
│  BadgerDB (FSM)     │       │  │(seg+ix)││(seg+ix)││(seg+ix)│  │
└─────────────────────┘       │  └────────┘└────────┘└────────┘  │
                              └──────────────────────────────────┘
```

**S3D** serves the S3 HTTP API with AWS Signature V4 authentication. The **s3db cluster** provides strongly consistent metadata via Raft (HashiCorp Raft + BoltDB + BadgerDB). **QUIC shard nodes** store erasure-coded object data in append-only segment files, with each shard occupying a contiguous extent indexed by a per-node BadgerDB. Inter-node communication uses persistent QUIC connections with pooled, multiplexed streams — eliminating per-request TLS handshakes.

See [DESIGN.md](DESIGN.md) for the full architecture reference, including the data model, QUIC protocol format, Raft consensus details, hash ring placement, and failure handling.

## Key Design Decisions

- **Reed-Solomon erasure coding** — objects are split into data + parity shards (configurable, e.g. RS(3,2) tolerates loss of any 2 nodes). No full replication overhead.
- **Raft consensus for metadata** — bucket and object metadata is strongly consistent across the cluster. Reads can go to any node; writes go through the leader.
- **QUIC transport** — node-to-node shard I/O uses QUIC over UDP with connection pooling. A single long-lived connection per node pair carries multiplexed streams, so shard writes cost only a stream ID allocation, not a TLS handshake.
- **Append-only segments** — each shard node writes data to large append-only segment files. A shard occupies a contiguous extent within one segment, pre-allocated to enable lock-free writing to disk. A per-node BadgerDB index maps shard keys to extents.
- **Consistent hash ring** — shard placement is deterministic via a hash ring with virtual nodes. Adding nodes bumps a ring epoch; old objects stay on the old epoch, new writes use the new one.
- **Single binary** — `./bin/s3d` runs one cluster node (S3 API server + Raft database + QUIC shard node). A cluster is N `s3d` processes pointed at the same config; `./scripts/start.sh` launches all of them locally on loopback aliases for development.

## S3 API Compatibility

Predastore implements key S3 operations compatible with AWS CLI, SDKs, and existing S3 tools:

| Category | Operations |
|----------|------------|
| **Buckets** | CreateBucket, DeleteBucket, ListBuckets, HeadBucket |
| **Objects** | PutObject, GetObject, DeleteObject, HeadObject, ListObjects/V2 |
| **Multipart** | InitiateMultipartUpload, UploadPart, CompleteMultipartUpload |
| **Auth** | AWS Signature V4 |

## Quick Start

### Build

```bash
make build              # builds ./bin/s3d (also generates dev TLS certs)
```

### Run a Dev Cluster

The `./scripts/` directory contains helpers for running a multi-node cluster
locally on loopback IP aliases — the recommended way to exercise the
distributed code paths in development:

```bash
./scripts/start.sh 3node        # launch a 3-node cluster
./scripts/start.sh -w 5node     # launch a 5-node cluster, wait until ready
./scripts/stop.sh               # stop all running clusters
./scripts/clean.sh              # stop and wipe cluster data
./scripts/bench.sh 3node        # run warp benchmark against a cluster
./scripts/bench.sh disk         # run raw-disk fio benchmark
```

Cluster runtime data (logs, PID files, segment files, BadgerDB indexes) lives
under `$PREDA_DIR` (default `/tmp/predastore/<clustername>/`). The start script
sets up loopback IP aliases (requires `sudo`) and generates TLS certs on first
run.

### Run a Single Node

`./bin/s3d` is a single-node process — for running one node of a cluster
directly (e.g. on a dedicated host in production, or for inspecting one
node in isolation):

```bash
./bin/s3d \
  --config config/3node.toml \
  --node 1 \
  --host 10.11.12.1 \
  --port 8443 \
  --base-path /tmp/predastore/3node \
  --tls-key /tmp/predastore/3node/server.key \
  --tls-cert /tmp/predastore/3node/server.pem
```

### Configuration

Cluster configurations live under `config/` as TOML files, one per topology:

```
config/
  3node.toml    # 3 db + 3 storage nodes
  5node.toml    # 5 db + 5 storage nodes
  7node.toml    # 7 db + 7 storage nodes
```

Each config defines `[[db]]` and `[[storage]]` sections specifying node IDs,
hosts, ports, and Reed-Solomon parameters.

TLS certificates are generated on first build:

```bash
make certs              # Generate certs/server.{pem,key}
```

### AWS CLI Examples

```bash
# Create a bucket
aws --endpoint-url https://10.11.12.1:8443/ s3 mb s3://my-bucket

# Upload a file
aws --endpoint-url https://10.11.12.1:8443/ s3 cp ./file.txt s3://my-bucket/

# List bucket contents
aws --endpoint-url https://10.11.12.1:8443/ s3 ls s3://my-bucket/

# Download a file
aws --endpoint-url https://10.11.12.1:8443/ s3 cp s3://my-bucket/file.txt ./downloaded.txt
```

## Storage Backend

Distributed storage with erasure coding, Raft-consensus metadata, and QUIC transport.
The simplest way to bring up a cluster locally:

```bash
./scripts/start.sh -w 3node     # 3-node cluster on loopback aliases
```

The distributed backend's data model:

| Unit | Size | Description |
|------|------|-------------|
| Object | arbitrary | RS-encoded end-to-end into K data + M parity shards |
| Shard | `⌈object_size / K⌉` | Per-node RS slice; occupies a contiguous extent |
| Fragment | 8 KB payload + 32 B header | On-disk unit; CRC-validated independently |
| Segment file | up to 4 GiB | Append-only container holding extents from one or more shards |

See [DESIGN.md](DESIGN.md) for full configuration reference, including database node setup, shard node setup, RS tuning, and deployment modes.

## Spinifex Integration

Predastore is the default S3 storage provider for [Spinifex](https://github.com/mulgadc/spinifex). When running as part of the Spinifex stack, Predastore integrates via NATS messaging and provides storage for:

- **EC2 AMI images** — machine images for VM launches
- **EBS volume snapshots** — via [Viperblock](https://github.com/mulgadc/viperblock), which uses Predastore as its S3-compatible backend
- **User data** — cloud-init configurations and system artifacts

Predastore subscribes to NATS topics (`s3.putobject`, `s3.getobject`, `s3.createbucket`, etc.) for seamless integration with the rest of the Spinifex control plane.

## Development

```bash
make build            # Build s3d binary (also generates TLS certs)
make certs            # Generate dev TLS certs
make test             # Run tests
make preflight        # Full CI checks (lint, govulncheck, tests, race detector)
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
- [ ] Segment compaction and garbage collection
- [ ] Automatic shard rebalancing
- [ ] Background read-repair
- [ ] Bucket versioning
- [ ] Lifecycle policies

## License

Apache 2.0 License. See [LICENSE](LICENSE) for details.
