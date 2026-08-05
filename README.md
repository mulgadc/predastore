<p align="center">
  <img src=".github/assets/banner.svg" alt="Predastore by Mulga — distributed, S3-compatible object storage with Reed-Solomon erasure coding, Raft metadata, QUIC transport and encryption at rest.” width="900">
</p>

<p align="center">
  <a href="https://go.dev"><img src="https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-AGPL--3.0-3fb950?style=flat-square" alt="License"></a>
  <a href="https://mulgadc.com"><img src="https://img.shields.io/badge/home-mulga-orange?style=flat-square&logo=data:image/svg%2bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGZpbGw9IiNmZmYiIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZD0iTTE2LjcxOCA4Ljg5MWMtMS4yODUgMS4zNy0zLjE3OCAyLjMxNy00LjY5NSAzLjQ0LS44NTQuNjMtMy4wODkgMi4yNy0xLjYxIDMuMzQ0IDEuNzg4IDEuMjk4IDYuMjQzLjE2NCA3Ljc4NS0xLjI2Ljg0LS43NzUuODE0LTEuODIyLS4zMjgtMi4yNTktMS4yMzUtLjQ3Mi0yLjY2LS4xMTEtMy45MTMuMDg3LS4wNDIuMDA3LS4xMzEuMDU1LS4xMjMtLjAyLjUyMy0uMzM2Ljk5My0uNzUgMS41MDMtMS4xMDIuNDkyLS4zNCAxLjA5Ny0uODI3IDEuNy0uODM4IDEuODcxLS4wMzQgMy43OTkuODkgNC4yODcgMi44MTUuODExIDMuMjAzLTMuMDA2IDUuNzE1LTUuNzg0IDUuOTQybDEuNjE0LS43NDdjLjYwNS0uMzYgMS4yMTctLjczNCAxLjc1Mi0xLjE5Ni4xMzMtLjExNS4yMy0uMjYzLjM0Mi0uMzczLjAyNy0uMDI2LjMwMi0uMTQ0LjE0NC0uMTUtMS40NDEuOTQ1LTMuMTI3IDEuNTcyLTQuODEzIDEuOTMyLS41OC4xMjMtMS4xOTUuMjQyLTEuNzg1LjI1Ny4wMTUuMDguMDg3LjA2NC4xNC4wOC40NzYuMTU0IDEuMDIuMjQ1IDEuNTE2LjMyLjA2OC4wNTItLjAwNi4wNzQtLjA2LjA4MS0uNDQ4LjA1OC0uOTIzLjE0My0xLjM3LjE2Ni0xLjI3LjA2NC0yLjU1LS4wNjgtMy43NzctLjM4bC0uNjktMS45NTdjLS4wNi4wMDYtLjA2My4wNi0uMDc5LjEwMy0uMDYuMTctLjMwNSAxLjQxNy0uMzg3IDEuNDM1LS42Ni0uMi0xLjI1MS0uNjQ3LTEuNzM2LTEuMTMybDEuMTUtMi4wNi0xLjgzNSAxLjAxMWMtLjI1OC0uNTgzLS4zMDUtMS4yNDMtLjIxOS0xLjg3bDIuMzc1LTEuMThjLS43MzktLjA0OS0xLjQ4Ni4wNjgtMi4yMi4xNDUuMTI1LS4zNi4yNzctLjcxOC40NjItMS4wNTEuMDY3LS4xMjIuNDI2LS43MjEuNTItLjcyNC44Ni4xNjQgMS43ODYuMjQxIDIuNjQ4LjA3NGwtMS44Ny0uNzcxLS4wNjktLjA5M2MuMzgyLS4zNi43Ny0uNzE3IDEuMTk5LTEuMDI0LjgwNC4yMzQgMS42My40MjIgMi40NzMuNDMzTDkuMzUgOS4zNDJhNyA3IDAgMCAxIC41NjctLjQwMWMuMTYtLjEwMy42MDktLjQwMi43NzItLjM4LjY1LjIyMSAxLjMyNC4zNzggMi4wMS40MzMtLjMxMy0uMzI1LS45MDktLjU2NC0xLjIxLS44NjgtLjAzMy0uMDM0LS4wNTgtLjAzNi0uMDQzLS4wOTguNzg3LS40NTIgMS42NjItLjkwMSAyLjM0LTEuNTE3LjY5LS42MjkgMS40MjEtMS42NTYuOTI0LTIuNjA1LjU3My4xODIgMS4wNjcuOTA2IDEuMDU0IDEuNTEyLS4wMzQgMS42NzYtMS44MjIgMy4xNy0yLjk0MyA0LjIyMiAxLjczMi0uNzI4IDMuNzE0LTIuMjMgMy43MS00LjMwNS0uMDAzLTEuNjg4LTEuNTQtMi4zNjUtMi45OTMtMi41MThhMy4yIDMuMiAwIDAgMS0uMzg1IDEuMDg3Yy0uNDI4LjcxOS0xLjMwMiAxLjE2OC0xLjc1OCAxLjkxNS0uMzExLjUxLS4zNyAxLjE5NS0xLjAzMSAxLjM5LS4yNy4wOC0uNjE3LjA5My0uODk3LjA5NGwuNjE1LS4zMjVjLjY4OS0uNTA2LjY3Ny0xLjQyIDEuMDk5LTIuMS0uMDUtLjA3LS41NDUtLjItLjY2LS4yMjktLjUxLS4xMjUtMS4zNzUtLjI4NS0xLjg4Ni0uMjUtLjE1Ny4wMS0uODY5LjEyMS0uOTMuMjQyLS4wODguMTc3LjM0MS45My40NjggMS4xMDEuMDI1LjAzNS4wNzcuMDI2LjA4LjAzLjAyNC4wMzEuMDA3LjEwNS0uMDYuMDhhNSA1IDAgMCAxLS40MDQtLjI4M2MtLjUwMy0uMzg4LTEuMDc4LS45NzQtMS40OTktMS40NDctLjA2OC0uMTY2LS4xNi0uMzUuMDAyLS40ODcuODg4LS41NSAxLjc2OC0xLjEyNiAyLjY3LTEuNjUxIDIuNjcxLTEuNTU4IDUuNzExLTMuMzE4IDguMjY3LS40NjUgMi4xMTkgMi4zNjYgMS41MTQgNS4yMTItLjUxMiA3LjM3MnptLTguMjI0LTUuMzhjLjY5OS0uMTIyIDIuMDE4LjU3MiAyLjIzNS0uNDU2LjAyMS0uMS0uMDM2LS4xNTcuMDItLjI1NS4wNDMtLjA3My4yODYtLjI1LjM3LS4zMTguMjctLjIxNy41NzctLjM4Ny44NDItLjYxMi0xLjAyOS4xNjYtMi4wNjUuNzU2LTIuOTY0IDEuMjc3LS4xNDkuMDg2LS4zMS4xNzYtLjQ1MS4yNzMtLjAzNy4wMjUtLjA3NS0uMDA2LS4wNTMuMDltLTQuODMgMTAuODQ0Yy0xLjcxNSAxLjg4MS0xLjMyNSA0LjU3LjQ5NCA2LjIxNyAxLjgxMSAxLjY0MSA0LjY2IDIuMjIzIDcuMDQ4IDIuMTA4IDIuMTUyLS4xMDMgNC4zMzctLjgxMiA2LjQ2LS4wOTEuODI1LjI4IDEuNTQ2LjgwNSAyLjE2IDEuNDExLS4wODMtLjQtLjMyNC0uODE5LS41NTgtMS4xNTktMi4xMy0zLjA5NS02LjI3LTEuOTM1LTkuNDI2LTIuNTUzLTEuNzExLS4zMzUtMy40OTEtMS4xMTgtNC41MzMtMi41NjctLjkwMS0xLjI1My0xLjA0Mi0yLjczLS41OTUtNC4xOTQtLjA5MS0uMDktLjk1Mi43MjEtMS4wNS44MjhtNi4zNjYtOC42NjdjLS4zODIuMzM5LS43ODcuNjYtMS4yMTIuOTQ4bC0xLjIwNy41OWMxLjAyMi4wMzkgMi4wOC0uNTQ2IDIuNDItMS41MzciLz48L3N2Zz4=" alt="mulgadc.com"></a>
</p>

<p align="center">
  <a href="#architecture">Architecture</a> ·
  <a href="#key-design-decisions">Key Design Decisions</a> ·
  <a href="#s3-api-compatibility">S3 API Compatibility</a> ·
  <a href="#quickstart">Quickstart</a> ·
  <a href="#storage-backend">Storage Backend</a> ·
  <a href="#spinifex-integration">Spinifex Integration</a> ·
  <a href="https://docs.mulgadc.com">Docs</a>
</p>

---

# Predastore: Distributed, S3-compatible object storage

Predastore developed by [Mulga Defense Corporation](https://mulgadc.com/) is a distributed, S3-compatible object storage system with Reed-Solomon erasure coding, built for bare-metal, edge, and on-premise deployments. It is the storage backend for [Spinifex](https://github.com/mulgadc/spinifex) — an AWS-compatible infrastructure stack for private clouds.

Predastore runs as a distributed cluster with erasure-coded shards, Raft-consensus metadata, and QUIC-based inter-node transport. For development, all nodes run in a single process on loopback.

## Architecture

<p align="center">
  <img src=".github/assets/platform.svg" alt="Predastore: S3 applications and tooling on top, authenticated S3 services backed by Raft metadata and erasure-coded storage, distributed across encrypted storage nodes over QUIC." width="900">
</p>

**S3D** serves the S3 HTTP API with AWS Signature V4 authentication. The **s3db cluster** provides strongly consistent metadata via Raft (HashiCorp Raft + BoltDB + BadgerDB). **QUIC shard nodes** store erasure-coded object data in append-only segment files, with each shard occupying a contiguous extent indexed by a per-node BadgerDB. Inter-node communication uses persistent QUIC connections with pooled, multiplexed streams — eliminating per-request TLS handshakes.

See [DESIGN.md](docs/DESIGN.md) for the full architecture reference, including the data model, QUIC protocol format, Raft consensus details, hash ring placement, and failure handling.

## Key Design Decisions

- **Reed-Solomon erasure coding** — objects are split into data + parity shards (configurable, e.g. RS(3,2) tolerates loss of any 2 nodes). No full replication overhead.
- **Raft consensus for metadata** — bucket and object metadata is strongly consistent across the cluster. Reads can go to any node; writes go through the leader.
- **QUIC transport** — node-to-node shard I/O uses QUIC over UDP with connection pooling. A single long-lived connection per node pair carries multiplexed streams, so shard writes cost only a stream ID allocation, not a TLS handshake.
- **Append-only segments** — each shard node writes data to large append-only segment files. A shard occupies a contiguous extent within one segment, pre-allocated to enable lock-free writing to disk. A per-node BadgerDB index maps shard keys to extents.
- **AES-256-GCM encryption at rest** — every 8 KiB fragment is sealed under a per-fragment GCM nonce with AAD binding it to its `(objectHash, shardIndex, shardNum, fragNum)` position, so tamper, replay, and cross-shard splice attempts fail to authenticate. GCM is the sole on-disk integrity authority (no separate CRC). A 32-byte cluster master key is loaded from a `0600` file path supplied via `-encryption-key-file` / `ENCRYPTION_KEY_FILE`.
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

The `./scripts/` directory contains helpers for running a multi-node cluster locally on loopback IP aliases.

```bash
./scripts/start.sh 3node        # launch a 3-node cluster
./scripts/start.sh -w 5node     # launch a 5-node cluster, wait until ready
./scripts/stop.sh               # stop all running clusters
./scripts/clean.sh              # stop and wipe cluster data
./scripts/bench.sh 3node        # run warp benchmark against a cluster
./scripts/bench.sh disk         # run raw-disk fio benchmark
```

Cluster runtime data (logs, PID files, segment files, BadgerDB indexes) lives under `$PREDA_DIR` (default `/tmp/predastore/<clustername>/`). The start script sets up loopback IP aliases (requires `sudo`) and generates TLS certs on first run.

### Run a Single Node

`./bin/s3d` is a single-node process — for running one node of a cluster directly (e.g. on a dedicated host in production, or for inspecting one node in isolation):

```bash
./bin/s3d \
  --config config/3node.toml \
  --node 1 \
  --host 10.11.12.1 \
  --port 8443 \
  --base-path /tmp/predastore/3node \
  --tls-key /tmp/predastore/3node/server.key \
  --tls-cert /tmp/predastore/3node/server.pem \
  --encryption-key-file /tmp/predastore/3node/master.key
```

The encryption key file must be exactly 32 raw bytes (no base64, no header) with mode `0600`. Generate one with `( umask 0177 && openssl rand -out master.key 32 )`. The same key must be supplied to every node in a cluster; rotating it is not currently supported (see Roadmap → envelope encryption).

### Configuration

Cluster configurations live under `config/` as TOML files, one per topology:

```
config/
  3node.toml    # 3 db + 3 storage nodes
  5node.toml    # 5 db + 5 storage nodes
  7node.toml    # 7 db + 7 storage nodes
```

Each config defines `[[db]]` and `[[storage]]` sections specifying node IDs, hosts, ports, and Reed-Solomon parameters.

TLS certificates are generated on first build:

```bash
make certs              # Generate certs/server.{pem,key}
```

### Standalone TLS Trust

When predastore is deployed by Spinifex, the cluster CA is installed into the host trust store automatically as part of node bootstrap — no manual action is required. Standalone operators must install the cluster CA into the host trust store before launching `s3d`, otherwise nodes cannot dial each other:

```bash
# Debian / Ubuntu
sudo cp cluster-ca.pem /usr/local/share/ca-certificates/predastore-cluster-ca.crt
sudo update-ca-certificates

# RHEL / Fedora / Amazon Linux
sudo cp cluster-ca.pem /etc/pki/ca-trust/source/anchors/predastore-cluster-ca.pem
sudo update-ca-trust
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

Distributed storage with erasure coding, Raft-consensus metadata, and QUIC transport. The simplest way to bring up a cluster locally:

```bash
./scripts/start.sh -w 3node     # 3-node cluster on loopback aliases
```

The distributed backend's data model:

| Unit | Size | Description |
|------|------|-------------|
| Object | arbitrary | RS-encoded end-to-end into K data + M parity shards |
| Shard | `⌈object_size / K⌉` | Per-node RS slice; occupies a contiguous extent |
| Fragment | 32 B header + 8 KiB body + 16 B GCM tag = 8240 B | On-disk unit; AES-256-GCM seals body with AAD bound to `(objectHash, shardIndex, shardNum, fragNum)` |
| Segment file | up to 4 GiB | Append-only container holding extents from one or more shards |

See [DESIGN.md](docs/DESIGN.md) for full configuration reference, including database node setup, shard node setup, RS tuning, and deployment modes.

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
- [x] AES-256-GCM encryption at rest (single cluster-wide master key)
- [ ] Envelope encryption (master key rotation, per-bucket / per-tenant keys)
- [ ] Gossip-based node discovery
- [ ] Segment compaction and garbage collection
- [ ] Automatic shard rebalancing
- [ ] Background read-repair
- [ ] Bucket versioning
- [ ] Lifecycle policies

## License

Predastore is licensed under the GNU Affero General Public License v3.0 (AGPLv3). See [LICENSE](LICENSE) for the full text.
