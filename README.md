<p align="center">
  <img src=".github/assets/banner.svg" alt="Predastore by Mulga — distributed, S3-compatible object storage with Reed-Solomon erasure coding, Raft metadata, QUIC transport and encryption at rest.” width="900">
</p>

<p align="center">
  <a href="https://go.dev"><img src="https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-AGPL--3.0-3fb950?style=flat-square" alt="License"></a>
  <a href="https://mulgadc.com"><img src="https://img.shields.io/badge/home-mulga-orange?style=flat-square&logo=data:image/svg%2bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGZpbGw9IiNmZmYiIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZD0iTTE2LjcxOCA4Ljg5MWMtMS4yODUgMS4zNy0zLjE3OCAyLjMxNy00LjY5NSAzLjQ0LS44NTQuNjMtMy4wODkgMi4yNy0xLjYxIDMuMzQ0IDEuNzg4IDEuMjk4IDYuMjQzLjE2NCA3Ljc4NS0xLjI2Ljg0LS43NzUuODE0LTEuODIyLS4zMjgtMi4yNTktMS4yMzUtLjQ3Mi0yLjY2LS4xMTEtMy45MTMuMDg3LS4wNDIuMDA3LS4xMzEuMDU1LS4xMjMtLjAyLjUyMy0uMzM2Ljk5My0uNzUgMS41MDMtMS4xMDIuNDkyLS4zNCAxLjA5Ny0uODI3IDEuNy0uODM4IDEuODcxLS4wMzQgMy43OTkuODkgNC4yODcgMi44MTUuODExIDMuMjAzLTMuMDA2IDUuNzE1LTUuNzg0IDUuOTQybDEuNjE0LS43NDdjLjYwNS0uMzYgMS4yMTctLjczNCAxLjc1Mi0xLjE5Ni4xMzMtLjExNS4yMy0uMjYzLjM0Mi0uMzczLjAyNy0uMDI2LjMwMi0uMTQ0LjE0NC0uMTUtMS40NDEuOTQ1LTMuMTI3IDEuNTcyLTQuODEzIDEuOTMyLS41OC4xMjMtMS4xOTUuMjQyLTEuNzg1LjI1Ny4wMTUuMDguMDg3LjA2NC4xNC4wOC40NzYuMTU0IDEuMDIuMjQ1IDEuNTE2LjMyLjA2OC4wNTItLjAwNi4wNzQtLjA2LjA4MS0uNDQ4LjA1OC0uOTIzLjE0My0xLjM3LjE2Ni0xLjI3LjA2NC0yLjU1LS4wNjgtMy43NzctLjM4bC0uNjktMS45NTdjLS4wNi4wMDYtLjA2My4wNi0uMDc5LjEwMy0uMDYuMTctLjMwNSAxLjQxNy0uMzg3IDEuNDM1LS42Ni0uMi0xLjI1MS0uNjQ3LTEuNzM2LTEuMTMybDEuMTUtMi4wNi0xLjgzNSAxLjAxMWMtLjI1OC0uNTgzLS4zMDUtMS4yNDMtLjIxOS0xLjg3bDIuMzc1LTEuMThjLS43MzktLjA0OS0xLjQ4Ni4wNjgtMi4yMi4xNDUuMTI1LS4zNi4yNzctLjcxOC40NjItMS4wNTEuMDY3LS4xMjIuNDI2LS43MjEuNTItLjcyNC44Ni4xNjQgMS43ODYuMjQxIDIuNjQ4LjA3NGwtMS44Ny0uNzcxLS4wNjktLjA5M2MuMzgyLS4zNi43Ny0uNzE3IDEuMTk5LTEuMDI0LjgwNC4yMzQgMS42My40MjIgMi40NzMuNDMzTDkuMzUgOS4zNDJhNyA3IDAgMCAxIC41NjctLjQwMWMuMTYtLjEwMy42MDktLjQwMi43NzItLjM4LjY1LjIyMSAxLjMyNC4zNzggMi4wMS40MzMtLjMxMy0uMzI1LS45MDktLjU2NC0xLjIxLS44NjgtLjAzMy0uMDM0LS4wNTgtLjAzNi0uMDQzLS4wOTguNzg3LS40NTIgMS42NjItLjkwMSAyLjM0LTEuNTE3LjY5LS42MjkgMS40MjEtMS42NTYuOTI0LTIuNjA1LjU3My4xODIgMS4wNjcuOTA2IDEuMDU0IDEuNTEyLS4wMzQgMS42NzYtMS44MjIgMy4xNy0yLjk0MyA0LjIyMiAxLjczMi0uNzI4IDMuNzE0LTIuMjMgMy43MS00LjMwNS0uMDAzLTEuNjg4LTEuNTQtMi4zNjUtMi45OTMtMi41MThhMy4yIDMuMiAwIDAgMS0uMzg1IDEuMDg3Yy0uNDI4LjcxOS0xLjMwMiAxLjE2OC0xLjc1OCAxLjkxNS0uMzExLjUxLS4zNyAxLjE5NS0xLjAzMSAxLjM5LS4yNy4wOC0uNjE3LjA5My0uODk3LjA5NGwuNjE1LS4zMjVjLjY4OS0uNTA2LjY3Ny0xLjQyIDEuMDk5LTIuMS0uMDUtLjA3LS41NDUtLjItLjY2LS4yMjktLjUxLS4xMjUtMS4zNzUtLjI4NS0xLjg4Ni0uMjUtLjE1Ny4wMS0uODY5LjEyMS0uOTMuMjQyLS4wODguMTc3LjM0MS45My40NjggMS4xMDEuMDI1LjAzNS4wNzcuMDI2LjA4LjAzLjAyNC4wMzEuMDA3LjEwNS0uMDYuMDhhNSA1IDAgMCAxLS40MDQtLjI4M2MtLjUwMy0uMzg4LTEuMDc4LS45NzQtMS40OTktMS40NDctLjA2OC0uMTY2LS4xNi0uMzUuMDAyLS40ODcuODg4LS41NSAxLjc2OC0xLjEyNiAyLjY3LTEuNjUxIDIuNjcxLTEuNTU4IDUuNzExLTMuMzE4IDguMjY3LS40NjUgMi4xMTkgMi4zNjYgMS41MTQgNS4yMTItLjUxMiA3LjM3MnptLTguMjI0LTUuMzhjLjY5OS0uMTIyIDIuMDE4LjU3MiAyLjIzNS0uNDU2LjAyMS0uMS0uMDM2LS4xNTcuMDItLjI1NS4wNDMtLjA3My4yODYtLjI1LjM3LS4zMTguMjctLjIxNy41NzctLjM4Ny44NDItLjYxMi0xLjAyOS4xNjYtMi4wNjUuNzU2LTIuOTY0IDEuMjc3LS4xNDkuMDg2LS4zMS4xNzYtLjQ1MS4yNzMtLjAzNy4wMjUtLjA3NS0uMDA2LS4wNTMuMDltLTQuODMgMTAuODQ0Yy0xLjcxNSAxLjg4MS0xLjMyNSA0LjU3LjQ5NCA2LjIxNyAxLjgxMSAxLjY0MSA0LjY2IDIuMjIzIDcuMDQ4IDIuMTA4IDIuMTUyLS4xMDMgNC4zMzctLjgxMiA2LjQ2LS4wOTEuODI1LjI4IDEuNTQ2LjgwNSAyLjE2IDEuNDExLS4wODMtLjQtLjMyNC0uODE5LS41NTgtMS4xNTktMi4xMy0zLjA5NS02LjI3LTEuOTM1LTkuNDI2LTIuNTUzLTEuNzExLS4zMzUtMy40OTEtMS4xMTgtNC41MzMtMi41NjctLjkwMS0xLjI1My0xLjA0Mi0yLjczLS41OTUtNC4xOTQtLjA5MS0uMDktLjk1Mi43MjEtMS4wNS44MjhtNi4zNjYtOC42NjdjLS4zODIuMzM5LS43ODcuNjYtMS4yMTIuOTQ4bC0xLjIwNy41OWMxLjAyMi4wMzkgMi4wOC0uNTQ2IDIuNDItMS41MzciLz48L3N2Zz4=" alt="mulgadc.com"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#s3-api-support">S3 API support</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#configuration">Configuration</a> ·
  <a href="#spinifex-integration">Spinifex integration</a> ·
  <a href="#development">Development</a> ·
  <a href="#roadmap">Roadmap</a> ·
  <a href="https://docs.mulgadc.com">Docs</a>
</p>

---

# Predastore: Distributed, S3-compatible object storage

Predastore is a distributed object-storage system implementing commonly used Amazon S3 APIs. It combines Raft metadata, Reed–Solomon erasure coding, QUIC transport and append-only storage segments.

Predastore can run independently and provides the default object-storage backend for Spinifex.

## Quick Start

### Build

```bash
make build
```

### Run a Development Cluster

`scripts/start.sh` launches a cluster from one of the profiles in `config/`, generating the TLS keypair and master key under `$PREDA_DIR` (default `/tmp/predastore`) on first run:

```bash
./scripts/start.sh -w 1host     # one process, S3 on https://127.0.0.1:8443
```

Three profiles ship with the repo. They differ only in how many hosts the nodes are spread over, which is what decides the transport between them:

| Profile | Hosts | RS | Inter-node transport | Needs `sudo` |
|---------|-------|----|----------------------|--------------|
| `1host` | 1 | (1, 0) | in-process pipe only | no |
| `3host` | 3 on `10.11.12.{1,2,3}` | (2, 1) | QUIC between hosts, pipe within | yes |
| `5host` | 5 on `10.11.12.{1..5}` | (3, 2) | QUIC between hosts, pipe within | yes |

The multi-host profiles put every host on one machine behind loopback aliases, so they need `sudo` to add those aliases and to install the generated certificate as a trust anchor. Each host answers S3 on its own address.

Stop, reset or benchmark the cluster with:

```bash
./scripts/stop.sh
./scripts/clean.sh
./scripts/bench.sh 3host
./scripts/bench.sh disk
```

### Run a Host

`./bin/s3d` runs one host of a cluster: the nodes the config pins to it, the S3 gate among them. `-host` names the `[[host]]` to run, and everything else about the process — which nodes, which addresses, which ports — follows from that entry:

```bash
./bin/s3d \
  -config cluster.toml \
  -host 1 \
  -data-dir /var/lib/predastore \
  -tls-cert /etc/predastore/server.pem \
  -tls-key /etc/predastore/server.key \
  -encryption-key /etc/predastore/master.key
```

| Flag | Description |
|------|-------------|
| `-config` | Path to the configuration file (required) |
| `-host` | ID of the `[[host]]` this process runs (required) |
| `-data-dir` | On-disk root for this host's nodes; overrides `data_dir` |
| `-encryption-key` | Path to the 32-byte AES-256 key protecting data at rest; overrides `encryption_key` |
| `-tls-cert`, `-tls-key` | This host's TLS identity; overrides `tls_cert` / `tls_key` |
| `-bind-addr` | Local listen address, without a port; overrides `bind_addr` |
| `-log-level` | `debug`, `info`, `warn` or `error` |

Each of these host-local settings may come from either the file or a flag, so the same configuration file can be deployed unchanged to every machine and the paths supplied per process. The S3 port is not a flag: it is the gate node's `port`.

A cluster whose `[[host.node]]` entries all sit under one `[[host]]` runs entirely in one process over the in-process pipe, with no inter-node socket and no certificate beyond the one the gate serves. That is a property of the config, not a launch mode.

The encryption key file must be exactly 32 raw bytes (no base64, no header) with mode `0600`. Generate one with `( umask 0177 && openssl rand -out master.key 32 )`. The same key must be supplied to every host in a cluster; rotating it is not currently supported (see Roadmap → envelope encryption).

## S3 API Support

| Area | Operations |
| --- | --- |
| Buckets | `CreateBucket`, `DeleteBucket`, `ListBuckets`, `HeadBucket` |
| Objects | `PutObject`, `GetObject`, `DeleteObject`, `HeadObject`, `ListObjects`, `ListObjectsV2` |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload` |
| Authentication | AWS Signature Version 4, presigned URLs |

Multi-object delete (`DeleteObjects`) is not implemented; delete objects one at a time.

### AWS CLI Examples

Against the `1host` dev profile. Its certificate is self-signed, so either install it as a trust anchor or pass `--no-verify-ssl`:

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=ap-southeast-2

# Create a bucket
aws --no-verify-ssl --endpoint-url https://127.0.0.1:8443/ s3 mb s3://my-bucket

# Upload a file
aws --no-verify-ssl --endpoint-url https://127.0.0.1:8443/ s3 cp ./file.txt s3://my-bucket/

# List bucket contents
aws --no-verify-ssl --endpoint-url https://127.0.0.1:8443/ s3 ls s3://my-bucket/

# Download a file
aws --no-verify-ssl --endpoint-url https://127.0.0.1:8443/ s3 cp s3://my-bucket/file.txt ./downloaded.txt
```

## Architecture

<p align="center">
  <img src=".github/assets/platform.svg" alt="Predastore: S3 applications and tooling on top, authenticated S3 services backed by Raft metadata and erasure-coded storage, distributed across encrypted storage nodes over QUIC." width="900">
</p>

A cluster is built from three node roles. `s3d` is one process running the roles a host is assigned, and any mix of them can share a host:

- **gate** — serves the S3 HTTP interface and Signature Version 4 authentication.
- **meta** — maintains strongly consistent metadata with HashiCorp Raft over a local embedded database.
- **blob** — holds erasure-coded shards in append-only segment files, sealed with AES-256-GCM.

Nodes sharing a host talk over an in-process pipe; nodes on different hosts talk over QUIC. Nothing in a deployment names a transport — it follows from where the config pins each node.

See [`docs/DESIGN.md`](docs/DESIGN.md) for the design in full.

## Capabilities

- Reed–Solomon erasure coding
- Raft-based metadata
- QUIC storage transport
- Append-only segment storage
- AES-256-GCM encryption at rest
- Consistent-hash placement
- Multipart uploads
- Single-binary deployment

## Configuration

A cluster configuration is a TOML file describing two levels: `[[host]]` entries, which are the s3d processes owning an address and a data directory, and the `[[host.node]]` entries nested under them, which are the roles that host runs. The same file also carries the Reed-Solomon parameters, the config-defined buckets and the S3 credentials.

The file is meant to be identical on every machine, so the settings only the local machine cares about — its data directory, TLS identity and encryption key — may be left out and supplied as flags instead.

```toml
version = 1
region  = "ap-southeast-2"

[rs]
data   = 2    # data shards
parity = 1    # parity shards; data + parity must not exceed the blob node count

# One host = one s3d process, launched with `-host <id>`.
[[host]]
id   = 1
addr = "10.11.12.1"        # what other hosts dial; no port — nodes carry those
# bind_addr = "0.0.0.0"    # optional local listen address, split from addr for NAT
# data_dir  = "/var/lib/predastore"   # absolute; -data-dir supplies it otherwise

  # A role this host runs. One of "gate", "blob" or "meta".
  [[host.node]]
  id   = 1
  role = "gate"            # the S3 endpoint; port is the S3 port
  port = 8443

  [[host.node]]
  id   = 2
  role = "meta"
  port = 6660

  [[host.node]]
  id   = 3
  role = "blob"
  port = 9991
```

Node ids are unique across the whole file; ports are unique within a host. A blob or meta node without its own `data_dir` derives one from the host's root and its node id, so separate disks are a per-node setting rather than a deployment layout.

Write every node under one `[[host]]` and the cluster runs in one process over the in-process pipe. Spread them across hosts and each process is launched separately with its own `-host` id. Nothing else changes.

`config/` holds three ready-made profiles — see [Run a Development Cluster](#run-a-development-cluster).

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

## Storage Backend

Distributed storage with erasure coding, Raft-consensus metadata, and QUIC transport. The data model:

| Unit | Size | Description |
|------|------|-------------|
| Object | arbitrary | RS-encoded end-to-end into K data + M parity shards |
| Shard | `⌈object_size / K⌉` | Per-node RS slice; occupies a contiguous extent |
| Fragment | 32 B header + 8 KiB body + 16 B GCM tag = 8240 B | On-disk unit; AES-256-GCM seals body with AAD bound to `(objectHash, shardIndex, shardNum, fragNum)` |
| Segment file | up to 4 GiB | Append-only container holding extents from one or more shards |

See [DESIGN.md](docs/DESIGN.md) §6 for the on-disk format: the segment layout, the fragment header field by field, and the AAD the GCM seal is bound to.

## Spinifex Integration

Predastore is the default S3 storage provider for [Spinifex](https://github.com/mulgadc/spinifex). It can store user-created S3 objects, EC2 machine images, EBS snapshot data written through Viperblock, and service artefacts.

- **EC2 AMI images** — machine images for VM launches
- **EBS volume snapshots** — via [Viperblock](https://github.com/mulgadc/viperblock), which uses Predastore as its S3-compatible backend
- **User data** — cloud-init configurations and system artifacts

Predastore serves these over the S3 API like any other client traffic. It uses NATS for one thing only: when an `[iam]` table is configured, access keys, users, roles and policies are read from JetStream KV buckets, layered over the config-defined service accounts.

## Development

```bash
make build            # Build the s3d binary
make certs            # Generate the dev TLS certs the integration tests serve
make test             # Run tests
make preflight        # Full CI checks (lint, govulncheck, coverage, integration)
make test-race        # Run tests under the race detector
make clean            # Clean build artifacts
```

`make preflight` must pass before committing; `make fix` auto-fixes what the linter can.

### Performance Tuning

For multi-host clusters, increase system socket buffers for QUIC:

```bash
sudo sysctl -w net.core.rmem_max=7500000
sudo sysctl -w net.core.wmem_max=7500000
```

## Roadmap

- [x] S3 API core (buckets, objects, multipart)
- [x] AWS Signature V4 authentication
- [x] Distributed storage with Reed-Solomon erasure coding
- [x] Raft-consensus metadata
- [x] QUIC transport with connection pooling
- [x] Consistent hash ring placement
- [x] AES-256-GCM encryption at rest (single cluster-wide master key)
- [x] Background segment compaction
- [ ] Envelope encryption (master key rotation, per-bucket / per-tenant keys)
- [ ] Gossip-based node discovery
- [ ] Multi-object delete (`DeleteObjects`)
- [ ] Automatic shard rebalancing
- [ ] Background read-repair
- [ ] Bucket versioning
- [ ] Lifecycle policies

Roadmap items describe direction and are not commitments to a release date.

## Trademarks

Amazon Web Services, AWS and Amazon S3 are trademarks of Amazon.com, Inc. or its affiliates. Predastore is not affiliated with or endorsed by Amazon Web Services.

## License

Predastore is licensed under the [GNU Affero General Public License v3.0 (AGPLv3)](LICENSE) license.
