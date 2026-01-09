# Distributed Backend Design

## Overview

The distributed backend provides fault-tolerant object storage using Reed-Solomon erasure coding across multiple nodes. Objects are split into data and parity shards, distributed via consistent hashing, and stored in append-only WAL files on each node.

Global state (object metadata, bucket ownership) is stored in a distributed Raft-based database cluster (s3db), while local shard data is stored in per-node WAL files with local Badger indexing.

## Configuration

See `s3/tests/config/cluster.toml` for example format.

For the current beta version, the `cluster.toml` must be shared between each physical node. This will be replaced by a gossip protocol in a future release.

### Starting a Node

```bash
# Run full predastore with distributed backend (auto-launches DB and QUIC servers)
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

### Reed-Solomon Encoding

```toml
[rs]
data = 3
parity = 2
```

Defines RS(3,2): 3 data shards, 2 parity shards. Can tolerate loss of any 2 nodes.

## Distributed Database (s3db)

Predastore uses a distributed Raft-based database (s3db) to store global state including bucket ownership, object metadata, and shard location mappings. This ensures consistency and fault-tolerance across multiple hosts.

### Architecture

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

### GlobalState Interface

The distributed backend uses a `GlobalState` interface to abstract storage operations, supporting both local and distributed modes:

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

### Database Node Configuration

```toml
# Specify distributed database nodes
[[db]]
id = 1
host = "192.168.1.10"
port = 6660
raft_port = 7660           # Optional, defaults to port + 1000
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

### Default Embedded Database

When no `[[db]]` configuration is provided and the distributed backend is used, s3d automatically launches a default embedded database:
- Listens on `127.0.0.1:6660`
- Data stored in `<base_path>/db/` or `data/db/`
- Uses credentials from first `[[auth]]` entry or defaults to `predastore:predastore`

### Database Operations

The s3db service provides a REST API with AWS Signature V4 authentication:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/put/{table}/{key}` | Store a key-value pair |
| GET | `/v1/get/{table}/{key}` | Retrieve a value |
| DELETE | `/v1/delete/{table}/{key}` | Delete a key |
| GET | `/v1/scan/{table}?prefix=X&limit=N` | Scan keys with prefix |
| GET | `/v1/leader` | Get current leader info |
| POST | `/v1/join` | Join a new node to cluster |
| GET | `/status` | Node status and Raft state |
| GET | `/health` | Health check |

### RAFT Consensus

Leveraging the Hashicorp library https://github.com/hashicorp/raft

- **Leader Election**: Automatic leader election when nodes start or leader fails
- **Log Replication**: All writes go through leader and are replicated to followers
- **Consistency**: Reads can go to any node; writes require majority consensus
- **Persistence**: Raft log and state stored in Badger DB

### Shard Node Configuration

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

## Architecture

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
        │                               │  │(Badger) │  │(Badger) │       │
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

## Data Model

### Global State (s3db Cluster)

Stores bucket ownership and object-to-shard mappings across the cluster:

| Table | Key Pattern | Value | Purpose |
|-------|-------------|-------|---------|
| objects | `arn:aws:s3:::<bucket>/<key>` | object hash (32 bytes) | Object listing |
| objects | `<object-hash>` | ObjectToShardNodes (gob) | Shard location map |
| objects | `deleted:<bucket>/<key>` | DeletedObjectInfo (gob) | Compaction tracking |
| buckets | `<bucket-name>` | bucket metadata | Bucket ownership |

### Local Badger (Per-QUIC-Node)

Each QUIC shard node maintains its own Badger for WAL indexing:

| Key Pattern         | Value              | Purpose                    |
|---------------------|--------------------|----------------------------|
| `<object-hash>`     | WAL file + offset  | Locate shard data in WAL   |

## Operations

### List Buckets

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

### PUT Object

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

### GET Object

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

### DELETE Object

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

## Deployment Modes

### Development Mode (Single Host)

When all nodes have the same host address, s3d runs everything locally:

```bash
# Launches embedded DB + all QUIC nodes as goroutines
./bin/s3d -backend distributed -config ./s3/tests/config/cluster.toml
```

### Production Mode (Multi-Host)

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

## Security

### TLS/HTTPS

All communication uses TLS:
- s3db server uses HTTPS with configurable certificates
- Client connections skip certificate verification for self-signed certs (configurable)
- Certificate paths: `-tls-cert` and `-tls-key` flags

### Authentication

- s3db uses AWS Signature V4 authentication
- Credentials defined in `[[db]]` or `[[auth]]` sections
- All database operations require valid signatures

## Future Work

- **Gossip Protocol**: Replace shared `cluster.toml` with dynamic node discovery
- **Bucket Operations**: Dynamic bucket creation/deletion stored in s3db
- **Compaction**: WAL file compaction and garbage collection using DeletedObjectInfo
- **Rebalancing**: Automatic shard redistribution when nodes join/leave
- **Read Replicas**: Allow reads from any s3db follower for better read scaling
