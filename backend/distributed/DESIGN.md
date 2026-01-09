# Distributed Backend Design

## Overview

The distributed backend provides fault-tolerant object storage using Reed-Solomon erasure coding across multiple nodes. Objects are split into data and parity shards, distributed via consistent hashing, and stored in append-only WAL files on each node.

## Configuration

See `s3/tests/config/cluster.toml` for example format.

For the current beta version, the `cluster.toml` must be shared between each physical node. This will be replaced by a gossip protocol in a future release.

### Starting a Node

```bash
./bin/s3d -backend distributed -config ./s3/tests/config/cluster.toml -node 1
```

This reads the specified config and launches the node matching the host, port, and path for node 1.

### Reed-Solomon Encoding

```toml
[rs]
data = 3
parity = 2
```

Defines RS(3,2): 3 data shards, 2 parity shards. Can tolerate loss of any 2 nodes.

## Distributed Database nodes

Predastore provides a lightweight DynamoDB service to store key information, including bucket owners, objects and prefixes stored in S3. This information is required to be distributed between multiple hosts for redundancy, using raft consensus and leader elections for maintaining state across the fleet.

When new buckets are created, or objects appended to a bucket, predastore will push these changes to the distributed database. 

Process flow:

AWS SDK / S3 client

|

S3 PUT - /my-bucket/prefix/img-01.png

|

S3d (predastore)

|

Confirm authentication, user has permissions over /my-bucket/

|

Insert key

`arn:aws:s3:::my-bucket/prefix/img-01.png`

Distributed Database (Badger)

Write to leader node, confirm majority committed transaction.

|

On success, append to S3 Shard nodes following existing pattern.

|

RS encoding > shard > insert each node.

|

Return success to client.

### Database flow

For development a single node can be used, however for production use it is recommended at least 3 to 5 nodes are used to improve reliability and redundancy. Preferably instances on different switches, racks and unique hardware to isolate failures.

The distributed database runs on a unique port to predastore, using a simple REST HTTP fiber service with AWS authentication mirroring basic functionality of Amazon DynamoDB.

HTTP (AWS auth)

Check provided access-key authenticated

|

INSERT:

POST - /put/{table}/{key}
Body: {value}

GET:

GET - /put/{table}/{key}
Body: {value}

DELETE:

DELETE - /delete/{table}/{key}


### RAFT Consensus

Leveraging the Hashicorp library https://github.com/hashicorp/raft

On insert / delete / update, `predastore` will contact the primary distributed badgerDB node to commit changes. The primary node is responsible to ensure a majority of other nodes in the cluster confirm the commit, prior to returning success. 

The distributed badgerDB will be responsible for leader election and state changes across the cluster for nodes out of sync.

### Database configuration

```
# Specify DB nodes for prefix/objects
# TODO
#[[db]]
#id = 1
#host = "0.0.0.0"
#port = 6661
#path = "s3/tests/data/distributed/db/node-1/"
#leader = true
#access_key_id = "AKIAIOSFODNN7EXAMPLE"
#secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
#epoch = 0
```

### Shard Node Configuration

```toml
[[nodes]]
id = 5
host = "0.0.0.0"
port = 9995
path = "s3/tests/data/distributed/nodes/node-5/"
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
│  │ Auth/SigV4  │  │  Routing    │  │  Backend    │  │ Global Badger │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └───────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
            ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
            │  QUIC Node  │   │  QUIC Node  │   │  QUIC Node  │
            │  (Shard 0)  │   │  (Shard 1)  │   │  (Shard 2)  │
            ├─────────────┤   ├─────────────┤   ├─────────────┤
            │ Local WAL   │   │ Local WAL   │   │ Local WAL   │
            │ Local Badger│   │ Local Badger│   │ Local Badger│
            └─────────────┘   └─────────────┘   └─────────────┘
```

## Data Model

### Global Badger (S3D Coordinator)

Stores bucket ownership and object-to-shard mappings:

| Key Pattern                              | Value                    | Purpose              |
|------------------------------------------|--------------------------|----------------------|
| `arn:aws:s3::<account>:<bucket>`         | bucket metadata          | Bucket ownership     |
| `arn:aws:s3:::<bucket>/<key>`            | object hash (32 bytes)   | Object listing       |
| `<object-hash>`                          | ObjectToShardNodes (gob) | Shard location map   |

### Local Badger (Per-Node)

Each QUIC node maintains its own Badger for WAL indexing:

| Key Pattern         | Value              | Purpose                    |
|---------------------|--------------------|----------------------------|
| `<object-hash>`     | WAL file + offset  | Locate shard data in WAL   |

## Operations

### List Buckets

```
┌──────────────┐     ┌─────────────┐     ┌─────────────────┐
│  S3 Client   │────▶│    S3D      │────▶│  Global Badger  │
│  aws s3 ls   │     │  (HTTP)     │     │                 │
└──────────────┘     └─────────────┘     └─────────────────┘
                            │                    │
                            │  1. Extract account ID from auth
                            │                    │
                            │  2. Range scan: arn:aws:s3::<account>:*
                            │                    │
                            ▼                    ▼
                     ┌─────────────────────────────┐
                     │  Return XML bucket list     │
                     └─────────────────────────────┘
```

Buckets are keyed by account ID from the authenticated access_key_id.

### List Objects (with prefix)

```
┌──────────────────────┐     ┌─────────────┐     ┌─────────────────┐
│      S3 Client       │────▶│    S3D      │────▶│  Global Badger  │
│ aws s3 ls s3://bucket│     │  (HTTP)     │     │                 │
└──────────────────────┘     └─────────────┘     └─────────────────┘
                                    │                    │
                                    │  1. Verify bucket access
                                    │                    │
                                    │  2. Range scan: arn:aws:s3:::<bucket>/<prefix>*
                                    │                    │
                                    ▼                    ▼
                             ┌─────────────────────────────┐
                             │  Return XML object list     │
                             └─────────────────────────────┘
```

Objects are keyed without account ID - authorization is checked beforehand.

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
│  2. Lookup object in Global Badger (arn:aws:s3:::<b>/<k>)   │
│  3. Get ObjectToShardNodes metadata via object hash         │
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
│                   Global Badger Update                      │
│  1. Store ARN key → object hash mapping                     │
│  2. Store object hash → ObjectToShardNodes metadata         │
│     (includes: shard node IDs, object size)                 │
│  3. Return ETag to client                                   │
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
│  2. Lookup object hash from ARN key in Global Badger        │
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
│  ┌───────────┐  │  │  ┌───────────┐  │       │  ┌───────────┐  │
│  │ WAL Files │  │  │  │ WAL Files │  │       │  │ WAL Files │  │
│  │  Deleted  │  │  │  │  Deleted  │  │       │  │  Deleted  │  │
│  └───────────┘  │  │  └───────────┘  │       │  └───────────┘  │
└─────────────────┘  └─────────────────┘       └─────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Global Badger Cleanup                     │
│  1. Delete ARN key → object hash mapping                    │
│  2. Delete object hash → ObjectToShardNodes metadata        │
│  3. Return 204 No Content                                   │
└─────────────────────────────────────────────────────────────┘
```

## Future Work

- **Gossip Protocol**: Replace shared `cluster.toml` with dynamic node discovery
- **Raft Consensus**: Coordinate Global Badger updates across multiple S3D coordinators
- **Compaction**: WAL file compaction and garbage collection
- **Rebalancing**: Automatic shard redistribution when nodes join/leave
