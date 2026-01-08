# Distributed backend design

## Config

See `s3/tests/config/cluster.toml` for example format for defining a multi-node system.

For the current beta version, the `cluster.toml` must be shared between each physical node to determine the configuration and other nodes in the network. This will be replaced by a gossip protocol to share node information and config state in a v1 release.

## Starting

`./bin/s3d -backend distributed -config ./s3/tests/config/cluster.toml -node 1` 

This reads the specified config, and launches the node configuration specified (matching the host, port, and path for node 1 in the config file)

## Reed Solomon encoding

```
# Specify reed solomon encoding
[rs]
data = 3
parity = 2
``

Defines the reed solomon encoding, rs(3,2), 3 data nodes, 2 parity.

## Nodes

Each node is specified with it's host and port number, and the data directory used to store object shards.

```
[[nodes]]
id = 5
host = "0.0.0.0"
port = 9995
path = "s3/tests/data/distributed/nodes/node-5/"
```

## Design

### List buckets:

Note, buckets are specified with the account-id as the owner.

AWS SDK / API tool (e.g `aws s3 ls`)
|

Predastore (s3d) HTTP (fiber) service

|

Global badger (s3d), list all buckets belonging to specified account ID

|

Retrieve account ID from AWS HTTP auth, specified access_key_id, lookup in `cluster.toml` for account owner.

|

Badger, range keys `arn:aws:s3::123456789012:` to list all buckets, e.g

arn:aws:s3::123456789012:test-bucket (test-bucket)
arn:aws:s3::123456789012:second-bucket (second-bucket)

|

Return response to client, XML formatted as expected.

### List prefixes:

No, objects and prefixes are specified without the account ID in the `arn` string. Authentication step beforehand confirms if the access/secret key has permissions over the specified bucket.

AWS SDK / API tool (e.g `aws s3 ls s3://test-bucket/prefix/img`)
|

Predastore (s3d) HTTP (fiber) service

|

Confirm authentication, specified access-key/secret can access the bucket.

|

Global badger (s3d), list all objects belonging to specified bucket and supplied prefix.

|

Badger, range keys `arn:aws:s3:::test-bucket/prefix/img` to list all buckets, e.g

arn:aws:s3:::test-bucket/prefix/img-01.png
arn:aws:s3:::test-bucket/prefix/img-02.png
arn:aws:s3:::test-bucket/prefix/img-03.jpg

|

Return response to client, XML formatted as expected.

### Get object

AWS SDK / API tool (e.g `aws s3 cp s3://test-bucket/prefix/img-01.png`)
|

Predastore (s3d) HTTP (fiber) service

|

Confirm authentication, specified access-key/secret can access the bucket.

|

Global badger (s3d), confirm the object exists

|

Badger, confirm key `arn:aws:s3:::test-bucket/prefix/img-01.png` exists

true

|

Generate object hash (bucket/object)

|

Determine hash ring shards for data + parity chunks.

`hashRingShards, err := b.hashRing.GetClosestN(key, b.rsDataShard+b.rsParityShard)`

|

For each data shard, connect to the specified node using `quic://host:port` as specified in the `cluster.toml`

|

QUIC client > fetch object hash

|

Shard server (QUIC server)

|

Local shard, badger lookup, determine object WAL location and offset

| 

Return shard chunk

|

S3d reassembles the data chunks, if a chunk is missing, rebuilds the object using the parity shards

|

QUIC client > parity node > fetch object hash

|

Reassembles object from data + parity nodes (if required)

| 

Returns data to client as expected.


### PUT object

AWS SDK / API tool (e.g `aws s3 cp /tmp/img-04.png s3://test-bucket/prefix/img-04.png`)
|

Predastore (s3d) HTTP (fiber) service

|

Confirm authentication, specified access-key/secret can access the bucket.

|

Global badger (s3d), confirm the object exists, if so overwrite.

|

Badger, confirm key `arn:aws:s3:::test-bucket/prefix/img-01.png` exists

false

|

Generate object hash (bucket/object)

|

Determine hash ring shards for data + parity chunks.

`hashRingShards, err := b.hashRing.GetClosestN(key, b.rsDataShard+b.rsParityShard)`

|

For each data shard, connect to the specified node using `quic://host:port` as specified in the `cluster.toml`

|

QUIC client > fetch object hash

|

Shard server (QUIC server)

|

Local shard, badger lookup, determine object WAL location and offset

| 

Return shard chunk

|

S3d reassembles the data chunks, if a chunk is missing, rebuilds the object using the parity shards

|

QUIC client > parity node > fetch object hash

|

Reassembles object from data + parity nodes (if required)

| 

Returns data to client as expected.
