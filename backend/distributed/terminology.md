# Predastore Storage Terminology

This document defines the storage nomenclature used in Predastore. These terms describe how objects move through the system, how Reed Solomon encoding is applied, how shards are written to each node, and how local write ahead logs are structured.

All naming is consistent, unambiguous, and suitable for use in documentation and code comments.

## Object
A complete user visible object stored inside a bucket.

Example: a 1 MB file uploaded by the client.

## Stripe
A horizontal slice of the object used as the unit of Reed Solomon encoding.

A stripe contains:

- k data blocks
- m parity blocks

Each stripe is processed independently.

## Block
A fixed size data unit that is either a data block or a parity block produced from a stripe.

All blocks in a stripe have the same size.

## Shard
The node level representation of a block.

Each block from the stripe is assigned to a node, creating a shard.

A shard is the data that a node is responsible for storing for a given stripe.

## Fragment
The small, fixed size IO unit inside a shard.

Fragment size is 8 KB.

Large shards are split into many fragments:

Shard = Fragment[0], Fragment[1], Fragment[2], ...

Fragments are the unit of WAL logging.

## WAL Entry
A single atomic write record inside the node local write ahead log.

Each WAL entry stores exactly one fragment.

A WAL entry contains:

- SeqNum (monotonic per node)
- ShardID
- FragmentOffset
- Length
- CRC
- Data payload

WAL entries are appended sequentially to WAL segments.

## WAL Segment
A physical on disk WAL file.

Segment size is 32 MB.

WAL segments roll over when full.
File naming convention example:

wal-000001234567.wal

## Local Fragment Index (Per Node)
A Badger database used by each node to locate fragment data inside WAL segments.

Key format:

(shardID, fragmentOffset)

Value format:

(walSegmentID, walOffset, length, seqNum)

This index allows reconstruction of any shard by reading the required fragments from WAL.

## Global Object Map (Cluster Level)
A Badger database that maps object keys to the shards that make up the object.

Key format:

bucket/key

Value format: an ordered list of shard metadata entries:

{
    nodeID,
    shardType (data or parity),
    stripeIndex
}

This map is used for reconstruction, GET operations, and cluster placement logic.

# Hierarchy Overview

Object
  └─ Stripe
        ├─ Block (data)
        ├─ Block (parity)
        └─ Shard (block assigned to node)
              └─ Fragment (8 KB)
                    └─ WAL Entry
                          └─ WAL Segment (32 MB)

# Usage Guidelines

- Use the term Object for anything that the S3 interface exposes.
- Use Stripe for RS operations.
- Use Block only for RS encoding units.
- Use Shard for the data that belongs to a node.
- Use Fragment only for the internal 8 KB WAL write unit.
- Use WAL Entry and WAL Segment exactly as defined to avoid confusion between logical writes and physical storage.
