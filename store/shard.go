package store

import (
	"encoding/binary"
)

// Shard is a single object-shard's on-disk footprint. Each
// Append/Lookup produces one Shard.
type Shard struct {
	ObjectHash [32]byte
	ShardIndex int
	TotalSize  int
	Locations  []Location
}

type Location struct {
	SegmentNum uint64
	Offset     uint64
	Size       uint64
}

// makeShardKey builds the 36-byte composite Badger key for a (objectHash,
// shardIndex) pair.
func makeShardKey(objectHash [32]byte, shardIndex int) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], uint32(shardIndex)) //nolint:gosec // G115: shardIndex bounded by RS shard count
	return key
}
