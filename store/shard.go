package store

import (
	"encoding/binary"
)

// Shard describes the on-disk footprint of a single object shard.
// Append and Lookup each return one.
type Shard struct {
	ObjectHash [32]byte
	ShardIndex int
	TotalSize  int
	Locations  []Location

	store *Store
}

// Location describes one contiguous byte range of shard payload within a segment file.
type Location struct {
	SegmentNum uint64
	Offset     uint64
	Size       uint64
}

// makeShardKey returns the 36-byte composite Badger key for an (objectHash, shardIndex) pair.
func makeShardKey(objectHash [32]byte, shardIndex int) []byte {
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], uint32(shardIndex)) //nolint:gosec // G115: shardIndex bounded by RS shard count
	return key
}
