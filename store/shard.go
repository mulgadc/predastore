package store

import "io"

// Shard is a single object-shard write's on-disk footprint. Each Append
// produces one Shard; reads use NewReader / NewRangeReader.
type Shard struct {
	ObjectHash [32]byte
	ShardIndex int
	TotalSize  int
	Segments   []SegmentSpan

	// dir is the Store's root directory, used by readers to open segment
	// files on demand. Set by Store when constructing or rehydrating the
	// value; zero until populated.
	dir string
}

// SegmentSpan locates a slot-aligned range within a single segment file.
type SegmentSpan struct {
	SegmentNum uint64
	Offset     uint64
	Len        uint64
}

// NewReader returns a streaming reader over the shard's full payload.
func (sh *Shard) NewReader() io.Reader {
	panic("store: Shard.NewReader not implemented")
}

// NewRangeReader returns a reader yielding bytes [start, end] of the
// shard inclusive. Returns an error if the range exceeds TotalSize.
func (sh *Shard) NewRangeReader(offset uint64, len uint64) (io.Reader, error) {
	panic("store: Shard.NewRangeReader not implemented")
}
