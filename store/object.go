package store

var _ ObjectReader = (*Object)(nil)

// Object describes the on-disk footprint of a stored object.
// Append and Lookup each return one.
type Object struct {
	totalSize   int64
	byteExtents []byteExtent

	store *Store
}

// byteExtent describes one contiguous byte range of object payload within a segment file.
type byteExtent struct {
	segmentNum uint64
	offset     uint64
	size       uint64
}

// Size returns the logical payload size in bytes, excluding all on-disk overhead.
func (obj *Object) Size() int64 {
	return obj.totalSize
}

// Close is a no-op for read-side Objects. Segment files are opened on demand per ReadAt call.
func (obj *Object) Close() error {
	return nil
}
