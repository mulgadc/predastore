package store

import "sync"

// slotBufferPool reuses slot-sized write buffers on the Append hot path.
// Buffer length is slotHeaderLen + slotCapacity = 8224 bytes.
var slotBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, int(slotHeaderLen+slotCapacity))
	},
}

// zeroPadBuffer is a pre-allocated zero slice used to wipe the padding
// region of a slot payload before checksumming.
var zeroPadBuffer = make([]byte, int(slotCapacity))

// encodeShard serialises a Shard for storage in Badger.
func encodeShard(sh *Shard) ([]byte, error) {
	panic("store: encodeShard not implemented")
}

// decodeShard deserialises a Shard from Badger storage.
func decodeShard(data []byte) (*Shard, error) {
	panic("store: decodeShard not implemented")
}

// segmentHeader returns the on-disk segment file header bytes. Length is
// exactly segmentHeaderLen.
func segmentHeader(magic [4]byte, version uint16) []byte {
	panic("store: segmentHeader not implemented")
}

// parseSegmentHeader decodes a segmentHeaderLen-byte segment header.
func parseSegmentHeader(data []byte) (magic [4]byte, version uint16, err error) {
	panic("store: parseSegmentHeader not implemented")
}
