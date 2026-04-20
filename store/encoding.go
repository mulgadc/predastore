package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

// slotBufferPool reuses slot-sized write buffers on the Append hot path.
// Buffer length is slotHeaderLen + slotCapacity = 8224 bytes.
var slotBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, int(slotHeaderSize+slotPayloadSize))
		return &b
	},
}

// zeroPadBuffer is a pre-allocated zero slice used to wipe the padding
// region of a slot payload before checksumming.
var zeroPadBuffer = make([]byte, int(slotPayloadSize))

// encodeShard serialises a Shard for storage in Badger.
func encodeShard(sh *Shard) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sh); err != nil {
		return nil, fmt.Errorf("store: encodeShard: %w", err)
	}
	return buf.Bytes(), nil
}

// decodeShard deserialises a Shard from Badger storage.
func decodeShard(data []byte) (*Shard, error) {
	var sh Shard
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&sh); err != nil {
		return nil, fmt.Errorf("store: decodeShard: %w", err)
	}
	return &sh, nil
}

// segmentHeader returns the on-disk segment file header bytes. Length is
// exactly segmentHeaderLen.
func segmentHeader(magic [4]byte, version uint16) []byte { //nolint:unused // stub for Stage 3
	panic("store: segmentHeader not implemented")
}

// parseSegmentHeader decodes a segmentHeaderLen-byte segment header.
func parseSegmentHeader(data []byte) (magic [4]byte, version uint16, err error) { //nolint:unused // stub for Stage 3
	panic("store: parseSegmentHeader not implemented")
}
