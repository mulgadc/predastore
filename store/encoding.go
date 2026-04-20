package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

// slotBufferPool reuses slot-sized buffers (slotHeaderSize + slotPayloadSize = 8224 bytes).
var slotBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, int(slotHeaderSize+slotPayloadSize))
		return &b
	},
}

// zeroPadBuffer is a pre-allocated zero slice for clearing the padding region of a slot payload.
var zeroPadBuffer = make([]byte, int(slotPayloadSize))

// encodeShard serialises a Shard into bytes for the Badger index.
func encodeShard(sh *Shard) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sh); err != nil {
		return nil, fmt.Errorf("store: encodeShard: %w", err)
	}
	return buf.Bytes(), nil
}

// decodeShard deserialises a Shard from bytes stored in the Badger index.
func decodeShard(data []byte) (shard *Shard, err error) {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&shard); err != nil {
		return nil, fmt.Errorf("store: decodeShard: %w", err)
	}
	return shard, nil
}

// segmentHeader returns the on-disk segment file header bytes.
func segmentHeader(magic [4]byte, version uint16) []byte { //nolint:unused // stub for Stage 3
	panic("store: segmentHeader not implemented")
}

// parseSegmentHeader decodes a segment file header.
func parseSegmentHeader(data []byte) (magic [4]byte, version uint16, err error) { //nolint:unused // stub for Stage 3
	panic("store: parseSegmentHeader not implemented")
}
