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
		b := make([]byte, int(slotHeaderSize+fragSize))
		return &b
	},
}

// zeroPadBuffer is a pre-allocated zero slice for clearing the padding region of a slot payload.
var zeroPadBuffer = make([]byte, int(fragSize))

// objectWire is the gob-serialisable form of objectReader. All fields are private,
// so we need exported fields for gob round-tripping.
type objectWire struct {
	TotalSize   int64
	ByteExtents []byteExtentWire
}

type byteExtentWire struct {
	SegmentNum uint64
	Offset     uint64
	Size       uint64
}

func encodeObject(obj *objectReader) ([]byte, error) {
	w := objectWire{TotalSize: obj.totalSize}
	w.ByteExtents = make([]byteExtentWire, len(obj.byteExtents))
	for i, ext := range obj.byteExtents {
		w.ByteExtents[i] = byteExtentWire{
			SegmentNum: ext.segmentNum,
			Offset:     ext.offset,
			Size:       ext.size,
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&w); err != nil {
		return nil, fmt.Errorf("store: encodeObject: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeObject(data []byte) (*objectReader, error) {
	var w objectWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return nil, fmt.Errorf("store: decodeObject: %w", err)
	}
	obj := &objectReader{totalSize: w.TotalSize}
	obj.byteExtents = make([]byteExtent, len(w.ByteExtents))
	for i, ext := range w.ByteExtents {
		obj.byteExtents[i] = byteExtent{
			segmentNum: ext.SegmentNum,
			offset:     ext.Offset,
			size:       ext.Size,
		}
	}
	return obj, nil
}
