package store

import (
	"bytes"
	"encoding/gob"
)

// extent locates a shard's data on disk. Stored gob-encoded in the index.
//   - PSize: physical (on-disk) size including fragment headers = fragCount * totalFragSize
//   - LSize: logical (data-only) size as seen by callers of Read/Write
type extent struct {
	SegNum uint64
	Off    int64
	PSize  int64
	LSize  int64
}

func (ext extent) encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&ext); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeExtent(buf []byte) (ext extent, err error) {
	if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&ext); err != nil {
		return ext, err
	}

	return ext, nil
}
