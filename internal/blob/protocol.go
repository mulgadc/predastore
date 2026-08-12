package blob

import (
	"encoding/binary"
	"fmt"

	"github.com/mulgadc/predastore/internal/rpc"
)

// Opcodes are allocated per service in non-overlapping ranges so a stream's
// opcode identifies the service that answers it. blob owns 0x2xxx; meta owns
// 0x0001 and 0x1xxx.
const (
	OpGet    rpc.Opcode = 0x2001
	OpPut    rpc.Opcode = 0x2002
	OpDelete rpc.Opcode = 0x2003
)

// Failure statuses with protocol meaning, in the same 0x2xxx range as the
// opcodes. Anything else arrives as rpc.StatusInternal with an opaque message.
const (
	StatusNotFound  rpc.Status = 0x2001
	StatusStoreFull rpc.Status = 0x2002
	StatusBadRange  rpc.Status = 0x2003
)

// PutRequest heads a put. The value's bytes follow the frame on the stream.
type PutRequest struct {
	Key []byte
	// Size is the number of body bytes to commit. A body delivering fewer
	// commits nothing.
	Size uint64
}

func (h *PutRequest) Append(buf []byte) ([]byte, error) {
	buf = binary.BigEndian.AppendUint64(buf, h.Size)
	// The key trails the fixed-width fields, so the frame's own length bounds
	// it and it carries none of its own.
	return append(buf, h.Key...), nil
}

func (h *PutRequest) Unmarshal(b []byte) error {
	if len(b) < 8 {
		return fmt.Errorf("put header is %d bytes, want at least 8", len(b))
	}
	h.Size = binary.BigEndian.Uint64(b[:8])
	// Whatever follows the fixed-width fields is the key, however long.
	h.Key = b[8:]
	return nil
}

// GetRequest heads a get, optionally bounding what it reads.
type GetRequest struct {
	Key []byte
	// Off is where the read starts; Len bounds it. A Len of zero reads to the
	// end of the value, so the zero request reads all of it.
	Off uint64
	Len uint64
}

func (h *GetRequest) Append(buf []byte) ([]byte, error) {
	buf = binary.BigEndian.AppendUint64(buf, h.Off)
	buf = binary.BigEndian.AppendUint64(buf, h.Len)
	return append(buf, h.Key...), nil
}

func (h *GetRequest) Unmarshal(b []byte) error {
	if len(b) < 16 {
		return fmt.Errorf("get header is %d bytes, want at least 16", len(b))
	}
	h.Off = binary.BigEndian.Uint64(b[:8])
	h.Len = binary.BigEndian.Uint64(b[8:16])
	h.Key = b[16:]
	return nil
}

// DeleteRequest heads a delete.
type DeleteRequest struct {
	Key []byte
}

// With no fixed-width fields ahead of it, the key is the whole payload.
func (h *DeleteRequest) Append(buf []byte) ([]byte, error) { return append(buf, h.Key...), nil }
func (h *DeleteRequest) Unmarshal(b []byte) error          { h.Key = b; return nil }

// PutResponse reports capacity pressure seen at commit time, so callers can
// back off before writes are rejected outright.
type PutResponse struct {
	PoolNearFull bool
}

func (h *PutResponse) Append(buf []byte) ([]byte, error) {
	return append(buf, boolByte(h.PoolNearFull)), nil
}

func (h *PutResponse) Unmarshal(b []byte) error {
	if len(b) < 1 {
		return fmt.Errorf("put response is %d bytes, want at least 1", len(b))
	}
	h.PoolNearFull = b[0] != 0
	return nil
}

// GetResponse carries nothing: the body follows the frame and ends when the
// node closes its write side.
type GetResponse struct{}

func (h *GetResponse) Append(buf []byte) ([]byte, error) { return buf, nil }
func (h *GetResponse) Unmarshal([]byte) error            { return nil }

// DeleteResponse reports whether the node held the value.
type DeleteResponse struct {
	Deleted bool
}

func (h *DeleteResponse) Append(buf []byte) ([]byte, error) {
	return append(buf, boolByte(h.Deleted)), nil
}

func (h *DeleteResponse) Unmarshal(b []byte) error {
	if len(b) < 1 {
		return fmt.Errorf("delete response is %d bytes, want at least 1", len(b))
	}
	h.Deleted = b[0] != 0
	return nil
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
