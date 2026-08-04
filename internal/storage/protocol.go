package storage

import (
	"encoding/json"

	"github.com/mulgadc/predastore/internal/rpc"
)

// Opcodes are allocated per service in non-overlapping ranges so a stream's
// opcode identifies the service that answers it. Storage owns 0x2xxx; state
// owns 0x0001 and 0x1xxx.
const (
	OpShardGet    rpc.Opcode = 0x2001
	OpShardPut    rpc.Opcode = 0x2002
	OpShardDelete rpc.Opcode = 0x2003
)

// Response error codes with protocol meaning; anything else in Err is an
// opaque failure message.
const (
	ErrCodeNotFound  = "not-found"
	ErrCodeStoreFull = "store-full"
)

// appendJSON implements the rpc.Header Append side for JSON-encoded headers.
func appendJSON(buf []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(buf, b...), nil
}

// ShardRequest is the header for every storage service operation. Put shard
// data travels in the stream body after the header.
type ShardRequest struct {
	Bucket     string   `json:"bucket"`
	Object     string   `json:"object"`
	ObjectHash [32]byte `json:"object_hash"`
	ShardIndex uint32   `json:"shard_index"`
	// ShardSize is the body length for puts.
	ShardSize int64 `json:"shard_size,omitempty"`
	// RangeStart and RangeEnd bound gets; -1 means unset.
	RangeStart int64 `json:"range_start"`
	RangeEnd   int64 `json:"range_end"`
}

func (h *ShardRequest) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *ShardRequest) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// ShardResponse is the JSON envelope answering every shard stream. It is
// newline-terminated; get responses stream BodyLen shard bytes after it.
type ShardResponse struct {
	Err string `json:"err,omitempty"`
	// ShardSize echoes the committed byte count for puts.
	ShardSize int64 `json:"shard_size,omitempty"`
	// PoolNearFull reports nearfull free-space pressure at commit time so
	// callers can back off before writes are rejected outright.
	PoolNearFull bool `json:"pool_near_full,omitempty"`
	// Deleted reports whether a delete removed an existing shard.
	Deleted bool `json:"deleted,omitempty"`
	// BodyLen is the number of shard bytes following the envelope.
	BodyLen int64 `json:"body_len,omitempty"`
}
