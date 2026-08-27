package blob

import (
	"encoding/json"

	"github.com/mulgadc/predastore/internal/rpc"
)

// Opcodes are allocated per service in non-overlapping ranges so a stream's
// opcode identifies the service that answers it. blob owns 0x2xxx; meta owns
// 0x0001 and 0x1xxx.
const (
	OpGet    rpc.Opcode = 0x2001
	OpPut    rpc.Opcode = 0x2002
	OpDelete rpc.Opcode = 0x2003
	OpCommit rpc.Opcode = 0x2004
	OpAbort  rpc.Opcode = 0x2005
)

// Response error codes with protocol meaning; anything else in Err is an
// opaque failure message.
const (
	ErrCodeNotFound = "not-found"
	// ErrCodeEpochMismatch is a node that holds the shard at a different write
	// epoch than the one asked for. Distinct from not-found because it names a
	// node that is up, answering and wrong, which is the one case repair owns.
	ErrCodeEpochMismatch = "epoch-mismatch"
	// ErrCodeNotPrepared is a commit for which no prepared extent exists under
	// that epoch. The answer is to rewrite the shard, not to retry the commit.
	ErrCodeNotPrepared = "not-prepared"
	ErrCodeStoreFull   = "store-full"
)

// appendJSON implements the rpc.Header Append side for JSON-encoded headers.
func appendJSON(buf []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(buf, b...), nil
}

// Request is the header for every storage service operation. Put data travels
// in the stream body after the header. Key is the client's to compute; a blob
// node only ever treats it as 32 opaque bytes naming a value set.
type Request struct {
	Key   [32]byte `json:"key"`
	Index uint32   `json:"index"`
	// Size is the body length for puts.
	Size int64 `json:"size,omitempty"`
	// RangeStart and RangeEnd bound gets; -1 means unset.
	RangeStart int64 `json:"range_start"`
	RangeEnd   int64 `json:"range_end"`
	// Epoch names the object generation this shard belongs to. Mandatory on
	// put, commit and abort; on get it is the generation the caller will
	// accept, and a node holding any other one answers epoch-mismatch.
	Epoch uint64 `json:"epoch,omitempty"`
}

func (h *Request) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *Request) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// Response is the JSON envelope answering every stream. It is
// newline-terminated; get responses stream BodyLen body bytes after it.
type Response struct {
	Err string `json:"err,omitempty"`
	// Size echoes the committed byte count for puts.
	Size int64 `json:"size,omitempty"`
	// PoolNearFull reports nearfull free-space pressure at commit time so
	// callers can back off before writes are rejected outright.
	PoolNearFull bool `json:"pool_near_full,omitempty"`
	// Deleted reports whether a delete removed an existing value.
	Deleted bool `json:"deleted,omitempty"`
	// Epoch is the write epoch the node holds the value under, returned on
	// get. On an epoch-mismatch it reports what the node actually has, which
	// is what makes a stale node identifiable rather than merely wrong.
	Epoch uint64 `json:"epoch,omitempty"`
	// BodyLen is the number of body bytes following the envelope.
	BodyLen int64 `json:"body_len,omitempty"`
}
