// Package wire defines the intra-cluster rpc protocol: opcode allocation,
// stream header types, and response envelopes. Every header carries the
// target node id, so all of a process's nodes multiplex over one socket and
// requests route in-band.
package wire

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/mulgadc/predastore/internal/rpc"
)

// Opcode allocation, by service.
const (
	// OpRaftDial opens a raft connection to a state replica; the stream
	// carries the hashicorp/raft wire protocol for its lifetime.
	OpRaftDial rpc.Opcode = 0x0001

	// State service: global-state reads and writes against a replica.
	OpStateGet    rpc.Opcode = 0x1001
	OpStatePut    rpc.Opcode = 0x1002
	OpStateDelete rpc.Opcode = 0x1003
	OpStateScan   rpc.Opcode = 0x1004

	// Storage service: erasure-coded shard operations against a store.
	OpShardGet    rpc.Opcode = 0x2001
	OpShardPut    rpc.Opcode = 0x2002
	OpShardDelete rpc.Opcode = 0x2003
)

// Response error codes with protocol meaning; anything else in Err is an
// opaque failure message.
const (
	ErrCodeNotFound  = "not-found"
	ErrCodeNotLeader = "not-leader"
)

// raftAddrPrefix builds the node-identifying raft advertise address space
// ("node-3"): raft stores these in its configuration, and the stream layer's
// dial function resolves them back to node ids.
const raftAddrPrefix = "node-"

// RaftAddress returns the raft advertise address for a state replica.
func RaftAddress(nodeID uint64) string {
	return raftAddrPrefix + strconv.FormatUint(nodeID, 10)
}

// ParseRaftAddress recovers the node id from a raft advertise address.
func ParseRaftAddress(addr string) (uint64, error) {
	id, err := strconv.ParseUint(strings.TrimPrefix(addr, raftAddrPrefix), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wire: bad raft address %q: %w", addr, err)
	}
	return id, nil
}

// appendJSON implements the rpc.Header Append side for JSON-encoded headers.
func appendJSON(buf []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(buf, b...), nil
}

// RaftDial routes an inbound raft connection to the target state replica.
type RaftDial struct {
	Target uint64 `json:"target"`
}

func (h *RaftDial) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *RaftDial) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// StateRequest is the header for every state service operation. Put values
// travel in the stream body after the header; Key doubles as the prefix for
// scans.
type StateRequest struct {
	Target uint64 `json:"target"`
	Table  string `json:"table"`
	Key    string `json:"key"`
	Limit  int    `json:"limit,omitempty"`
}

func (h *StateRequest) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *StateRequest) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// StateResponse is the JSON envelope closing every state stream.
type StateResponse struct {
	// Err is empty on success, a protocol code (ErrCode*) when the client
	// should act on it, or an opaque failure message.
	Err string `json:"err,omitempty"`
	// Leader is the raft advertise address of the current leader,
	// populated alongside ErrCodeNotLeader when known.
	Leader string `json:"leader,omitempty"`
	// Value is the payload for get responses.
	Value []byte `json:"value,omitempty"`
	// Items is the payload for scan responses.
	Items []ScanItem `json:"items,omitempty"`
}

// ScanItem is one key-value pair in a scan response.
type ScanItem struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}
