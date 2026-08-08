package state

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/topology"
)

// Opcodes are allocated per service in non-overlapping ranges so a stream's
// opcode identifies the service that answers it. State owns 0x0001 and 0x1xxx;
// storage owns 0x2xxx.
const (
	// OpRaftDial opens a raft connection to a state replica; the stream
	// carries the hashicorp/raft wire protocol for its lifetime.
	OpRaftDial rpc.Opcode = 0x0001

	// Global-state reads and writes against a replica.
	OpStateGet    rpc.Opcode = 0x1001
	OpStatePut    rpc.Opcode = 0x1002
	OpStateDelete rpc.Opcode = 0x1003
	OpStateScan   rpc.Opcode = 0x1004
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
func RaftAddress(nodeID topology.NodeID) string {
	return raftAddrPrefix + strconv.FormatUint(uint64(nodeID), 10)
}

// ParseRaftAddress recovers the node id from a raft advertise address.
func ParseRaftAddress(addr string) (topology.NodeID, error) {
	id, err := strconv.ParseUint(strings.TrimPrefix(addr, raftAddrPrefix), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("state: bad raft address %q: %w", addr, err)
	}
	return topology.NodeID(id), nil
}

// appendJSON implements the rpc.Header Append side for JSON-encoded headers.
func appendJSON(buf []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(buf, b...), nil
}

// RaftDial opens a raft connection to the state replica the stream is
// addressed to. It carries no fields: the address selects the replica, and
// the stream is the raft wire protocol from here on.
type RaftDial struct{}

func (h *RaftDial) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *RaftDial) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// StateRequest is the header for every state service operation. Put values
// travel in the stream body after the header; Key doubles as the prefix for
// scans.
type StateRequest struct {
	// Key is bytes rather than a string because object metadata is keyed by a
	// raw sha256: JSON rewrites every byte of a string that is not valid UTF-8
	// to U+FFFD, so a string key would not survive the wire.
	Key   []byte `json:"key"`
	Limit int    `json:"limit,omitempty"`
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

// ScanItem is one key-value pair in a scan response. Key is bytes for the same
// reason StateRequest.Key is.
type ScanItem struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
