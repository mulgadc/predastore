package meta

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
)

// Opcodes are allocated per service in non-overlapping ranges so a stream's
// opcode identifies the service that answers it. State owns 0x0001 and 0x1xxx;
// storage owns 0x2xxx.
const (
	// OpRaftDial opens a raft connection to a meta replica; the stream
	// carries the hashicorp/raft wire protocol for its lifetime.
	OpRaftDial rpc.Opcode = 0x0001

	// Global-state reads and writes against a replica.
	OpMetaGet    rpc.Opcode = 0x1001
	OpMetaPut    rpc.Opcode = 0x1002
	OpMetaDelete rpc.Opcode = 0x1003
	OpMetaScan   rpc.Opcode = 0x1004
	OpMetaPutMax rpc.Opcode = 0x1006
	// OpMetaSwap writes a value and answers with the one it replaced.
	OpMetaSwap rpc.Opcode = 0x1007

	// OpMetaStatus asks a replica to report its own raft state, rather than
	// any data it holds.
	OpMetaStatus rpc.Opcode = 0x1005
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

// RaftAddress returns the raft advertise address for a meta replica.
func RaftAddress(nodeID config.NodeID) string {
	return raftAddrPrefix + strconv.FormatUint(uint64(nodeID), 10)
}

// ParseRaftAddress recovers the node id from a raft advertise address.
func ParseRaftAddress(addr string) (config.NodeID, error) {
	id, err := strconv.ParseUint(strings.TrimPrefix(addr, raftAddrPrefix), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad raft address %q: %w", addr, err)
	}
	return config.NodeID(id), nil
}

// appendJSON implements the rpc.Header Append side for JSON-encoded headers.
func appendJSON(buf []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(buf, b...), nil
}

// RaftDial opens a raft connection to the meta replica the stream is
// addressed to. It carries no fields: the address selects the replica, and
// the stream is the raft wire protocol from here on.
type RaftDial struct{}

func (h *RaftDial) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *RaftDial) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// MetaRequest is the header for every state service operation. Put values
// travel in the stream body after the header; Key doubles as the prefix for
// scans.
type MetaRequest struct {
	// Key is bytes rather than a string because object metadata is keyed by a
	// raw sha256: JSON rewrites every byte of a string that is not valid UTF-8
	// to U+FFFD, so a string key would not survive the wire.
	Key   []byte `json:"key"`
	Limit int    `json:"limit,omitempty"`
	// After resumes a scan past a key already seen, so a caller can page
	// through a prefix rather than asking for the first N matches forever. It
	// is exclusive, and empty starts at the beginning of the prefix.
	After []byte `json:"after,omitempty"`
	// Epoch is used by OpMetaPutMax to reject a placement older than the one
	// already published for the object.
	Epoch uint64 `json:"epoch,omitempty"`
}

func (h *MetaRequest) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *MetaRequest) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// MetaResponse is the JSON envelope closing every state stream.
type MetaResponse struct {
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
// reason MetaRequest.Key is.
type ScanItem struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// MetaStatusRequest carries no fields: OpMetaStatus asks about the replica
// answering the stream, never about a key.
type MetaStatusRequest struct{}

func (h *MetaStatusRequest) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *MetaStatusRequest) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }

// MetaStatus is one replica's raft state, closing an OpMetaStatus stream. A
// caller outside this module already decodes this exact JSON shape, so the
// field names are a wire contract and must not change independently of it.
//
// Leader and LeaderAddr are both empty while raft has no leader, which is a
// successful answer rather than an error: "answered, but reports no leader"
// is the condition a status probe exists to surface.
type MetaStatus struct {
	// NodeID is the replica that answered, not the one asked about.
	NodeID string `json:"node_id"`
	// State is this replica's own raft.State(): "Follower", "Candidate",
	// "Leader" or "Shutdown".
	State string `json:"state"`
	// Leader is the raft advertise address ("node-<id>") of the leader this
	// replica currently observes, empty when it observes none.
	Leader string `json:"leader"`
	// LeaderAddr is the address this replica would dial Leader on, resolved
	// from the same routing table its own rpc traffic uses. It is left
	// empty rather than repeating Leader when no distinct dial address is
	// known — including when this replica is itself the leader, since a
	// replica holds no route to dial itself.
	LeaderAddr string `json:"leader_addr"`
	// Term, CommitIndex and AppliedIndex come from raft.Stats(), which
	// already renders them as strings.
	Term         string `json:"term"`
	CommitIndex  string `json:"commit_index"`
	AppliedIndex string `json:"applied_index"`
	// IsLeader is State == "Leader", spelled out for callers that only care
	// about that.
	IsLeader bool `json:"is_leader"`
}

func (h *MetaStatus) Append(buf []byte) ([]byte, error) { return appendJSON(buf, h) }
func (h *MetaStatus) Unmarshal(b []byte) error          { return json.Unmarshal(b, h) }
