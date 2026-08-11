package meta

import (
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// Config is everything one meta replica runs on. The listeners arrive already
// bound and the resolver already built, because the process that owns the
// transports registers them before any colocated node dials; everything else
// the replica acquires itself.
type Config struct {
	NodeID    config.NodeID   // The node this replica serves. Required.
	DataDir   string          // Where raft and the FSM live; Run creates it. Required.
	Peers     []config.NodeID // The voting members bootstrap installs.
	Bootstrap bool            // Whether to bootstrap the cluster on start.

	Listeners []transport.Listener // The bound listeners the replica answers rpc on.
	Resolver  *rpc.Resolver        // Turns a peer id into a route to dial. Required.

	// OnLeader is called once, as soon as a leader is observed or
	// LeaderTimeout expires: a slow election must still release a caller that
	// gates serving on it, rather than never serving.
	OnLeader      func()
	LeaderTimeout time.Duration // Zero defaults to 30s.

	// Raft tuning. Every zero value is replaced in New, so a zero-valued
	// Config cannot overwrite raft's own timeouts with zeros.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	TrailingLogs       uint64
	LeaderLeaseTimeout time.Duration
}
