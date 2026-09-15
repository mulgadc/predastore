package repair

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/meta"
)

// Readiness reports whether the cluster is settled enough for a pass to trust
// what it reads. A nil error means ready; the error text says what is not, and
// must name counts and node ids only, since it is served on the admin port.
type Readiness interface {
	Ready(ctx context.Context) error
}

// StatusClient is the meta call readiness asks each replica.
type StatusClient interface {
	Status(ctx context.Context, target config.NodeID) (meta.MetaStatus, error)
}

// StatProber is the blob call readiness asks each node.
type StatProber interface {
	Stat(ctx context.Context, node config.NodeID, req blob.StatRequest) (*blob.StatResponse, error)
}

var (
	_ StatusClient = (*meta.Client)(nil)
	_ StatProber   = (*blob.Client)(nil)
	_ Readiness    = (*ClusterReadiness)(nil)
)

// minAnsweringBlobNodes is how many blob nodes must answer before a pass runs.
// Fewer cannot rebuild anything under any code this cluster uses.
const minAnsweringBlobNodes = 2

// ClusterReadiness holds repair until three things are true: a meta replica
// reports itself leader, the replicas colocated with this process have applied
// what that leader had committed, and enough blob nodes answer a stat.
//
// The startup burst it prevents was a pass that ran against a replica still
// replaying its log and blob nodes still opening their stores. Every shard
// looked owed, and none was.
type ClusterReadiness struct {
	Meta         StatusClient
	MetaReplicas []config.NodeID
	// LocalMeta are the replicas sharing this process. Empty judges the leader
	// alone, which is what a host running no replica of its own has.
	LocalMeta []config.NodeID

	Blob      StatProber
	BlobNodes []config.NodeID

	// MinBlobNodes defaults to two, or every blob node on a smaller cluster, so
	// a single-node development setup is not held forever.
	MinBlobNodes int

	mu sync.Mutex
	// target is the leader commit index the local replicas are catching up to.
	// It is held across checks so a busy leader cannot keep moving the goal.
	target uint64
}

// probeShardKey names a shard nothing writes. Not-found is the healthy answer.
var probeShardKey [32]byte

func (r *ClusterReadiness) Ready(ctx context.Context) error {
	var problems []string
	if err := r.metaReady(ctx); err != nil {
		problems = append(problems, err.Error())
	}
	if err := r.blobReady(ctx); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) == 0 {
		return nil
	}

	return errors.New(strings.Join(problems, "; "))
}

func (r *ClusterReadiness) metaReady(ctx context.Context) error {
	statuses := make(map[config.NodeID]meta.MetaStatus, len(r.MetaReplicas))
	var leaderCommit uint64
	leaderFound := false
	for _, id := range r.MetaReplicas {
		st, err := r.Meta.Status(ctx, id)
		if err != nil {
			continue
		}
		statuses[id] = st
		if commit, ok := parseIndex(st.CommitIndex); st.IsLeader && ok {
			leaderFound = true
			leaderCommit = commit
		}
	}
	if !leaderFound {
		return fmt.Errorf("no meta leader known (%d of %d replicas answered)",
			len(statuses), len(r.MetaReplicas))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.target == 0 {
		r.target = leaderCommit
	}
	for _, id := range r.LocalMeta {
		st, ok := statuses[id]
		if !ok {
			return fmt.Errorf("local meta replica %d did not answer", id)
		}
		if applied, ok := parseIndex(st.AppliedIndex); !ok || applied < r.target {
			return fmt.Errorf("local meta replica %d has applied %d of leader commit %d",
				id, applied, r.target)
		}
	}
	// Caught up to this goal; the next check samples a fresh one.
	r.target = 0

	return nil
}

func (r *ClusterReadiness) blobReady(ctx context.Context) error {
	need := r.MinBlobNodes
	if need <= 0 {
		need = minAnsweringBlobNodes
	}
	need = min(need, len(r.BlobNodes))

	var answered atomic.Int64
	var wg sync.WaitGroup
	for _, id := range r.BlobNodes {
		wg.Go(func() {
			_, err := r.Blob.Stat(ctx, id, blob.StatRequest{Key: probeShardKey})
			if err == nil || errors.Is(err, blob.ErrNotFound) {
				answered.Add(1)
			}
		})
	}
	wg.Wait()

	if got := int(answered.Load()); got < need {
		return fmt.Errorf("%d of %d blob nodes answered, need %d", got, len(r.BlobNodes), need)
	}

	return nil
}

// parseIndex reads a raft index as raft.Stats renders it. An unreadable one is
// reported rather than read as zero, which would let any replica pass as current.
func parseIndex(s string) (uint64, bool) {
	v, err := strconv.ParseUint(s, 10, 64)

	return v, err == nil
}
