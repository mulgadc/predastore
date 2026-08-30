package predastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/meta"
)

type fakeMetaReader struct{ err error }

func (f fakeMetaReader) Get(context.Context, string) ([]byte, error) {
	return nil, f.err
}

// fakeShardProber answers for the nodes named in up and fails for every other,
// counting the reads so a probe that skips a node is caught.
type fakeShardProber struct {
	up     map[NodeID]bool
	closed atomic.Int64
	reads  atomic.Int64
}

func (f *fakeShardProber) Get(_ context.Context, node NodeID, _ blob.GetRequest) (io.ReadCloser, error) {
	f.reads.Add(1)
	if !f.up[node] {
		return nil, fmt.Errorf("get from node %d: %w", node, errors.New("connection refused"))
	}
	return &countingCloser{Reader: bytes.NewReader(nil), closed: &f.closed}, nil
}

type countingCloser struct {
	io.Reader

	closed *atomic.Int64
}

func (c *countingCloser) Close() error {
	c.closed.Add(1)
	return nil
}

type fakeReplica bool

func (f fakeReplica) LeaderKnown() bool { return bool(f) }

// A replica with no leader cannot commit a write, so a gate in front of it
// would accept requests it has no way to serve.
func TestLeaderObservedFollowsTheReplica(t *testing.T) {
	if err := leaderObserved(fakeReplica(true)).Probe(context.Background()); err != nil {
		t.Errorf("probe returned %v with a leader observed, want nil", err)
	}
	if err := leaderObserved(fakeReplica(false)).Probe(context.Background()); err == nil {
		t.Error("probe returned nil with no leader observed")
	}
}

// A missing key is the healthy answer: the read reached a replica and came
// back. Treating it as a failure would leave every cluster permanently unready.
func TestMetaReachableTreatsNotFoundAsReached(t *testing.T) {
	check := metaReachable(fakeMetaReader{err: meta.ErrNotFound})

	if err := check.Probe(context.Background()); err != nil {
		t.Errorf("probe returned %v for a missing key, want nil", err)
	}
}

func TestMetaReachableFailsWhenThePlaneDoesNotAnswer(t *testing.T) {
	check := metaReachable(fakeMetaReader{err: errors.New("no meta replica answered")})

	if err := check.Probe(context.Background()); err == nil {
		t.Error("probe returned nil for an unreachable meta plane")
	}
}

func TestMetaReachableAcceptsAKeyThatExists(t *testing.T) {
	check := metaReachable(fakeMetaReader{})

	if err := check.Probe(context.Background()); err != nil {
		t.Errorf("probe returned %v, want nil", err)
	}
}

// The threshold is the data-shard count: below it a read cannot be
// reconstructed at all, which is the point at which the gate stops serving.
func TestBlobNodesReachableUsesTheDataShardThreshold(t *testing.T) {
	nodes := []NodeID{1, 2, 3, 4}

	t.Run("ready at the threshold", func(t *testing.T) {
		bc := &fakeShardProber{up: map[NodeID]bool{1: true, 2: true}}
		check := blobNodesReachable(bc, nodes, 2)

		if err := check.Probe(context.Background()); err != nil {
			t.Errorf("probe returned %v with exactly K nodes up, want nil", err)
		}
		if got := bc.reads.Load(); got != 4 {
			t.Errorf("probed %d nodes, want 4", got)
		}
	})

	t.Run("unready below it", func(t *testing.T) {
		bc := &fakeShardProber{up: map[NodeID]bool{1: true}}
		check := blobNodesReachable(bc, nodes, 2)

		err := check.Probe(context.Background())
		if err == nil {
			t.Fatal("probe returned nil with fewer than K nodes up")
		}
		if want := "1 of 4 blob nodes answered, need 2"; err.Error() != want {
			t.Errorf("probe error = %q, want %q", err, want)
		}
	})

	t.Run("unready with none up", func(t *testing.T) {
		check := blobNodesReachable(&fakeShardProber{}, nodes, 1)

		if err := check.Probe(context.Background()); err == nil {
			t.Error("probe returned nil with every blob node down")
		}
	})
}

// A node that says the shard is missing has opened its store and answered,
// which is the whole question a reachability probe asks.
func TestBlobNodeAnsweringNotFoundCountsAsReached(t *testing.T) {
	bc := notFoundProber{}
	check := blobNodesReachable(bc, []NodeID{1, 2}, 2)

	if err := check.Probe(context.Background()); err != nil {
		t.Errorf("probe returned %v for nodes reporting a missing shard, want nil", err)
	}
}

type notFoundProber struct{}

func (notFoundProber) Get(_ context.Context, node NodeID, _ blob.GetRequest) (io.ReadCloser, error) {
	return nil, fmt.Errorf("get from node %d: %w", node, blob.ErrNotFound)
}

// The probe opens a stream per node on every scrape, so one that is not closed
// leaks a stream per node per probe interval for the life of the process.
func TestBlobProbeClosesWhatItOpens(t *testing.T) {
	bc := &fakeShardProber{up: map[NodeID]bool{1: true, 2: true}}
	check := blobNodesReachable(bc, []NodeID{1, 2}, 2)

	if err := check.Probe(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got := bc.closed.Load(); got != 2 {
		t.Errorf("closed %d of 2 probe reads", got)
	}
}
