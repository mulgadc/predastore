package predastore

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type verifyFakeMeta struct {
	mu        sync.Mutex
	rows      map[string][]byte
	scanErr   error
	onRecheck func()
	rechecked bool
}

func (f *verifyFakeMeta) LeaderScanFrom(_ context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	keys := make([]string, 0, len(f.rows))
	for k := range f.rows {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	var items []meta.Item
	for _, k := range keys {
		if len(items) == limit {
			break
		}
		items = append(items, meta.Item{Key: k, Value: f.rows[k]})
	}

	return items, nil
}

func (f *verifyFakeMeta) LeaderGet(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	hook := f.onRecheck
	if f.rechecked {
		hook = nil
	}
	f.rechecked = true
	f.mu.Unlock()
	if hook != nil {
		hook()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.rows[key]
	if !ok {
		return nil, meta.ErrNotFound
	}

	return v, nil
}

type verifyPos struct {
	node  NodeID
	hash  [32]byte
	index uint32
}

type verifyHeld struct {
	epoch uint64
	err   error
}

type verifyFakeBlob struct {
	mu   sync.Mutex
	held map[verifyPos]verifyHeld
}

func (f *verifyFakeBlob) set(node NodeID, hash [32]byte, index int, h verifyHeld) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held[verifyPos{node, hash, uint32(index)}] = h
}

func (f *verifyFakeBlob) Stat(_ context.Context, node NodeID, req blob.StatRequest) (*blob.StatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.held[verifyPos{node, req.Key, req.Index}]
	switch {
	case !ok:
		return nil, blob.ErrNotFound
	case h.err != nil:
		return nil, h.err
	}

	return &blob.StatResponse{Epoch: h.epoch}, nil
}

type verifyFixture struct {
	t    *testing.T
	meta *verifyFakeMeta
	blob *verifyFakeBlob
}

func newVerifyFixture(t *testing.T) *verifyFixture {
	return &verifyFixture{
		t:    t,
		meta: &verifyFakeMeta{rows: map[string][]byte{}},
		blob: &verifyFakeBlob{held: map[verifyPos]verifyHeld{}},
	}
}

// object records a 2+1 placement on nodes and, unless told otherwise, has
// every node hold its shard at the record's epoch.
func (x *verifyFixture) object(name string, epoch uint64, nodes ...NodeID) ([32]byte, handlers.ObjectToShardNodes) {
	x.t.Helper()
	hash := sha256.Sum256([]byte(name))
	place := handlers.ObjectToShardNodes{
		DataShardNodes: nodes[:2], ParityShardNodes: nodes[2:], Size: 4096, WriteEpoch: epoch,
	}
	x.record(hash, place)
	for i, n := range nodes {
		x.blob.set(n, hash, i, verifyHeld{epoch: epoch})
	}

	return hash, place
}

func (x *verifyFixture) record(hash [32]byte, place handlers.ObjectToShardNodes) {
	x.t.Helper()
	raw, err := handlers.EncodePlacement(place)
	require.NoError(x.t, err)
	x.meta.mu.Lock()
	defer x.meta.mu.Unlock()
	x.meta.rows[handlers.TableKey(model.TableObjects, string(hash[:]))] = raw
}

// run verifies with a recheck short enough for a test to wait out.
func (x *verifyFixture) run(opts VerifyOptions) (ShardReport, error) {
	opts.Recheck = time.Millisecond

	return verifyShards(x.t.Context(), x.meta, x.blob, []NodeID{1, 2, 3, 4}, opts)
}

func TestVerifyShardsCountsEachPositionAgainstTheRecord(t *testing.T) {
	t.Parallel()

	x := newVerifyFixture(t)
	x.object("full", 10, 1, 2, 3)

	damaged, _ := x.object("damaged", 20, 2, 3, 4)
	x.blob.set(3, damaged, 1, verifyHeld{epoch: 19})
	delete(x.blob.held, verifyPos{4, damaged, 2})
	x.meta.rows[handlers.TableKey(model.TableObjects, "arn:aws:s3:::bucket/damaged")] = damaged[:]

	unreadable, _ := x.object("unreadable", 30, 1, 3, 4)
	x.blob.set(4, unreadable, 2, verifyHeld{err: errors.New("stat: disk on fire")})

	x.record(sha256.Sum256([]byte("empty")), handlers.ObjectToShardNodes{
		DataShardNodes: []config.NodeID{1, 2}, ParityShardNodes: []config.NodeID{3}, WriteEpoch: 40,
	})
	undecodable := sha256.Sum256([]byte("undecodable"))
	x.meta.rows[handlers.TableKey(model.TableObjects, string(undecodable[:]))] = []byte("junk")

	report, err := x.run(VerifyOptions{PageSize: 2})
	require.NoError(t, err)

	assert.Equal(t, 3, report.Records)
	assert.Equal(t, 1, report.Empty)
	assert.Equal(t, 1, report.Undecodable)
	assert.Equal(t, 3, report.Checked)
	assert.Equal(t, 1, report.Full)
	assert.Equal(t, map[int]int{3: 1, 2: 1, 1: 1}, report.ByMatch)

	require.Len(t, report.NonFull, 2)
	worst := report.NonFull[0]
	assert.Equal(t, 1, worst.Match, "the least intact object is listed first")
	assert.Equal(t, "bucket/damaged", worst.Name)
	assert.Equal(t, []string{ShardMatch, ShardOlder, ShardMissing},
		[]string{worst.Shards[0].State, worst.Shards[1].State, worst.Shards[2].State})
	assert.Equal(t, uint64(19), worst.Shards[1].HeldEpoch)
	assert.Equal(t, ShardError, report.NonFull[1].Shards[2].State)
	assert.Contains(t, report.NonFull[1].Shards[2].Error, "disk on fire")

	require.Len(t, report.Nodes, 4)
	assert.Equal(t, NodeShardCounts{Node: 3, Owned: 3, Match: 2, Older: 1}, report.Nodes[2])
	assert.Equal(t, NodeShardCounts{Node: 4, Owned: 2, Missing: 1, Error: 1}, report.Nodes[3])
}

// TestVerifyShardsRechecksAnObjectWrittenMidScan keeps a write in flight, or
// a delete, from being reported as damage.
func TestVerifyShardsRechecksAnObjectWrittenMidScan(t *testing.T) {
	t.Parallel()

	x := newVerifyFixture(t)
	rewritten, place := x.object("rewritten", 10, 1, 2, 3)
	x.blob.set(3, rewritten, 2, verifyHeld{epoch: 11})
	deleted, _ := x.object("deleted", 10, 2, 3, 4)
	delete(x.blob.held, verifyPos{2, deleted, 0})

	x.meta.onRecheck = func() {
		place.WriteEpoch = 11
		x.record(rewritten, place)
		for i, n := range place.AllNodes() {
			x.blob.set(n, rewritten, i, verifyHeld{epoch: 11})
		}
		x.meta.mu.Lock()
		delete(x.meta.rows, handlers.TableKey(model.TableObjects, string(deleted[:])))
		x.meta.mu.Unlock()
	}

	report, err := x.run(VerifyOptions{})
	require.NoError(t, err)
	assert.Equal(t, 2, report.Records)
	assert.Equal(t, 1, report.Vanished)
	assert.Equal(t, 1, report.Checked)
	assert.Equal(t, 1, report.Full)
	assert.Empty(t, report.NonFull)
}

func TestVerifyShardsFailsWhenTheScanFails(t *testing.T) {
	t.Parallel()

	x := newVerifyFixture(t)
	x.meta.scanErr = meta.ErrNoLeaderRead

	_, err := x.run(VerifyOptions{})
	require.ErrorIs(t, err, meta.ErrNoLeaderRead)
}

func TestVerifyShardsNeedsMetaAndBlobNodes(t *testing.T) {
	t.Parallel()

	cfg := &Config{Hosts: []config.Host{{ID: 1, Addr: "127.0.0.1", Nodes: []config.Node{{ID: 1, Role: config.RoleBlob, Port: 1}}}}}
	_, err := VerifyShards(t.Context(), cfg, nil, VerifyOptions{})
	require.ErrorContains(t, err, "0 meta and 1 blob nodes")
}
