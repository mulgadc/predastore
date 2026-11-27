package reaper

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/require"
)

// The fakes below stand in for a cluster, not for the clients: they model the
// shape a real node and a real meta replica present, so a test exercises the
// same decisions the sweep makes against production.

type shardAddr struct {
	node  config.NodeID
	key   [32]byte
	index uint32
}

// fakeBlob stands in for the blob nodes. A position holds a row per
// generation, as the blob engine does, and a node can be held down, which is
// how a test models one that is unreachable when a delete needs it.
type fakeBlob struct {
	mu     sync.Mutex
	shards map[shardAddr]map[uint64]bool
	down   map[config.NodeID]bool

	deletes []shardAddr
	epochs  []uint64

	// beforeDelete runs once, ahead of every delete, which is where a test
	// lands a write the sweep did not see. Set it before the pass starts.
	beforeDelete func()
	landOnce     sync.Once
}

func newFakeBlob() *fakeBlob {
	return &fakeBlob{shards: make(map[shardAddr]map[uint64]bool), down: make(map[config.NodeID]bool)}
}

// hold marks a shard as present on a node at the generation that wrote it, as
// a completed write left it.
func (f *fakeBlob) hold(node config.NodeID, key [32]byte, index uint32, epoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	addr := shardAddr{node, key, index}
	if f.shards[addr] == nil {
		f.shards[addr] = make(map[uint64]bool)
	}
	f.shards[addr][epoch] = true
}

func (f *fakeBlob) stop(nodes ...config.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range nodes {
		f.down[n] = true
	}
}

func (f *fakeBlob) resume(nodes ...config.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range nodes {
		delete(f.down, n)
	}
}

func (f *fakeBlob) has(node config.NodeID, key [32]byte, index uint32) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.shards[shardAddr{node, key, index}]) > 0
}

// hasEpoch reports whether a node holds one generation at a position.
func (f *fakeBlob) hasEpoch(node config.NodeID, key [32]byte, index uint32, epoch uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shards[shardAddr{node, key, index}][epoch]
}

// epochAt reports the newest generation a node holds at a position.
func (f *fakeBlob) epochAt(node config.NodeID, key [32]byte, index uint32) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	held := f.shards[shardAddr{node, key, index}]
	if len(held) == 0 {
		return 0, false
	}
	return slices.Max(slices.Collect(maps.Keys(held))), true
}

var errNodeDown = errors.New("node unreachable")

// DeleteEpoch removes a shard only when the node holds it at the named
// generation, as the blob engine does.
func (f *fakeBlob) DeleteEpoch(_ context.Context, node config.NodeID, req blob.CommitRequest) (*blob.DeleteResponse, error) {
	if f.beforeDelete != nil {
		f.landOnce.Do(f.beforeDelete)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down[node] {
		return nil, errNodeDown
	}

	addr := shardAddr{node, req.Key, req.Index}
	f.deletes = append(f.deletes, addr)
	f.epochs = append(f.epochs, req.Epoch)
	if !f.shards[addr][req.Epoch] {
		return &blob.DeleteResponse{}, nil
	}
	delete(f.shards[addr], req.Epoch)

	return &blob.DeleteResponse{Deleted: true}, nil
}

var _ BlobClient = (*fakeBlob)(nil)

// fakeMeta stands in for a meta group. rows is what the leader has applied.
// A follower can lag: a key in follower answers Get with that older value,
// and leaderErr makes every leader read fail, as when no leader is elected.
type fakeMeta struct {
	mu        sync.Mutex
	rows      map[string][]byte
	follower  map[string][]byte
	leaderErr error
}

func newFakeMeta() *fakeMeta {
	return &fakeMeta{rows: make(map[string][]byte), follower: make(map[string][]byte)}
}

// lag makes a follower answer Get for key with an older value.
func (m *fakeMeta) lag(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.follower[key] = value
}

// failLeader makes every leader read answer err, or answer again when nil.
func (m *fakeMeta) failLeader(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderErr = err
}

func (m *fakeMeta) put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[key] = value
}

func (m *fakeMeta) has(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.rows[key]
	return ok
}

// Get is a read any replica may answer, so a lagging follower's value wins.
func (m *fakeMeta) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.follower[key]; ok {
		return v, nil
	}
	v, ok := m.rows[key]
	if !ok {
		return nil, meta.ErrNotFound
	}
	return v, nil
}

// LeaderGet answers only from what the leader has applied.
func (m *fakeMeta) LeaderGet(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leaderErr != nil {
		return nil, m.leaderErr
	}
	v, ok := m.rows[key]
	if !ok {
		return nil, meta.ErrNotFound
	}
	return v, nil
}

func (m *fakeMeta) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, key)
	return nil
}

// ScanFrom sorts, because a cursor only means anything over an ordered
// enumeration and badger iterates in key order.
func (m *fakeMeta) ScanFrom(_ context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys := make([]string, 0, len(m.rows))
	for k := range maps.Keys(m.rows) {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	items := make([]meta.Item, 0, len(keys))
	for _, k := range keys {
		items = append(items, meta.Item{Key: k, Value: m.rows[k]})
		if limit > 0 && len(items) == limit {
			break
		}
	}

	return items, nil
}

var _ MetaClient = (*fakeMeta)(nil)

// cluster is a fake blob set and a fake meta store, wired the way a gate
// wires the reaper.
type cluster struct {
	t    *testing.T
	blob *fakeBlob
	meta *fakeMeta
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	return &cluster{t: t, blob: newFakeBlob(), meta: newFakeMeta()}
}

// service builds a reaper sweeping this cluster.
func (c *cluster) service(pageSize int) *Service {
	c.t.Helper()
	s, err := New(Config{Meta: c.meta, Blob: c.blob, Workers: 4, PageSize: pageSize})
	require.NoError(c.t, err)
	return s
}
