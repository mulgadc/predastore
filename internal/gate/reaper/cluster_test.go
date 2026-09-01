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

// fakeBlob stands in for the blob nodes. A node can be held down, which is
// how a test models one that is unreachable when a delete needs it.
type fakeBlob struct {
	mu     sync.Mutex
	shards map[shardAddr]bool
	down   map[config.NodeID]bool

	deletes []shardAddr
}

func newFakeBlob() *fakeBlob {
	return &fakeBlob{shards: make(map[shardAddr]bool), down: make(map[config.NodeID]bool)}
}

// hold marks a shard as present on a node, as a completed write left it.
func (f *fakeBlob) hold(node config.NodeID, key [32]byte, index uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shards[shardAddr{node, key, index}] = true
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
	return f.shards[shardAddr{node, key, index}]
}

var errNodeDown = errors.New("node unreachable")

func (f *fakeBlob) Delete(_ context.Context, node config.NodeID, req blob.DeleteRequest) (*blob.DeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down[node] {
		return nil, errNodeDown
	}

	addr := shardAddr{node, req.Key, req.Index}
	f.deletes = append(f.deletes, addr)
	held := f.shards[addr]
	delete(f.shards, addr)

	return &blob.DeleteResponse{Deleted: held}, nil
}

var _ BlobClient = (*fakeBlob)(nil)

type fakeMeta struct {
	mu   sync.Mutex
	rows map[string][]byte
}

func newFakeMeta() *fakeMeta { return &fakeMeta{rows: make(map[string][]byte)} }

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

func (m *fakeMeta) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
