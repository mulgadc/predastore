package repair

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/require"
)

// The fakes below stand in for a cluster rather than for the clients: they
// implement the prepare/commit split and the epoch gate the real blob node
// enforces, because those are the properties repair is written against and a
// stub that just returned bytes would pass whatever repair did.

type shardAddr struct {
	node  config.NodeID
	key   [32]byte
	index uint32
}

type shardState struct {
	epoch uint64
	body  []byte
}

type fakeBlob struct {
	mu        sync.Mutex
	committed map[shardAddr]shardState
	prepared  map[shardAddr]shardState
	down      map[config.NodeID]bool

	// onPut runs after a put lands, so a test can move the world underneath a
	// repair that is already in flight.
	onPut func()

	stats, gets, puts, commits, aborts, deletes atomic.Int64
}

func newFakeBlob() *fakeBlob {
	return &fakeBlob{
		committed: make(map[shardAddr]shardState),
		prepared:  make(map[shardAddr]shardState),
		down:      make(map[config.NodeID]bool),
	}
}

var errNodeDown = errors.New("node unreachable")

func (f *fakeBlob) stop(nodes ...config.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range nodes {
		f.down[n] = true
	}
}

// hold publishes a shard directly, as a completed write would leave it.
func (f *fakeBlob) hold(node config.NodeID, key [32]byte, index int, epoch uint64, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed[shardAddr{node, key, uint32(index)}] = shardState{epoch: epoch, body: body}
}

// forget removes a published shard, as a wiped disk or a write that never
// reached the node leaves it.
func (f *fakeBlob) forget(node config.NodeID, key [32]byte, index int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.committed, shardAddr{node, key, uint32(index)})
}

// prepare leaves a shard durable but unpublished, as an interrupted write does.
func (f *fakeBlob) prepare(node config.NodeID, key [32]byte, index int, epoch uint64, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepared[shardAddr{node, key, uint32(index)}] = shardState{epoch: epoch, body: body}
}

func (f *fakeBlob) held(node config.NodeID, key [32]byte, index int) (shardState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.committed[shardAddr{node, key, uint32(index)}]

	return s, ok
}

func (f *fakeBlob) Stat(
	_ context.Context, node config.NodeID, req blob.StatRequest,
) (*blob.StatResponse, error) {
	f.stats.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[node] {
		return nil, errNodeDown
	}
	s, ok := f.committed[shardAddr{node, req.Key, req.Index}]
	if !ok {
		return nil, blob.ErrNotFound
	}

	return &blob.StatResponse{Epoch: s.epoch, Size: int64(len(s.body))}, nil
}

func (f *fakeBlob) Get(
	_ context.Context, node config.NodeID, req blob.GetRequest,
) (io.ReadCloser, error) {
	f.gets.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[node] {
		return nil, errNodeDown
	}
	s, ok := f.committed[shardAddr{node, req.Key, req.Index}]
	if !ok {
		return nil, blob.ErrNotFound
	}
	if req.Epoch != 0 && req.Epoch != s.epoch {
		return nil, fmt.Errorf("epoch-mismatch: node holds %016x, caller asked for %016x",
			s.epoch, req.Epoch)
	}

	return io.NopCloser(bytes.NewReader(s.body)), nil
}

func (f *fakeBlob) Put(
	_ context.Context, node config.NodeID, req blob.PutRequest, body io.Reader,
) (*blob.PutResponse, error) {
	f.puts.Add(1)
	f.mu.Lock()
	down := f.down[node]
	f.mu.Unlock()
	if down {
		return nil, errNodeDown
	}

	buf, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	// The real node writes exactly the length the header declares, so a rebuild
	// that produced a different one must not look like a success here.
	if int64(len(buf)) != req.Size {
		return nil, fmt.Errorf("put declared %d bytes, body carried %d", req.Size, len(buf))
	}

	f.mu.Lock()
	f.prepared[shardAddr{node, req.Key, req.Index}] = shardState{epoch: req.Epoch, body: buf}
	onPut := f.onPut
	f.mu.Unlock()
	if onPut != nil {
		onPut()
	}

	return &blob.PutResponse{}, nil
}

func (f *fakeBlob) Commit(_ context.Context, node config.NodeID, req blob.CommitRequest) error {
	f.commits.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[node] {
		return errNodeDown
	}
	addr := shardAddr{node, req.Key, req.Index}
	s, ok := f.prepared[addr]
	if !ok || s.epoch != req.Epoch {
		return blob.ErrNotPrepared
	}
	delete(f.prepared, addr)
	f.committed[addr] = s

	return nil
}

func (f *fakeBlob) Abort(_ context.Context, node config.NodeID, req blob.CommitRequest) error {
	f.aborts.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[node] {
		return errNodeDown
	}
	delete(f.prepared, shardAddr{node, req.Key, req.Index})

	return nil
}

func (f *fakeBlob) Delete(
	_ context.Context, node config.NodeID, req blob.DeleteRequest,
) (*blob.DeleteResponse, error) {
	f.deletes.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[node] {
		return nil, errNodeDown
	}
	addr := shardAddr{node, req.Key, req.Index}
	_, held := f.committed[addr]
	delete(f.committed, addr)
	delete(f.prepared, addr)

	return &blob.DeleteResponse{Deleted: held}, nil
}

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

func (m *fakeMeta) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.rows[key]
	if !ok {
		return nil, meta.ErrNotFound
	}

	return v, nil
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

// object is one stored object and everything a test needs to assert about it.
type object struct {
	hash   [32]byte
	place  handlers.ObjectToShardNodes
	shards [][]byte
	body   []byte
}

// cluster is a fake blob set, a fake meta store and the erasure code they were
// written under.
type cluster struct {
	t      *testing.T
	blob   *fakeBlob
	meta   *fakeMeta
	nodes  []config.NodeID
	ring   *placement.Ring
	k, m   int
	epochs uint64
}

func newCluster(t *testing.T, k, m, nodeCount int) *cluster {
	t.Helper()
	require.GreaterOrEqual(t, nodeCount, k+m, "the ring needs a node per shard")

	nodes := make([]config.NodeID, nodeCount)
	for i := range nodes {
		nodes[i] = config.NodeID(i + 1)
	}

	return &cluster{
		t: t, blob: newFakeBlob(), meta: newFakeMeta(),
		nodes: nodes, ring: placement.NewRing(nodes), k: k, m: m,
	}
}

// store writes an object the way a completed PUT leaves one: every shard
// published at one epoch, and the placement record naming it.
func (c *cluster) store(name string, size int) *object {
	c.t.Helper()

	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i*31 + len(name))
	}
	hash := sha256.Sum256([]byte(name))
	c.epochs++
	epoch := c.epochs

	place := c.placeFor(hash, int64(size), epoch)
	shards := encodeStripe(c.t, c.k, c.m, body)
	for index, node := range place.AllNodes() {
		if shards[index] != nil {
			c.blob.hold(node, hash, index, epoch, shards[index])
		}
	}
	c.record(hash, place)

	return &object{hash: hash, place: place, shards: shards, body: body}
}

// placeFor spreads a stripe across the ring the way the write path does.
func (c *cluster) placeFor(hash [32]byte, size int64, epoch uint64) handlers.ObjectToShardNodes {
	c.t.Helper()
	nodes, err := c.ring.Nodes(hash, c.k+c.m)
	require.NoError(c.t, err)

	return handlers.ObjectToShardNodes{
		DataShardNodes:   nodes[:c.k],
		ParityShardNodes: nodes[c.k:],
		Size:             size,
		WriteEpoch:       epoch,
	}
}

func (c *cluster) record(hash [32]byte, place handlers.ObjectToShardNodes) {
	c.t.Helper()
	raw, err := handlers.EncodePlacement(place)
	require.NoError(c.t, err)
	c.meta.put(handlers.TableKey(model.TableObjects, string(hash[:])), raw)
}

// service builds a repair service sweeping for the named nodes.
func (c *cluster) service(nodes ...config.NodeID) *Service {
	c.t.Helper()
	s, err := New(Config{
		Nodes: nodes, Ring: c.ring, Meta: c.meta, Blob: c.blob,
		DataShards: c.k, ParityShards: c.m, Workers: 4, PageSize: 8,
	})
	require.NoError(c.t, err)

	return s
}

// encodeStripe splits and encodes exactly as the write path does, so a rebuilt
// shard can be compared byte for byte against what a PUT would have stored.
func encodeStripe(t *testing.T, k, m int, body []byte) [][]byte {
	t.Helper()
	if len(body) == 0 {
		// The write path never encodes an empty object: its record exists so the
		// GET can be served, and no node holds anything for it.
		return make([][]byte, k+m)
	}
	enc, err := reedsolomon.NewStream(k, m)
	require.NoError(t, err)

	size := int64(len(body))
	shardSize := int((size + int64(k) - 1) / int64(k))

	shards := make([][]byte, k+m)
	dataWriters := make([]io.Writer, k)
	buffers := make([]*bytes.Buffer, k+m)
	for i := range k {
		buffers[i] = bytes.NewBuffer(make([]byte, 0, shardSize))
		dataWriters[i] = buffers[i]
	}
	require.NoError(t, enc.Split(bytes.NewReader(body), dataWriters, size))

	dataReaders := make([]io.Reader, k)
	for i := range k {
		shards[i] = buffers[i].Bytes()
		dataReaders[i] = bytes.NewReader(shards[i])
	}
	if m > 0 {
		parityWriters := make([]io.Writer, m)
		for i := range m {
			buffers[k+i] = bytes.NewBuffer(make([]byte, 0, shardSize))
			parityWriters[i] = buffers[k+i]
		}
		require.NoError(t, enc.Encode(dataReaders, parityWriters))
		for i := range m {
			shards[k+i] = buffers[k+i].Bytes()
		}
	}

	return shards
}
