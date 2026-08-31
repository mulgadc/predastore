package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMeta is a map standing in for the raft-backed global state.
type fakeMeta struct {
	mu   sync.Mutex
	rows map[string][]byte
}

func newFakeMeta() *fakeMeta { return &fakeMeta{rows: make(map[string][]byte)} }

func (m *fakeMeta) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.rows[key]
	if !ok {
		// The real client's sentinel: callers branch on it to tell a missing
		// row from a broken one.
		return nil, meta.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *fakeMeta) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[key] = append([]byte(nil), value...)
	return nil
}

func (m *fakeMeta) PutMax(ctx context.Context, key string, value []byte, _ uint64) error {
	return m.Put(ctx, key, value)
}

func (m *fakeMeta) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, key)
	return nil
}

func (m *fakeMeta) Scan(_ context.Context, prefix string, limit int) ([]meta.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var items []meta.Item
	for k, v := range m.rows {
		if strings.HasPrefix(k, prefix) {
			items = append(items, meta.Item{Key: k, Value: append([]byte(nil), v...)})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

// shardID identifies one shard of one object, which is what a blob node keys by.
type shardID struct {
	key   [32]byte
	index uint32
}

// fakeShard is one stored shard: its bytes and the epoch it was written under.
type fakeShard struct {
	data  []byte
	epoch uint64
}

// fakeBlob stands in for the blob nodes. It records the largest single Write it
// was handed, which is how a streaming write is told from a staged one: a
// staged write arrives as one buffer the size of the object.
//
// It models prepare/commit and the epoch gate the same way a node does, so a
// handler test exercises the real two-phase write rather than a shortcut.
type fakeBlob struct {
	mu       sync.Mutex
	shards   map[shardID]fakeShard
	prepared map[shardID]fakeShard

	maxWrite     atomic.Int64
	putCalls     atomic.Int64
	commitCalls  atomic.Int64
	abortCalls   atomic.Int64
	declaring    func(size int64) error
	failPutOn    func(index uint32) bool
	failCommitOn func(index uint32) bool
}

func newFakeBlob() *fakeBlob {
	return &fakeBlob{
		shards:   make(map[shardID]fakeShard),
		prepared: make(map[shardID]fakeShard),
	}
}

func (b *fakeBlob) Put(_ context.Context, _ config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	b.putCalls.Add(1)
	if req.Size <= 0 {
		return nil, errors.New("no size specified")
	}
	if req.Epoch == 0 {
		return nil, errors.New("no write epoch specified")
	}
	if b.declaring != nil {
		if err := b.declaring(req.Size); err != nil {
			return nil, err
		}
	}
	if b.failPutOn != nil && b.failPutOn(req.Index) {
		return nil, fmt.Errorf("node holding shard %d is not answering", req.Index)
	}

	// Mirror the node: never read past the declared size, and observe the
	// chunking the caller actually produced.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, &observingReader{r: io.LimitReader(body, req.Size), blob: b}); err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.prepared[shardID{key: req.Key, index: req.Index}] = fakeShard{data: buf.Bytes(), epoch: req.Epoch}
	b.mu.Unlock()

	return &blob.PutResponse{Size: int64(buf.Len()), Epoch: req.Epoch}, nil
}

func (b *fakeBlob) Commit(_ context.Context, _ config.NodeID, req blob.CommitRequest) (bool, error) {
	b.commitCalls.Add(1)
	if b.failCommitOn != nil && b.failCommitOn(req.Index) {
		return false, errors.New("commit refused")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return false, b.commitLocked(shardID{key: req.Key, index: req.Index}, req.Epoch)
}

// commitLocked publishes a prepared shard, idempotently against the epoch.
func (b *fakeBlob) commitLocked(id shardID, epoch uint64) error {
	if prepared, ok := b.prepared[id]; ok && prepared.epoch == epoch {
		b.shards[id] = prepared
		delete(b.prepared, id)

		return nil
	}
	if live, ok := b.shards[id]; ok && live.epoch == epoch {
		return nil
	}

	return blob.ErrNotPrepared
}

func (b *fakeBlob) Abort(_ context.Context, _ config.NodeID, req blob.CommitRequest) error {
	b.abortCalls.Add(1)

	b.mu.Lock()
	defer b.mu.Unlock()

	id := shardID{key: req.Key, index: req.Index}
	if prepared, ok := b.prepared[id]; ok && prepared.epoch == req.Epoch {
		delete(b.prepared, id)
	}

	return nil
}

func (b *fakeBlob) Get(_ context.Context, _ config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := shardID{key: req.Key, index: req.Index}
	shard, ok := b.shards[id]
	if !ok {
		return nil, fmt.Errorf("shard %x/%d: %w", req.Key[:4], req.Index, blob.ErrNotFound)
	}
	if req.Epoch != 0 && shard.epoch != req.Epoch {
		// A prepared shard under the requested epoch means the writer published
		// the record and died before committing, exactly as the node does.
		if err := b.commitLocked(id, req.Epoch); err != nil {
			return nil, fmt.Errorf("shard %x/%d: %w", req.Key[:4], req.Index, blob.ErrEpochMismatch)
		}
		shard = b.shards[id]
	}

	data := shard.data
	if req.RangeStart >= 0 && req.RangeEnd >= 0 {
		data = data[req.RangeStart : req.RangeEnd+1]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *fakeBlob) Delete(_ context.Context, _ config.NodeID, req blob.DeleteRequest) (*blob.DeleteResponse, error) {
	id := shardID{key: req.Key, index: req.Index}
	b.mu.Lock()
	_, ok := b.shards[id]
	delete(b.shards, id)
	delete(b.prepared, id)
	b.mu.Unlock()
	return &blob.DeleteResponse{Deleted: ok}, nil
}

// observingReader records the largest chunk handed to the node.
type observingReader struct {
	r    io.Reader
	blob *fakeBlob
}

func (o *observingReader) Read(p []byte) (int, error) {
	n, err := o.r.Read(p)
	for {
		prev := o.blob.maxWrite.Load()
		if int64(n) <= prev || o.blob.maxWrite.CompareAndSwap(prev, int64(n)) {
			break
		}
	}
	return n, err
}

var _ MetaClient = (*fakeMeta)(nil)
var _ BlobClient = (*fakeBlob)(nil)

// write runs the fixture's write path the way a handler does: place the object,
// mint its epoch, prepare every shard, then publish. The placement it returns
// is the one the shards carry, so it is what a read has to name — deriving a
// second one would mint a different epoch and match nothing.
func (f writeFixture) write(ctx context.Context, objectHash [32]byte, body io.Reader, size int64) (ObjectToShardNodes, writeResult, error) {
	place, err := placeShards(f.ring, f.cfg, objectHash, size)
	if err != nil {
		return place, writeResult{}, err
	}
	written, err := writeObject(ctx, f.bc, f.cfg, f.ring, body, size, objectHash, place)
	if err != nil {
		return place, written, err
	}
	commitShards(ctx, f.bc, objectHash, place, written)

	return place, written, nil
}

// writeFixture is a gate write path at one erasure width.
type writeFixture struct {
	mc   *fakeMeta
	bc   *fakeBlob
	ring *placement.Ring
	cfg  Config
}

func newWriteFixture(data, parity int) writeFixture {
	nodes := make([]config.NodeID, data+parity)
	for i := range nodes {
		nodes[i] = config.NodeID(i + 1)
	}
	return writeFixture{
		mc:   newFakeMeta(),
		bc:   newFakeBlob(),
		ring: placement.NewRing(nodes),
		cfg: Config{
			Region: "ap-southeast-2", DataShards: data, ParityShards: parity,
			Epochs: mustEpochs(1),
		},
	}
}

// roundTrip writes an object and reads it back through the same placement the
// handlers record, which is the only thing that proves a write is retrievable.
func (f writeFixture) roundTrip(t *testing.T, bucket, key string, body []byte) []byte {
	t.Helper()
	ctx := context.Background()
	objectHash := model.ObjectHash(bucket, key)

	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	got, _, err := readObject(ctx, f.bc, f.cfg, bucket, key, place, place.Size, 0)
	require.NoError(t, err)
	return got
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// The fast path has to produce exactly what the general path would, at every
// size, or RS(1,0) is a different storage format rather than a shortcut.
func TestWriteObjectSingleShardRoundTrips(t *testing.T) {
	t.Parallel()

	for _, size := range []int{1, 7, 8192, 8193, 1 << 20} {
		t.Run(fmt.Sprintf("%d-bytes", size), func(t *testing.T) {
			t.Parallel()
			f := newWriteFixture(1, 0)
			want := randomBytes(t, size)
			assert.Equal(t, want, f.roundTrip(t, "b", "k", want))
		})
	}
}

// RS(1,0) is one shard, so the object must be stored whole and unencoded — not
// split, and not padded out to a shard boundary.
func TestWriteObjectSingleShardStoresTheObjectWhole(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	want := randomBytes(t, 4096)
	f.roundTrip(t, "b", "k", want)

	f.bc.mu.Lock()
	defer f.bc.mu.Unlock()
	require.Len(t, f.bc.shards, 1, "RS(1,0) writes exactly one shard")
	for id, shard := range f.bc.shards {
		assert.Zero(t, id.index)
		assert.Equal(t, want, shard.data, "the shard is the object")
	}
}

// The point of the fast path is that the object never lands in one buffer, so
// this asserts on how the body reaches the node rather than on the result.
func TestWriteObjectSingleShardDoesNotStageTheObject(t *testing.T) {
	t.Parallel()

	const size = 4 << 20
	f := newWriteFixture(1, 0)
	f.roundTrip(t, "b", "k", randomBytes(t, size))

	assert.Less(t, f.bc.maxWrite.Load(), int64(size),
		"a %d-byte object reached the node in one write, so it was staged whole", size)
}

// A wider code still has to work: the fast path must not have changed it.
func TestWriteObjectMultiShardRoundTrips(t *testing.T) {
	t.Parallel()

	for _, rs := range []struct{ data, parity int }{{2, 1}, {3, 2}} {
		t.Run(fmt.Sprintf("rs-%d-%d", rs.data, rs.parity), func(t *testing.T) {
			t.Parallel()
			f := newWriteFixture(rs.data, rs.parity)
			want := randomBytes(t, 1<<20)
			assert.Equal(t, want, f.roundTrip(t, "b", "k", want))
			assert.Equal(t, int64(rs.data+rs.parity), f.bc.putCalls.Load())
		})
	}
}

// The blob protocol has no zero-length value, so an empty object is placement
// and nothing else. It used to fail the write outright.
func TestWriteObjectEmptyWritesNoShards(t *testing.T) {
	t.Parallel()

	for _, rs := range []struct{ data, parity int }{{1, 0}, {2, 1}} {
		t.Run(fmt.Sprintf("rs-%d-%d", rs.data, rs.parity), func(t *testing.T) {
			t.Parallel()
			f := newWriteFixture(rs.data, rs.parity)
			assert.Empty(t, f.roundTrip(t, "b", "empty", []byte{}))
			assert.Zero(t, f.bc.putCalls.Load(), "an empty object has no shard to write")
		})
	}
}

// Streaming gives up the splitter's short-body check, so the fast path has to
// make that check itself: placement records the declared size, and a short
// value would read back as a corrupt object.
func TestWriteObjectSingleShardRejectsAShortBody(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	body := bytes.NewReader(randomBytes(t, 100))

	_, _, err := f.write(context.Background(), model.ObjectHash("b", "k"), body, 200)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "declared 200")
}

// A pool-pressure signal from the node has to survive the fast path, because it
// is what makes a nearfull write set the backoff header.
func TestWriteObjectSingleShardReportsPoolPressure(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	f.bc.declaring = func(int64) error { return nil }

	nearFull := &nearFullBlob{fakeBlob: f.bc}
	objectHash := model.ObjectHash("b", "k")
	place, err := placeShards(f.ring, f.cfg, objectHash, 7)
	require.NoError(t, err)

	written, err := writeObject(
		context.Background(), nearFull, f.cfg, f.ring,
		bytes.NewReader([]byte("payload")), 7, objectHash, place,
	)

	require.NoError(t, err)
	assert.True(t, written.poolNearFull)
}

// A parity shard used to be streamed through an io.Pipe that enc.Encode wrote
// into on this goroutine, so a Put that gave up without draining it left the
// write side blocked forever and io.Pipe does not observe ctx. The parity is
// buffered now and the deadlock cannot recur, but a node refusing its body must
// still return rather than hang, which is what this holds.
func TestWriteObjectReturnsWhenParityPutAbandonsItsBody(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	bc := &parityRejectingBlob{fakeBlob: f.bc, dataShards: f.cfg.DataShards}
	body := randomBytes(t, 64*1024)

	objectHash := model.ObjectHash("b", "k")
	place, err := placeShards(f.ring, f.cfg, objectHash, int64(len(body)))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := writeObject(
			context.Background(), bc, f.cfg, f.ring,
			bytes.NewReader(body), int64(len(body)), objectHash, place,
		)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a rejected parity shard has to fail the write at full width")
	case <-time.After(10 * time.Second):
		t.Fatal("writeObject blocked rather than reporting the refused shard")
	}
}

// parityRejectingBlob refuses every parity shard without consuming its body,
// which is how a node that stalls mid-transfer looks to the gate.
type parityRejectingBlob struct {
	*fakeBlob

	dataShards int
}

func (p *parityRejectingBlob) Put(
	ctx context.Context, id config.NodeID, req blob.PutRequest, body io.Reader,
) (*blob.PutResponse, error) {
	if int(req.Index) >= p.dataShards {
		return nil, errors.New("node stalled")
	}
	return p.fakeBlob.Put(ctx, id, req, body)
}

// nearFullBlob answers every put with pressure set.
type nearFullBlob struct{ *fakeBlob }

func (n *nearFullBlob) Put(ctx context.Context, id config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	resp, err := n.fakeBlob.Put(ctx, id, req, body)
	if err != nil {
		return nil, err
	}
	resp.PoolNearFull = true
	return resp, nil
}
