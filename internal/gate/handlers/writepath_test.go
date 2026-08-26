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

// fakeBlob stands in for the blob nodes. It records the largest single Write it
// was handed, which is how a streaming write is told from a staged one: a
// staged write arrives as one buffer the size of the object.
type fakeBlob struct {
	mu     sync.Mutex
	shards map[shardID][]byte

	maxWrite  atomic.Int64
	putCalls  atomic.Int64
	declaring func(size int64) error
}

func newFakeBlob() *fakeBlob { return &fakeBlob{shards: make(map[shardID][]byte)} }

func (b *fakeBlob) Put(_ context.Context, _ config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	b.putCalls.Add(1)
	if req.Size <= 0 {
		return nil, errors.New("no size specified")
	}
	if b.declaring != nil {
		if err := b.declaring(req.Size); err != nil {
			return nil, err
		}
	}

	// Mirror the node: never read past the declared size, and observe the
	// chunking the caller actually produced.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, &observingReader{r: io.LimitReader(body, req.Size), blob: b}); err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.shards[shardID{key: req.Key, index: req.Index}] = buf.Bytes()
	b.mu.Unlock()

	return &blob.PutResponse{Size: int64(buf.Len())}, nil
}

func (b *fakeBlob) Get(_ context.Context, _ config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.mu.Lock()
	data, ok := b.shards[shardID{key: req.Key, index: req.Index}]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("shard %x/%d not found", req.Key[:4], req.Index)
	}
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
		cfg:  Config{Region: "ap-southeast-2", DataShards: data, ParityShards: parity},
	}
}

// roundTrip writes an object and reads it back through the same placement the
// handlers record, which is the only thing that proves a write is retrievable.
func (f writeFixture) roundTrip(t *testing.T, bucket, key string, body []byte) []byte {
	t.Helper()
	ctx := context.Background()
	objectHash := model.ObjectHash(bucket, key)

	_, err := writeObject(ctx, f.bc, f.ring, f.cfg, bytes.NewReader(body), int64(len(body)), objectHash)
	require.NoError(t, err)

	place, err := placeShards(f.ring, f.cfg, objectHash, int64(len(body)))
	require.NoError(t, err)

	got, err := readObject(ctx, f.bc, f.cfg, bucket, key, place, place.Size)
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
	for id, data := range f.bc.shards {
		assert.Zero(t, id.index)
		assert.Equal(t, want, data, "the shard is the object")
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

	_, err := writeObject(context.Background(), f.bc, f.ring, f.cfg, body, 200, model.ObjectHash("b", "k"))

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
	poolNearFull, err := writeObject(
		context.Background(), nearFull, f.ring, f.cfg,
		bytes.NewReader([]byte("payload")), 7, model.ObjectHash("b", "k"),
	)

	require.NoError(t, err)
	assert.True(t, poolNearFull)
}

// A parity shard is streamed through an io.Pipe that enc.Encode writes into on
// this goroutine. A Put that gives up without draining it used to leave the
// write side blocked forever, and io.Pipe does not observe ctx.
func TestWriteObjectReturnsWhenParityPutAbandonsItsBody(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	bc := &parityRejectingBlob{fakeBlob: f.bc, dataShards: f.cfg.DataShards}
	body := randomBytes(t, 64*1024)

	done := make(chan error, 1)
	go func() {
		_, err := writeObject(
			context.Background(), bc, f.ring, f.cfg,
			bytes.NewReader(body), int64(len(body)), model.ObjectHash("b", "k"),
		)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a rejected parity shard has to fail the write")
	case <-time.After(10 * time.Second):
		t.Fatal("writeObject blocked: the parity pipe was left without a reader")
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
