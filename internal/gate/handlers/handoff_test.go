package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nodeAddr keys a shard by the node holding it. The write-path fake stores by
// key and index alone, which cannot tell an owner's copy from a holder's, and
// that distinction is the whole of what handoff does.
type nodeAddr struct {
	node  config.NodeID
	key   [32]byte
	index uint32
}

// nodeBlob is a blob cluster that knows which node it is talking to.
type nodeBlob struct {
	mu        sync.Mutex
	committed map[nodeAddr]fakeShard
	prepared  map[nodeAddr]fakeShard
	down      map[config.NodeID]bool

	puts, deletes atomic.Int64
}

func newNodeBlob(down ...config.NodeID) *nodeBlob {
	b := &nodeBlob{
		committed: make(map[nodeAddr]fakeShard),
		prepared:  make(map[nodeAddr]fakeShard),
		down:      make(map[config.NodeID]bool),
	}
	for _, n := range down {
		b.down[n] = true
	}

	return b
}

func (b *nodeBlob) Put(_ context.Context, node config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	b.puts.Add(1)
	b.mu.Lock()
	down := b.down[node]
	b.mu.Unlock()
	if down {
		return nil, fmt.Errorf("node %d is not answering", node)
	}

	buf, err := io.ReadAll(io.LimitReader(body, req.Size))
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.prepared[nodeAddr{node, req.Key, req.Index}] = fakeShard{data: buf, epoch: req.Epoch}

	return &blob.PutResponse{Size: int64(len(buf)), Epoch: req.Epoch}, nil
}

func (b *nodeBlob) Commit(_ context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down[node] {
		return false, fmt.Errorf("node %d is not answering", node)
	}
	addr := nodeAddr{node, req.Key, req.Index}
	shard, ok := b.prepared[addr]
	if !ok || shard.epoch != req.Epoch {
		return false, blob.ErrNotPrepared
	}
	delete(b.prepared, addr)
	b.committed[addr] = shard

	return false, nil
}

func (b *nodeBlob) Abort(_ context.Context, node config.NodeID, req blob.CommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.prepared, nodeAddr{node, req.Key, req.Index})

	return nil
}

func (b *nodeBlob) Release(_ context.Context, _ config.NodeID, _ blob.ReleaseRequest) error {
	return nil
}

func (b *nodeBlob) Get(_ context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down[node] {
		return nil, fmt.Errorf("node %d is not answering", node)
	}
	shard, ok := b.committed[nodeAddr{node, req.Key, req.Index}]
	if !ok {
		return nil, fmt.Errorf("shard %x/%d: %w", req.Key[:4], req.Index, blob.ErrNotFound)
	}
	if req.Epoch != 0 && shard.epoch != req.Epoch {
		return nil, fmt.Errorf("shard %x/%d: %w", req.Key[:4], req.Index, blob.ErrEpochMismatch)
	}
	data := shard.data
	if req.RangeStart >= 0 && req.RangeEnd >= 0 {
		data = data[req.RangeStart : req.RangeEnd+1]
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *nodeBlob) Delete(_ context.Context, node config.NodeID, req blob.DeleteRequest) (*blob.DeleteResponse, error) {
	b.deletes.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	addr := nodeAddr{node, req.Key, req.Index}
	_, held := b.committed[addr]
	delete(b.committed, addr)
	delete(b.prepared, addr)

	return &blob.DeleteResponse{Deleted: held}, nil
}

func (b *nodeBlob) held(node config.NodeID, key [32]byte, index uint32) (fakeShard, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	shard, ok := b.committed[nodeAddr{node, key, index}]

	return shard, ok
}

var _ BlobClient = (*nodeBlob)(nil)

// objectGet builds a GET carrying the resolved resource the router would have
// attached, so the handler is exercised the way the routes reach it.
func objectGet(key string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/"+testBucket+"/"+key, nil)

	return r.WithContext(WithObject(r.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: key,
	}))
}

// handoffFixture is a write path over a cluster with one node to spare, so the
// ring has somewhere to put a shard its owner refuses.
type handoffFixture struct {
	writeFixture

	bc    *nodeBlob
	nodes []config.NodeID
}

func newHandoffFixture(data, parity, spare int, down ...config.NodeID) handoffFixture {
	nodes := make([]config.NodeID, data+parity+spare)
	for i := range nodes {
		nodes[i] = config.NodeID(i + 1)
	}
	bc := newNodeBlob(down...)

	return handoffFixture{
		writeFixture: writeFixture{
			mc:   newFakeMeta(),
			ring: placement.NewRing(nodes),
			cfg: Config{
				Region: "ap-southeast-2", DataShards: data, ParityShards: parity,
				DegradedWrites: true, HintedHandoff: true,
				Epochs: mustEpochs(1),
			},
		},
		bc:    bc,
		nodes: nodes,
	}
}

// write mirrors the handler sequence: place, prepare, publish the record, commit.
func (f handoffFixture) write(ctx context.Context, objectHash [32]byte, body []byte) (ObjectToShardNodes, writeResult, error) {
	place, err := placeShards(f.ring, f.cfg, objectHash, int64(len(body)))
	if err != nil {
		return place, writeResult{}, err
	}
	written, err := writeObject(ctx, f.bc, f.cfg, f.ring, bytes.NewReader(body), int64(len(body)), objectHash, place)
	if err != nil {
		return place, written, err
	}
	commitShards(ctx, f.bc, objectHash, place, written)

	return place, written, nil
}

// The point of handoff: a node down costs the stripe nothing. The write is
// acknowledged at full width because every shard is somewhere, not because the
// missing one was excused.
func TestHandoffPlacesARefusedShardOnTheSpareNode(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 4)
	require.NoError(t, err)
	owner, holder := owners[1], owners[3]
	f.bc.mu.Lock()
	f.bc.down[owner] = true
	f.bc.mu.Unlock()

	want := randomBytes(t, 1<<16)
	place, written, err := f.write(context.Background(), objectHash, want)

	require.NoError(t, err)
	assert.False(t, written.degraded(), "every shard landed, so nothing is missing")
	assert.Empty(t, written.missing)
	assert.Equal(t, []int{1}, written.handoff)
	assert.Equal(t, f.cfg.TotalShards(), written.landedCount())

	shard, ok := f.bc.held(holder, objectHash, 1)
	require.True(t, ok, "the refused shard has to be on the spare node")
	assert.Equal(t, place.WriteEpoch, shard.epoch, "and published under the record's epoch")

	_, ok = f.bc.held(owner, objectHash, 1)
	assert.False(t, ok, "the owner was down and must hold nothing")
}

// A handed-off shard has to be readable, or the stripe is complete only on
// paper: the bytes exist but the gate cannot reach them, and one more loss
// takes the object.
func TestReadServesAHandedOffShardWithoutReconstructing(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 4)
	require.NoError(t, err)
	f.bc.mu.Lock()
	f.bc.down[owners[0]] = true
	f.bc.mu.Unlock()

	want := randomBytes(t, 1<<16)
	place, written, err := f.write(context.Background(), objectHash, want)
	require.NoError(t, err)
	require.Equal(t, []int{0}, written.handoff)

	// The owner is back and simply does not hold the shard, which is the state
	// a reader meets after the write.
	f.bc.mu.Lock()
	f.bc.down[owners[0]] = false
	f.bc.mu.Unlock()

	ctx := context.Background()
	got, degraded, err := readObject(ctx, f.bc, f.cfg, "b", "k", place, place.Size,
		handoffNode(f.ring, f.cfg, objectHash))

	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Zero(t, degraded, "the shard was fetched from its holder, not rebuilt from parity")
}

// Without the fallback the same read still returns the object, because parity
// covers it. This is what the fallback saves, and it is why the assertion above
// is about the reconstruction count rather than the bytes.
func TestWithoutTheFallbackTheSameReadReconstructs(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 4)
	require.NoError(t, err)
	f.bc.mu.Lock()
	f.bc.down[owners[0]] = true
	f.bc.mu.Unlock()

	want := randomBytes(t, 1<<16)
	place, _, err := f.write(context.Background(), objectHash, want)
	require.NoError(t, err)

	f.bc.mu.Lock()
	f.bc.down[owners[0]] = false
	f.bc.mu.Unlock()

	got, degraded, err := readObject(context.Background(), f.bc, f.cfg, "b", "k", place, place.Size, 0)

	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 1, degraded)
}

// Off is the default, and off means a refused shard is simply missing. A
// cluster that has not asked for handoff must not find shards on nodes its
// records never name.
func TestHandoffIsOffByDefault(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	f.cfg.HintedHandoff = false
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 4)
	require.NoError(t, err)
	f.bc.mu.Lock()
	f.bc.down[owners[2]] = true
	f.bc.mu.Unlock()

	_, written, err := f.write(context.Background(), objectHash, randomBytes(t, 1<<15))

	require.NoError(t, err)
	assert.Empty(t, written.handoff)
	assert.Equal(t, []config.NodeID{owners[2]}, written.missing)

	_, ok := f.bc.held(owners[3], objectHash, 2)
	assert.False(t, ok, "the spare node must not have been written to")
}

// A cluster with no node to spare has nowhere to hand off to, and must fall
// back to the degraded write rather than putting a second shard on a node that
// already holds one of this object's.
func TestNoSpareNodeLeavesTheShardMissing(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 0)
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 3)
	require.NoError(t, err)
	f.bc.mu.Lock()
	f.bc.down[owners[1]] = true
	f.bc.mu.Unlock()

	require.Zero(t, handoffNode(f.ring, f.cfg, objectHash))

	_, written, err := f.write(context.Background(), objectHash, randomBytes(t, 1<<15))

	require.NoError(t, err)
	assert.Empty(t, written.handoff)
	assert.Equal(t, []config.NodeID{owners[1]}, written.missing)
	assert.True(t, written.degraded())
}

// The spare node refusing too is the case handoff cannot rescue, and it has to
// read as the degraded write it is rather than as a full-width one.
func TestASpareNodeThatRefusesLeavesTheShardMissing(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 4)
	require.NoError(t, err)
	f.bc.mu.Lock()
	f.bc.down[owners[1]] = true
	f.bc.down[owners[3]] = true
	f.bc.mu.Unlock()

	_, written, err := f.write(context.Background(), objectHash, randomBytes(t, 1<<15))

	require.NoError(t, err)
	assert.Empty(t, written.handoff)
	assert.Equal(t, []config.NodeID{owners[1]}, written.missing)
	assert.True(t, written.degraded())
}

// A commit follows the shard rather than the record. Committing at the owner
// would be told the shard was never prepared, and the holder's copy would stay
// invisible: durable, unreferenced and unreadable.
func TestCommitFollowsTheShardToItsHolder(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(3, 2, 1)
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 6)
	require.NoError(t, err)
	f.bc.mu.Lock()
	f.bc.down[owners[4]] = true
	f.bc.mu.Unlock()

	place, written, err := f.write(context.Background(), objectHash, randomBytes(t, 1<<16))
	require.NoError(t, err)
	require.Equal(t, []int{4}, written.handoff)

	shard, ok := f.bc.held(owners[5], objectHash, 4)
	require.True(t, ok, "the handed-off shard was prepared but never published")
	assert.Equal(t, place.WriteEpoch, shard.epoch)

	f.bc.mu.Lock()
	defer f.bc.mu.Unlock()
	assert.Empty(t, f.bc.prepared, "nothing may be left unpublished")
}

// The header says the stripe is complete but not yet where the record says. It
// is not an error and must not read as one, and it is not a degraded write
// either: those are different outstanding states with different consequences.
func TestPutObjectReportsHandoffInAHeader(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	owners, err := f.ring.Nodes(model.ObjectHash(testBucket, "k"), 4)
	require.NoError(t, err)
	f.bc.mu.Lock()
	f.bc.down[owners[1]] = true
	f.bc.mu.Unlock()

	w := httptest.NewRecorder()
	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, objectPut("k", randomBytes(t, 1<<15)))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "1", w.Header().Get(handoffHeader))
	assert.Empty(t, w.Header().Get(degradedWriteHeader), "nothing is missing, so nothing is degraded")
}

// A write that placed every shard at home says nothing, so the header means
// what it says.
func TestPutObjectOmitsTheHandoffHeaderWhenNothingMoved(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	w := httptest.NewRecorder()

	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, objectPut("k", randomBytes(t, 1<<15)))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get(handoffHeader))
}

// The whole object has to come back byte for byte at every width, with any one
// owner down, through the handlers rather than through the write path alone.
func TestHandoffRoundTripsThroughTheHandlers(t *testing.T) {
	t.Parallel()

	for _, rs := range []struct{ data, parity int }{{2, 1}, {3, 2}} {
		for down := range rs.data + rs.parity {
			t.Run(fmt.Sprintf("rs-%d-%d/owner-%d", rs.data, rs.parity, down), func(t *testing.T) {
				t.Parallel()

				f := newHandoffFixture(rs.data, rs.parity, 1)
				owners, err := f.ring.Nodes(model.ObjectHash(testBucket, "k"), rs.data+rs.parity)
				require.NoError(t, err)
				f.bc.mu.Lock()
				f.bc.down[owners[down]] = true
				f.bc.mu.Unlock()

				want := randomBytes(t, 1<<16)
				w := httptest.NewRecorder()
				PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
					ServeHTTP(w, objectPut("k", want))
				require.Equal(t, http.StatusOK, w.Code)
				require.Equal(t, "1", w.Header().Get(handoffHeader))

				f.bc.mu.Lock()
				f.bc.down[owners[down]] = false
				f.bc.mu.Unlock()

				got := httptest.NewRecorder()
				GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
					ServeHTTP(got, objectGet("k"))

				require.Equal(t, http.StatusOK, got.Code)
				assert.Equal(t, want, got.Body.Bytes())
			})
		}
	}
}
