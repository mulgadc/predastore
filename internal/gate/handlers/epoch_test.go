package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staleBlob answers reads for named shard indices as a node holding an older
// generation would: up, answering, and wrong. That is the case the epoch exists
// to catch, and the one a missing node does not exercise.
type staleBlob struct {
	*fakeBlob

	stale map[int]bool
}

func (b *staleBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	if b.stale[int(req.Index)] {
		return nil, fmt.Errorf("shard %d: %w", req.Index, blob.ErrEpochMismatch)
	}
	return b.fakeBlob.Get(ctx, node, req)
}

var _ BlobClient = (*staleBlob)(nil)

// A stale shard is counted exactly as a missing one: a nil entry the
// reconstructor fills. Joining it instead would return an object spliced from
// two generations, which is the corruption this whole design exists to remove.
func TestReadObjectReconstructsAroundAStaleShard(t *testing.T) {
	t.Parallel()

	for _, rs := range []struct{ data, parity int }{{2, 1}, {3, 2}} {
		for stale := range rs.data + rs.parity {
			t.Run(fmt.Sprintf("rs-%d-%d/shard-%d", rs.data, rs.parity, stale), func(t *testing.T) {
				t.Parallel()
				f := newWriteFixture(rs.data, rs.parity)
				want := randomBytes(t, 1<<16)

				ctx := context.Background()
				objectHash := model.ObjectHash("b", "k")
				place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
				require.NoError(t, err)

				bc := &staleBlob{fakeBlob: f.bc, stale: map[int]bool{stale: true}}
				got, degraded, err := readObject(ctx, bc, f.cfg, "b", "k", place, place.Size, 0)

				require.NoError(t, err, "one stale shard is what the parity is for")
				assert.Equal(t, want, got, "the object must be the generation the record names")
				// Not an exact count: the hedge returns as soon as enough
				// shards have landed, so a parity shard can beat a healthy data
				// shard and leave that one reconstructed too.
				if stale < rs.data {
					assert.Positive(t, degraded, "a stale data shard has to be reconstructed")
				}
			})
		}
	}
}

// Beyond what the parity covers the read must fail, not return a short or
// spliced object. Stale shards must not be treated as more recoverable than
// absent ones just because the node answered.
func TestReadObjectFailsWhenTooManyShardsAreStale(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	// RS(2,1) survives one loss; two is past what the parity covers.
	bc := &staleBlob{fakeBlob: f.bc, stale: map[int]bool{0: true, 1: true}}
	_, _, err = readObject(ctx, bc, f.cfg, "b", "k", place, place.Size, 0)

	require.Error(t, err, "losing more than the parity covers must fail loudly")
}

// The write path stamps every shard with the record's epoch, so a read naming
// that record finds them all. A second placement would mint a different epoch
// and match nothing, which is why the record is derived once.
func TestWritePathStampsEveryShardWithTheRecordEpoch(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")

	place, _, err := f.write(ctx, objectHash, bytes.NewReader(randomBytes(t, 1<<16)), 1<<16)
	require.NoError(t, err)

	require.NotZero(t, place.WriteEpoch, "zero is reserved as invalid")

	f.bc.mu.Lock()
	defer f.bc.mu.Unlock()
	require.Len(t, f.bc.shards, 3)
	require.Empty(t, f.bc.prepared, "every shard must be published once the record has landed")
	for id, shard := range f.bc.shards {
		assert.Equalf(t, place.WriteEpoch, shard.epoch,
			"shard %d carries epoch %016x, not the record's", id.index, shard.epoch)
	}
}

// A retry is a separate request and mints its own epoch, so a partial first
// attempt can never be mistaken for a complete second one.
func TestEachWriteMintsItsOwnEpoch(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	body := randomBytes(t, 1<<15)

	first, _, err := f.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	second, _, err := f.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	assert.NotEqual(t, first.WriteEpoch, second.WriteEpoch,
		"two writes shared an epoch, so a torn first attempt would pass as the second")
	assert.Equal(t, first.DataShardNodes, second.DataShardNodes,
		"placement is a function of the object hash and must not move between attempts")
}

// A write that fails before its record lands must leave nothing pending on the
// nodes: the space is released now rather than waiting out the node's reaper.
// One shard refusing is what fails it here, because with degraded writes off
// the floor is the full stripe, and it leaves two shards prepared that the
// abort has to find.
func TestFailedWriteAbortsItsPreparedShards(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	f.bc.failPutOn = func(index uint32) bool { return index == 2 }

	w := httptest.NewRecorder()
	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, objectPut("k", randomBytes(t, 100)))

	require.NotEqual(t, http.StatusOK, w.Code, "the write must fail")

	f.bc.mu.Lock()
	defer f.bc.mu.Unlock()
	assert.Empty(t, f.bc.prepared, "a failed write left shards prepared on the nodes")
	assert.Empty(t, f.bc.shards, "a failed write published a shard")
	assert.Equal(t, int64(2), f.bc.abortCalls.Load(),
		"the two shards that did prepare must be aborted, and the one that never did left alone")
}

// A write that placed nothing has nothing to abort, and asking a node to
// discard a shard it never prepared would name a generation it does not hold.
func TestWriteThatPlacedNothingAbortsNothing(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	f.bc.declaring = func(int64) error { return errShardRefused }

	w := httptest.NewRecorder()
	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, objectPut("k", randomBytes(t, 100)))

	require.NotEqual(t, http.StatusOK, w.Code, "the write must fail")
	assert.Zero(t, f.bc.abortCalls.Load(), "nothing prepared, so nothing to discard")
}

var errShardRefused = fmt.Errorf("node refused the shard")

// A degraded read is reported and never refused: the response is a complete,
// correct object, and the header only says what it cost.
func TestGetObjectReportsADegradedReadInAHeader(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)
	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")

	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)
	f.publish(t, objectHash, place)

	bc := &staleBlob{fakeBlob: f.bc, stale: map[int]bool{0: true}}
	w := httptest.NewRecorder()
	GetObject(f.mc, bc, f.ring, testCache(), f.cfg).ServeHTTP(w, objectRequest(http.MethodGet, "k", ""))

	require.Equal(t, http.StatusOK, w.Code, "a degraded read is served, never refused")
	assert.Equal(t, want, w.Body.Bytes(), "the bytes must be correct regardless")
	assert.Equal(t, "1", w.Header().Get(degradedHeader))
}

// A healthy read says nothing, so the header means what it says.
func TestGetObjectOmitsTheDegradedHeaderWhenHealthy(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)
	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")

	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)
	f.publish(t, objectHash, place)

	w := httptest.NewRecorder()
	GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, objectRequest(http.MethodGet, "k", ""))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get(degradedHeader))
}

// The gate must ask for the generation the record names. Reading without an
// epoch would take whatever a node happened to hold, which is the bug.
func TestReadShardDemandsTheRecordEpoch(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")

	place, _, err := f.write(ctx, objectHash, bytes.NewReader(randomBytes(t, 1<<15)), 1<<15)
	require.NoError(t, err)

	bc := &epochRecordingBlob{fakeBlob: f.bc}
	_, _, err = readObject(ctx, bc, f.cfg, "b", "k", place, place.Size, 0)
	require.NoError(t, err)

	require.NotEmpty(t, bc.seen)
	for _, got := range bc.seen {
		assert.Equal(t, place.WriteEpoch, got, "a shard was read without naming the record's epoch")
	}
}

// epochRecordingBlob records the epoch each read asked for.
type epochRecordingBlob struct {
	*fakeBlob

	seen []uint64
}

func (b *epochRecordingBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.mu.Lock()
	b.seen = append(b.seen, req.Epoch)
	b.mu.Unlock()

	return b.fakeBlob.Get(ctx, node, req)
}

var _ BlobClient = (*epochRecordingBlob)(nil)

// objectPut builds a PUT carrying a body and the resolved resource the router
// would have attached.
func objectPut(key string, body []byte) *http.Request {
	r := httptest.NewRequest(http.MethodPut, "/"+testBucket+"/"+key, bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	return r.WithContext(WithObject(r.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: key,
	}))
}

// publish records the placement under the object hash, the way a write does,
// so a read can find the object and the epoch it names.
func (f writeFixture) publish(t *testing.T, objectHash [32]byte, place ObjectToShardNodes) {
	t.Helper()
	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(context.Background(), f.mc, model.TableObjects, string(objectHash[:]), record))
}
