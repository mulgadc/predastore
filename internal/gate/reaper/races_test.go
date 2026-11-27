package reaper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deleteHookBlob is the blob tier as a deleter sees it. It records the epoch
// of every scoped shard delete, runs pause once ahead of the first, and fails
// the deletes sent to a node in fail.
type deleteHookBlob struct {
	*blob.Client

	fail  map[config.NodeID]bool
	once  sync.Once
	pause func()

	mu     sync.Mutex
	epochs []uint64
}

func (b *deleteHookBlob) DeleteEpoch(ctx context.Context, node config.NodeID, req blob.CommitRequest) (*blob.DeleteResponse, error) {
	b.mu.Lock()
	b.epochs = append(b.epochs, req.Epoch)
	b.mu.Unlock()
	if b.pause != nil {
		b.once.Do(b.pause)
	}
	if b.fail[node] {
		return nil, errNodeDown
	}
	return b.Client.DeleteEpoch(ctx, node, req)
}

func (b *deleteHookBlob) deletedEpochs() []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]uint64(nil), b.epochs...)
}

var (
	_ BlobClient          = (*deleteHookBlob)(nil)
	_ handlers.BlobClient = (*deleteHookBlob)(nil)
)

// crashOnDeleteMeta is the store as a deleter sees it: the delete of one row
// fails, as when the gate stops just before it.
type crashOnDeleteMeta struct {
	*gateMeta

	crashKey string
}

func (m *crashOnDeleteMeta) Delete(ctx context.Context, key string) error {
	if key == m.crashKey {
		return errCrashed
	}
	return m.gateMeta.Delete(ctx, key)
}

var _ handlers.MetaClient = (*crashOnDeleteMeta)(nil)

// pauseBeforeIntentMeta is the writer's store with a hook that runs once,
// just before the writer puts its intent row.
type pauseBeforeIntentMeta struct {
	*writerMeta

	once   sync.Once
	before func()
}

func (m *pauseBeforeIntentMeta) Put(ctx context.Context, key string, value []byte) error {
	if strings.HasPrefix(key, handlers.IntentPrefix) {
		m.once.Do(m.before)
	}
	return m.writerMeta.Put(ctx, key, value)
}

var _ handlers.MetaClient = (*pauseBeforeIntentMeta)(nil)

var ifNoneMatchAll = http.Header{"If-None-Match": {"*"}}

func (g *intentGate) placementKey(key string) string {
	hash := model.ObjectHash("bkt", key)
	return handlers.TableKey(model.TableObjects, string(hash[:]))
}

// placementEpoch is the epoch of the placement a key resolves through.
func (g *intentGate) placementEpoch(key string) uint64 {
	g.t.Helper()
	raw, err := g.store.Get(context.Background(), g.placementKey(key))
	require.NoError(g.t, err)
	place, err := handlers.DecodePlacement(raw)
	require.NoError(g.t, err)
	return place.WriteEpoch
}

// liveWrite is the intent identity of the write a key resolves through, with
// its owners as its candidates.
func (g *intentGate) liveWrite(key string) (handlers.IntentID, handlers.Intent) {
	g.t.Helper()
	hash := model.ObjectHash("bkt", key)
	id := handlers.IntentID{Hash: hash, Epoch: g.placementEpoch(key)}
	return id, handlers.Intent{Nodes: g.owners(key)}
}

func (g *intentGate) deleteObject(mc handlers.MetaClient, bc handlers.BlobClient, key string) *httptest.ResponseRecorder {
	return g.serve(handlers.DeleteObject(mc, bc, g.cache, g.cfg), http.MethodDelete, key, nil, nil)
}

// A tombstone reclaim that read no placement, then paused while a conditional
// create answered 200, must leave the new body readable when it resumes.
// Verifies: ADR-0003:S7.
func TestS7_AReclaimPausedAcrossAConditionalCreateLeavesTheNewBodyReadable(t *testing.T) {
	g := newIntentGate(t)
	g.mustPut("lock", body1)
	deleted := g.placementEpoch("lock")
	failing := &deleteHookBlob{Client: g.bc, fail: map[config.NodeID]bool{g.owners("lock")[0]: true}}
	rr := g.deleteObject(g.store, failing, "lock")
	require.Equal(t, http.StatusNoContent, rr.Code, "delete: %s", rr.Body.String())
	require.Len(t, g.store.tombstones(t), 1, "setup: the failed delete must leave a tombstone")

	var created *httptest.ResponseRecorder
	var placementBeforeCreate error
	reclaim := &deleteHookBlob{Client: g.bc, pause: func() {
		_, placementBeforeCreate = g.store.Get(context.Background(), g.placementKey("lock"))
		created = g.condPut("lock", body2, ifNoneMatchAll)
	}}
	svc, err := New(Config{Meta: g.rm, Blob: reclaim, Workers: 4, PageSize: 2, IntentMaxAge: testIntentAge, Now: g.clock.Now})
	require.NoError(t, err)

	require.NoError(t, svc.Pass(context.Background()))

	require.NotNil(t, created, "setup: the reclaim never reached its shard deletes")
	require.ErrorIs(t, placementBeforeCreate, meta.ErrNotFound, "setup: the reclaim must read no placement")
	require.Equal(t, http.StatusOK, created.Code, "create: %s", created.Body.String())
	for _, epoch := range reclaim.deletedEpochs() {
		assert.Equal(t, deleted, epoch, "a shard delete named a generation the tombstone does not owe")
	}
	assert.Empty(t, g.store.tombstones(t), "the tombstone must go once its shards are reclaimed")
	id, in := g.liveWrite("lock")
	require.NotEqual(t, deleted, id.Epoch)
	g.requireLiveAt(id, in)
	g.requireBody("lock", body2)
}

// An inline delete that read its placement, paused while a conditional write
// committed, then stopped before its placement delete, leaves the write
// readable.
// Verifies: ADR-0003:S7.
func TestS7_AnInlineDeletePausedAcrossAConditionalWriteThatStopsLeavesTheWriteReadable(t *testing.T) {
	g := newIntentGate(t)
	etag := g.mustPut("lock", body1)
	read := g.placementEpoch("lock")

	var written *httptest.ResponseRecorder
	bc := &deleteHookBlob{Client: g.bc, pause: func() { written = g.condPut("lock", body2, ifMatch(etag)) }}
	mc := &crashOnDeleteMeta{gateMeta: g.store, crashKey: g.placementKey("lock")}

	rr := g.deleteObject(mc, bc, "lock")

	require.NotNil(t, written, "setup: the delete never reached its shard deletes")
	require.Equal(t, http.StatusOK, written.Code, "write: %s", written.Body.String())
	require.Equal(t, http.StatusInternalServerError, rr.Code, "setup: the delete must stop at its placement delete")
	for _, epoch := range bc.deletedEpochs() {
		assert.Equal(t, read, epoch, "a shard delete named a generation the delete did not read")
	}
	id, in := g.liveWrite("lock")
	require.NotEqual(t, read, id.Epoch, "setup: the write must mint a later generation")
	g.requireLiveAt(id, in)
	g.requireBody("lock", body2)
}

// A writer paused after it opened its held streams and before its intent,
// then resumed after an aged intent pass, finds nothing fenced: the write
// settles, with no intent and no held shard left.
// Verifies: ADR-0003:S7, ADR-0003:S11.
func TestS7_AWriterPausedBeforeItsIntentAcrossAPassSettles(t *testing.T) {
	g := newIntentGate(t)
	etag := g.mustPut("lock", body1)
	paused := false
	wm := &pauseBeforeIntentMeta{writerMeta: g.wm, before: func() {
		paused = true
		g.age()
		g.pass()
	}}

	rr := g.serve(handlers.PutObject(wm, g.wb, g.ring, g.cache, g.cfg), http.MethodPut, "lock", body2, ifMatch(etag))

	require.True(t, paused, "setup: the writer never reached its intent")
	assert.Zero(t, g.rm.txnCount(), "the pass fenced a write that had no intent")
	assert.Zero(t, g.rb.total(), "the pass touched shards of a write that had no intent")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	in, err := handlers.DecodeIntent(g.wm.pendingRow(t))
	require.NoError(t, err)
	id := handlers.IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: g.placementEpoch("lock")}
	g.requireConverged(id, in, true)
}
