package handlers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// OpenHeld reaches the real nodes behind the fault wrapper, so a conditional
// write through it holds its shards.
func (f *faultyBlob) OpenHeld(ctx context.Context, node config.NodeID, req blob.PutRequest) (*blob.HeldPut, error) {
	return f.BlobClient.(blobHeldOpener).OpenHeld(ctx, node, req)
}

// txnFunc is the transaction a hook forwards to the store.
type txnFunc func() (meta.TxnOutcome, []meta.TxnValue, error)

// hookMeta is the store a conditional writer sees. A hook runs at each of its
// transactions, numbered from one, and decides when and whether it applies.
type hookMeta struct {
	*fakeMeta

	log *eventLog

	mu   sync.Mutex
	txns int
	hook func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error)

	// onPut, when set, sees the key of every row the writer puts.
	onPut func(key string)
}

func (m *hookMeta) setHook(hook func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hook = hook
}

func (m *hookMeta) txnCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.txns
}

func (m *hookMeta) Txn(ctx context.Context, checks []meta.TxnCheck, writes []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	m.mu.Lock()
	m.txns++
	n, hook := m.txns, m.hook
	m.mu.Unlock()
	next := func() (meta.TxnOutcome, []meta.TxnValue, error) { return m.fakeMeta.Txn(ctx, checks, writes) }
	if hook == nil {
		return next()
	}
	return hook(n, next)
}

func (m *hookMeta) Put(ctx context.Context, key string, value []byte) error {
	if strings.HasPrefix(key, IntentPrefix) {
		m.log.add("intent")
	}
	err := m.fakeMeta.Put(ctx, key, value)
	if m.onPut != nil {
		m.onPut(key)
	}
	return err
}

// eventLog records the order of the writer's steps.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = nil
}

func (l *eventLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// hookBlob is the blob tier a conditional writer sees. It can refuse a held
// open per node, as a node from before held writes does, hold a commit at a
// barrier, and fail an abort.
type hookBlob struct {
	BlobClient

	log *eventLog

	unsupported map[config.NodeID]bool
	unreachable map[config.NodeID]bool
	notHolding  bool
	abortFails  map[config.NodeID]bool
	// commitGate, when set, holds every commit until it is closed, and
	// commitReached is closed when the first commit arrives.
	commitGate    chan struct{}
	commitReached chan struct{}
	reachedOnce   sync.Once
}

func (b *hookBlob) OpenHeldPut(ctx context.Context, node config.NodeID, req blob.PutRequest) (HeldPut, error) {
	switch {
	case b.unsupported[node]:
		return nil, fmt.Errorf("node %d: %w", node, blob.ErrHoldUnsupported)
	case b.unreachable[node]:
		return nil, errInjected
	}
	b.log.add(fmt.Sprintf("open-held %d/%d", node, req.Index))
	hp, err := heldClientOf(b.BlobClient).OpenHeldPut(ctx, node, req)
	if err != nil {
		return nil, err
	}
	return &hookHeld{HeldPut: hp, b: b}, nil
}

type hookHeld struct {
	HeldPut

	b *hookBlob
}

func (h *hookHeld) Send(ctx context.Context, body io.Reader) (*blob.PutResponse, error) {
	resp, err := h.HeldPut.Send(ctx, body)
	if err == nil && h.b.notHolding {
		resp.Holding = false
	}
	return resp, err
}

func (b *hookBlob) Put(ctx context.Context, node config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	b.log.add(fmt.Sprintf("put %d/%d", node, req.Index))
	return b.BlobClient.Put(ctx, node, req, body)
}

func (b *hookBlob) Commit(ctx context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	if b.commitGate != nil {
		b.reachedOnce.Do(func() { close(b.commitReached) })
		<-b.commitGate
	}
	return b.BlobClient.Commit(ctx, node, req)
}

func (b *hookBlob) Abort(ctx context.Context, node config.NodeID, req blob.CommitRequest) error {
	if b.abortFails[node] {
		return errInjected
	}
	return b.BlobClient.Abort(ctx, node, req)
}

// condHarness runs conditional writes through the hooks, over the same real
// nodes and store that plain writes, reads and deletes use directly.
type condHarness struct {
	*realStoreGate

	t   *testing.T
	hm  *hookMeta
	hb  *hookBlob
	log *eventLog
}

func newCondHarness(t *testing.T, p rsProfile) *condHarness {
	t.Helper()
	g := newRealStoreGate(t, p)
	log := &eventLog{}
	hm := &hookMeta{fakeMeta: g.mc, log: log}
	return &condHarness{
		realStoreGate: g, t: t, log: log, hm: hm,
		hb: &hookBlob{BlobClient: g.bc, log: log},
	}
}

// owners are the nodes that hold a key's shards, by index.
func (h *condHarness) owners(hash [32]byte) []config.NodeID {
	h.t.Helper()
	nodes, err := h.ring.Nodes(hash, h.cfg.TotalShards())
	require.NoError(h.t, err)
	return nodes
}

func (h *condHarness) condPut(key string, body []byte, header http.Header) *httptest.ResponseRecorder {
	return h.serve(PutObject(h.hm, h.hb, h.ring, h.cache, h.cfg), http.MethodPut, "/bkt/"+key, key, body, header)
}

func (h *condHarness) del(key string) {
	h.t.Helper()
	rr := h.serve(DeleteObject(h.mc, h.bc, h.cache, h.cfg), http.MethodDelete, "/bkt/"+key, key, nil, nil)
	require.Equal(h.t, http.StatusNoContent, rr.Code, "delete: %s", rr.Body.String())
}

func (h *condHarness) mustPut(key string, body []byte) string {
	h.t.Helper()
	rr := h.put(key, body)
	require.Equal(h.t, http.StatusOK, rr.Code, "put: %s", rr.Body.String())
	return rr.Header().Get("ETag")
}

func (h *condHarness) requireBody(key string, want []byte) {
	h.t.Helper()
	rr := h.get(key)
	require.Equal(h.t, http.StatusOK, rr.Code, "get: %s", rr.Body.String())
	require.Equal(h.t, want, rr.Body.Bytes())
}

func (h *condHarness) enableVersioning() {
	h.t.Helper()
	require.NoError(h.t, setBucketVersioning(context.Background(), h.mc, h.cache, "bkt", VersioningEnabled))
}

// intents are the intent rows the store holds, by stored key.
func (h *condHarness) intents() map[string][]byte {
	h.t.Helper()
	items, err := h.mc.Scan(context.Background(), IntentPrefix, 0)
	require.NoError(h.t, err)
	out := map[string][]byte{}
	for _, it := range items {
		out[it.Key] = it.Value
	}
	return out
}

// onlyIntent is the one intent row a crashed or paused writer left.
func (h *condHarness) onlyIntent() (IntentID, Intent) {
	h.t.Helper()
	rows := h.intents()
	require.Len(h.t, rows, 1)
	for k, v := range rows {
		id, ok := ParseIntentKey(k)
		require.True(h.t, ok)
		in, err := DecodeIntent(v)
		require.NoError(h.t, err)
		return id, in
	}
	panic("unreachable")
}

// requireNothingPrepared fails if any candidate of a write still holds a
// prepared row, or any row, at the write's epoch.
func (h *condHarness) requireNothingPrepared(id IntentID, in Intent) {
	h.t.Helper()
	for _, p := range in.candidates() {
		_, err := h.bc.BlobClient.Commit(context.Background(), p.node, commitRequest(id, p.index))
		require.ErrorIs(h.t, err, blob.ErrNotPrepared, "node %d index %d still holds epoch %016x", p.node, p.index, id.Epoch)
	}
}

// requireLiveAt fails unless every owner of a write serves its epoch.
func (h *condHarness) requireLiveAt(id IntentID, in Intent) {
	h.t.Helper()
	for i, n := range in.Nodes {
		st, err := h.bc.BlobClient.(*blob.Client).Stat(context.Background(), n, blob.StatRequest{Key: id.Hash, Index: uint32(i)})
		require.NoError(h.t, err)
		require.Equal(h.t, id.Epoch, st.Epoch, "node %d index %d", n, i)
	}
}

// placementEpoch is the epoch the record at an unversioned key names.
func (h *condHarness) placementEpoch(key string) uint64 {
	h.t.Helper()
	hash := model.ObjectHash("bkt", key)
	row, err := h.mc.Get(context.Background(), TableKey(model.TableObjects, string(hash[:])))
	require.NoError(h.t, err)
	place, err := DecodePlacement(row)
	require.NoError(h.t, err)
	return place.WriteEpoch
}

// recordIntents keeps every intent a writer wrote, in order.
func (h *condHarness) recordIntents() func() []IntentID {
	var mu sync.Mutex
	var ids []IntentID
	h.hm.onPut = func(key string) {
		if id, ok := ParseIntentKey(key); ok {
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
		}
	}
	return func() []IntentID {
		mu.Lock()
		defer mu.Unlock()
		return append([]IntentID(nil), ids...)
	}
}

var (
	ifNoneMatchAll = http.Header{"If-None-Match": {"*"}}
	body1, body2   = []byte(strings.Repeat("first body ", 300)), []byte(strings.Repeat("second body ", 300))
)

func ifMatch(etag string) http.Header { return http.Header{"If-Match": {etag}} }

// Two If-None-Match: * writes of one absent key, both past their early check
// and both at the transaction: exactly one answers 200 and the other 412,
// and the loser's shards are aborted.
// Verifies: Q-68.
func TestConcurrentCreatesOfOneKeyOneWins(t *testing.T) {
	for _, p := range rsProfiles {
		t.Run(p.String(), func(t *testing.T) {
			h := newCondHarness(t, p)
			written := h.recordIntents()
			arrived, release := make(chan struct{}, 2), make(chan struct{})
			h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
				if n <= 2 {
					arrived <- struct{}{}
					<-release
				}
				return next()
			})

			codes := make(chan *httptest.ResponseRecorder, 2)
			for _, body := range [][]byte{body1, body2} {
				go func() { codes <- h.condPut("lock", body, ifNoneMatchAll) }()
			}
			<-arrived
			<-arrived
			close(release)
			a, b := <-codes, <-codes

			got := []int{a.Code, b.Code}
			assert.ElementsMatch(t, []int{http.StatusOK, http.StatusPreconditionFailed}, got,
				"bodies: %s / %s", a.Body.String(), b.Body.String())
			winner := a
			if b.Code == http.StatusOK {
				winner = b
			}
			rr := h.get("lock")
			require.Equal(t, http.StatusOK, rr.Code)
			assert.Equal(t, winner.Header().Get("ETag"), rr.Header().Get("ETag"))
			assert.Empty(t, h.intents())
			winnerEpoch := h.placementEpoch("lock")
			for _, id := range written() {
				if id.Epoch != winnerEpoch {
					h.requireNothingPrepared(id, Intent{Nodes: h.owners(id.Hash), Handoff: handoffNode(h.ring, h.cfg, id.Hash)})
				}
			}
		})
	}
}

// A write whose early check passed, and whose transaction runs after another
// write changed the key, answers 412: for a stale If-Match and for
// If-None-Match: * alike. The other write stays current.
// Verifies: Q-68.
func TestAStrandedWriteFailsItsCondition(t *testing.T) {
	for name, header := range map[string]func(etag string) http.Header{
		"If-Match":        ifMatch,
		"If-None-Match *": func(string) http.Header { return ifNoneMatchAll },
	} {
		t.Run(name, func(t *testing.T) {
			h := newCondHarness(t, rsProfiles[0])
			if name == "If-None-Match *" {
				h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
					if n == 1 {
						h.mustPut("lock", body2)
					}
					return next()
				})
				requirePreconditionFailedRR(t, h.condPut("lock", body1, header("")))
			} else {
				etag := h.mustPut("lock", body1)
				h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
					if n == 1 {
						h.mustPut("lock", body2)
					}
					return next()
				})
				requirePreconditionFailedRR(t, h.condPut("lock", []byte("third"), header(etag)))
			}
			h.requireBody("lock", body2)
			assert.Equal(t, 1, h.hm.txnCount(), "a failed condition sent another transaction")
			assert.Empty(t, h.intents())
		})
	}
}

// If-Match on a key deleted after the early check answers 404; on a key
// absent from the start it answers 404 before any shard moves.
// Verifies: Q-68.
func TestAStrandedIfMatchOnADeletedKeyIsNoSuchKey(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	etag := h.mustPut("lock", body1)
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n == 1 {
			h.del("lock")
		}
		return next()
	})

	rr := h.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusNotFound, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, http.StatusNotFound, h.get("lock").Code)
	assert.Empty(t, h.intents())

	h.log.reset()
	rr = h.condPut("absent", body2, ifMatch(etag))
	require.Equal(t, http.StatusNotFound, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, 1, h.hm.txnCount(), "the absent key reached a transaction")
	assert.NotContains(t, h.log.all(), "intent", "the absent key wrote an intent")
}

// An If-Match write that keeps meeting changes leaving the same ETag tries
// three transactions, then answers 409. The other writes stay current.
// Verifies: Q-68.
func TestIfMatchMeetingSameETagChangesConflicts(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	etag := h.mustPut("lock", body1)
	h.hm.setHook(func(_ int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		require.Equal(t, etag, h.mustPut("lock", body1))
		return next()
	})

	rr := h.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusConflict, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>ConditionalRequestConflict</Code>")
	assert.Equal(t, maxConditionalMismatches, h.hm.txnCount())
	h.requireBody("lock", body1)
	assert.Empty(t, h.intents())
}

// An If-Match write that meets one same-ETag change retries with the new
// expectations and is written.
// Verifies: Q-68.
func TestIfMatchRetriesAfterOneSameETagChange(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	etag := h.mustPut("lock", body1)
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n == 1 {
			h.mustPut("lock", body1)
		}
		return next()
	})

	rr := h.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, 2, h.hm.txnCount())
	h.requireBody("lock", body2)
	assert.Empty(t, h.intents())
}

// An If-None-Match: * write whose transaction meets a create, and whose
// re-read then finds the key deleted, answers 409.
// Verifies: Q-68.
func TestACreateThatMeetsACreateAndThenADeleteConflicts(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n != 1 {
			return next()
		}
		h.mustPut("lock", body2)
		outcome, current, err := next()
		require.Equal(t, meta.TxnMismatch, outcome)
		h.del("lock")
		return outcome, current, err
	})

	rr := h.condPut("lock", body1, ifNoneMatchAll)

	require.Equal(t, http.StatusConflict, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>ConditionalRequestConflict</Code>")
	assert.Equal(t, http.StatusNotFound, h.get("lock").Code)
	assert.Empty(t, h.intents())
}

// An If-None-Match: * write during which another client creates and deletes
// the key, both before the commit, finds the key absent at the commit and
// answers 200, and its body reads back.
// Verifies: Q-68.
func TestACreateAcrossACreateAndDeleteIsWritten(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n == 1 {
			h.mustPut("lock", body2)
			h.del("lock")
		}
		return next()
	})

	rr := h.condPut("lock", body1, ifNoneMatchAll)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	h.requireBody("lock", body1)
	assert.Empty(t, h.intents())
}

// A transaction whose reply is lost after it applied is settled by the
// fence, which finds the intent committed: the write answers 200 through the
// normal success path, with its ETag and version, and reads back.
// Verifies: ADR-0003:S7.
func TestS7_ALostReplyAfterTheTransactionAppliedAnswersTheWrite(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	h.enableVersioning()
	h.mustPut("lock", body1)
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n != 1 {
			return next()
		}
		outcome, _, _ := next()
		require.Equal(t, meta.TxnApplied, outcome)
		return meta.TxnUnknown, nil, meta.ErrTxnUnknown
	})

	rr := h.condPut("lock", body2, ifNoneMatchAllExceptCurrent(h))

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, md5ETag(body2), rr.Header().Get("ETag"))
	assert.True(t, wellFormedVersionID(rr.Header().Get("X-Amz-Version-Id")))
	got := h.get("lock")
	require.Equal(t, http.StatusOK, got.Code)
	assert.Equal(t, body2, got.Body.Bytes())
	assert.Equal(t, rr.Header().Get("X-Amz-Version-Id"), got.Header().Get("X-Amz-Version-Id"))
	assert.Equal(t, 2, h.hm.txnCount(), "the write was resent instead of fenced")
	assert.Empty(t, h.intents())
}

// ifNoneMatchAllExceptCurrent is If-Match on the current object.
func ifNoneMatchAllExceptCurrent(h *condHarness) http.Header {
	rr := h.get("lock")
	require.Equal(h.t, http.StatusOK, rr.Code)
	return ifMatch(rr.Header().Get("ETag"))
}

// A transaction whose reply is lost before it applied is fenced: the write
// answers 500, its shards are aborted, and the previous object is current.
// Verifies: ADR-0003:S7.
func TestS7_ALostReplyBeforeTheTransactionAppliedKeepsThePreviousObject(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	etag := h.mustPut("lock", body1)
	written := h.recordIntents()
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n == 1 {
			return meta.TxnUnknown, nil, meta.ErrTxnUnknown
		}
		return next()
	})

	rr := h.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusInternalServerError, rr.Code, "body: %s", rr.Body.String())
	h.requireBody("lock", body1)
	assert.Empty(t, h.intents())
	ids := written()
	require.Len(t, ids, 1)
	h.requireNothingPrepared(ids[0], Intent{Nodes: h.owners(ids[0].Hash), Handoff: handoffNode(h.ring, h.cfg, ids[0].Hash)})
}

// Two writes of one ETag: the first's transaction applies and its reply is
// lost, and the second writes over it before the first is resolved. The
// first's late shard commit must not demote the second, which stays current.
// Verifies: ADR-0003:S7.
func TestS7_AnEqualETagWriteStaysCurrentAfterAnUnknownOutcomeResolves(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	etag := h.mustPut("lock", body1)
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n != 1 {
			return next()
		}
		outcome, _, _ := next()
		require.Equal(t, meta.TxnApplied, outcome)
		h.mustPut("lock", body2)
		return meta.TxnUnknown, nil, meta.ErrTxnUnknown
	})

	rr := h.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	second := h.placementEpoch("lock")
	hash := model.ObjectHash("bkt", "lock")
	h.requireLiveAt(IntentID{Hash: hash, Epoch: second}, Intent{Nodes: h.owners(hash)})
	h.requireBody("lock", body2)
	assert.Empty(t, h.intents())
}

// A refused transaction answers 503 SlowDown and leaves the previous
// object. A candidate that cannot confirm its abort keeps the intent for the
// reaper.
// Verifies: ADR-0003:S7.
func TestS7_ARefusedTransactionIsSlowDownAndAnUnconfirmedAbortKeepsTheIntent(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	etag := h.mustPut("lock", body1)
	hash := model.ObjectHash("bkt", "lock")
	owners := h.owners(hash)
	h.hb.abortFails = map[config.NodeID]bool{owners[0]: true}
	h.hm.setHook(func(int, txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		return meta.TxnRefused, nil, meta.ErrTxnRefused
	})

	rr := h.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusServiceUnavailable, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<Code>SlowDown</Code>")
	h.requireBody("lock", body1)
	_, in := h.onlyIntent()
	assert.Equal(t, IntentPending, in.State)
}

// A write fenced by the reaper while it streamed cannot publish: its own
// transaction fails the pending check, it answers 500, and the previous
// object stays readable.
// Verifies: ADR-0003:S7.
func TestS7_AFencedWriterFailsAndKeepsThePreviousObject(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	etag := h.mustPut("lock", body1)
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n == 1 {
			for key, row := range h.intents() {
				id, _ := ParseIntentKey(key)
				result, _, err := FenceIntent(context.Background(), h.mc, id, row)
				require.NoError(t, err)
				require.Equal(t, FenceApplied, result)
			}
		}
		return next()
	})

	rr := h.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusInternalServerError, rr.Code, "body: %s", rr.Body.String())
	h.requireBody("lock", body1)
	assert.Empty(t, h.intents())
}

// A record that becomes visible is readable before any shard commit: a read
// paused right after the transaction commits the held shards it names, for a
// first write and for a new version.
// Verifies: ADR-0003:S7.
func TestS7_AWriteIsReadableBetweenItsTransactionAndItsShardCommit(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("versioned=%t", versioned), func(t *testing.T) {
			h := newCondHarness(t, rsProfiles[0])
			header := ifNoneMatchAll
			if versioned {
				h.enableVersioning()
				header = ifMatch(h.mustPut("lock", body1))
			}
			h.hb.commitGate, h.hb.commitReached = make(chan struct{}), make(chan struct{})

			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- h.condPut("lock", body2, header) }()
			<-h.hb.commitReached
			h.requireBody("lock", body2)
			close(h.hb.commitGate)

			rr := <-done
			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
			h.requireBody("lock", body2)
			assert.Empty(t, h.intents())
		})
	}
}

// swapGateMeta holds every placement swap at a barrier until release closes.
type swapGateMeta struct {
	*fakeMeta

	reached chan struct{}
	release chan struct{}
}

func (m *swapGateMeta) Swap(ctx context.Context, key string, value []byte) ([]byte, error) {
	close(m.reached)
	<-m.release
	return m.fakeMeta.Swap(ctx, key, value)
}

// A newer plain write commits its shards and stops before its swap; an older
// conditional write then publishes its record and stops before its shard
// commit. A read of the record serves the conditional body.
// Verifies: ADR-0003:S7.
func TestS7_AnOlderConditionalWriteIsReadableUnderANewerWritesShards(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	txnReached, txnRelease := make(chan struct{}), make(chan struct{})
	h.hm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n == 1 {
			close(txnReached)
			<-txnRelease
		}
		return next()
	})
	h.hb.commitGate, h.hb.commitReached = make(chan struct{}), make(chan struct{})

	condDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { condDone <- h.condPut("lock", body2, ifNoneMatchAll) }()
	<-txnReached

	sm := &swapGateMeta{fakeMeta: h.mc, reached: make(chan struct{}), release: make(chan struct{})}
	plainDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		plainDone <- h.serve(PutObject(sm, h.bc, h.ring, h.cache, h.cfg), http.MethodPut, "/bkt/lock", "lock", body1, nil)
	}()
	<-sm.reached

	close(txnRelease)
	<-h.hb.commitReached
	cond := h.placementEpoch("lock")
	h.requireBody("lock", body2)

	close(h.hb.commitGate)
	rr := <-condDone
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	close(sm.release)
	rr = <-plainDone
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Greater(t, h.placementEpoch("lock"), cond, "the plain write must be the newer one")
	h.requireBody("lock", body1)
}

// Every owner's held stream is registered before the intent is written, and
// no stream opens after it except an unheld handoff stream.
// Verifies: ADR-0003:S7.
func TestS7_HeldStreamsOpenBeforeTheIntentAndHandoffIsUnheld(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	hash := model.ObjectHash("bkt", "lock")
	owners := h.owners(hash)
	handoff := handoffNode(h.ring, h.cfg, hash)
	require.NotZero(t, handoff)
	h.hb.unreachable = map[config.NodeID]bool{owners[1]: true}

	rr := h.condPut("lock", body1, ifNoneMatchAll)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "1", rr.Header().Get(handoffHeader))
	events := h.log.all()
	intentAt := -1
	for i, e := range events {
		if e == "intent" {
			intentAt = i
		}
	}
	require.GreaterOrEqual(t, intentAt, 0, "no intent written: %v", events)
	for i, e := range events {
		switch {
		case strings.HasPrefix(e, "open-held"):
			assert.Less(t, i, intentAt, "held stream %q opened after the intent", e)
		case strings.HasPrefix(e, "put"):
			assert.Greater(t, i, intentAt, "unheld stream %q opened before the intent", e)
			assert.Equal(t, fmt.Sprintf("put %d/1", handoff), e)
		}
	}
	h.requireBody("lock", body1)
}

// Shards on nodes without held writes do not count toward the floor: when
// they are what the write lacks it answers 501, writes nothing and leaves no
// intent. When the held shards reach the floor the write succeeds.
// Verifies: Q-68.
func TestAShortfallOfHeldShardsIsNotImplemented(t *testing.T) {
	h := newCondHarness(t, rsProfiles[0])
	hash := model.ObjectHash("bkt", "lock")
	owners := h.owners(hash)

	h.hb.unsupported = map[config.NodeID]bool{owners[0]: true, owners[1]: true}
	rr := h.condPut("lock", body1, ifNoneMatchAll)
	require.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
	assert.NotContains(t, h.log.all(), "intent")
	assert.Equal(t, http.StatusNotFound, h.get("lock").Code)

	h.hb.unsupported = nil
	h.hb.notHolding = true
	written := h.recordIntents()
	rr = h.condPut("lock", body1, ifNoneMatchAll)
	require.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, http.StatusNotFound, h.get("lock").Code)
	assert.Empty(t, h.intents())
	ids := written()
	require.Len(t, ids, 1)
	h.requireNothingPrepared(ids[0], Intent{Nodes: owners, Handoff: handoffNode(h.ring, h.cfg, hash)})

	h.hb.notHolding = false
	h.hb.unsupported = map[config.NodeID]bool{owners[2]: true}
	rr = h.condPut("lock", body1, ifNoneMatchAll)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	h.requireBody("lock", body1)
}

func requirePreconditionFailedRR(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusPreconditionFailed, rr.Code, "body: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), "<Code>PreconditionFailed</Code>")
}
