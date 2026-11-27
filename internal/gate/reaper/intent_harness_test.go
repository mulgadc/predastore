package reaper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

var errCrashed = errors.New("injected crash")

// startBlobNodes runs n real blob nodes, each on its own engine store, and a
// client that reaches them over the pipe transport.
func startBlobNodes(t *testing.T, n int) (*blob.Client, []config.NodeID) {
	t.Helper()
	host := fmt.Sprintf("intent-%d-%s", pipeHostSeq.Add(1), strings.NewReplacer("/", "-", " ", "-").Replace(t.Name()))
	const clientNode, basePort = config.NodeID(99), 7600
	ids := make([]config.NodeID, n)
	nodes := make([]config.Node, 0, n+1)
	for i := range ids {
		ids[i] = config.NodeID(i + 1)
		nodes = append(nodes, config.Node{ID: ids[i], Role: config.RoleBlob, Port: basePort + i + 1})
	}
	nodes = append(nodes, config.Node{ID: clientNode, Role: config.RoleBlob, Port: basePort + int(clientNode)})

	for i, id := range ids {
		tr := transport.NewPipeTransport(host, basePort+i+1)
		t.Cleanup(func() { tr.Close() })
		ln, err := tr.Listen()
		require.NoError(t, err)
		srv, err := blob.New(blob.Config{
			NodeID: id, DataDir: t.TempDir(), AEAD: storetest.TestAEAD(), Listeners: []transport.Listener{ln},
		})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- srv.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			<-done
		})
	}

	clientTr := transport.NewPipeTransport(host, basePort+int(clientNode))
	t.Cleanup(func() { clientTr.Close() })
	res, err := rpc.NewResolver(&config.Config{Hosts: []config.Host{{ID: 1, Addr: host, Nodes: nodes}}}, clientNode, clientTr)
	require.NoError(t, err)
	pool := rpc.NewConnPool(clientNode, res)
	t.Cleanup(func() { _ = pool.Close() })
	client, err := blob.NewClient(blob.ClientConfig{
		Client: rpc.NewClient(pool), EnvelopeTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return client, ids
}

var pipeHostSeq atomic.Uint64

type txnFunc func() (meta.TxnOutcome, []meta.TxnValue, error)

// writerMeta is the store as a conditional writer sees it. A hook runs at each
// of its transactions, numbered from one. Once dead, every call fails and
// changes nothing, as after a crash.
type writerMeta struct {
	*gateMeta

	dead atomic.Bool

	mu   sync.Mutex
	txns int
	hook func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error)
	// crashAfterIntent kills the writer once its intent row is stored, and
	// crashOnIntentDelete at the delete of its intent row.
	crashAfterIntent    bool
	crashOnIntentDelete bool
	// intentRows are the intent rows the writer put, as it put them.
	intentRows [][]byte
}

func (m *writerMeta) setHook(hook func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hook = hook
}

func (m *writerMeta) Get(ctx context.Context, key string) ([]byte, error) {
	if m.dead.Load() {
		return nil, errCrashed
	}
	return m.gateMeta.Get(ctx, key)
}

func (m *writerMeta) LeaderGet(ctx context.Context, key string) ([]byte, error) {
	if m.dead.Load() {
		return nil, errCrashed
	}
	return m.gateMeta.LeaderGet(ctx, key)
}

func (m *writerMeta) Put(ctx context.Context, key string, value []byte) error {
	if m.dead.Load() {
		return errCrashed
	}
	err := m.gateMeta.Put(ctx, key, value)
	if strings.HasPrefix(key, handlers.IntentPrefix) {
		m.mu.Lock()
		m.intentRows = append(m.intentRows, append([]byte(nil), value...))
		m.mu.Unlock()
		if m.crashAfterIntent {
			m.dead.Store(true)
		}
	}
	return err
}

func (m *writerMeta) Swap(ctx context.Context, key string, value []byte) ([]byte, error) {
	if m.dead.Load() {
		return nil, errCrashed
	}
	return m.gateMeta.Swap(ctx, key, value)
}

func (m *writerMeta) Delete(ctx context.Context, key string) error {
	if m.dead.Load() {
		return errCrashed
	}
	if m.crashOnIntentDelete && strings.HasPrefix(key, handlers.IntentPrefix) {
		m.dead.Store(true)
		return errCrashed
	}
	return m.gateMeta.Delete(ctx, key)
}

func (m *writerMeta) Scan(ctx context.Context, prefix string, limit int) ([]meta.Item, error) {
	if m.dead.Load() {
		return nil, errCrashed
	}
	return m.gateMeta.Scan(ctx, prefix, limit)
}

func (m *writerMeta) Txn(ctx context.Context, checks []meta.TxnCheck, writes []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	if m.dead.Load() {
		return meta.TxnUnknown, nil, errCrashed
	}
	m.mu.Lock()
	m.txns++
	n, hook := m.txns, m.hook
	m.mu.Unlock()
	next := func() (meta.TxnOutcome, []meta.TxnValue, error) { return m.gateMeta.Txn(ctx, checks, writes) }
	if hook == nil {
		return next()
	}
	return hook(n, next)
}

// pendingRow is the first intent row the writer put.
func (m *writerMeta) pendingRow(t *testing.T) []byte {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.NotEmpty(t, m.intentRows, "setup: the writer put no intent")
	return m.intentRows[0]
}

var _ handlers.MetaClient = (*writerMeta)(nil)

// writerBlob is the blob tier as a conditional writer sees it. It shares the
// writer's crash, refuses a held open per node, and fails a commit or an
// abort per node. A commit gate, when set, holds every commit until closed.
type writerBlob struct {
	*blob.Client

	dead *atomic.Bool

	refuseHeld  map[config.NodeID]bool
	abortFails  map[config.NodeID]bool
	commitFails map[config.NodeID]bool
	commitGate  chan struct{}
}

func (b *writerBlob) OpenHeldPut(ctx context.Context, node config.NodeID, req blob.PutRequest) (handlers.HeldPut, error) {
	if b.dead.Load() || b.refuseHeld[node] {
		return nil, errCrashed
	}
	hp, err := b.OpenHeld(ctx, node, req)
	if err != nil {
		return nil, err
	}
	return &writerHeld{HeldPut: hp, dead: b.dead}, nil
}

func (b *writerBlob) Put(ctx context.Context, node config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	if b.dead.Load() {
		return nil, errCrashed
	}
	return b.Client.Put(ctx, node, req, body)
}

func (b *writerBlob) Commit(ctx context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	if b.commitGate != nil {
		<-b.commitGate
	}
	if b.dead.Load() || b.commitFails[node] {
		return false, errCrashed
	}
	return b.Client.Commit(ctx, node, req)
}

func (b *writerBlob) Abort(ctx context.Context, node config.NodeID, req blob.CommitRequest) error {
	if b.dead.Load() || b.abortFails[node] {
		return errCrashed
	}
	return b.Client.Abort(ctx, node, req)
}

func (b *writerBlob) Release(ctx context.Context, node config.NodeID, req blob.ReleaseRequest) error {
	if b.dead.Load() {
		return errCrashed
	}
	return b.Client.Release(ctx, node, req)
}

var (
	_ handlers.BlobClient     = (*writerBlob)(nil)
	_ handlers.HeldBlobClient = (*writerBlob)(nil)
)

type writerHeld struct {
	*blob.HeldPut

	dead *atomic.Bool
}

func (h *writerHeld) Send(ctx context.Context, body io.Reader) (*blob.PutResponse, error) {
	if h.dead.Load() {
		h.Abandon()
		return nil, errCrashed
	}
	return h.HeldPut.Send(ctx, body)
}

// reaperMeta is the store as the reaper sees it. A lagging follower can
// answer the scan with older rows, every transaction can fail as with no
// leader, and a hook runs before each transaction.
type reaperMeta struct {
	*gateMeta

	mu        sync.Mutex
	follower  map[string][]byte
	txnErr    error
	beforeTxn func()
	txns      int
}

func (m *reaperMeta) lag(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.follower == nil {
		m.follower = map[string][]byte{}
	}
	m.follower[key] = value
}

func (m *reaperMeta) failTxns(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txnErr = err
}

func (m *reaperMeta) txnCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.txns
}

// ScanFrom answers as a follower would: a row it lags on has its older value.
func (m *reaperMeta) ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	items, err := m.gateMeta.ScanFrom(ctx, prefix, after, limit)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, it := range items {
		if v, ok := m.follower[it.Key]; ok {
			items[i].Value = append([]byte(nil), v...)
		}
	}
	return items, nil
}

func (m *reaperMeta) Txn(ctx context.Context, checks []meta.TxnCheck, writes []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	m.mu.Lock()
	m.txns++
	hook, txnErr := m.beforeTxn, m.txnErr
	m.mu.Unlock()
	if hook != nil {
		hook()
	}
	if txnErr != nil {
		return meta.TxnUnknown, nil, txnErr
	}
	return m.gateMeta.Txn(ctx, checks, writes)
}

var _ MetaClient = (*reaperMeta)(nil)

// reaperBlob is the blob tier as the reaper sees it: it records every call by
// operation and epoch, and a node can be held down.
type reaperBlob struct {
	*blob.Client

	mu    sync.Mutex
	down  map[config.NodeID]bool
	calls map[string]int
}

func (b *reaperBlob) record(op string, node config.NodeID, epoch uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.calls == nil {
		b.calls = map[string]int{}
	}
	b.calls[op]++
	b.calls[fmt.Sprintf("%s@%d", op, epoch)]++
	if b.down[node] {
		return errNodeDown
	}
	return nil
}

func (b *reaperBlob) setDown(node config.NodeID, down bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down == nil {
		b.down = map[config.NodeID]bool{}
	}
	b.down[node] = down
}

// count is how many calls of op the reaper sent, at epoch when one is given.
func (b *reaperBlob) count(op string, epoch ...uint64) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(epoch) == 1 {
		return b.calls[fmt.Sprintf("%s@%d", op, epoch[0])]
	}
	return b.calls[op]
}

func (b *reaperBlob) total() int {
	return b.count("commit") + b.count("abort") + b.count("release") + b.count("delete")
}

func (b *reaperBlob) Commit(ctx context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	if err := b.record("commit", node, req.Epoch); err != nil {
		return false, err
	}
	return b.Client.Commit(ctx, node, req)
}

func (b *reaperBlob) Abort(ctx context.Context, node config.NodeID, req blob.CommitRequest) error {
	if err := b.record("abort", node, req.Epoch); err != nil {
		return err
	}
	return b.Client.Abort(ctx, node, req)
}

func (b *reaperBlob) Release(ctx context.Context, node config.NodeID, req blob.ReleaseRequest) error {
	if err := b.record("release", node, req.Epoch); err != nil {
		return err
	}
	return b.Client.Release(ctx, node, req)
}

func (b *reaperBlob) DeleteEpoch(ctx context.Context, node config.NodeID, req blob.CommitRequest) (*blob.DeleteResponse, error) {
	if err := b.record("delete", node, req.Epoch); err != nil {
		return nil, err
	}
	return b.Client.DeleteEpoch(ctx, node, req)
}

var _ BlobClient = (*reaperBlob)(nil)

// testClock is the reaper's injected clock.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

const testIntentAge = time.Hour

// intentGate is a gate's object handlers and its reaper over one meta store
// and real blob nodes. Conditional writes go through the writer hooks, and
// the reaper through its own, so each side can fail alone.
type intentGate struct {
	t     *testing.T
	store *gateMeta
	bc    *blob.Client
	ring  *placement.Ring
	cfg   handlers.Config
	cache *handlers.BucketCache

	wm    *writerMeta
	wb    *writerBlob
	rm    *reaperMeta
	rb    *reaperBlob
	clock *testClock
	svc   *Service
}

func newIntentGate(t *testing.T) *intentGate {
	t.Helper()
	bc, ids := startBlobNodes(t, 4)
	epochs, err := handlers.NewEpochMinter(99)
	require.NoError(t, err)
	store := newGateMeta()
	g := &intentGate{
		t: t, store: store, bc: bc, ring: placement.NewRing(ids),
		cfg: handlers.Config{
			Region: "ap-southeast-2", DataShards: 2, ParityShards: 1,
			DegradedWrites: true, HintedHandoff: true, Epochs: epochs,
		},
		cache: handlers.NewBucketCache([]handlers.BucketConfig{{Name: "bkt"}}),
		rm:    &reaperMeta{gateMeta: store},
		rb:    &reaperBlob{Client: bc},
		clock: &testClock{now: time.Now()},
	}
	g.wm = &writerMeta{gateMeta: store}
	g.wb = &writerBlob{Client: bc, dead: &g.wm.dead}
	g.svc, err = New(Config{
		Meta: g.rm, Blob: g.rb, Workers: 4, PageSize: 2,
		IntentMaxAge: testIntentAge, Now: g.clock.Now,
	})
	require.NoError(t, err)
	return g
}

func (g *intentGate) serve(h http.Handler, method, key string, body []byte, header http.Header) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	ctx := handlers.WithObject(handlers.WithBucket(context.Background(), model.Bucket{Name: "bkt"}),
		model.Object{Bucket: model.Bucket{Name: "bkt"}, Key: key})
	req := httptest.NewRequest(method, "/bkt/"+key, rd).WithContext(ctx)
	req.ContentLength = int64(len(body))
	maps.Copy(req.Header, header)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// mustPut writes a plain object, with no hook in the way, and answers its ETag.
func (g *intentGate) mustPut(key string, body []byte) string {
	g.t.Helper()
	rr := g.serve(handlers.PutObject(g.store, g.bc, g.ring, g.cache, g.cfg), http.MethodPut, key, body, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "put: %s", rr.Body.String())
	return rr.Header().Get("ETag")
}

// condPut is a conditional write through the writer hooks.
func (g *intentGate) condPut(key string, body []byte, header http.Header) *httptest.ResponseRecorder {
	return g.serve(handlers.PutObject(g.wm, g.wb, g.ring, g.cache, g.cfg), http.MethodPut, key, body, header)
}

func (g *intentGate) requireBody(key string, want []byte) {
	g.t.Helper()
	rr := g.serve(handlers.GetObject(g.store, g.bc, g.ring, g.cache, g.cfg), http.MethodGet, key, nil, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "get: %s", rr.Body.String())
	require.Equal(g.t, want, rr.Body.Bytes())
}

// intents are the intent rows the leader holds, by stored key.
func (g *intentGate) intents() map[string][]byte {
	g.t.Helper()
	items, err := g.store.Scan(context.Background(), handlers.IntentPrefix, 0)
	require.NoError(g.t, err)
	out := map[string][]byte{}
	for _, it := range items {
		out[it.Key] = it.Value
	}
	return out
}

// onlyIntent is the one intent row a crashed or failed writer left.
func (g *intentGate) onlyIntent() (handlers.IntentID, handlers.Intent, []byte) {
	g.t.Helper()
	rows := g.intents()
	require.Len(g.t, rows, 1, "setup: the writer must leave exactly one intent")
	for k, v := range rows {
		id, ok := handlers.ParseIntentKey(k)
		require.True(g.t, ok)
		in, err := handlers.DecodeIntent(v)
		require.NoError(g.t, err)
		return id, in, v
	}
	panic("unreachable")
}

// age moves the reaper's clock past the age of every intent written so far.
func (g *intentGate) age() { g.clock.set(time.Now().Add(testIntentAge + time.Minute)) }

func (g *intentGate) pass() {
	g.t.Helper()
	require.NoError(g.t, g.svc.Pass(context.Background()))
}

// candidates are every (node, index) that can hold a shard of the write.
func candidates(in handlers.Intent) [][2]int {
	var out [][2]int
	for i, n := range in.Nodes {
		out = append(out, [2]int{int(n), i})
		if in.Handoff != 0 && in.Handoff != n {
			out = append(out, [2]int{int(in.Handoff), i})
		}
	}
	return out
}

// requireNothingPrepared fails if any candidate of a write still holds any
// row at the write's epoch.
func (g *intentGate) requireNothingPrepared(id handlers.IntentID, in handlers.Intent) {
	g.t.Helper()
	for _, c := range candidates(in) {
		_, err := g.bc.Commit(context.Background(), config.NodeID(c[0]), blob.CommitRequest{
			Key: id.Hash, Index: uint32(c[1]), Epoch: id.Epoch, //nolint:gosec // G115: a small shard index.
		})
		require.ErrorIs(g.t, err, blob.ErrNotPrepared, "node %d index %d still holds epoch %016x", c[0], c[1], id.Epoch)
	}
}

// requireLiveAt fails unless every index of a write serves its epoch, on its
// owner or on the handoff node.
func (g *intentGate) requireLiveAt(id handlers.IntentID, in handlers.Intent) {
	g.t.Helper()
	for i, n := range in.Nodes {
		live := false
		for _, node := range []config.NodeID{n, in.Handoff} {
			if node == 0 {
				continue
			}
			st, err := g.bc.Stat(context.Background(), node, blob.StatRequest{Key: id.Hash, Index: uint32(i)}) //nolint:gosec // G115: a small shard index.
			if err == nil && st.Epoch == id.Epoch {
				live = true
			}
		}
		require.True(g.t, live, "index %d is live at epoch %016x on neither its owner %d nor the handoff node", i, id.Epoch, n)
	}
}

// owners are the nodes that hold a key's shards, by index.
func (g *intentGate) owners(key string) []config.NodeID {
	g.t.Helper()
	nodes, err := g.ring.Nodes(model.ObjectHash("bkt", key), g.cfg.TotalShards())
	require.NoError(g.t, err)
	return nodes
}

var (
	body1 = []byte(strings.Repeat("first body ", 300))
	body2 = []byte(strings.Repeat("second body ", 300))
)

func ifMatch(etag string) http.Header { return http.Header{"If-Match": {etag}} }
