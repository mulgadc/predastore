package handlers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errInjected stands in for the transport failures a stalled or restarting
// peer produces: the connection closes and the caller learns nothing more.
var errInjected = errors.New("injected: connection closed")

// faultyBlob forwards to real blob nodes, except where a test names a fault.
// A failed put drains the body first, so the write path sees a shard that
// failed after it was sent rather than one its owner refused, which is the
// case hinted handoff does not cover. A dropped commit never reaches the node.
type faultyBlob struct {
	BlobClient

	mu         sync.Mutex
	failPut    map[config.NodeID]bool
	dropCommit map[config.NodeID]bool
}

func (f *faultyBlob) faults(put, commit config.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPut = map[config.NodeID]bool{put: true}
	f.dropCommit = map[config.NodeID]bool{commit: true}
}

func (f *faultyBlob) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPut, f.dropCommit = nil, nil
}

func (f *faultyBlob) Put(
	ctx context.Context, node config.NodeID, req blob.PutRequest, body io.Reader,
) (*blob.PutResponse, error) {
	f.mu.Lock()
	fail := f.failPut[node]
	f.mu.Unlock()
	if fail {
		_, _ = io.Copy(io.Discard, body)
		return nil, errInjected
	}
	return f.BlobClient.Put(ctx, node, req, body)
}

func (f *faultyBlob) Commit(ctx context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	f.mu.Lock()
	drop := f.dropCommit[node]
	f.mu.Unlock()
	if drop {
		return false, errInjected
	}
	return f.BlobClient.Commit(ctx, node, req)
}

// realStoreGate is the gate's object handlers over four real blob nodes, each
// on its own engine store, reached through the real blob client over the pipe
// transport. Only metadata is faked: meta quorum is not what these tests ask.
type realStoreGate struct {
	mc    *fakeMeta
	bc    *faultyBlob
	ring  *placement.Ring
	cfg   Config
	cache *BucketCache
}

func newRealStoreGate(t *testing.T) *realStoreGate {
	t.Helper()

	host := "acked-" + strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	const clientNode, basePort = config.NodeID(9), 7400
	ids := []config.NodeID{1, 2, 3, 4}

	nodes := make([]config.Node, 0, len(ids)+1)
	for _, id := range ids {
		nodes = append(nodes, config.Node{ID: id, Role: config.RoleBlob, Port: basePort + int(id)})
	}
	nodes = append(nodes, config.Node{ID: clientNode, Role: config.RoleBlob, Port: basePort + int(clientNode)})
	clusterCfg := &config.Config{Hosts: []config.Host{{ID: 1, Addr: host, Nodes: nodes}}}

	for _, id := range ids {
		tr := transport.NewPipeTransport(host, basePort+int(id))
		t.Cleanup(func() { tr.Close() })
		ln, err := tr.Listen()
		require.NoError(t, err)

		srv, err := blob.New(blob.Config{
			NodeID:    id,
			DataDir:   t.TempDir(),
			AEAD:      storetest.TestAEAD(),
			Listeners: []transport.Listener{ln},
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
	res, err := rpc.NewResolver(clusterCfg, clientNode, clientTr)
	require.NoError(t, err)
	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(clientNode, res)),
		EnvelopeTimeout: 5 * time.Second,
		IdleTimeout:     5 * time.Second,
	})
	require.NoError(t, err)

	// The production profile: RS(2,1) over four hosts, degraded writes and
	// hinted handoff on.
	return &realStoreGate{
		mc:   newFakeMeta(),
		bc:   &faultyBlob{BlobClient: client},
		ring: placement.NewRing(ids),
		cfg: Config{
			Region: "ap-southeast-2", DataShards: 2, ParityShards: 1,
			DegradedWrites: true, HintedHandoff: true,
			Epochs: mustEpochs(clientNode),
		},
		cache: NewBucketCache([]BucketConfig{{Name: "b"}}),
	}
}

func (g *realStoreGate) put(key string, body []byte) *httptest.ResponseRecorder {
	req, _ := http.NewRequestWithContext(objectCtx("b", key), http.MethodPut, "/b/"+key, bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	PutObject(g.mc, g.bc, g.ring, g.cache, g.cfg).ServeHTTP(rr, req)
	return rr
}

func (g *realStoreGate) get(key string) *httptest.ResponseRecorder {
	req, _ := http.NewRequestWithContext(objectCtx("b", key), http.MethodGet, "/b/"+key, nil)
	rr := httptest.NewRecorder()
	GetObject(g.mc, g.bc, g.ring, g.cache, g.cfg).ServeHTTP(rr, req)
	return rr
}

// holders names the stripe's three nodes for a key, in shard order.
func (g *realStoreGate) holders(t *testing.T, key string) []config.NodeID {
	t.Helper()
	nodes, err := g.ring.Nodes(model.ObjectHash("b", key), g.cfg.TotalShards())
	require.NoError(t, err)
	return nodes
}

// The prod failure of 2026-11-23, on real stores: one shard fails after it was
// sent, and one of the two that landed loses its commit. One committed shard is
// fewer than any read needs, so the write must not be acknowledged, and the
// previous version must still be the one that reads back.
func TestAnAcknowledgedWriteReadsBack(t *testing.T) {
	g := newRealStoreGate(t)
	before := bytes.Repeat([]byte("v1"), 1<<19)
	after := bytes.Repeat([]byte("v2"), 1<<19)

	require.Equal(t, http.StatusOK, g.put("chunk", before).Code)

	h := g.holders(t, "chunk")
	g.bc.faults(h[1], h[0])
	put := g.put("chunk", after)
	g.bc.clear()

	got := g.get("chunk")
	if put.Code == http.StatusOK {
		require.Equal(t, http.StatusOK, got.Code, "a write answered 200 and did not read back: %s", got.Body.String())
		assert.Equal(t, after, got.Body.Bytes())
		return
	}
	require.Equal(t, http.StatusOK, got.Code, "a failed write left the key unreadable: %s", got.Body.String())
	assert.Equal(t, before, got.Body.Bytes(), "a failed write must leave the previous version")
}

// The incident's exact shape: the chunk was a new key, so there is no previous
// version to fall back to, and a failed write must read as absent rather than
// as a 200 the store cannot serve.
func TestAnAcknowledgedNewKeyReadsBack(t *testing.T) {
	g := newRealStoreGate(t)
	body := bytes.Repeat([]byte("c"), 1<<20)

	h := g.holders(t, "fresh")
	g.bc.faults(h[1], h[0])
	put := g.put("fresh", body)
	g.bc.clear()

	got := g.get("fresh")
	if put.Code == http.StatusOK {
		require.Equal(t, http.StatusOK, got.Code, "a write answered 200 and did not read back: %s", got.Body.String())
		assert.Equal(t, body, got.Body.Bytes())
		return
	}
	assert.Equal(t, http.StatusNotFound, got.Code, "a failed first write must leave the key absent: %s", got.Body.String())
}
